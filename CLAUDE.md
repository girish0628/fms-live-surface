# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**fms-live-surface** is a Python + ArcGIS geospatial ETL pipeline that processes elevation point cloud data from mining equipment (Minestar `.snp` binary and Modular `.csv`) into live surface rasters for the WAIO (Western Australia Iron Ore) Schedman UI. It runs hourly via Jenkins across 7 sites in parallel (`WB`, `ER`, `SF`, `YND`, `JB`, `NWW`, `MAC`) and produces a consolidated daily output nightly.

## Commands

### Setup
```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.dev.txt
```

### Run hourly pipeline (dev, single site)
```bash
python -m src.runners.fms_runner \
    --config config/app_config.yaml \
    --logging config/logging.yaml \
    --site WB \
    --env DEV \
    --skip-monitoring \
    [--FMS_ForceDate YYYYMMDD]
```

### Run finalize job (after all parallel site stages)
```bash
python -m src.runners.fms_finalize_runner \
    --config config/app_config.yaml \
    --logging config/logging.yaml \
    --run-timestamp 20260503110000 \
    --env DEV
```

### Run daily merge job
```bash
python -m src.runners.daily_merge_runner \
    --config config/app_config.yaml \
    --logging config/logging.yaml \
    --FMS_ForceDate 20260503 \
    --env DEV
```

### Run archive job (nightly)
```bash
python -m src.runners.archive_runner \
    --config config/app_config.yaml \
    --logging config/logging.yaml \
    --env DEV \
    [--site WB] \
    [--dry-run]
```

### Test, lint, type-check
```bash
pytest tests/ -v
ruff check src tests
mypy src
```

## Architecture

### Two job types: Hourly and Daily

**Hourly** — runs every hour (e.g. 1:15, 2:15, 3:15 AM etc.) across 7 sites in parallel.

**Daily** — runs once per day between 00:00–01:00 AM: mosaics all hourly TIFFs per site into a daily TIFF, dissolves all hourly boundaries into a single daily boundary, calls FME INGEST, then deletes the hourly output folders.

### Output folder structure

**Hourly** (one folder shared by all sites per run, named by Year/Month/Day/Hour with minutes and seconds zeroed):
```
<output_root>/
└── FMS_<YYYYMMDDHH0000>/          e.g. FMS_20260503110000
    ├── FMS_<YYYYMMDDHH0000>_<SITE>.tif      e.g. FMS_20260503110000_MAC.tif
    ├── FMS_<YYYYMMDDHH0000>_boundary.shp    merged boundary (written by finalize)
    └── Source/
        ├── FMS_<YYYYMMDDHH0000>_boundary_<SITE>.shp   per-site boundary shapefile
        └── FMS_<YYYYMMDDHH0000>_boundary_<SITE>.csv   per-site boundary vertices CSV
```

**Daily** (one folder per calendar date, created by daily_merge_runner after all hourly runs):
```
<output_root>/
└── FMS_<YYYYMMDD>/                e.g. FMS_20260503
    ├── FMS_<YYYYMMDD>_<SITE>.tif            e.g. FMS_20260503_MAC.tif
    └── FMS_<YYYYMMDD>_boundary.shp          dissolved union of all site boundaries
```

Hourly folders are **deleted by daily_merge_runner** after the daily FME INGEST completes.

### Staging folder

Intermediate CSVs (point clouds from snippet/modular conversion) are written to:
```
<staging_folder>/FMS_<YYYYMMDDHH0000>/<SITE>/
```
The entire staging run folder is **deleted by fms_finalize_runner** after the FME INGEST call succeeds.

### Four-runner Jenkins orchestration

1. **`fms_runner.py`** — Per-site hourly job (runs in parallel across all 7 sites).
   - Monitoring check → snippet/modular conversion (CSV to staging) → FMS pipeline (raster + boundary + boundary CSV) → optional publishing for non-fme_webhook modes.
   - All parallel processes share one `FMS_<YYYYMMDDHH0000>/` folder via `FMS_RUN_TIMESTAMP` env var.

2. **`fms_finalize_runner.py`** — Runs once after all parallel site stages complete.
   - Merges `Source/FMS_<ts>_boundary_<SITE>.shp` files → dissolved `FMS_<ts>_boundary.shp`.
   - Calls FME INGEST webhook (SITE=Hourly).
   - Deletes the staging run folder.
   - Writes idempotency flag to prevent double-ingest on Jenkins re-run.

3. **`daily_merge_runner.py`** — Runs daily (e.g. 00:30 AM).
   - Mosaics all hourly `FMS_<date>HH0000/<SITE>.tif` files per site → `FMS_<date>/<SITE>.tif`.
   - Merges + dissolves all hourly per-site boundary SHPs → `FMS_<date>_boundary.shp`.
   - Calls FME INGEST webhook (SITE=Daily).
   - Deletes all hourly `FMS_<date>HH0000/` folders.

4. **`archive_runner.py`** — Nightly. Zips `.snp` files under `<archive_root>/<site>/<YYYY>/<MM>/` then clears the landing zone.

Additional runners:
- **`daily_cleanup_runner.py`** — Queries MTD mosaic for SITE='Hourly' surveys and deletes them via FME DELETE webhook.
- **`weekly_cleanup_runner.py`** — Archives old `FMS_<YYYYMMDD>` daily folders to Azure Blob Storage and deletes local copies.

### Naming conventions (`src/utils/naming_utils.py`)

| Function | Format | Example |
|----------|--------|---------|
| `to_hourly_ts(ts)` | `YYYYMMDDHH0000` | `20260503110000` |
| `hourly_survey_name(ts)` | `YYYYMMDDHH0000_FMS` | `20260503110000_FMS` |
| `output_folder_name(ts)` | `FMS_YYYYMMDDHH0000` | `FMS_20260503110000` |
| `daily_survey_name(date)` | `FMS_YYYYMMDD` | `FMS_20260503` |
| `daily_folder_name(date)` | `FMS_YYYYMMDD` | `FMS_20260503` |

`FMS_RUN_TIMESTAMP` env var (set by Jenkins) is normalised to `YYYYMMDDHH0000` via `to_hourly_ts()` so that minute/second differences between sites do not create separate folders.

### Per-site pipeline stages (fms_runner.py)

1. **MonitoringService** — checks newest `.snp` file age vs. `monitoring_threshold_minutes`; emails `gis-alerts@waio.bhp.com` and copies from `failover_share` if stale
2. **SnippetConversionService** (Minestar sites: WB, ER, YND, MAC) — parses `.snp` binary (40-byte records, `0x0B` marker, ×0.01 scale), applies Z datum shift (ADPH → AHD), **3-pass** grid-based despike, reprojects to MGA50 via arcpy, clips to AOI; outputs `<site>_points.csv` to staging
3. **ModularCsvService** (Modular sites: SF, JB, NWW) — reads equipment CSV with configurable column indices; **1-pass** despike; same downstream output
4. **FmsPipelineService** — module-level functions: CSV → 3D Feature Class → TIN (Delaunay) → GeoTIFF (2 m cells, LINEAR) + boundary shapefile in `Source/` + boundary vertices CSV in `Source/`; requires 3D Analyst + Spatial Analyst licences

### Publishing integration modes

`PublishingService` supports three modes (set in `publishing.integration_mode`):

| Mode | Trigger point | Semantics |
|------|--------------|-----------|
| `fme_webhook` | `fms_finalize_runner.py` only | POSTs JSON to FME Server job submitter; reads token from `FME-TOKEN` env var; boundary-merge must complete first |
| `file_trigger` | `fms_runner.py` per site | Verifies `ready.flag` exists; optionally polls for `done.flag` |
| `direct_api` | `fms_runner.py` per site | Dynamically imports and calls a `publish()` function from the module named in `publishing.api_module` |

### Service implementation pattern

All services in `src/services/` are **frozen dataclasses** — immutable, instantiated inline in runners with explicit config-derived parameters. There is no DI framework. Pattern:

```python
svc = SnippetConversionService(
    site=site,
    input_folder=landing_zone,
    z_adjustment=get_config_value(cfg, f"sites.{site}.z_adjustment", 0.0),
    # ...
)
result = svc.convert()  # returns dict[str, Any]
```

`fms_pipeline_service.py` is the exception — it exposes module-level functions (`process_fms_pipeline`, `batch_process_fms`) rather than a class.

### Key design constraints

- **Jenkins parallelisation safety**: all intermediate ArcGIS datasets use UUID prefixes; no shared mutable state between site processes
- **Shared timestamp**: `FMS_RUN_TIMESTAMP` is set once by Jenkins; `to_hourly_ts()` normalises it to `YYYYMMDDHH0000` so all sites write to the same folder regardless of when they start within the hour
- **arcpy-free test mode**: services degrade gracefully when arcpy is unavailable so unit tests run without ArcGIS Pro
- **Coordinate systems**: inputs are site-specific (`WB94`, `ER94`, etc. — `.prj` files referenced in config); all processing output is MGA50 (GDA2020)
- **Config lookup**: `get_config_value(cfg, "dotted.key", default)` in `config_loader.py` navigates nested YAML dicts; site-specific keys override shared `processing` defaults
- **Error handling**: `src/core/exceptions.py` defines a typed hierarchy (`SnippetConversionError`, `RasterGenerationError`, `PublishingError`, etc.) — catch the specific type, not the base `ServiceExecutionError`, unless you mean to catch all service failures

### Configuration

`config/app_config.yaml` is the single source of truth. Key sections:
- `paths` — filesystem roots: `landing_zone_root`, `staging_folder`, `scratch_gdb`, `output_root`, `archive_root`
- `sites` — per-site overrides: `source_type` (`minestar`|`modular`|`both`), `z_adjustment`, `input_spatial_ref` (.prj path), `aoi_where_clause`, `failover_share`; Modular sites also set `csv_col_x/y/z/timestamp`
- `processing` — shared: `max_z`, `grid_size`, `decimal_digits`, `despike`, `min_neighbours`, `aoi_feature_class`, `exclusion_fc`, `monitoring_threshold_minutes`
- `publishing` — `integration_mode`, `fme_webhook_url`, `fme_token_env_var`, `api_module`, `poll_interval`, `poll_timeout`
- `fme` — `ingest_url`, `delete_url`, `token_env_var`, `timeout`, `max_retries`, `user_email`
- `mosaic` — MTD mosaic dataset path and field names (used by daily_cleanup_runner)
- `blob_storage` — Azure Blob credentials env var and container (used by weekly_cleanup_runner)
- `weekly.output_retention_days` — how many days of daily folders to keep before blob archival

Use `config/logging.yaml` for DEV (DEBUG, console + rotating file), `config/logging.prod.yaml` for PROD (INFO, rotating files only).

### Reference code

`ReferenceCode/` contains the original legacy implementations (`FMSUtility.py`, `minestarsnippettocsv.py`, `modularcsvtocsv.py`). Treat as read-only reference — do not import from it.
