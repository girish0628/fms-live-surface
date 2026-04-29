# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**fms-live-surface** is a Python + ArcGIS geospatial ETL pipeline that processes elevation point cloud data from mining equipment (Minestar `.snp` binary and Modular `.csv`) into live surface rasters for the WAIO (Western Australia Iron Ore) Schedman UI. It runs hourly via Jenkins across 7 sites in parallel (`WB`, `ER`, `SF`, `YND`, `JB`, `NWW`, `MAC`) and archives nightly.

## Commands

### Setup
```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.dev.txt
```

### Run pipeline (dev, single site)
```bash
python -m src.runners.fms_runner \
    --config config/app_config.yaml \
    --logging config/logging.yaml \
    --site WB \
    --env DEV \
    --skip-monitoring
```

### Run finalize job (after all parallel site stages)
```bash
python -m src.runners.fms_finalize_runner \
    --config config/app_config.yaml \
    --logging config/logging.yaml \
    --run-timestamp 20240101_0800 \
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

### Run a single test file
```bash
pytest tests/test_services/test_snippet_conversion_service.py -v
```

## Architecture

### Three-runner Jenkins orchestration

The pipeline uses three distinct runners, not one:

1. **`fms_runner.py`** — Per-site hourly job (runs in parallel across all sites). Steps: monitoring check → snippet/modular conversion → FMS pipeline (raster + boundary) → output assembly. Writes results to a shared `FMS_<timestamp>/` folder keyed by `FMS_RUN_TIMESTAMP` env var.

2. **`fms_finalize_runner.py`** — Runs once after all parallel site stages complete. Merges per-site boundary shapefiles into a single `FMS_<ts>_boundary.shp`, writes `ready.flag`, then triggers publishing. This is the only runner that fires `fme_webhook` mode.

3. **`archive_runner.py`** — Nightly job. Zips all `.snp` files under `<archive_root>/<site>/<YYYY>/<MM>/` then clears the landing zone. Supports `--site ALL` or a specific site, and `--dry-run`.

**`FMS_RUN_TIMESTAMP`** is the shared env var (set by Jenkins at multijob start) that all parallel site stages use to write into the same output folder. `fms_finalize_runner.py` reads this var to locate and merge outputs.

### Per-site pipeline stages (fms_runner.py)

1. **MonitoringService** — checks newest `.snp` file age vs. `monitoring_threshold_minutes`; emails `gis-alerts@waio.bhp.com` and copies from `failover_share` if stale
2. **SnippetConversionService** (Minestar sites) — parses `.snp` binary (40-byte records, `0x0B` marker, ×0.01 scale), applies Z datum shift (ADPH → AHD), **3-pass** grid-based despike, reprojects to MGA50 via arcpy, clips to AOI; outputs `<site>_points.csv` + `config.json`
3. **ModularCsvService** (Modular sites: SF, JB, NWW) — reads equipment CSV with configurable column indices; **1-pass** despike (vs. 3 for Minestar); same downstream outputs
4. **FmsPipelineService** — module-level functions (not a class): CSV → 3D Feature Class → TIN (Delaunay) → GeoTIFF (2 m cells, LINEAR) + convex-hull boundary minus road buffer; requires 3D Analyst + Spatial Analyst licences; writes `ProcessData.json`
5. **OutputHandlerService** — assembles `FMS_Output/<site>/<YYYYMMDD_HHMM>/` with raster, boundary, `metadata.json`, and `ready.flag`; enforces `retention_hours` cleanup

### Publishing integration modes

`PublishingService` supports three modes (set in `publishing.integration_mode`):

| Mode | Trigger point | Semantics |
|------|--------------|-----------|
| `fme_webhook` | `fms_finalize_runner.py` only | POSTs JSON to FME Server job submitter; reads token from `FME_TOKEN` env var; boundary-merge must complete first |
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
- **arcpy-free test mode**: services degrade gracefully when arcpy is unavailable so unit tests run without ArcGIS Pro
- **Coordinate systems**: inputs are site-specific (`WB94`, `ER94`, etc. — `.prj` files referenced in config); all processing output is MGA50 (GDA2020)
- **Config lookup**: `get_config_value(cfg, "dotted.key", default)` in `config_loader.py` navigates nested YAML dicts; site-specific keys override shared `processing` defaults
- **Error handling**: `src/core/exceptions.py` defines a typed hierarchy (`SnippetConversionError`, `RasterGenerationError`, `PublishingError`, etc.) — catch the specific type, not the base `ServiceExecutionError`, unless you mean to catch all service failures

### Configuration

`config/app_config.yaml` is the single source of truth. Key sections:
- `paths` — all filesystem roots (landing zone, staging, scratch GDB, output, archive)
- `sites` — per-site overrides: `source_type` (`minestar`|`modular`|`both`), `z_adjustment`, `input_spatial_ref` (.prj path), `aoi_where_clause`, `failover_share`; Modular sites also set `csv_col_x/y/z/timestamp`
- `processing` — shared: `max_z`, `grid_size`, `decimal_digits`, `despike`, `min_neighbours`, `aoi_feature_class`, `exclusion_fc`, `monitoring_threshold_minutes`
- `publishing` — `integration_mode`, `fme_webhook_url`, `fme_token_env_var`, `api_module`, `poll_interval`, `poll_timeout`
- `output.retention_hours` — auto-cleanup of old output folders

Use `config/logging.yaml` for DEV (DEBUG, console + rotating file), `config/logging.prod.yaml` for PROD (INFO, rotating files only).

### Reference code

`ReferenceCode/` contains the original legacy implementations (`FMSUtility.py`, `minestarsnippettocsv.py`, `modularcsvtocsv.py`). Treat as read-only reference — do not import from it.
