# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**fms-live-surface** is a Python + ArcGIS geospatial ETL pipeline that processes elevation point cloud data from mining equipment (Minestar `.snp` binary and Modular `.csv`) into live surface rasters for the WAIO (Western Australia Iron Ore) Schedman UI. It runs hourly via Jenkins across 5 sites in parallel and archives nightly.

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

### Run archive job (nightly)
```bash
python -m src.runners.archive_runner \
    --config config/app_config.yaml \
    --logging config/logging.yaml \
    --env DEV
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

### Pipeline stages (fms_runner.py orchestrates in order)

1. **MonitoringService** — checks file freshness in the GIP landing zone; emails `gis-alerts@waio.bhp.com` and optionally copies from a PROD failover share if files are stale (`monitoring_threshold_minutes` in config)
2. **SnippetConversionService** — parses Minestar `.snp` binary (40-byte records, `0x0B` marker, ×0.01 scale), applies Z datum shift (ADPH → AHD, configurable per site), 3-pass grid-based despike, reprojects to MGA50 via arcpy, clips to AOI; outputs `<site>_points.csv` + `config.json`
3. **ModularCsvService** — alternative input path for sites using modular CSV instead of `.snp`; same downstream outputs
4. **FmsPipelineService** — CSV → 3D Feature Class → TIN (Delaunay) → GeoTIFF (2 m cells, LINEAR) + convex-hull boundary minus road buffer; requires 3D Analyst + Spatial Analyst licenses
5. **OutputHandlerService** — assembles `FMS_Output/<site>/<YYYYMMDD_HHMM>/` with raster, boundary, `metadata.json`, and `ready.flag`
6. **PublishingService** — handoff to existing enterprise solution; two modes: `file_trigger` (polls `ready.flag`) or `direct_api`

Archive runner (`archive_runner.py`) runs independently: zips daily `.snp` files to `<archive_root>/<site>/<YYYY>/<MM>/` then clears the landing zone.

### Key design constraints

- **Jenkins parallelisation safety**: all intermediate ArcGIS datasets use UUID prefixes; no shared mutable state between site processes
- **arcpy-free test mode**: services degrade gracefully when arcpy is unavailable so unit tests run without ArcGIS Pro
- **Coordinate systems**: inputs are site-specific (`WB94`, `ER94`, etc. — `.prj` files referenced in config); all processing output is MGA50 (GDA2020)
- **Config lookup**: `config_loader.py` provides dotted-key access (e.g. `config.get("processing.grid_size")`) over the YAML; site-specific keys override shared `processing` defaults

### Configuration

`config/app_config.yaml` is the single source of truth. Key sections:
- `paths` — all filesystem roots (landing zone, staging, scratch GDB, output, archive)
- `sites` — per-site overrides: `source_type`, `z_adjustment`, `input_spatial_ref` (.prj path), `aoi_where_clause`, `failover_share`
- `processing` — shared: `max_z`, `grid_size`, `decimal_digits`, `despike`, `min_neighbours`, `aoi_feature_class`, `exclusion_fc`
- `publishing` — `mode: file_trigger | direct_api`
- `output.retention_hours` — auto-cleanup of old output folders

Use `config/logging.yaml` for DEV (DEBUG), `config/logging.prod.yaml` for PROD (INFO, rotating files).

### Reference code

`ReferenceCode/` contains the original legacy implementations (`FMSUtility.py`, `minestarsnippettocsv.py`, `modularcsvtocsv.py`). Treat as read-only reference — do not import from it.
