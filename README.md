# fms-live-surface

Python implementation of the FMS Live Surface hourly data pipeline for WAIO
mine sites (WB, ER, TG, JB, NM).

## Project Structure

```
fms-live-surface/
├── src/
│   ├── core/               # Config loading, logging, exceptions
│   ├── services/           # Business logic — one service per pipeline stage
│   │   ├── snippet_conversion_service.py   # .snp → CSV (arcpy reproject)
│   │   ├── modular_csv_service.py          # Modular CSV → MGA50 CSV
│   │   ├── raster_generation_service.py    # CSV → TIN → Raster + boundary (arcpy)
│   │   ├── output_handler_service.py       # Output folder + metadata.json + ready.flag
│   │   ├── publishing_service.py           # Handoff to existing publishing solution
│   │   ├── archive_service.py              # Nightly .snp archival
│   │   └── monitoring_service.py           # File delivery health check
│   ├── runners/            # Jenkins entry points
│   │   ├── fms_runner.py       # MTD - Hourly FMS (main pipeline)
│   │   └── archive_runner.py   # FMS - Archive Snippet Files (nightly)
│   └── utils/
│       └── file_utils.py
├── config/
│   ├── app_config.yaml         # Site + processing configuration (update paths)
│   ├── logging.yaml            # Dev logging (DEBUG level)
│   └── logging.prod.yaml       # Production logging (INFO level)
├── tests/
│   └── test_services/
├── Jenkinsfile
└── requirements.txt
```

## Pipeline Overview

```
GIP Landing Zone (.snp / .csv)
    ↓
[1] Monitoring check (file freshness)
    ↓
[2] Snippet conversion / Modular CSV reprojection → CSV (MGA50) + config.json
    ↓
[3] Raster generation (arcpy): CSV → 3D FC → TIN → GeoTIFF + boundary.shp
    ↓
[4] Output handler: copy to FMS_Output/<site>/<timestamp>/ + metadata.json + ready.flag
    ↓
[5] Publishing handoff → existing enterprise publishing solution
    ↓
FMS_Output consumed by publishing solution → SDE Mosaic Dataset → Schedman UI
```

## Setup

```cmd
python -m venv venv
venv\Scripts\activate
pip install -r requirements.dev.txt
```

## Run Locally (DEV)

```cmd
python -m src.runners.fms_runner ^
    --config config/app_config.yaml ^
    --logging config/logging.yaml ^
    --site WB ^
    --env DEV ^
    --skip-monitoring
```

## Run Tests

```cmd
pytest tests/ -v
```

## Run Linting

```cmd
ruff check src tests
```

## Jenkins (Production)

The `Jenkinsfile` runs all 5 sites in parallel on an hourly cron trigger.
See `Jenkinsfile` for the full job parameters.

```batch
python -m src.runners.fms_runner ^
    --config %CONFIG_PATH% ^
    --logging %LOGGING_PATH% ^
    --env %ENV% ^
    --site %SITE%
```

Nightly archive job:

```batch
python -m src.runners.archive_runner ^
    --config %CONFIG_PATH% ^
    --logging %LOGGING_PATH% ^
    --env %ENV%
```

## Configuration

Update `config/app_config.yaml` before deployment:

- `paths.*` — update all server paths to match your environment
- `sites.*` — verify landing zones and Z adjustment values per site
- `processing.aoi_feature_class` — path to the AOI polygon feature class
- `processing.exclusion_fc` — path to MTD_Live_RoadsBuffered
- `publishing.integration_mode` — `file_trigger` (default) or `direct_api`
- `monitoring.alert_email` — recipient for delivery failure alerts

## ArcGIS Licensing

`raster_generation_service.py` requires:
- **ArcGIS Pro** with **3D Analyst** and **Spatial Analyst** extensions
- Services run gracefully without arcpy in DEV/test mode (reprojection and
  raster generation steps are skipped with warnings)
