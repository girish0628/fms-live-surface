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

## LOGGER SETUP

With level: INFO set on both the handler and the root logger, any logger.debug(...) calls in the code are silently
  discarded — they never reach the console or the log file.

  The hierarchy is:

- logger.debug(...)-  → level DEBUG  → below INFO threshold → DROPPED
- logger.info(...)  -  → level INFO   → meets threshold      → written
- logger.warning(...) → level WARNING → above threshold     → written
- logger.error(...)   → level ERROR  → above threshold      → written

 Jenkins console output and the rotating log files (fms_production.log) will only ever show INFO and above. The

- logger.debug() calls that exist in the code (like the blob upload line logger.debug("Uploading → %s", blob_name)) produce
  zero output.

  handlers:
    console:
      class: logging.StreamHandler
      level: DEBUG
      level: INFO
      formatter: detailed
      stream: ext://sys.stdout
    file:
        class: logging.handlers.RotatingFileHandler
        level: DEBUG
        level: INFO
        formatter: detailed
        filename: logs/fms_dev.log
        maxBytes: 10485760   # 10 MB
        encoding: utf-8

    root:
        level: DEBUG
        level: INFO
        handlers: [console, file]

## Changes

  src/services/archive_service.py

- New fields: destination ("network" default), blob_connection_string_env_var, blob_container_name, blob_prefix
- blob mode: zips to a tempfile.TemporaryDirectory(), uploads, discards temp
- both mode: writes zip to network (original path), then uploads that same zip to blob
- network mode: unchanged from before
- Credentials validated upfront (fail-fast before any I/O) for blob/both
- Staging folder cleaned up in a finally block so it's never left behind on error
- Blob path: fms-snippets/{site}/{YYYY}/{MM}/{archive_name}.zip

  src/runners/archive_runner.py

- New --destination network|blob|both arg (default None → reads from config)
- Threads blob config from blob_storage section and blob_prefix from archive section
- Summary log line now shows both archive_path and blob_path

  config/app_config.yaml

- New archive section with destination: "network" and blob_prefix: "fms-snippets/"
- blob_storage section comment updated to note it's shared by both weekly cleanup and snippet archive

  Jenkinsfile

- New ARCHIVE_DESTINATION choice parameter (dropdown: network / blob / both, default network)
- --destination "${params.ARCHIVE_DESTINATION}" passed to archive runner
- Dashboard Summary shows Archive (network) / Archive (blob) / Archive (both) label

 The weekly cleanup runner is a maintenance job that archives old daily output folders to Azure Blob Storage and cleans up
  stale staging files. Here's what it does:

## Trigger

- Runs every Sunday at 02:30 AM (dayOfWeek == 7 && hour == 2 && minute == 30)
- Currently disabled: ENABLE_WEEKLY = 'false' (hardcoded)

  Two cleanup passes

  1. Output folder archival (with blob upload)

- Scans output_root for FMS_* folders older than retention_days (default 7 days, based on folder mtime)
- For each old folder:
    a. Uploads every file to Azure Blob Storage
    b. Deletes the local folder with shutil.rmtree
- Blob path format: fms-live-surface/FMS_20260518/FMS_20260518_WB.tif
- This covers both daily (FMS_YYYYMMDD) and any residual hourly (FMS_YYYYMMDDHH0000) folders

  1. Staging folder purge (no upload)

- Deletes individual files in the staging folder older than staging_retention_days (default 2 days)
- No blob upload — staging CSVs are intermediate work product, not worth keeping

  Error handling

- Per-folder failures are caught and collected — a single bad folder doesn't abort the whole run
- If any folder fails, the service returns PARTIAL status → runner exits sys.exit(1) to signal Jenkins failure

  Azure credentials

- Reads AZURE_STORAGE_CONNECTION_STRING env var
- Validates upfront that azure-storage-blob is installed — fails fast before any work starts
- --dry-run flag logs all planned actions without touching anything

  Relationship to other runners

  ┌───────────────────────┬──────────────────────────────────────────────┬────────────────────────────────────┐
  │        Runner         │               What it removes                │                When                │
  ├───────────────────────┼──────────────────────────────────────────────┼────────────────────────────────────┤
  │ fms_finalize_runner   │ Hourly staging folder                        │ After hourly FME INGEST            │
  ├───────────────────────┼──────────────────────────────────────────────┼────────────────────────────────────┤
  │ daily_finalize_runner │ Hourly output folders + daily staging folder │ After daily FME INGEST             │
  ├───────────────────────┼──────────────────────────────────────────────┼────────────────────────────────────┤
  │ weekly_cleanup_runner │ Old daily output folders (7+ days)           │ Sunday 02:30 AM, after blob upload │
  └───────────────────────┴──────────────────────────────────────────────┴────────────────────────────────────┘


## What it is

  The Daily Cleanup is a safety sweep of the MTD mosaic dataset. It finds every row in the mosaic that is tagged SITE =
  'Hourly' and calls FME DELETE for all of them.

  It has nothing to do with local files or folders — it talks directly to the ArcGIS mosaic dataset via arcpy.da.SearchCursor.

  ---
  Step-by-step logic

  Step 1 — DailyCleanupService.run() (daily_cleanup_service.py:81–96)

  Opens a SearchCursor on the mosaic dataset with this WHERE clause:
  SITE = 'Hourly'
  Returns every matching row as {"survey_name": "<name>", "capture_method": "FMS"}.

  Step 2 — FmeWebhookClient.delete()

  POSTs the full list to the FME DELETE webhook with:
  ACTION           = DELETE
  TYPE             = Terrain
  SURVEYS          = [all rows from Step 1]
  DELETE_PERMANENTLY = FALSE
  DELETE_CACHE     = TRUE
  COMMENTS         = FMS scheduled deletion of hourly surveys

  ---
  Key differences from daily_finalize_runner.py

  ┌────────────────────┬─────────────────────────────────────────────────────────┬─────────────────────────────────────────┐
  │                    │                  daily_cleanup_runner                   │          daily_finalize_runner          │
  ├────────────────────┼─────────────────────────────────────────────────────────┼─────────────────────────────────────────┤
  │ Source of survey   │ Queries the live mosaic dataset via arcpy               │ Scans local FMS_<date>HH0000/ output    │
  │ list               │                                                         │ folders                                 │
  ├────────────────────┼─────────────────────────────────────────────────────────┼─────────────────────────────────────────┤
  │ Date scope         │ All SITE='Hourly' entries regardless of date — sweeps   │ Only the current run date's folders     │
  │                    │ everything                                              │                                         │
  ├────────────────────┼─────────────────────────────────────────────────────────┼─────────────────────────────────────────┤
  │ When it runs       │ Originally 00:30 AM, after daily merge                  │ Immediately after daily site stages     │
  ├────────────────────┼─────────────────────────────────────────────────────────┼─────────────────────────────────────────┤
  │ Requires ArcGIS    │ Yes — direct mosaic query                               │ No — just directory listing             │
  ├────────────────────┼─────────────────────────────────────────────────────────┼─────────────────────────────────────────┤
  │ Current status     │ ENABLE_DAILY_CLEANUP = 'false' — disabled               │ Active (runs as part of daily-merge     │
  │                    │                                                         │ mode)                                   │
  └────────────────────┴─────────────────────────────────────────────────────────┴─────────────────────────────────────────┘

  ---
  Purpose

  It was designed as a catch-all fallback: if an hourly survey was ingested into the mosaic but the finalize runner's FME
  DELETE call failed or was never reached, the cleanup runner would still clear all lingering SITE='Hourly' entries the next
  morning.

  Because the daily_finalize_runner now handles FME DELETE via folder names for the current day, daily_cleanup_runner acts as
  the broader safety net — it would catch any Hourly surveys from any previous day that slipped through, not just today's.

  Whether you need it active depends on how reliable the finalize DELETE step is in practice.



  What changed

  fms_pipeline_service.py — output_label parameter

  Added optional output_label: str | None = None to both process_fms_pipeline and batch_process_fms. When provided it replaces
  the auto-derived YYYYMMDDHH0000 label in all output paths:

  output_label=None   →  FMS_20260518110000/FMS_20260518110000_WB.tif   (hourly, unchanged)
  output_label="20260518"  →  FMS_20260518/FMS_20260518_WB.tif           (daily)

  Fully backward-compatible — existing callers pass nothing and behave identically.

  ---
  daily_merge_runner.py — complete rewrite

  ┌────────┬───────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │  Step  │                                                 What it does                                                  │
  ├────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ 1      │ Per-site snippet/modular conversion with filter_since = run_date 01:00, filter_until = run_date+1 00:00. Same │
  │ (new)  │  services and config as fms_runner.py.                                                                        │
  ├────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ 2      │ process_fms_pipeline with output_label=date → writes directly to FMS_<YYYYMMDD>/. No hourly folder involved.  │
  │ (new)  │                                                                                                               │
  ├────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ 3      │ Merge all Source/FMS_<date>_boundary_<SITE>.shp → single dissolved FMS_<date>_boundary.shp.                   │
  │ (new)  │                                                                                                               │
  ├────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ 4      │ FME INGEST site="Daily" (same as before)                                                                      │
  ├────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ 5      │ FME DELETE — hourly folder names as SURVEYS (same as before)                                                  │
  ├────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ 6      │ Delete local FMS_<date>HH0000/ folders (same as before)                                                       │
  ├────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ 7      │ Delete daily staging folder                                                                                   │
  │ (new)  │                                                                                                               │
  └────────┴───────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

  If all sites fail, it aborts before the FME calls. Partial site failures are logged and processing continues for the
  remaining sites.

  ---
  Jenkinsfile — yesterday's date for daily-merge

  def yesterday = new Date(now.time - 24 * 60 * 60 * 1000L)
  env.FMS_RUN_TIMESTAMP = (FMS_RUN_TIMESTAMP == '') ? yesterday.format('yyyyMMdd', perthTz) : FMS_RUN_TIMESTAMP

  When the job auto-triggers at 00:15 on May 19, FMS_RUN_TIMESTAMP = 20260518 (the data date). For manual reruns, pass the data
   date explicitly via the FMS_RUN_TIMESTAMP parameter.