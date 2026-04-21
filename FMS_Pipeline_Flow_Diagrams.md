# FMS Live Surface — Pipeline Flow Diagrams

Each diagram covers exactly what happens inside that stage, including inputs, decisions, outputs, and error paths.

---

## Overall Pipeline

```mermaid
flowchart TD
    A([Jenkins cron trigger\nhourly @ 0 * * * *]) --> B

    subgraph PARALLEL["All 5 sites in parallel (WB · ER · TG · JB · NM)"]
        B[Step 1\nMonitoring Check] -->|OK| C[Step 2\nSnippet Conversion]
        B -->|ALERT| B1[Send email alert\nTrigger failover copy]
        B1 --> C
        C --> D[Step 3\nRaster Generation]
        D --> E[Step 4\nOutput Handler]
        E --> F[Step 5\nPublishing Handoff]
    end

    F --> G([SDE Mosaic Dataset\n→ Schedman UI])

    style PARALLEL fill:#f0f4ff,stroke:#aac
```

---

## Step 1 — Monitoring Check (`monitoring_service.py`)

Checks that GIP has delivered fresh `.snp` files before processing starts.

```mermaid
flowchart TD
    START([Start: MonitoringService.check]) --> SCAN

    SCAN["Scan landing zone\nfor *.snp files"]

    SCAN -->|No files found| EMPTY[ALERT: No files]
    SCAN -->|Files found| NEWEST["Find newest file\nby mtime"]

    NEWEST --> AGE["Calculate age\n(now − mtime) / 60"]
    AGE --> THRESH{Age > threshold?\ndefault 10 min}

    THRESH -->|No| OK["Return status=OK\nfile_count, age_minutes"]
    THRESH -->|Yes| STALE[ALERT: Files are stale]

    EMPTY --> EMAIL{smtp_host\n+ alert_email set?}
    STALE --> EMAIL

    EMAIL -->|Yes| SEND[Send alert email\nvia SMTP relay]
    EMAIL -->|No| SKIP[Skip email\nlog only]

    SEND --> FAILOVER{failover_share\nconfigured?}
    SKIP --> FAILOVER

    FAILOVER -->|Yes| COPY["Copy *.snp from PROD\nfailover share to\nlanding zone"]
    FAILOVER -->|No| ALERTRET

    COPY --> ALERTRET["Return status=ALERT\nfile_count, age_minutes\nalert_sent=True"]

    OK --> DONE([Pipeline continues\nto Step 2])
    ALERTRET --> DONE2([Pipeline continues\nwith stale data warning])
```

**Key data:**
| Input | Output |
|-------|--------|
| `landing_zone/*.snp` | `{status, site, file_count, newest_file_age_minutes, alert_sent}` |

---

## Step 2 — Snippet Conversion (`snippet_conversion_service.py`)

Converts binary Minestar `.snp` files to a filtered, reprojected MGA50 CSV.

```mermaid
flowchart TD
    START([Start: SnippetConversionService.convert]) --> GLOB

    GLOB["Glob *.snp files\nin input_folder"]

    GLOB --> LOOP

    subgraph LOOP["For each .snp file"]
        VAL["Validate: magic number\n0xBFFF0173 at byte 0\n+ marker byte 0x0B at 31/36"]
        VAL -->|Invalid| SKIP[Skip file\nlog warning]
        VAL -->|Valid| SCAN2["Scan bytes from offset 20\nlooking for marker 0x0B"]
        SCAN2 --> RECORD["For each marker found:\nread 40-byte record\n10×uint32 (little-endian)"]
        RECORD --> PARSE["Parse: X=item[0]×0.01\nY=item[1]×0.01\nZ1-Z4, T1-T4 pairs\nscale by 0.01"]
        PARSE --> DEDUP["Dedup by XY key:\nif same XY exists\naverage Z values\nkeep newer timestamp"]
    end

    LOOP --> ZADJ["Z datum adjustment\nZ += z_adjustment\n(default +3.155 m\nADPH → AHD)"]

    ZADJ --> ZFILT["Noise filter:\nRemove points where\nZ > max_z (default 4000 m)"]

    ZFILT --> DESPIKE{despike=True?}

    DESPIKE -->|Yes| D3["3× despike passes"]

    subgraph D3["Despike: 3 passes"]
        NBRS["For each point:\nfind up to 8 neighbours\nat ±grid_size distance"]
        NBRS --> NCOUNT{< min_neighbours\nneighbours found?\ndefault 3}
        NCOUNT -->|Yes| FLAG[Flag for removal]
        NCOUNT -->|No| ZCHECK["Compute neighbour\nmedian + std-dev"]
        ZCHECK --> ZDEV{|median − Z|\n> std-dev?}
        ZDEV -->|Yes| REPLACE[Replace Z\nwith median]
        ZDEV -->|No| KEEP[Keep Z]
        FLAG --> REMOVE[Remove flagged\npoints after pass]
    end

    DESPIKE -->|No| REPROJECT
    D3 --> REPROJECT

    REPROJECT["Reproject via arcpy:\ninput_spatial_ref → MGA50\n(WB94/ER94 → GDA2020)"]
    REPROJECT --> AOIFILT{aoi_feature_class\nconfigured?}

    AOIFILT -->|Yes| AOI["AOI filter:\nRemove points outside\nboundary polygon"]
    AOIFILT -->|No| WRITE

    AOI --> WRITE

    WRITE["Write outputs to\n<output_folder>/<site>/<YYYYMMDD_HHMM>/"]
    WRITE --> CSV["<site>_points.csv\nColumns: X, Y, Z, TIMESTAMP"]
    WRITE --> JSON["config.json\n{site, timestamp, csvPath,\nsnippetCount, totalPoints,\nvalidPoints, processing params}"]

    CSV --> DONE([Return result dict\nstatus, csv_path,\nconfig_path, valid_points])
    JSON --> DONE
```

**Key data:**
| Input | Output |
|-------|--------|
| `*.snp` (Minestar binary, WB94/ER94 coords) | `<site>_points.csv` (MGA50, X/Y/Z/TIMESTAMP) |
| | `config.json` (processing metadata) |

---

## Step 3 — Raster Generation (`raster_generation_service.py`)

Converts the MGA50 point CSV into an elevation GeoTIFF raster and boundary polygon.

```mermaid
flowchart TD
    START([Start: RasterGenerationService.generate]) --> ARCPY

    ARCPY["Check out ArcGIS\n3D Analyst + Spatial Analyst\nextensions"]

    ARCPY --> GDB{scratch GDB\nexists?}
    GDB -->|No| CREATEGDB["Create File GDB\nin scratch_gdb path"]
    GDB -->|Yes| S1
    CREATEGDB --> S1

    S1["Step 1: CSV → 3D Feature Class\narcpy.management.XYTableToPoint\n  x_field=X, y_field=Y, z_field=Z\n  coordinate_system=MGA50"]

    S1 --> S1C["GetCount → log\npoint_count"]

    S1C --> S2["Step 2: 3D Feature Class → TIN\narcpy.ddd.CreateTin\n  in_features: Shape.Z masspoints\n  spatial_reference: MGA50"]

    S2 --> S3["Step 3: TIN → Raw GeoTIFF\narcpy.ddd.TinRaster\n  data_type=FLOAT\n  method=LINEAR\n  cell_size=2.0 m (default)"]

    S3 --> S4["Step 4: Boundary polygon\narcpy.management.MinimumBoundingGeometry\n  geometry_type=CONVEX_HULL\n  group_option=ALL"]

    S4 --> EXCL{exclusion_fc\nconfigured &\nexists?}

    EXCL -->|Yes| ERASE["Erase road buffer\narcpy.analysis.Erase\n  erase_features=MTD_Live_RoadsBuffered\n  → boundary without roads"]
    EXCL -->|No| CLIP

    ERASE --> CLIP

    CLIP["Step 5: Clip raster to boundary\narcpy.management.Clip\n  clipping_geometry=ClippingGeometry\n  → <site>_elevation.tif"]

    CLIP --> EXPORT["Step 6: Export boundary\narcpy.conversion.FeaturesToJSON\n  → <site>_boundary.geojson\narcpy.management.CopyFeatures\n  → <site>_boundary.shp"]

    EXPORT --> CLEANUP["Delete raw raster\n(intermediate file)"]

    CLEANUP --> GDBCLEAN{GDB was created\nthis run?}
    GDBCLEAN -->|Yes| DELGDB["arcpy.management.Delete\nscratch GDB\n(best-effort)"]
    GDBCLEAN -->|No| CHECKIN
    DELGDB --> CHECKIN

    CHECKIN["Check in extensions\n3D Analyst + Spatial Analyst"]

    CHECKIN --> DONE([Return result dict\nstatus, raster_path,\nboundary_path, point_count])
```

**Key data:**
| Input | Output |
|-------|--------|
| `<site>_points.csv` (MGA50) | `<site>_elevation.tif` (GeoTIFF, 2 m cells) |
| | `<site>_boundary.shp` + `.geojson` (convex hull − road buffer) |

---

## Step 4 — Output Handler (`output_handler_service.py`)

Assembles the standardised output folder that the publishing solution reads.

```mermaid
flowchart TD
    START([Start: OutputHandlerService.publish_outputs]) --> MKDIR

    MKDIR["Create output directory\nFMS_Output/<site>/<YYYYMMDD_HHMM>/"]

    MKDIR --> RASTER["Copy raster\n<site>_elevation.tif → output_dir"]

    RASTER --> BOUNDARY["Copy boundary files\n.shp  .dbf  .shx  .prj  .geojson\n(each copied if it exists)"]

    BOUNDARY --> META["Write metadata.json\n{\n  site, timestamp,\n  output.rasterPath,\n  output.boundaryPath,\n  output.format=GeoTIFF,\n  output.cellSize=2,\n  output.spatialReference=MGA50,\n  sourceFiles.*,\n  processing.*,\n  status=ready_for_publish\n}"]

    META --> FLAG["Write ready.flag\n'ready\\ntimestamp=...\\nsite=...'"]

    FLAG --> DONE([Return result dict\nstatus, output_dir,\nraster, boundary,\nmetadata_path, flag_path])

    FLAG -.->|Background| CLEANUP

    subgraph CLEANUP["cleanup_old_outputs (called separately)"]
        SCAN["Scan site_dir for\ntimestamped folders"]
        SCAN --> AGE2["Calculate age\n(now − folder_name_timestamp)"]
        AGE2 --> OLD{age > retention_hours\ndefault 48 h?}
        OLD -->|Yes| DEL["shutil.rmtree(folder)"]
        OLD -->|No| KEEP2[Keep folder]
    end
```

**Output folder layout:**
```
FMS_Output/
└── WB/
    └── 20260414_1400/
        ├── WB_elevation.tif
        ├── WB_boundary.shp
        ├── WB_boundary.dbf
        ├── WB_boundary.shx
        ├── WB_boundary.prj
        ├── WB_boundary.geojson
        ├── metadata.json
        └── ready.flag          ← triggers publishing solution
```

---

## Step 5 — Publishing Handoff (`publishing_service.py`)

Notifies the enterprise publishing solution that outputs are ready.

```mermaid
flowchart TD
    START([Start: PublishingService.trigger]) --> MODE

    MODE{integration_mode?}

    MODE -->|file_trigger| FT_CHECK
    MODE -->|direct_api| API_CHECK
    MODE -->|other| ERR_MODE[Raise PublishingError\nUnknown mode]

    subgraph FILE_TRIGGER["Mode: file_trigger (default)"]
        FT_CHECK["Verify ready.flag\nexists in output_dir"]
        FT_CHECK -->|Missing| ERR_FLAG[Raise PublishingError\nready.flag not found]
        FT_CHECK -->|Found| FT_LOG["Log raster + boundary\npaths from metadata.json"]
        FT_LOG --> POLL{poll_timeout > 0?}
        POLL -->|No| FF["Fire-and-forget\nReturn published=False"]
        POLL -->|Yes| WAIT["Poll for done.flag\nevery poll_interval s\n(default 30 s)"]
        WAIT --> DONE_Q{done.flag\nappeared?}
        DONE_Q -->|Yes| PUB["Return published=True"]
        DONE_Q -->|No, elapsed < poll_timeout| WAIT
        DONE_Q -->|Timeout exceeded| ERR_TO[Raise PublishingError\nTimeout waiting for done.flag]
    end

    subgraph DIRECT_API["Mode: direct_api"]
        API_CHECK["Verify publishing_api_module\nis set + metadata.json exists"]
        API_CHECK -->|Missing| ERR_API[Raise PublishingError]
        API_CHECK -->|OK| LOAD["importlib.import_module\n(publishing_api_module)"]
        LOAD --> CALL["call publish_to_mosaic(\n  raster_path,\n  boundary_path,\n  config,\n  timeout\n)"]
        CALL -->|Success| API_OK[Return published=True]
        CALL -->|Exception| ERR_CALL[Raise PublishingError\nAPI call failed]
    end

    FILE_TRIGGER --> DOWNSTREAM
    DIRECT_API --> DOWNSTREAM

    DOWNSTREAM([Existing enterprise publishing solution\nAddRastersToMosaicDataset\n→ SDE / MTD Mosaic Dataset\n→ Schedman UI])
```

---

## Nightly Archive Job (`archive_service.py` + `archive_runner.py`)

Runs once per night. Compresses the day's `.snp` files and clears the landing zone.

```mermaid
flowchart TD
    START([Jenkins cron trigger\nnightly — archive_runner]) --> SITES

    subgraph SITES["For each site (WB · ER · TG · JB · NM)"]
        GLOB["Glob *.snp\nin landing_zone"]
        GLOB -->|No files| NONE["Return files_archived=0\nNo archive created"]
        GLOB -->|Files found| COUNT["Count files\nCalculate total bytes"]

        COUNT --> DRYRUN{dry_run=True?}

        DRYRUN -->|Yes| DRYLOG["Log: would create archive\nLog: would remove N files\n(no files touched)"]
        DRYRUN -->|No| STAGE["Create _archive_staging/\nunder landing zone"]

        STAGE --> COPY2["Copy *.snp →\n_archive_staging/"]

        COPY2 --> ZIP["shutil.make_archive\n→ <archive_root>/<site>/<YYYY>/<MM>/\n    <site>_snippets_<YYYYMMDD_HHMMSS>.zip"]

        ZIP --> RMSTAGE["Remove _archive_staging/"]

        RMSTAGE --> CLEAR["Delete original *.snp\nfrom landing zone"]

        CLEAR --> RESULT["Return:\n  archive_path,\n  files_archived,\n  bytes_archived"]

        DRYLOG --> DRYRESULT["Return:\n  dry_run=True,\n  files_archived (counted)\n  bytes_archived (counted)"]
    end

    RESULT --> DONE([Landing zone cleared\nArchive retained in\narchive_root tree])
    DRYRESULT --> DONE
```

**Archive folder layout:**
```
archive_root/
└── WB/
    └── 2026/
        └── 04/
            └── WB_snippets_20260414_020000.zip
```
