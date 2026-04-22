"""
FMS Pipeline Service.

Standalone implementation of the end-to-end FMS elevation processing workflow.
No external custom dependencies — uses standard Python + arcpy only.

Pipeline:
    CSV (XYZ) → 3D Feature Class → TIN → Raster (GeoTIFF)
    → Boundary polygon (RasterToPolygon + Dissolve) → File GDB
    → ProcessData.json

Output folder layout (created under output_base_folder):
    <YYYYmmddHHMMSS>_FMS_<SITENAME>/
    ├── FMS_<SITENAME>.gdb/
    │   └── Boundary          ← dissolved polygon feature class
    ├── Source/
    │   └── <YYYYmmddHHMMSS>_FMS_<SITENAME>.csv
    ├── <YYYYmmddHHMMSS>_FMS_<SITENAME>.tif
    └── ProcessData.json

Jenkins usage:
    Each site is a separate process invocation — parallel execution is safe
    because every run generates a unique timestamped output folder and uses
    random UUIDs for all intermediate IN_MEMORY / scratchFolder datasets.
"""
from __future__ import annotations

import json
import logging
import os
import traceback
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------

def _validate_inputs(
    input_csv: str,
    output_base_folder: str,
    site_name: str,
    config: dict,
) -> None:
    """Fail fast on bad inputs before touching arcpy."""
    if not os.path.isfile(input_csv):
        raise FileNotFoundError(f"Input CSV not found: {input_csv}")
    if not site_name or not site_name.strip():
        raise ValueError("site_name must not be empty")
    if not output_base_folder:
        raise ValueError("output_base_folder must not be empty")
    if not isinstance(config, dict):
        raise TypeError(f"config must be a dict, got {type(config).__name__}")


# ---------------------------------------------------------------------------
# ArcGIS extension management
# ---------------------------------------------------------------------------

def _checkout_extensions() -> None:
    import arcpy
    for ext in ("3D", "Spatial"):
        result = arcpy.CheckOutExtension(ext)
        if result not in ("CheckedOut", "AlreadyCheckedOut"):
            raise RuntimeError(
                f"Could not check out {ext} Analyst extension — "
                f"check licence availability. ArcGIS returned: {result}"
            )
    logger.debug("Extensions checked out: 3D Analyst, Spatial Analyst")


def _checkin_extensions() -> None:
    try:
        import arcpy
        arcpy.CheckInExtension("3D")
        arcpy.CheckInExtension("Spatial")
        logger.debug("Extensions checked in")
    except Exception:
        pass  # Never raise during cleanup


# ---------------------------------------------------------------------------
# Spatial reference helper
# ---------------------------------------------------------------------------

def _resolve_sr(value: Any):
    """
    Return an arcpy.SpatialReference from:
    - an existing SpatialReference object (returned unchanged)
    - a path to a .prj file (WKT loaded from file)
    - a WKT string
    Returns None if value is falsy.
    """
    import arcpy
    if not value:
        return None
    if hasattr(value, "factoryCode"):
        return value
    if isinstance(value, str) and value.lower().endswith(".prj"):
        prj_text = Path(value).read_text(encoding="utf-8").strip()
        sr = arcpy.SpatialReference()
        sr.loadFromString(prj_text)
        return sr
    if isinstance(value, str):
        sr = arcpy.SpatialReference()
        sr.loadFromString(value)
        return sr
    return None


# ---------------------------------------------------------------------------
# AOI geometry reader
# ---------------------------------------------------------------------------

def _get_aoi(aoi_fc: str, aoi_where: str):
    """
    Read and union all AOI features matching aoi_where.
    Returns a single arcpy.Geometry or None if aoi_fc is not configured.
    """
    import arcpy
    if not aoi_fc or not arcpy.Exists(aoi_fc):
        if aoi_fc:
            logger.warning("AOI feature class not found — skipping clip: %s", aoi_fc)
        return None
    logger.debug("Reading AOI: %s  WHERE: %s", aoi_fc, aoi_where or "(none)")
    aoi_geom = None
    with arcpy.da.SearchCursor(aoi_fc, ["SHAPE@"], aoi_where or None) as cursor:
        for (shape,) in cursor:
            aoi_geom = shape if aoi_geom is None else aoi_geom.union(shape)
    if aoi_geom is None:
        logger.warning("AOI query returned no features — skipping clip")
    return aoi_geom


# ---------------------------------------------------------------------------
# CSV copy with configurable decimal rounding on X, Y, Z
# ---------------------------------------------------------------------------

def _write_csv_rounded(src: str, dst: str, decimal_places: int) -> int:
    """
    Copy *src* CSV to *dst* with X, Y, Z values rounded to *decimal_places*.

    Expects a header row containing X, Y, Z column names (case-insensitive).
    Any extra columns (e.g. TIMESTAMP) are preserved unchanged.
    Returns the number of data rows written.
    """
    import csv

    fmt = f"{{:.{decimal_places}f}}"

    with open(src, newline="", encoding="utf-8") as fin, \
         open(dst, "w", newline="", encoding="utf-8") as fout:

        reader = csv.DictReader(fin)
        if reader.fieldnames is None:
            raise ValueError(f"CSV has no header row: {src}")

        # Identify X / Y / Z columns (case-insensitive)
        header = list(reader.fieldnames)
        upper = [h.upper() for h in header]
        xyz_indices = {upper.index(c) for c in ("X", "Y", "Z") if c in upper}
        xyz_names = {header[i] for i in xyz_indices}

        writer = csv.DictWriter(fout, fieldnames=header, lineterminator="\n")
        writer.writeheader()

        rows_written = 0
        for row in reader:
            for col in xyz_names:
                if row[col].strip():
                    row[col] = fmt.format(float(row[col]))
            writer.writerow(row)
            rows_written += 1

    logger.debug(
        "CSV rounded to %d dp — %d rows written: %s", decimal_places, rows_written, dst
    )
    return rows_written


# ---------------------------------------------------------------------------
# Raster generation: CSV → 3D Feature Class → TIN → GeoTIFF
# ---------------------------------------------------------------------------

def _generate_raster(
    csv_path: str,
    output_raster: str,
    input_sr,
    output_sr,
    cell_size: int,
    avg_point_spacing: float,
    tin_delineate_value: float,
    aoi_geom,
) -> None:
    """
    Full TIN-based raster pipeline, matching the original MRD server workflow.

    All intermediate datasets live in IN_MEMORY or arcpy.env.scratchFolder
    and are always deleted in the finally block.  UUID prefixes make every
    dataset name unique, so concurrent Jenkins builds on different sites
    never collide in shared scratch space.
    """
    import arcpy

    run_id = uuid.uuid4().hex[:8]
    fc_3d = f"IN_MEMORY/{run_id}_3D"
    scratch_tin = os.path.join(arcpy.env.scratchFolder, f"tin_{run_id}")
    pre_clip = f"IN_MEMORY/{run_id}_raw" if aoi_geom else None

    try:
        logger.info("Step 1: CSV → 3D Feature Class  (avg_spacing=%.1f)", avg_point_spacing)
        arcpy.ASCII3DToFeatureClass_3d(
            csv_path, "XYZ", fc_3d, "MULTIPOINT", "1",
            input_sr, avg_point_spacing, "", "DECIMAL_POINT",
        )

        logger.info("Step 2: 3D Feature Class → TIN")
        arcpy.CreateTin_3d(
            scratch_tin, output_sr,
            f"{fc_3d} Shape.Z Mass_Points <None>",
            "DELAUNAY",
        )

        logger.info("Step 3: Delineate TIN data area  (max_edge=%.1f)", tin_delineate_value)
        arcpy.DelineateTinDataArea_3d(scratch_tin, tin_delineate_value, "ALL")

        logger.info("Step 4: TIN → Raster  (cell_size=%d m)", cell_size)
        arcpy.env.extent = "DEFAULT"
        cell_arg = f"CELLSIZE {int(cell_size)}"

        if aoi_geom:
            arcpy.TinRaster_3d(scratch_tin, pre_clip, "FLOAT", "LINEAR", cell_arg, "1")
            logger.info("Step 5: Clip raster to AOI")
            arcpy.Clip_management(pre_clip, "#", output_raster, aoi_geom, "0", "NONE")
        else:
            arcpy.TinRaster_3d(scratch_tin, output_raster, "FLOAT", "LINEAR", cell_arg, "1")

    finally:
        for temp in filter(None, [fc_3d, pre_clip]):
            try:
                if arcpy.Exists(temp):
                    arcpy.Delete_management(temp)
            except Exception:
                pass
        try:
            if arcpy.Exists(scratch_tin):
                arcpy.Delete_management(scratch_tin)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Boundary generation: Raster → Polygon → Dissolve → File GDB FC
# ---------------------------------------------------------------------------

def _generate_boundary(input_raster: str, output_fc: str, item_name: str) -> None:
    """
    Convert the valid raster data area to a dissolved boundary polygon
    and store it inside the output File GDB.

    Uses the XOR-to-zero technique (raster ^ raster = integer 0 mask)
    so that RasterToPolygon captures exactly the footprint of non-NoData cells.
    """
    import arcpy

    run_id = uuid.uuid4().hex[:8]
    poly_fc = f"IN_MEMORY/{run_id}_Poly"
    dissolved_fc = f"IN_MEMORY/{run_id}_DPoly"

    try:
        logger.info("Raster → Polygon")
        raster = arcpy.sa.Raster(input_raster)
        int_raster = raster ^ raster  # XOR → all-zero integer mask over valid cells
        arcpy.RasterToPolygon_conversion(int_raster, poly_fc, "NO_SIMPLIFY", "VALUE")

        logger.info("Dissolve → single multi-part boundary")
        arcpy.Dissolve_management(poly_fc, dissolved_fc, multi_part="MULTI_PART")

    finally:
        try:
            if arcpy.Exists(poly_fc):
                arcpy.Delete_management(poly_fc)
        except Exception:
            pass

    try:
        arcpy.AddField_management(dissolved_fc, "Name", "TEXT", field_length=200)
        arcpy.CalculateField_management(
            dissolved_fc, "Name", f"'{item_name}'", "PYTHON3"
        )
        if arcpy.Exists(output_fc):
            arcpy.Delete_management(output_fc)
        arcpy.FeatureClassToFeatureClass_conversion(
            dissolved_fc,
            os.path.dirname(output_fc),
            os.path.basename(output_fc),
        )
        logger.info("Boundary FC written: %s", output_fc)
    finally:
        try:
            if arcpy.Exists(dissolved_fc):
                arcpy.Delete_management(dissolved_fc)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# ProcessData.json writer
# ---------------------------------------------------------------------------

def _write_process_data_json(
    json_path: str,
    acquisition_date: str,
    group_name: str,
    site_code: str,
    profile: str = "Elevation_FMS_Minestar_CSV",
) -> None:
    payload = {
        "Fields": {
            "AcquisitionDate": acquisition_date,
            "CaptureCategory": "Hourly",
            "GroupName": group_name,
            "InputHorizontalProjection": "MGA50",
            "InputVerticalProjection": "AHD",
            "Site": site_code,
        },
        "Profile": profile,
        "site": site_code,
    }
    Path(json_path).write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logger.info("ProcessData.json written: %s", json_path)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def process_fms_pipeline(
    input_csv: str,
    output_base_folder: str,
    site_name: str,
    config: dict[str, Any],
) -> dict[str, Any]:
    """
    Run the complete FMS elevation pipeline for one site.

    Parameters
    ----------
    input_csv : str
        Path to the MGA50-projected XYZ CSV (X, Y, Z [, TIMESTAMP] columns).
    output_base_folder : str
        Root directory where the timestamped output folder is created.
    site_name : str
        Site code, e.g. ``YAN``, ``MAC``, ``JBAH``.
    config : dict
        Processing parameters:

        Key                   Type    Default   Description
        ──────────────────────────────────────────────────────────────────
        cellSize              int     1         Raster cell size (metres)
        decimalPlaces         int     2         Decimal places for X, Y, Z in Source CSV
        snapRaster            str     ""        Optional snap raster path
        inputSpatialRef       str     ""        Input CRS (.prj path or WKT)
        outputSpatialRef      str     ""        Output CRS (.prj path or WKT)
        aoiFeatureClass       str     ""        AOI feature class path
        aoiWhereClause        str     ""        SQL WHERE for AOI selection
        useAOI                bool    False     Apply AOI clip
        averagePointSpacing   float   1.0       ASCII3DToFeatureClass spacing
        tinDelineateValue     float   10.0      TIN delineation max edge length
        profile               str     "Elevation_FMS_Minestar_CSV"

    Returns
    -------
    dict
        status, group_name, output_folder, raster_path,
        boundary_path, json_path.

    Raises
    ------
    FileNotFoundError
        If *input_csv* does not exist.
    RuntimeError
        If arcpy is unavailable or an extension cannot be checked out.
    """
    _validate_inputs(input_csv, output_base_folder, site_name, config)

    try:
        import arcpy
    except ImportError as exc:
        raise RuntimeError(
            "arcpy is not available — ArcGIS Pro must be installed and licensed."
        ) from exc

    now = datetime.now()
    dt_str = now.strftime("%Y%m%d%H%M%S")
    acq_date = now.strftime("%Y-%m-%d %H:%M:%S")
    group_name = f"{dt_str}_FMS_{site_name}"

    output_folder = Path(output_base_folder) / group_name
    source_folder = output_folder / "Source"
    gdb_name = f"FMS_{site_name}.gdb"
    gdb_path = str(output_folder / gdb_name)
    raster_path = str(output_folder / f"{group_name}.tif")
    csv_dest = str(source_folder / f"{group_name}.csv")
    boundary_fc = os.path.join(gdb_path, "Boundary")
    json_path = str(output_folder / "ProcessData.json")

    logger.info("=" * 60)
    logger.info("FMS Pipeline — site: %s  group: %s", site_name, group_name)
    logger.info("Output folder: %s", output_folder)

    try:
        # ----------------------------------------------------------------
        # 1. Output folder structure
        # ----------------------------------------------------------------
        output_folder.mkdir(parents=True, exist_ok=True)
        source_folder.mkdir(exist_ok=True)
        logger.info("Output structure created")

        # ----------------------------------------------------------------
        # 2. Copy + rename CSV into Source/
        # ----------------------------------------------------------------
        decimal_places = int(config.get("decimalPlaces", 2))
        rows = _write_csv_rounded(input_csv, csv_dest, decimal_places)
        logger.info("CSV → Source (%d dp, %d rows): %s", decimal_places, rows, csv_dest)

        # ----------------------------------------------------------------
        # 3. Create File GDB
        # ----------------------------------------------------------------
        arcpy.env.overwriteOutput = True
        if arcpy.Exists(gdb_path):
            arcpy.Delete_management(gdb_path)
        arcpy.CreateFileGDB_management(str(output_folder), gdb_name)
        logger.info("File GDB: %s", gdb_path)

        # ----------------------------------------------------------------
        # 4. Resolve spatial references and AOI
        # ----------------------------------------------------------------
        input_sr = _resolve_sr(config.get("inputSpatialRef"))
        output_sr = _resolve_sr(config.get("outputSpatialRef"))

        aoi_geom = None
        if config.get("useAOI"):
            aoi_geom = _get_aoi(
                config.get("aoiFeatureClass", ""),
                config.get("aoiWhereClause", ""),
            )

        if config.get("snapRaster"):
            arcpy.env.snapRaster = config["snapRaster"]

        # ----------------------------------------------------------------
        # 5. Raster generation (CSV → TIN → GeoTIFF)
        # ----------------------------------------------------------------
        _checkout_extensions()
        try:
            logger.info("--- Raster generation ---")
            _generate_raster(
                csv_path=input_csv,
                output_raster=raster_path,
                input_sr=input_sr,
                output_sr=output_sr,
                cell_size=int(config.get("cellSize", 1)),
                avg_point_spacing=float(config.get("averagePointSpacing", 1.0)),
                tin_delineate_value=float(config.get("tinDelineateValue", 10.0)),
                aoi_geom=aoi_geom,
            )
            logger.info("Raster: %s", raster_path)

            # ----------------------------------------------------------------
            # 6. Boundary polygon inside GDB
            # ----------------------------------------------------------------
            logger.info("--- Boundary generation ---")
            _generate_boundary(
                input_raster=raster_path,
                output_fc=boundary_fc,
                item_name=group_name,
            )

        finally:
            _checkin_extensions()

        # ----------------------------------------------------------------
        # 7. ProcessData.json
        # ----------------------------------------------------------------
        _write_process_data_json(
            json_path=json_path,
            acquisition_date=acq_date,
            group_name=group_name,
            site_code=site_name,
            profile=config.get("profile", "Elevation_FMS_Minestar_CSV"),
        )

        logger.info("FMS Pipeline complete — %s", group_name)
        return {
            "status": "SUCCESS",
            "group_name": group_name,
            "output_folder": str(output_folder),
            "raster_path": raster_path,
            "boundary_path": boundary_fc,
            "json_path": json_path,
        }

    except Exception:
        logger.error("Pipeline FAILED — site: %s\n%s", site_name, traceback.format_exc())
        raise


# ---------------------------------------------------------------------------
# Batch helper (sequential, Jenkins-parallel-safe)
# ---------------------------------------------------------------------------

def batch_process_fms(
    jobs: list[tuple[str, str]],
    output_base_folder: str,
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    """
    Run process_fms_pipeline sequentially for a list of (csv_path, site_name) jobs.

    Jenkins parallelises across sites by running separate OS processes, so this
    helper is primarily useful when a single Jenkins stage must handle multiple
    CSV inputs for one site (e.g. two delivery streams merged before raster gen).

    A failure in one job is captured and recorded; processing continues for the
    remaining jobs so a single bad file does not block the entire batch.

    Returns
    -------
    list of result dicts — one per job, in input order.
    Successful jobs have ``status == "SUCCESS"``.
    Failed jobs have ``status == "FAILED"`` and an ``error`` key.
    """
    results: list[dict[str, Any]] = []
    for csv_path, site_name in jobs:
        logger.info("Batch job: site=%s  csv=%s", site_name, csv_path)
        try:
            result = process_fms_pipeline(csv_path, output_base_folder, site_name, config)
        except Exception:  # noqa: BLE001
            result = {
                "status": "FAILED",
                "site_name": site_name,
                "csv_path": csv_path,
                "error": traceback.format_exc(),
            }
            logger.error("Batch job FAILED: site=%s", site_name)
        results.append(result)

    succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
    logger.info("Batch complete — %d/%d succeeded", succeeded, len(results))
    return results
