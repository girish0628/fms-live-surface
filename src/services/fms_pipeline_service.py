"""
FMS Pipeline Service.

Standalone implementation of the end-to-end FMS elevation processing workflow.
No external custom dependencies — uses standard Python + arcpy only.

Pipeline:
    CSV (XYZ) → 3D Feature Class → TIN → Raster (GeoTIFF)
    → Boundary polygon (RasterToPolygon + Dissolve) → Shapefile + CSV

Output folder layout (shared across all sites for one run):
    FMS_<YYYYMMDDHH0000>/                        ← shared by all parallel sites
    ├── FMS_<YYYYMMDDHH0000>_<SITE>.tif           ← raster per site
    └── Source/
        ├── FMS_<YYYYMMDDHH0000>_boundary_<SITE>.shp  ← boundary shapefile per site
        └── FMS_<YYYYMMDDHH0000>_boundary_<SITE>.csv  ← boundary polygon vertices per site

The run_timestamp is normalised to YYYYMMDDHH0000 so that all parallel Jenkins
site processes for the same hour write into the same shared folder.
"""
from __future__ import annotations

import csv as csv_mod
import logging
import os
import traceback
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from src.utils.naming_utils import to_hourly_ts

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
        pass


# ---------------------------------------------------------------------------
# Spatial reference helper
# ---------------------------------------------------------------------------

def _resolve_sr(value: Any):
    """
    Return an arcpy.SpatialReference from a .prj path, WKT string, or existing
    SpatialReference object.  Returns None if value is falsy.
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
    """Read and union all AOI features matching aoi_where."""
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
    Full TIN-based raster pipeline.

    All intermediate datasets use UUID prefixes so concurrent Jenkins builds
    for different sites never collide in shared scratch space.
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
# Boundary generation: Raster → Polygon → Dissolve → Shapefile
# ---------------------------------------------------------------------------

def _generate_boundary(input_raster: str, output_shp: str, item_name: str) -> None:
    """
    Convert the valid raster data area to a dissolved boundary shapefile.

    Uses the XOR-to-zero technique so RasterToPolygon captures exactly
    the footprint of non-NoData cells.
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
        if arcpy.Exists(output_shp):
            arcpy.Delete_management(output_shp)
        arcpy.CopyFeatures_management(dissolved_fc, output_shp)
        logger.info("Boundary SHP written: %s", output_shp)
    finally:
        try:
            if arcpy.Exists(dissolved_fc):
                arcpy.Delete_management(dissolved_fc)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Boundary CSV export: polygon vertices → CSV
# ---------------------------------------------------------------------------

def _export_boundary_to_csv(boundary_shp: str, csv_path: str) -> int:
    """
    Export boundary polygon vertices to CSV (RING_ID, PART_ID, POINT_ID, X, Y).
    Returns the number of vertex rows written.
    """
    import arcpy

    rows = 0
    with open(csv_path, "w", newline="", encoding="utf-8") as fout:
        writer = csv_mod.writer(fout)
        writer.writerow(["RING_ID", "PART_ID", "POINT_ID", "X", "Y"])
        ring_id = 0
        with arcpy.da.SearchCursor(boundary_shp, ["SHAPE@"]) as cursor:
            for (shape,) in cursor:
                for part_idx, part in enumerate(shape):
                    for pt_idx, pt in enumerate(part):
                        if pt:
                            writer.writerow([
                                ring_id, part_idx, pt_idx,
                                round(pt.X, 2), round(pt.Y, 2),
                            ])
                            rows += 1
                ring_id += 1
    logger.info("Boundary CSV written (%d vertices): %s", rows, csv_path)
    return rows


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def process_fms_pipeline(
    input_csv: str,
    output_base_folder: str,
    site_name: str,
    config: dict[str, Any],
    run_timestamp: str | None = None,
) -> dict[str, Any]:
    """
    Run the complete FMS elevation pipeline for one site.

    Parameters
    ----------
    input_csv : str
        Path to the MGA50-projected XYZ CSV (X, Y, Z [, TIMESTAMP] columns).
    output_base_folder : str
        Root directory where the shared ``FMS_<YYYYMMDDHH0000>`` folder lives.
    site_name : str
        Site code, e.g. ``WB``, ``MAC``, ``JB``.
    config : dict
        Processing parameters:

        Key                   Type    Default   Description
        ──────────────────────────────────────────────────────────────────
        cellSize              int     1         Raster cell size (metres)
        snapRaster            str     ""        Optional snap raster path
        inputSpatialRef       str     ""        Input CRS (.prj path or WKT)
        outputSpatialRef      str     ""        Output CRS (.prj path or WKT)
        aoiFeatureClass       str     ""        AOI feature class path
        aoiWhereClause        str     ""        SQL WHERE for AOI selection
        useAOI                bool    False     Apply AOI clip
        averagePointSpacing   float   1.0       ASCII3DToFeatureClass spacing
        tinDelineateValue     float   10.0      TIN delineation max edge length
        profile               str     "Elevation_FMS_Minestar_CSV"
    run_timestamp : str or None
        Raw timestamp string.  Normalised to YYYYMMDDHH0000 internally.
        Reads ``FMS_RUN_TIMESTAMP`` env var if not passed; falls back to clock.

    Returns
    -------
    dict
        status, group_name, output_folder, raster_path,
        boundary_path (SHP in Source/), boundary_csv.
    """
    _validate_inputs(input_csv, output_base_folder, site_name, config)

    try:
        import arcpy
    except ImportError as exc:
        raise RuntimeError(
            "arcpy is not available — ArcGIS Pro must be installed and licensed."
        ) from exc

    if run_timestamp is None:
        run_timestamp = os.environ.get("FMS_RUN_TIMESTAMP") or datetime.now().strftime("%Y%m%d%H%M%S")

    hourly_ts = to_hourly_ts(run_timestamp)
    group_name = f"FMS_{hourly_ts}_{site_name}"

    output_folder = Path(output_base_folder) / f"FMS_{hourly_ts}"
    source_folder = output_folder / "Source"
    raster_path   = str(output_folder / f"FMS_{hourly_ts}_{site_name}.tif")
    boundary_shp  = str(source_folder / f"FMS_{hourly_ts}_boundary_{site_name}.shp")
    boundary_csv  = str(source_folder / f"FMS_{hourly_ts}_boundary_{site_name}.csv")

    logger.info("=" * 60)
    logger.info("FMS Pipeline — site: %s  group: %s", site_name, group_name)
    logger.info("Output folder: %s", output_folder)

    try:
        # 1. Output folder structure
        output_folder.mkdir(parents=True, exist_ok=True)
        source_folder.mkdir(exist_ok=True)
        logger.info("Output structure created")

        # 2. Resolve spatial references and AOI
        arcpy.env.overwriteOutput = True
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

        # 3. Raster generation (CSV → TIN → GeoTIFF)
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

            # 4. Boundary polygon → shapefile in Source/
            logger.info("--- Boundary generation ---")
            _generate_boundary(
                input_raster=raster_path,
                output_shp=boundary_shp,
                item_name=group_name,
            )

            # 5. Export boundary vertices to CSV in Source/
            _export_boundary_to_csv(boundary_shp, boundary_csv)

        finally:
            _checkin_extensions()

        logger.info("FMS Pipeline complete — %s", group_name)
        return {
            "status": "SUCCESS",
            "group_name": group_name,
            "output_folder": str(output_folder),
            "raster_path": raster_path,
            "boundary_path": boundary_shp,
            "boundary_csv": boundary_csv,
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
    run_timestamp: str | None = None,
) -> list[dict[str, Any]]:
    """
    Run process_fms_pipeline sequentially for a list of (csv_path, site_name) jobs.

    A failure in one job is captured; processing continues for remaining jobs.

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
            result = process_fms_pipeline(csv_path, output_base_folder, site_name, config, run_timestamp)
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
