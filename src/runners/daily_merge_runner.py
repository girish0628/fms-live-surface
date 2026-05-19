"""
Daily Merge Runner — per-site processor.

Jenkins trigger: 00:00–01:00 AM, runs in parallel for each mine site
(same parallel pattern as fms_runner.py for hourly).

Identical to fms_runner.py except:
  - Time window: [run_date 01:00:00, run_date+1 00:00:00) instead of
    [midnight, current hour)
  - Output folder label is YYYYMMDD so files land in FMS_<YYYYMMDD>/
    instead of FMS_<YYYYMMDDHH0000>/

Run daily_finalize_runner after all parallel site stages complete.

Usage (Jenkins, parallel per site):
    python -m src.runners.daily_merge_runner \\
        --config config/app_config.yaml \\
        --logging config/logging.prod.yaml \\
        --site WB \\
        --env PROD \\
        --FMS_ForceDate YYYYMMDD
"""
from __future__ import annotations

import argparse
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from src.core.config_loader import ConfigLoader, get_config_value
from src.core.logger import get_logger, setup_logging
from src.services.fms_pipeline_service import process_fms_pipeline
from src.services.modular_csv_service import ModularCsvService
from src.services.snippet_conversion_service import SnippetConversionService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="FMS Daily per-site processor — snippet/CSV conversion + raster pipeline"
    )
    parser.add_argument("--config",  required=True, help="Path to app_config.yaml")
    parser.add_argument("--logging", required=True, help="Path to logging YAML config")
    parser.add_argument("--site",    required=True, help="Mine site code (WB, ER, SF, ...)")
    parser.add_argument(
        "--FMS_ForceDate", action="store", type=str, default="",
        help="Data date (YYYYMMDD). Defaults to yesterday.",
    )
    parser.add_argument(
        "--env", default="PROD", choices=["NPE", "PROD", "DEV"],
        help="Deployment environment",
    )
    return parser.parse_args()


def _resolve_run_date(force_date: str) -> str:
    """Return data date as YYYYMMDD — provided value, or yesterday."""
    if force_date.strip():
        return force_date.strip()
    return (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")


def _build_filter_window(run_date: str) -> tuple[datetime, datetime]:
    """
    Daily time window: [run_date 01:00:00, run_date+1 00:00:00)

    Covers all files delivered from 1 AM up to midnight — the full
    preceding calendar day, matching what the hourly runs accumulated.
    """
    y, m, d = int(run_date[:4]), int(run_date[4:6]), int(run_date[6:8])
    filter_since = datetime(y, m, d, 1, 0, 0)
    filter_until = datetime(y, m, d, 0, 0, 0) + timedelta(days=1)
    return filter_since, filter_until


def run(cfg: dict[str, Any], site: str, run_date: str) -> None:
    logger = get_logger(__name__)

    site_cfg       = get_config_value(cfg, f"sites.{site}", {})
    paths_cfg      = get_config_value(cfg, "paths", {})
    processing_cfg = get_config_value(cfg, "processing", {})

    landing_zone   = site_cfg.get("landing_zone", paths_cfg.get("landing_zone_root", ""))
    staging_folder = paths_cfg.get("staging_folder", "")
    output_root    = paths_cfg.get("output_root", "")
    source_type    = site_cfg.get("source_type", "minestar")

    filter_since, filter_until = _build_filter_window(run_date)
    staging_run_folder = str(Path(staging_folder) / f"FMS_{run_date}") if staging_folder else ""

    logger.info("=" * 60)
    logger.info("Daily per-site run — site=%s  date=%s", site, run_date)
    logger.info("File window: [%s, %s)", filter_since, filter_until)
    logger.info("=" * 60)

    # ------------------------------------------------------------------
    # Step 1a: Minestar snippet conversion
    # ------------------------------------------------------------------
    conversion_result: dict[str, Any] = {}
    if source_type in ("minestar", "both"):
        logger.info("--- Step 1a: Snippet conversion (Minestar) ---")
        snippet_svc = SnippetConversionService(
            site=site,
            input_folder=landing_zone,
            output_folder=staging_run_folder,
            z_adjustment=site_cfg.get("z_adjustment", processing_cfg.get("z_adjustment", 3.155)),
            min_neighbours=processing_cfg.get("min_neighbours", 3),
            max_z=processing_cfg.get("max_z", 4000.0),
            decimal_digits=processing_cfg.get("decimal_digits", 2),
            grid_size=processing_cfg.get("grid_size", 2),
            input_spatial_ref=site_cfg.get("input_spatial_ref", ""),
            output_spatial_ref=processing_cfg.get("output_spatial_ref", ""),
            aoi_feature_class=processing_cfg.get("aoi_feature_class", ""),
            aoi_where_clause=site_cfg.get("aoi_where_clause", f"MineSite='{site}'"),
            despike=processing_cfg.get("despike", True),
            filter_since=filter_since,
            filter_until=filter_until,
        )
        conversion_result = snippet_svc.convert()
        logger.info("Snippet conversion: %s", conversion_result["status"])

    # ------------------------------------------------------------------
    # Step 1b: Modular CSV conversion
    # ------------------------------------------------------------------
    if source_type in ("modular", "both"):
        logger.info("--- Step 1b: Modular CSV conversion ---")
        modular_svc = ModularCsvService(
            site=site,
            input_folder=landing_zone,
            output_folder=staging_run_folder,
            z_adjustment=site_cfg.get("z_adjustment", 0.0),
            min_neighbours=processing_cfg.get("min_neighbours", 3),
            max_z=processing_cfg.get("max_z", 4000.0),
            decimal_digits=processing_cfg.get("decimal_digits", 2),
            grid_size=int(processing_cfg.get("grid_size", 2)),
            csv_col_x=site_cfg.get("csv_col_x", 1),
            csv_col_y=site_cfg.get("csv_col_y", 2),
            csv_col_z=site_cfg.get("csv_col_z", 3),
            csv_col_timestamp=site_cfg.get("csv_col_timestamp", 4),
            input_spatial_ref=site_cfg.get("modular_spatial_ref", site_cfg.get("input_spatial_ref", "")),
            output_spatial_ref=processing_cfg.get("output_spatial_ref", ""),
            aoi_feature_class=processing_cfg.get("aoi_feature_class", ""),
            aoi_where_clause=site_cfg.get("aoi_where_clause", f"MineSite='{site}'"),
            despike=processing_cfg.get("despike", True),
            filter_since=filter_since,
            filter_until=filter_until,
        )
        modular_result = modular_svc.process()
        if source_type == "modular":
            conversion_result = modular_result
        logger.info("Modular conversion: %s", modular_result["status"])

    if not conversion_result or conversion_result.get("status") != "SUCCESS":
        raise RuntimeError(f"File conversion failed for site {site}")

    csv_path = conversion_result["csv_path"]

    # ------------------------------------------------------------------
    # Step 2: FMS Pipeline — raster + boundary
    # output_label=run_date writes to FMS_<YYYYMMDD>/ so all sites share
    # the same daily output folder, mirroring how hourly uses FMS_<ts>/.
    # ------------------------------------------------------------------
    logger.info("--- Step 2: FMS Pipeline (raster + boundary) ---")
    fms_config: dict[str, Any] = {
        "cellSize":            int(processing_cfg.get("grid_size", 1)),
        "snapRaster":          processing_cfg.get("snap_raster", ""),
        "inputSpatialRef":     site_cfg.get("input_spatial_ref", ""),
        "outputSpatialRef":    processing_cfg.get("output_spatial_ref", ""),
        "aoiFeatureClass":     processing_cfg.get("aoi_feature_class", ""),
        "aoiWhereClause":      site_cfg.get("aoi_where_clause", f"MineSite='{site}'"),
        "useAOI":              bool(processing_cfg.get("aoi_feature_class", "")),
        "averagePointSpacing": float(processing_cfg.get("average_point_spacing", 1.0)),
        "tinDelineateValue":   float(processing_cfg.get("tin_delineate_value", 10.0)),
        "profile":             "Elevation_FMS_Minestar_CSV",
    }
    pipeline_result = process_fms_pipeline(
        input_csv=csv_path,
        output_base_folder=output_root,
        site_name=site,
        config=fms_config,
        output_label=run_date,   # → FMS_<YYYYMMDD>/FMS_<YYYYMMDD>_<SITE>.tif
    )
    logger.info("FMS Pipeline: %s", pipeline_result["status"])

    logger.info("=" * 60)
    logger.info("Daily per-site run COMPLETE — site=%s  date=%s", site, run_date)
    logger.info("=" * 60)


def main() -> None:
    args = parse_args()
    setup_logging(args.logging)
    logger = get_logger(__name__)
    logger.info("Environment: %s  Site: %s", args.env, args.site)

    run_date = _resolve_run_date(args.FMS_ForceDate)
    logger.info("Run date (data date): %s", run_date)

    try:
        cfg = ConfigLoader(args.config).load()
        run(cfg, site=args.site, run_date=run_date)
    except Exception:
        logger.error("Daily per-site run failed\n%s", traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
