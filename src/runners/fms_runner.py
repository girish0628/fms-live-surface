"""
FMS Live Surface — main workflow runner.

Entry point for the Jenkins ``MTD - Hourly FMS`` multijob.  Orchestrates
the full pipeline for a single mine site:

  1. Monitoring check (file delivery freshness)
  2. Snippet file conversion  →  MGA50 CSV + JSON config
  3. Modular CSV reprojection (if Modular site)
  4. Raster + boundary generation (arcpy)
  5. Publishing handoff (file-trigger or direct API — fme_webhook is deferred
     to fms_finalize_runner which runs after all parallel sites complete)

Usage (Jenkins):
    python -m src.runners.fms_runner \\
        --config config/app_config.yaml \\
        --logging config/logging.prod.yaml \\
        --site WB \\
        --env PROD
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from src.core.config_loader import ConfigLoader, get_config_value
from src.core.logger import get_logger, setup_logging
from src.services.fms_pipeline_service import process_fms_pipeline
from src.services.modular_csv_service import ModularCsvService
from src.services.monitoring_service import MonitoringService
from src.services.publishing_service import PublishingService
from src.services.snippet_conversion_service import SnippetConversionService
from src.utils.naming_utils import to_hourly_ts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="FMS Live Surface — Hourly Workflow Runner")
    parser.add_argument("--config", required=True, help="Path to app_config.yaml")
    parser.add_argument("--logging", required=True, help="Path to logging YAML config")
    parser.add_argument("--site", required=True, help="Mine site code (WB, ER, SF, YND, JB, NWW, MAC)")
    parser.add_argument(
        "--env", default="PROD", choices=["DEV", "UAT", "PROD"],
        help="Deployment environment"
    )
    parser.add_argument(
        "--skip-monitoring", action="store_true",
        help="Skip monitoring check (useful in DEV)"
    )
    return parser.parse_args()


def run(cfg: dict[str, Any], site: str, skip_monitoring: bool = False) -> None:
    logger = get_logger(__name__)
    logger.info("=" * 60)
    logger.info("FMS Live Surface Workflow — site: %s", site)
    logger.info("=" * 60)

    site_cfg       = get_config_value(cfg, f"sites.{site}", {})
    paths_cfg      = get_config_value(cfg, "paths", {})
    processing_cfg = get_config_value(cfg, "processing", {})
    publishing_cfg = get_config_value(cfg, "publishing", {})

    landing_zone  = site_cfg.get("landing_zone", paths_cfg.get("landing_zone_root", ""))
    staging_folder = paths_cfg.get("staging_folder", "")
    output_root   = paths_cfg.get("output_root", "")
    source_type   = site_cfg.get("source_type", "minestar")

    # Normalise to YYYYMMDDHH0000 so all parallel site stages share one folder.
    raw_timestamp = os.environ.get("FMS_RUN_TIMESTAMP") or datetime.now().strftime("%Y%m%d%H%M%S")
    run_timestamp = to_hourly_ts(raw_timestamp)
    logger.info("Run timestamp (normalised): %s", run_timestamp)

    staging_run_folder = str(Path(staging_folder) / f"FMS_{run_timestamp}")

    # ------------------------------------------------------------------
    # Step 0: Monitoring check
    # ------------------------------------------------------------------
    if not skip_monitoring:
        monitoring = MonitoringService(
            site=site,
            landing_zone=landing_zone,
            threshold_minutes=processing_cfg.get("monitoring_threshold_minutes", 10),
            alert_email=get_config_value(cfg, "monitoring.alert_email", ""),
            smtp_host=get_config_value(cfg, "monitoring.smtp_host", ""),
            failover_share=site_cfg.get("failover_share", ""),
        )
        mon_result = monitoring.check()
        if mon_result["status"] == "ALERT":
            logger.warning("Monitoring alert — continuing with available files")

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
        )
        conversion_result = snippet_svc.convert()
        logger.info("Snippet conversion result: %s", conversion_result["status"])

    # ------------------------------------------------------------------
    # Step 1b: Modular CSV reprojection
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
        )
        modular_result = modular_svc.process()
        logger.info("Modular CSV result: %s", modular_result["status"])
        if source_type == "modular":
            conversion_result = modular_result

    if not conversion_result or conversion_result.get("status") != "SUCCESS":
        raise RuntimeError(f"File conversion failed for site {site}")

    csv_path = conversion_result["csv_path"]

    # ------------------------------------------------------------------
    # Step 2: FMS Pipeline — raster + boundary + Source folder
    # ------------------------------------------------------------------
    logger.info("--- Step 2: FMS Pipeline (raster + boundary) ---")
    fms_config: dict[str, Any] = {
        "cellSize": int(processing_cfg.get("grid_size", 1)),
        "snapRaster": processing_cfg.get("snap_raster", ""),
        "inputSpatialRef": site_cfg.get("input_spatial_ref", ""),
        "outputSpatialRef": processing_cfg.get("output_spatial_ref", ""),
        "aoiFeatureClass": processing_cfg.get("aoi_feature_class", ""),
        "aoiWhereClause": site_cfg.get("aoi_where_clause", f"MineSite='{site}'"),
        "useAOI": bool(processing_cfg.get("aoi_feature_class", "")),
        "averagePointSpacing": float(processing_cfg.get("average_point_spacing", 1.0)),
        "tinDelineateValue": float(processing_cfg.get("tin_delineate_value", 10.0)),
        "profile": "Elevation_FMS_Minestar_CSV",
    }
    pipeline_result = process_fms_pipeline(
        input_csv=csv_path,
        output_base_folder=output_root,
        site_name=site,
        config=fms_config,
        run_timestamp=run_timestamp,
    )
    logger.info("FMS Pipeline result: %s", pipeline_result["status"])

    # ------------------------------------------------------------------
    # Step 3: Publishing handoff
    # fme_webhook mode: deferred to fms_finalize_runner after all sites complete.
    # file_trigger / direct_api modes: trigger per-site immediately.
    # ------------------------------------------------------------------
    integration_mode = publishing_cfg.get("integration_mode", "fme_webhook")
    if integration_mode == "fme_webhook":
        logger.info(
            "--- Step 3: Publishing deferred to fms_finalize_runner (fme_webhook mode) ---"
        )
    else:
        logger.info("--- Step 3: Publishing handoff (%s) ---", integration_mode)
        pub_svc = PublishingService(
            site=site,
            output_dir=pipeline_result["output_folder"],
            integration_mode=integration_mode,
            publishing_api_module=publishing_cfg.get("api_module", ""),
            api_timeout=int(publishing_cfg.get("api_timeout", 300)),
            poll_interval=int(publishing_cfg.get("poll_interval", 30)),
            poll_timeout=int(publishing_cfg.get("poll_timeout", 0)),
        )
        pub_result = pub_svc.trigger()
        logger.info("Publishing result: %s", pub_result["status"])

    logger.info("=" * 60)
    logger.info("FMS Live Surface Workflow COMPLETE — site: %s", site)
    logger.info("=" * 60)


def main() -> None:
    args = parse_args()

    setup_logging(args.logging)
    logger = get_logger(__name__)
    logger.info("Environment: %s  Site: %s", args.env, args.site)

    try:
        cfg = ConfigLoader(args.config).load()
        run(cfg, args.site, skip_monitoring=args.skip_monitoring)
    except Exception:
        logger.error("Workflow failed", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
