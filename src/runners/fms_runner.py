"""
FMS Live Surface — main workflow runner.

Entry point for the Jenkins ``MTD - Hourly FMS`` multijob.  Orchestrates
the full pipeline for a single mine site:

  1. Monitoring check (file delivery freshness)
  2. Snippet file conversion  →  MGA50 CSV + JSON config
  3. Modular CSV reprojection (if Modular site)
  4. Raster + boundary generation (arcpy)
  5. Output folder management + metadata.json + ready.flag
  6. Publishing handoff (file-trigger or direct API)

Usage (Jenkins):
    python -m src.runners.fms_runner \\
        --config config/app_config.yaml \\
        --logging config/logging.prod.yaml \\
        --site WB \\
        --env PROD
"""
from __future__ import annotations

import argparse
import sys
from typing import Any

from src.core.config_loader import ConfigLoader, get_config_value
from src.core.logger import get_logger, setup_logging
from src.services.archive_service import ArchiveService
from src.services.modular_csv_service import ModularCsvService
from src.services.monitoring_service import MonitoringService
from src.services.output_handler_service import OutputHandlerService
from src.services.publishing_service import PublishingService
from src.services.raster_generation_service import RasterGenerationService
from src.services.snippet_conversion_service import SnippetConversionService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="FMS Live Surface — Hourly Workflow Runner")
    parser.add_argument("--config", required=True, help="Path to app_config.yaml")
    parser.add_argument("--logging", required=True, help="Path to logging YAML config")
    parser.add_argument("--site", required=True, help="Mine site code (WB, ER, TG, JB, NM)")
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

    site_cfg = get_config_value(cfg, f"sites.{site}", {})
    paths_cfg = get_config_value(cfg, "paths", {})
    processing_cfg = get_config_value(cfg, "processing", {})
    publishing_cfg = get_config_value(cfg, "publishing", {})

    landing_zone = site_cfg.get("landing_zone", paths_cfg.get("landing_zone_root", ""))
    staging_folder = paths_cfg.get("staging_folder", "")
    output_root = paths_cfg.get("output_root", "")
    scratch_gdb = paths_cfg.get("scratch_gdb", "")
    source_type = site_cfg.get("source_type", "minestar")  # "minestar" | "modular" | "both"

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
            output_folder=staging_folder,
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
        logger.info("--- Step 1b: Modular CSV reprojection ---")
        modular_svc = ModularCsvService(
            site=site,
            input_folder=landing_zone,
            output_folder=staging_folder,
            input_spatial_ref=site_cfg.get("modular_spatial_ref", site_cfg.get("input_spatial_ref", "")),
            output_spatial_ref=processing_cfg.get("output_spatial_ref", ""),
        )
        modular_result = modular_svc.process()
        logger.info("Modular CSV result: %s", modular_result["status"])
        # If only Modular source, use its CSV for raster generation
        if source_type == "modular":
            conversion_result = modular_result

    if not conversion_result or conversion_result.get("status") != "SUCCESS":
        raise RuntimeError(f"File conversion failed for site {site}")

    csv_path = conversion_result["csv_path"]
    output_dir_staging = conversion_result["output_dir"]

    # ------------------------------------------------------------------
    # Step 2: Raster + boundary generation (arcpy)
    # ------------------------------------------------------------------
    logger.info("--- Step 2: Raster generation ---")
    raster_svc = RasterGenerationService(
        site=site,
        csv_path=csv_path,
        output_dir=output_dir_staging,
        scratch_gdb=scratch_gdb,
        spatial_ref_prj=processing_cfg.get("output_spatial_ref", ""),
        cell_size=float(processing_cfg.get("grid_size", 2.0)),
        exclusion_fc=processing_cfg.get("exclusion_fc", ""),
    )
    raster_result = raster_svc.generate()
    logger.info("Raster generation result: %s", raster_result["status"])

    # ------------------------------------------------------------------
    # Step 3: Output folder management
    # ------------------------------------------------------------------
    logger.info("--- Step 3: Output handler ---")
    processing_meta: dict[str, Any] = {
        "cell_size": raster_result.get("cell_size"),
        "sourceFiles": conversion_result,
        "processing": processing_cfg,
    }
    output_svc = OutputHandlerService(
        site=site,
        output_root=output_root,
        raster_path=raster_result["raster_path"],
        boundary_path=raster_result["boundary_path"],
        processing_metadata=processing_meta,
        retention_hours=int(get_config_value(cfg, "output.retention_hours", 48)),
    )
    output_result = output_svc.publish_outputs()
    output_svc.cleanup_old_outputs()
    logger.info("Output handler result: %s", output_result["status"])

    # ------------------------------------------------------------------
    # Step 4: Handoff to publishing solution
    # ------------------------------------------------------------------
    logger.info("--- Step 4: Publishing handoff ---")
    pub_svc = PublishingService(
        site=site,
        output_dir=output_result["output_dir"],
        integration_mode=publishing_cfg.get("integration_mode", "file_trigger"),
        publishing_api_module=publishing_cfg.get("api_module", ""),
        api_timeout=int(publishing_cfg.get("api_timeout", 300)),
        poll_interval=int(publishing_cfg.get("poll_interval", 30)),
        poll_timeout=int(publishing_cfg.get("poll_timeout", 0)),  # 0 = fire-and-forget
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
