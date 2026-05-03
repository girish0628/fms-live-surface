"""
FMS Finalize Runner.

Run this Jenkins stage AFTER all parallel per-site fms_runner stages complete.
Performs three actions on the shared FMS_<YYYYMMDDHH0000> output folder:

  1. Merge all per-site boundary shapefiles from the Source/ sub-folder into a
     single FMS_<ts>_boundary.shp covering all mine sites.

  2. Call the FME INGEST webhook (TYPE=Terrain, SITE=Hourly).

  3. Delete the staging run folder (intermediate CSVs no longer needed).

  4. Write an idempotency flag so Jenkins re-runs do not double-ingest.

Usage (Jenkins, after parallel site stages):
    python -m src.runners.fms_finalize_runner \\
        --config config/app_config.yaml \\
        --logging config/logging.prod.yaml \\
        --run-timestamp %FMS_RUN_TIMESTAMP% \\
        --env PROD
"""
from __future__ import annotations

import argparse
import os
import shutil
import sys
import traceback
from pathlib import Path
from typing import Any

from src.core.config_loader import ConfigLoader, get_config_value
from src.core.logger import get_logger, setup_logging
from src.services.fme_webhook_client import FmeWebhookClient, IngestParams, fme_client_from_config
from src.utils.naming_utils import hourly_survey_name, to_hourly_ts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="FMS Finalize — merge boundaries + FME INGEST webhook"
    )
    parser.add_argument("--config",  required=True, help="Path to app_config.yaml")
    parser.add_argument("--logging", required=True, help="Path to logging YAML config")
    parser.add_argument(
        "--run-timestamp", default=None,
        help="Timestamp of the FMS run (reads FMS_RUN_TIMESTAMP env var if omitted)",
    )
    parser.add_argument(
        "--env", default="PROD", choices=["NPE", "PROD", "DEV"],
        help="Deployment environment",
    )
    return parser.parse_args()


def _merge_boundaries(output_folder: Path, hourly_ts: str) -> str:
    """
    Merge all per-site FMS_<ts>_boundary_<SITE>.shp files from Source/ into a
    single dissolved FMS_<ts>_boundary.shp in the output folder root.

    Returns the path to the merged/dissolved shapefile.
    """
    import arcpy

    logger = get_logger(__name__)
    source_folder = output_folder / "Source"
    pattern = f"FMS_{hourly_ts}_boundary_*.shp"
    per_site_shps = sorted(source_folder.glob(pattern))

    if not per_site_shps:
        raise RuntimeError(
            f"No per-site boundary files matching '{pattern}' in {source_folder}. "
            "Ensure all site fms_runner stages completed successfully."
        )

    arcpy.env.overwriteOutput = True

    merged_tmp = str(output_folder / f"FMS_{hourly_ts}_boundary_merged_tmp.shp")
    logger.info("Merging %d per-site boundaries → %s", len(per_site_shps), merged_tmp)
    arcpy.Merge_management([str(p) for p in per_site_shps], merged_tmp)

    dissolved_shp = str(output_folder / f"FMS_{hourly_ts}_boundary.shp")
    logger.info("Dissolving → %s", dissolved_shp)
    arcpy.Dissolve_management(merged_tmp, dissolved_shp)

    for ext in (".shp", ".dbf", ".shx", ".prj", ".cpg"):
        p = Path(merged_tmp.replace(".shp", ext))
        if p.exists():
            p.unlink()

    logger.info("Dissolved boundary written: %s", dissolved_shp)
    return dissolved_shp


def finalize(cfg: dict[str, Any], run_timestamp: str) -> None:
    logger = get_logger(__name__)

    paths_cfg      = get_config_value(cfg, "paths", {})
    processing_cfg = get_config_value(cfg, "processing", {})
    fme_cfg        = get_config_value(cfg, "fme", {})

    output_root    = paths_cfg.get("output_root", "")
    staging_folder = paths_cfg.get("staging_folder", "")

    if not output_root:
        raise ValueError("paths.output_root must be set in app_config.yaml")

    hourly_ts = to_hourly_ts(run_timestamp)

    logger.info("=" * 60)
    logger.info("FMS Finalize — hourly_ts=%s", hourly_ts)
    logger.info("=" * 60)

    output_folder = Path(output_root) / f"FMS_{hourly_ts}"
    if not output_folder.exists():
        raise FileNotFoundError(
            f"Output folder not found: {output_folder}. "
            "Ensure at least one site fms_runner stage completed successfully."
        )

    survey_name = hourly_survey_name(hourly_ts)
    ingest_flag = output_folder / f"{survey_name}.ingested.flag"
    if ingest_flag.exists():
        logger.warning(
            "Survey '%s' already ingested (flag found). Skipping.", survey_name
        )
        return

    # ----------------------------------------------------------------
    # Step 1: Merge + dissolve per-site boundary shapefiles from Source/
    # ----------------------------------------------------------------
    logger.info("--- Step 1: Merge + dissolve boundaries ---")
    merged_boundary = _merge_boundaries(output_folder, hourly_ts)

    # ----------------------------------------------------------------
    # Step 2: Write ready.flag
    # ----------------------------------------------------------------
    flag_path = output_folder / "ready.flag"
    flag_path.write_text(
        f"ready\ntimestamp={hourly_ts}\nboundary={merged_boundary}\n",
        encoding="utf-8",
    )
    logger.info("ready.flag written: %s", flag_path)

    # ----------------------------------------------------------------
    # Step 3: FME INGEST webhook
    # ----------------------------------------------------------------
    logger.info("--- Step 3: FME INGEST webhook (SITE=Hourly) ---")

    coord_sys_wkt = processing_cfg.get("coordinate_system_wkt", "")
    cell_size     = str(int(processing_cfg.get("grid_size", 2)))
    user_email    = fme_cfg.get("user_email", "")
    acq_date      = hourly_ts  # YYYYMMDDHH0000

    fme_client = fme_client_from_config(cfg)
    ingest_params = IngestParams(
        tiff_path=str(output_folder),
        survey_name=survey_name,
        acquisition_date=acq_date,
        project_extent=merged_boundary,
        site="Hourly",
        coordinate_system=coord_sys_wkt,
        resolution=cell_size,
        user_email=user_email,
    )
    ingest_result = fme_client.ingest(ingest_params)
    logger.info("FME INGEST result: %s", ingest_result["status"])

    ingest_flag.write_text(
        f"ingested\nsurvey={survey_name}\ntimestamp={hourly_ts}\n",
        encoding="utf-8",
    )
    logger.info("Ingest flag written: %s", ingest_flag)

    # ----------------------------------------------------------------
    # Step 4: Delete staging run folder (intermediate CSVs no longer needed)
    # ----------------------------------------------------------------
    if staging_folder:
        staging_run = Path(staging_folder) / f"FMS_{hourly_ts}"
        if staging_run.exists():
            shutil.rmtree(staging_run)
            logger.info("Staging folder deleted: %s", staging_run)

    logger.info("=" * 60)
    logger.info("FMS Finalize COMPLETE — hourly_ts=%s  survey=%s", hourly_ts, survey_name)
    logger.info("=" * 60)


def main() -> None:
    args = parse_args()
    setup_logging(args.logging)
    logger = get_logger(__name__)

    run_timestamp = args.run_timestamp or os.environ.get("FMS_RUN_TIMESTAMP", "")
    if not run_timestamp:
        logger.error(
            "run_timestamp is required. Pass --run-timestamp or set FMS_RUN_TIMESTAMP env var."
        )
        sys.exit(1)

    logger.info("Environment: %s  run_timestamp: %s", args.env, run_timestamp)

    try:
        cfg = ConfigLoader(args.config).load()
        finalize(cfg, run_timestamp)
    except Exception:
        logger.error("Finalize failed\n%s", traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
