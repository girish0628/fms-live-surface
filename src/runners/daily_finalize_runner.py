"""
Daily Finalize Runner.

Runs once after all parallel daily_merge_runner site stages complete
(same pattern as fms_finalize_runner.py for hourly).

Workflow:
  1. Merge all per-site FMS_<date>_boundary_<SITE>.shp from Source/ →
     single dissolved FMS_<date>_boundary.shp
  2. FME INGEST (SITE=Daily) with idempotency flag guard
  3. FME DELETE — SURVEYS = all FMS_<date>HH0000 folder names for run_date
  4. Delete local FMS_<date>HH0000/ hourly output folders
  5. Delete daily staging folder (FMS_<date> under staging root)

Usage (Jenkins, after all parallel daily site stages):
    python -m src.runners.daily_finalize_runner \\
        --config config/app_config.yaml \\
        --logging config/logging.prod.yaml \\
        --run-date YYYYMMDD \\
        --env PROD
"""
from __future__ import annotations

import argparse
import shutil
import sys
import traceback
from pathlib import Path
from typing import Any

from src.core.config_loader import ConfigLoader, get_config_value
from src.core.logger import get_logger, setup_logging
from src.services.fme_webhook_client import DeleteParams, IngestParams, fme_client_from_config
from src.utils.file_utils import read_prj
from src.utils.naming_utils import daily_folder_name, daily_survey_name


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="FMS Daily Finalize — merge boundaries + FME INGEST + FME DELETE + cleanup"
    )
    parser.add_argument("--config",   required=True, help="Path to app_config.yaml")
    parser.add_argument("--logging",  required=True, help="Path to logging YAML config")
    parser.add_argument(
        "--run-date", required=True,
        help="Data date (YYYYMMDD). Passed from Jenkins via FMS_RUN_TIMESTAMP.",
    )
    parser.add_argument(
        "--env", default="PROD", choices=["NPE", "PROD", "DEV"],
        help="Deployment environment",
    )
    return parser.parse_args()


def _merge_site_boundaries(output_folder: Path, date: str) -> str:
    """
    Merge all per-site FMS_<date>_boundary_<SITE>.shp from Source/ into a
    single dissolved FMS_<date>_boundary.shp at the daily folder root.
    Returns the merged SHP path, or "" when no per-site files are found.
    """
    import arcpy

    logger = get_logger(__name__)
    source_folder = output_folder / "Source"
    per_site_shps = sorted(source_folder.glob(f"FMS_{date}_boundary_*.shp"))

    if not per_site_shps:
        logger.warning("No per-site boundary SHPs in %s — boundary merge skipped", source_folder)
        return ""

    arcpy.env.overwriteOutput = True

    merged_tmp    = str(output_folder / f"FMS_{date}_boundary_merged_tmp.shp")
    dissolved_shp = str(output_folder / f"FMS_{date}_boundary.shp")

    logger.info("Merging %d per-site boundaries → %s", len(per_site_shps), merged_tmp)
    arcpy.Merge_management([str(p) for p in per_site_shps], merged_tmp)

    logger.info("Dissolving → %s", dissolved_shp)
    arcpy.Dissolve_management(merged_tmp, dissolved_shp)

    for ext in (".shp", ".dbf", ".shx", ".prj", ".cpg"):
        tmp = Path(merged_tmp.replace(".shp", ext))
        if tmp.exists():
            tmp.unlink()

    logger.info("Dissolved boundary written: %s", dissolved_shp)
    return dissolved_shp


def _delete_hourly_folders(output_root: str, date: str) -> int:
    """Delete all FMS_<date>HH0000/ hourly output folders for the given date."""
    logger = get_logger(__name__)
    root = Path(output_root)
    deleted = 0
    for folder in sorted(root.glob(f"FMS_{date}??????")):
        if folder.is_dir():
            shutil.rmtree(folder)
            logger.info("Deleted hourly folder: %s", folder.name)
            deleted += 1
    return deleted


def finalize(cfg: dict[str, Any], run_date: str) -> None:
    logger = get_logger(__name__)

    paths_cfg  = get_config_value(cfg, "paths", {})
    processing = get_config_value(cfg, "processing", {})
    fme_cfg    = get_config_value(cfg, "fme", {})

    output_root    = paths_cfg.get("output_root", "")
    staging_folder = paths_cfg.get("staging_folder", "")
    prj_path       = processing.get("output_spatial_ref", "")
    coord_sys_wkt  = read_prj(prj_path) if prj_path else ""
    cell_size      = int(processing.get("grid_size", 2))
    user_email     = fme_cfg.get("user_email", "")

    if not output_root:
        raise ValueError("paths.output_root must be set in app_config.yaml")

    survey_name  = daily_survey_name(run_date)          # FMS_YYYYMMDD
    daily_folder = Path(output_root) / daily_folder_name(run_date)

    if not daily_folder.exists():
        raise FileNotFoundError(
            f"Daily output folder not found: {daily_folder}. "
            "Ensure at least one daily site stage completed successfully."
        )

    logger.info("=" * 60)
    logger.info("Daily Finalize — date=%s  survey=%s", run_date, survey_name)
    logger.info("=" * 60)

    # ----------------------------------------------------------------
    # Step 1: Merge per-site boundaries → dissolved daily boundary
    # ----------------------------------------------------------------
    logger.info("--- Step 1: Merge boundaries ---")
    daily_boundary = _merge_site_boundaries(daily_folder, run_date)

    # ----------------------------------------------------------------
    # Idempotency guard
    # ----------------------------------------------------------------
    ingest_flag = daily_folder / f"{survey_name}.ingested.flag"
    if ingest_flag.exists():
        logger.warning(
            "Survey '%s' already ingested (flag: %s). Skipping FME calls.",
            survey_name, ingest_flag,
        )
        return

    # ----------------------------------------------------------------
    # Step 2: FME INGEST — daily output (SITE=Daily)
    # ----------------------------------------------------------------
    logger.info("--- Step 2: FME INGEST (SITE=Daily) ---")
    fme_client = fme_client_from_config(cfg)

    ingest_params = IngestParams(
        tiff_path=str(daily_folder),
        survey_name=survey_name,
        acquisition_date=f"{run_date}000000",
        project_extent=daily_boundary,
        site="Daily",
        coordinate_system=coord_sys_wkt,
        resolution=str(cell_size),
        user_email=user_email,
    )
    fme_result = fme_client.ingest(ingest_params)
    logger.info("FME INGEST result: %s", fme_result["status"])

    ingest_flag.write_text(
        f"ingested\nsurvey={survey_name}\ndate={run_date}\n",
        encoding="utf-8",
    )
    logger.info("Ingest flag written: %s", ingest_flag)

    # ----------------------------------------------------------------
    # Step 3: FME DELETE — remove hourly surveys for this date
    # Survey names are the hourly output folder names (FMS_<date>HH0000),
    # collected before Step 4 removes them from disk.
    # ----------------------------------------------------------------
    logger.info("--- Step 3: FME DELETE (hourly surveys for date=%s) ---", run_date)
    hourly_folders = sorted(
        f for f in Path(output_root).glob(f"FMS_{run_date}??????") if f.is_dir()
    )
    if hourly_folders:
        surveys = [
            {"survey_name": folder.name, "capture_method": "FMS"}
            for folder in hourly_folders
        ]
        logger.info(
            "Surveys to delete (%d): %s",
            len(surveys), [s["survey_name"] for s in surveys],
        )
        delete_params = DeleteParams(
            surveys=surveys,
            user_email=user_email,
            action="DELETE",
            type="Terrain",
            delete_permanently="FALSE",
            delete_cache="TRUE",
            comments="FMS scheduled deletion",
        )
        delete_result = fme_client.delete(delete_params)
        logger.info(
            "FME DELETE result: %s — %d surveys removed",
            delete_result["status"], len(surveys),
        )
    else:
        logger.warning("No hourly folders found for date=%s — FME DELETE skipped", run_date)

    # ----------------------------------------------------------------
    # Step 4: Delete hourly output folders for this date
    # ----------------------------------------------------------------
    logger.info("--- Step 4: Delete hourly folders for date=%s ---", run_date)
    deleted = _delete_hourly_folders(output_root, run_date)
    logger.info("Deleted %d hourly folder(s)", deleted)

    # ----------------------------------------------------------------
    # Step 5: Delete daily staging folder
    # ----------------------------------------------------------------
    if staging_folder:
        staging_run = Path(staging_folder) / f"FMS_{run_date}"
        if staging_run.exists():
            shutil.rmtree(staging_run)
            logger.info("Daily staging folder deleted: %s", staging_run)

    logger.info("=" * 60)
    logger.info("Daily Finalize COMPLETE — date=%s  survey=%s", run_date, survey_name)
    logger.info("=" * 60)


def main() -> None:
    args = parse_args()
    setup_logging(args.logging)
    logger = get_logger(__name__)
    logger.info("Environment: %s  run_date: %s", args.env, args.run_date)

    try:
        cfg = ConfigLoader(args.config).load()
        finalize(cfg, args.run_date)
    except Exception:
        logger.error("Daily finalize failed\n%s", traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
