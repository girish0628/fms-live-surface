"""
Daily Merge Runner.

Jenkins trigger: daily (e.g. 00:30 AM), after all hourly jobs have run.

Workflow:
  1. DailyMergeService   — mosaic today's hourly TIFFs → one daily TIFF
  2. FmeWebhookClient    — INGEST the daily TIFF (SITE='Daily')

Idempotency:
  A ``<survey_name>.ingested.flag`` file is written to daily_output_root
  after a successful FME INGEST call.  Re-running the job on the same day
  detects the flag and skips the webhook, preventing duplicate ingest.

Usage (Jenkins):
    python -m src.runners.daily_merge_runner \\
        --config config/app_config.yaml \\
        --logging config/logging.prod.yaml \\
        --env PROD
"""
from __future__ import annotations

import argparse
import sys
import traceback
from pathlib import Path
from typing import Any

from src.core.config_loader import ConfigLoader, get_config_value
from src.core.logger import get_logger, setup_logging
from src.services.daily_merge_service import DailyMergeService
from src.services.fme_webhook_client import FmeWebhookClient, IngestParams, fme_client_from_config
from src.utils.naming_utils import current_date_str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="FMS Daily Merge — mosaic hourly TIFFs + FME INGEST")
    parser.add_argument("--config",  required=True, help="Path to app_config.yaml")
    parser.add_argument("--logging", required=True, help="Path to logging YAML config")
    parser.add_argument(
        "--date", default="",
        help="Date to process (YYYYMMDD). Defaults to today.",
    )
    parser.add_argument(
        "--env", default="PROD", choices=["NPE", "PROD", "DEV"],
        help="Deployment environment (already set in Jenkins env var; used for logging)",
    )
    return parser.parse_args()


def run(cfg: dict[str, Any], run_date: str = "") -> None:
    logger = get_logger(__name__)
    date = run_date or current_date_str()

    paths_cfg    = get_config_value(cfg, "paths", {})
    processing   = get_config_value(cfg, "processing", {})
    fme_cfg      = get_config_value(cfg, "fme", {})

    output_root       = paths_cfg.get("output_root", "")
    daily_output_root = paths_cfg.get("daily_output_root", output_root)
    coord_sys_wkt     = processing.get("coordinate_system_wkt", "")
    cell_size         = int(processing.get("grid_size", 2))
    user_email        = fme_cfg.get("user_email", "")

    logger.info("=" * 60)
    logger.info("Daily Merge Runner — date=%s", date)
    logger.info("=" * 60)

    # ----------------------------------------------------------------
    # Step 1: Mosaic hourly TIFFs → daily TIFF
    # ----------------------------------------------------------------
    logger.info("--- Step 1: Mosaic hourly TIFFs ---")
    merge_svc = DailyMergeService(
        output_root=output_root,
        daily_output_root=daily_output_root,
        run_date=date,
        coordinate_system_wkt=coord_sys_wkt,
        cell_size=cell_size,
    )
    merge_result = merge_svc.merge()
    survey_name   = merge_result["survey_name"]
    daily_folder  = Path(merge_result["daily_folder"])

    # ----------------------------------------------------------------
    # Idempotency guard — skip if this survey was already ingested
    # ----------------------------------------------------------------
    ingest_flag = daily_folder / f"{survey_name}.ingested.flag"
    if ingest_flag.exists():
        logger.warning(
            "Survey '%s' already ingested (flag found: %s). Skipping FME call.",
            survey_name, ingest_flag,
        )
        return

    # ----------------------------------------------------------------
    # Step 2: FME INGEST — daily TIFF
    # ----------------------------------------------------------------
    logger.info("--- Step 2: FME INGEST (SITE=Daily) ---")
    fme_client = fme_client_from_config(cfg)

    ingest_params = IngestParams(
        tiff_path=str(daily_folder),
        survey_name=survey_name,
        acquisition_date=merge_result["acquisition_date"],
        project_extent="",                       # No boundary for daily merged TIFF
        site="Daily",
        coordinate_system=coord_sys_wkt,
        resolution=str(cell_size),
        user_email=user_email,
    )
    fme_result = fme_client.ingest(ingest_params)
    logger.info("FME INGEST result: %s", fme_result["status"])

    # Write idempotency flag
    ingest_flag.write_text(
        f"ingested\nsurvey={survey_name}\ndate={date}\n",
        encoding="utf-8",
    )
    logger.info("Ingest flag written: %s", ingest_flag)

    logger.info("=" * 60)
    logger.info("Daily Merge Runner COMPLETE — survey=%s", survey_name)
    logger.info("=" * 60)


def main() -> None:
    args = parse_args()
    setup_logging(args.logging)
    logger = get_logger(__name__)
    logger.info("Environment: %s", args.env)

    try:
        cfg = ConfigLoader(args.config).load()
        run(cfg, run_date=args.date)
    except Exception:
        logger.error("Daily merge failed\n%s", traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
