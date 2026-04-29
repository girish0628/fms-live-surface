"""
Daily Cleanup Runner.

Jenkins trigger: daily (e.g. 01:00 AM), after daily_merge_runner completes.

Workflow:
  1. DailyCleanupService  — query mosaic dataset for SITE='Hourly' surveys
  2. FmeWebhookClient     — DELETE those surveys from the mosaic dataset

This prevents the mosaic dataset from accumulating 24+ hourly surveys per
day; only the merged daily TIFF survives in the mosaic.

Usage (Jenkins):
    python -m src.runners.daily_cleanup_runner \\
        --config config/app_config.yaml \\
        --logging config/logging.prod.yaml \\
        --env PROD
"""
from __future__ import annotations

import argparse
import sys
import traceback
from typing import Any

from src.core.config_loader import ConfigLoader, get_config_value
from src.core.logger import get_logger, setup_logging
from src.services.daily_cleanup_service import DailyCleanupService
from src.services.fme_webhook_client import DeleteParams, fme_client_from_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="FMS Daily Cleanup — remove hourly surveys from mosaic via FME DELETE"
    )
    parser.add_argument("--config",  required=True, help="Path to app_config.yaml")
    parser.add_argument("--logging", required=True, help="Path to logging YAML config")
    parser.add_argument(
        "--env", default="PROD", choices=["NPE", "PROD", "DEV"],
        help="Deployment environment (already set in Jenkins env var; used for logging)",
    )
    return parser.parse_args()


def run(cfg: dict[str, Any]) -> None:
    logger = get_logger(__name__)
    logger.info("=" * 60)
    logger.info("Daily Cleanup Runner")
    logger.info("=" * 60)

    mosaic_cfg = get_config_value(cfg, "mosaic", {})
    fme_cfg    = get_config_value(cfg, "fme", {})

    mosaic_path       = mosaic_cfg.get("dataset_path", "")
    survey_name_field = mosaic_cfg.get("survey_name_field", "Name")
    site_field        = mosaic_cfg.get("site_field", "SITE")
    site_value        = mosaic_cfg.get("hourly_site_value", "Hourly")
    user_email        = fme_cfg.get("user_email", "")

    if not mosaic_path:
        raise ValueError("mosaic.dataset_path must be set in app_config.yaml")

    # ----------------------------------------------------------------
    # Step 1: Query mosaic dataset for hourly surveys
    # ----------------------------------------------------------------
    logger.info("--- Step 1: Query mosaic dataset for SITE='%s' ---", site_value)
    cleanup_svc = DailyCleanupService(
        mosaic_dataset_path=mosaic_path,
        survey_name_field=survey_name_field,
        site_field=site_field,
        site_value=site_value,
    )
    cleanup_result = cleanup_svc.run()

    if cleanup_result["status"] == "NO_SURVEYS":
        logger.info("No hourly surveys found in mosaic dataset — nothing to delete.")
        return

    surveys = cleanup_result["surveys"]
    logger.info("Surveys to delete (%d):", len(surveys))
    for s in surveys:
        logger.info("  %s", s["survey_name"])

    # ----------------------------------------------------------------
    # Step 2: FME DELETE
    # ----------------------------------------------------------------
    logger.info("--- Step 2: FME DELETE ---")
    fme_client = fme_client_from_config(cfg)

    delete_params = DeleteParams(
        surveys=surveys,
        user_email=user_email,
        action="DELETE",
        type="Terrain",
        delete_permanently="FALSE",
        delete_cache="TRUE",
        comments="FMS scheduled deletion of hourly surveys",
    )
    delete_result = fme_client.delete(delete_params)
    logger.info(
        "FME DELETE result: %s — %d surveys removed",
        delete_result["status"], len(surveys),
    )

    logger.info("=" * 60)
    logger.info("Daily Cleanup Runner COMPLETE")
    logger.info("=" * 60)


def main() -> None:
    args = parse_args()
    setup_logging(args.logging)
    logger = get_logger(__name__)
    logger.info("Environment: %s", args.env)

    try:
        cfg = ConfigLoader(args.config).load()
        run(cfg)
    except Exception:
        logger.error("Daily cleanup failed\n%s", traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
