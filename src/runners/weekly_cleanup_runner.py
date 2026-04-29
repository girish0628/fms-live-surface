"""
Weekly Cleanup Runner.

Jenkins trigger: weekly (e.g. Sunday 02:00 AM).

Workflow:
  1. WeeklyCleanupService — archive old FMS output folders to Azure Blob
                            Storage, then delete them from the local file share
  2. Purge old staging intermediate files

Usage (Jenkins):
    python -m src.runners.weekly_cleanup_runner \\
        --config config/app_config.yaml \\
        --logging config/logging.prod.yaml \\
        --env PROD

    # Dry run (log only, no changes):
    python -m src.runners.weekly_cleanup_runner \\
        --config config/app_config.yaml \\
        --logging config/logging.prod.yaml \\
        --dry-run
"""
from __future__ import annotations

import argparse
import sys
import traceback
from typing import Any

from src.core.config_loader import ConfigLoader, get_config_value
from src.core.logger import get_logger, setup_logging
from src.services.weekly_cleanup_service import WeeklyCleanupService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="FMS Weekly Cleanup — archive to blob + purge local file share"
    )
    parser.add_argument("--config",  required=True, help="Path to app_config.yaml")
    parser.add_argument("--logging", required=True, help="Path to logging YAML config")
    parser.add_argument(
        "--env", default="PROD", choices=["NPE", "PROD", "DEV"],
        help="Deployment environment (already set in Jenkins env var; used for logging)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Log actions without uploading or deleting anything",
    )
    return parser.parse_args()


def run(cfg: dict[str, Any], dry_run: bool = False) -> None:
    logger = get_logger(__name__)
    logger.info("=" * 60)
    logger.info("Weekly Cleanup Runner  dry_run=%s", dry_run)
    logger.info("=" * 60)

    paths_cfg   = get_config_value(cfg, "paths", {})
    blob_cfg    = get_config_value(cfg, "blob_storage", {})
    weekly_cfg  = get_config_value(cfg, "weekly", {})

    output_root    = paths_cfg.get("output_root", "")
    staging_folder = paths_cfg.get("staging_folder", "")

    svc = WeeklyCleanupService(
        output_root=output_root,
        staging_folder=staging_folder,
        blob_connection_string_env_var=blob_cfg.get(
            "connection_string_env_var", "AZURE_STORAGE_CONNECTION_STRING"
        ),
        blob_container_name=blob_cfg.get("container_name", "fms-archive"),
        blob_prefix=blob_cfg.get("prefix", "fms-live-surface/"),
        retention_days=int(weekly_cfg.get("output_retention_days", 7)),
        staging_retention_days=int(weekly_cfg.get("staging_retention_days", 2)),
        dry_run=dry_run,
    )
    result = svc.cleanup()

    if result["status"] == "PARTIAL":
        logger.warning(
            "Weekly cleanup completed with %d error(s):", result["error_count"]
        )
        for err in result["errors"]:
            logger.warning("  %s", err)
        sys.exit(1)  # Signal Jenkins failure so ops team is notified

    logger.info("=" * 60)
    logger.info(
        "Weekly Cleanup COMPLETE — archived=%d  deleted=%d  staging_purged=%d",
        result["archived_count"], result["deleted_count"], result["staging_purged_count"],
    )
    logger.info("=" * 60)


def main() -> None:
    args = parse_args()
    setup_logging(args.logging)
    logger = get_logger(__name__)
    logger.info("Environment: %s", args.env)

    try:
        cfg = ConfigLoader(args.config).load()
        run(cfg, dry_run=args.dry_run)
    except Exception:
        logger.error("Weekly cleanup failed\n%s", traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
