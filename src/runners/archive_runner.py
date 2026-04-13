"""
FMS Archive Runner — nightly snippet file archival.

Entry point for the Jenkins ``FMS - Archive Snippet Files`` job.
Archives .snp files for all configured mine sites.

Usage (Jenkins):
    python -m src.runners.archive_runner \\
        --config config/app_config.yaml \\
        --logging config/logging.prod.yaml \\
        --env PROD
"""
from __future__ import annotations

import argparse
import sys
from typing import Any

from src.core.config_loader import ConfigLoader, get_config_value
from src.core.logger import get_logger, setup_logging
from src.services.archive_service import ArchiveService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="FMS — Nightly Snippet File Archive Runner")
    parser.add_argument("--config", required=True, help="Path to app_config.yaml")
    parser.add_argument("--logging", required=True, help="Path to logging YAML config")
    parser.add_argument(
        "--env", default="PROD", choices=["DEV", "UAT", "PROD"],
        help="Deployment environment"
    )
    parser.add_argument(
        "--site", default="ALL",
        help="Mine site to archive (default: ALL)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Log what would happen without modifying files",
    )
    return parser.parse_args()


def run(cfg: dict[str, Any], site_filter: str, dry_run: bool) -> None:
    logger = get_logger(__name__)
    logger.info("=" * 60)
    logger.info("FMS Archive Runner — site_filter=%s  dry_run=%s", site_filter, dry_run)
    logger.info("=" * 60)

    sites: dict[str, Any] = get_config_value(cfg, "sites", {})
    archive_root = get_config_value(cfg, "paths.archive_root", "")

    target_sites = (
        {k: v for k, v in sites.items() if k == site_filter}
        if site_filter != "ALL"
        else sites
    )

    if not target_sites:
        logger.warning("No matching sites found for filter: %s", site_filter)
        return

    summary: list[dict[str, Any]] = []
    for site, site_cfg in target_sites.items():
        logger.info("Archiving site: %s", site)
        svc = ArchiveService(
            site=site,
            landing_zone=site_cfg.get("landing_zone", ""),
            archive_root=archive_root,
            dry_run=dry_run,
        )
        result = svc.archive()
        summary.append(result)
        logger.info(
            "  %s → %d files, %.2f MB",
            result["status"],
            result["files_archived"],
            result["bytes_archived"] / 1_048_576,
        )

    logger.info("=" * 60)
    logger.info("Archive complete — %d sites processed", len(summary))
    logger.info("=" * 60)


def main() -> None:
    args = parse_args()
    setup_logging(args.logging)
    logger = get_logger(__name__)
    logger.info("Environment: %s", args.env)

    try:
        cfg = ConfigLoader(args.config).load()
        run(cfg, site_filter=args.site, dry_run=args.dry_run)
    except Exception:
        logger.error("Archive runner failed", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
