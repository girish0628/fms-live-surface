"""
FMS Archive Runner — nightly input file archival.

Entry point for the Jenkins ``FMS - Archive Snippet Files`` job.
Archives .snp (Minestar) and/or .csv (Modular) input files for all
configured mine sites based on each site's source_type.

ZIP performance options (compression, chunking, etc.) are read from the
``archive`` section of app_config.yaml — not from CLI arguments.

Usage (Jenkins):
    python -m src.runners.archive_runner \\
        --config config/app_config.yaml \\
        --logging config/logging.prod.yaml \\
        --env PROD \\
        --destination blob \\
        [--FMS_ForceDate YYYYMMDD] \\
        [--dry-run]
"""
from __future__ import annotations

import argparse
import sys
from typing import Any

from src.core.config_loader import ConfigLoader, get_config_value
from src.core.logger import get_logger, setup_logging
from src.services.archive_service import ArchiveService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="FMS — Nightly Input File Archive Runner")
    parser.add_argument("--config",  required=True, help="Path to app_config.yaml")
    parser.add_argument("--logging", required=True, help="Path to logging YAML config")
    parser.add_argument(
        "--env", default="PROD", choices=["DEV", "UAT", "PROD"],
        help="Deployment environment",
    )
    parser.add_argument(
        "--site", default="ALL",
        help="Mine site to archive (default: ALL)",
    )
    parser.add_argument(
        "--destination",
        default=None,
        choices=["network", "blob", "both"],
        help=(
            "Archive destination: network, blob, or both. "
            "Overrides archive.destination in app_config.yaml."
        ),
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Log what would happen without modifying files",
    )
    parser.add_argument(
        "--FMS_ForceDate",
        default=None,
        metavar="YYYYMMDD",
        help="Override date for file matching, archive folder and filename (default: today)",
    )
    return parser.parse_args()


def run(
    cfg: dict[str, Any],
    site_filter: str,
    destination: str | None,
    dry_run: bool,
    run_timestamp: str | None = None,
) -> None:
    logger = get_logger(__name__)
    logger.info("=" * 60)
    logger.info(
        "FMS Archive Runner — site_filter=%s  run_timestamp=%s"
        "  destination=%s  dry_run=%s",
        site_filter, run_timestamp or "(today)",
        destination or "(from config)", dry_run,
    )
    logger.info("=" * 60)

    sites: dict[str, Any]      = get_config_value(cfg, "sites", {})
    archive_root                = get_config_value(cfg, "paths.archive_root", "")
    archive_cfg: dict[str, Any] = get_config_value(cfg, "archive", {})
    blob_cfg: dict[str, Any]    = get_config_value(cfg, "blob_storage", {})

    # --destination CLI arg overrides config; all other options come from config only
    effective_destination = destination or archive_cfg.get("destination", "network")

    logger.info(
        "Config — destination=%s  compression=%s  chunking=%s(%d)  delete_local=%s",
        effective_destination,
        archive_cfg.get("compression_method", "stored"),
        archive_cfg.get("enable_chunking", False),
        archive_cfg.get("files_per_chunk", 5000),
        archive_cfg.get("delete_local_zip_after_upload", False),
    )

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
        source_type = site_cfg.get("source_type", "minestar")
        logger.info("Archiving site: %s  source_type: %s", site, source_type)
        svc = ArchiveService(
            site=site,
            landing_zone=site_cfg.get("landing_zone", ""),
            archive_root=archive_root,
            source_type=source_type,
            force_date=run_timestamp,
            destination=effective_destination,
            compression_method=archive_cfg.get("compression_method", "stored"),
            compress_level=archive_cfg.get("compress_level", 1),
            enable_chunking=archive_cfg.get("enable_chunking", False),
            files_per_chunk=archive_cfg.get("files_per_chunk", 5000),
            delete_local_zip_after_upload=archive_cfg.get("delete_local_zip_after_upload", False),
            blob_connection_string_env_var=blob_cfg.get(
                "connection_string_env_var", "AZURE_STORAGE_CONNECTION_STRING"
            ),
            blob_container_name=blob_cfg.get("container_name", "fms-archive"),
            blob_prefix=archive_cfg.get("blob_prefix", "fms-snippets/"),
            dry_run=dry_run,
        )
        result = svc.archive()
        summary.append(result)

        parts      = max(len(result.get("archive_paths", [])), len(result.get("blob_paths", [])), 1)
        first_arch = (result.get("archive_paths") or ["-"])[0]
        first_blob = (result.get("blob_paths")    or ["-"])[0]
        logger.info(
            "  %s → %d files  %.2f MB  parts=%d  duration=%.1fs  archive=%s  blob=%s",
            result["status"],
            result["files_archived"],
            result["bytes_archived"] / 1_048_576,
            parts,
            result.get("duration_seconds", 0),
            first_arch,
            first_blob,
        )

    total_files = sum(r["files_archived"] for r in summary)
    total_dur   = sum(r.get("duration_seconds", 0) for r in summary)
    logger.info("=" * 60)
    logger.info(
        "Archive complete — %d sites  %d files total  %.1fs total",
        len(summary), total_files, total_dur,
    )
    logger.info("=" * 60)


def main() -> None:
    args = parse_args()
    setup_logging(args.logging)
    logger = get_logger(__name__)
    logger.info("Environment: %s", args.env)
    logger.info("Run timestamp (normalised): %s", args.FMS_ForceDate or "(today)")

    try:
        cfg = ConfigLoader(args.config).load()
        run(
            cfg,
            site_filter=args.site,
            destination=args.destination,
            dry_run=args.dry_run,
            run_timestamp=args.FMS_ForceDate,
        )
    except Exception:
        logger.error("Archive runner failed", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
