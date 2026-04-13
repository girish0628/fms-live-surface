"""
Archive service — nightly snippet file compaction.

Compresses all .snp files in the GIP landing zone for a site into a
dated ZIP archive, then clears the landing zone.  Mirrors the existing
``FMS - Archive Snippet Files`` Jenkins job behaviour.
"""
from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.core.exceptions import ArchiveError
from src.core.logger import get_logger


@dataclass(frozen=True)
class ArchiveService:
    """
    Compress and archive Minestar snippet files for a mine site.

    Parameters
    ----------
    site : str
        Mine site code (WB, ER, TG, JB, NM).
    landing_zone : str
        Path to the GIP landing zone folder containing .snp files.
    archive_root : str
        Root archive directory; archives are written under
        ``<archive_root>/<site>/<YYYY>/<MM>/``.
    dry_run : bool
        When True, log what would happen without moving or deleting files.
    """

    site: str
    landing_zone: str
    archive_root: str
    dry_run: bool = False

    def archive(self) -> dict[str, Any]:
        """
        Compress all .snp files in the landing zone and clear it.

        Returns
        -------
        dict[str, Any]
            ``status``, ``site``, ``archive_path``, ``files_archived``,
            ``bytes_archived``.

        Raises
        ------
        ArchiveError
            If any file operation fails.
        """
        logger = get_logger(__name__)
        now = datetime.now(tz=timezone.utc)
        logger.info("Archive started — site: %s  dry_run=%s", self.site, self.dry_run)

        try:
            landing = Path(self.landing_zone)
            snp_files = sorted(landing.glob("*.snp"))

            if not snp_files:
                logger.info("No .snp files found in %s — nothing to archive", landing)
                return {
                    "status": "SUCCESS",
                    "site": self.site,
                    "archive_path": None,
                    "files_archived": 0,
                    "bytes_archived": 0,
                }

            total_bytes = sum(f.stat().st_size for f in snp_files)
            logger.info(
                "Found %d .snp files (%.2f MB) to archive",
                len(snp_files), total_bytes / 1_048_576,
            )

            # Archive destination: <archive_root>/<site>/<YYYY>/<MM>/
            archive_dir = (
                Path(self.archive_root)
                / self.site
                / now.strftime("%Y")
                / now.strftime("%m")
            )
            timestamp_str = now.strftime("%Y%m%d_%H%M%S")
            archive_name = f"{self.site}_snippets_{timestamp_str}"
            archive_path = archive_dir / archive_name  # shutil adds .zip

            if not self.dry_run:
                archive_dir.mkdir(parents=True, exist_ok=True)

                # Copy .snp files to a temp staging folder, then zip
                staging = landing / "_archive_staging"
                staging.mkdir(exist_ok=True)
                for snp in snp_files:
                    shutil.copy2(snp, staging / snp.name)

                shutil.make_archive(str(archive_path), "zip", root_dir=str(staging))
                shutil.rmtree(staging)

                final_archive = Path(str(archive_path) + ".zip")
                logger.info(
                    "Archive created: %s (%.2f MB)",
                    final_archive,
                    final_archive.stat().st_size / 1_048_576,
                )

                # Clear landing zone
                for snp in snp_files:
                    snp.unlink()
                logger.info("Landing zone cleared: %d files removed", len(snp_files))
            else:
                logger.info("[DRY RUN] Would create archive: %s.zip", archive_path)
                logger.info("[DRY RUN] Would remove %d .snp files from %s", len(snp_files), landing)
                final_archive = Path(str(archive_path) + ".zip")

            return {
                "status": "SUCCESS",
                "site": self.site,
                "archive_path": str(final_archive),
                "files_archived": len(snp_files),
                "bytes_archived": total_bytes,
                "dry_run": self.dry_run,
            }

        except Exception as exc:
            logger.error("Archive failed", exc_info=True)
            raise ArchiveError(f"Archive failed for site {self.site}") from exc
