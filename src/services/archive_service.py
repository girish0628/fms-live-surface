"""
Archive service — nightly snippet file compaction.

Compresses all .snp files in the GIP landing zone for a site into a
dated ZIP archive.  Supports three destinations:

  network — write zip to a network share (default, original behaviour)
  blob    — upload zip to Azure Blob Storage; no local file kept
  both    — write to network AND upload that same zip to blob
"""
from __future__ import annotations

import shutil
import tempfile
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
        Mine site code (WB, ER, SF, YND, JB, NWW, MAC).
    landing_zone : str
        Path to the GIP landing zone folder containing .snp files.
    archive_root : str
        Root archive directory; archives are written under
        ``<archive_root>/<site>/<YYYY>/<MM>/``.
        Required when destination is 'network' or 'both'.
    destination : str
        Where to write the archive: 'network', 'blob', or 'both'.
    blob_connection_string_env_var : str
        OS env var holding the Azure Storage connection string.
        Required when destination is 'blob' or 'both'.
    blob_container_name : str
        Target Azure Blob container name.
    blob_prefix : str
        Virtual folder prefix inside the container.
        Zip lands at: ``{blob_prefix}{site}/{YYYY}/{MM}/{archive_name}.zip``
    dry_run : bool
        When True, log what would happen without moving or deleting files.
    """

    site: str
    landing_zone: str
    archive_root: str
    destination: str = "network"
    blob_connection_string_env_var: str = "AZURE_STORAGE_CONNECTION_STRING"
    blob_container_name: str = "fms-archive"
    blob_prefix: str = "fms-snippets/"
    dry_run: bool = False

    def archive(self) -> dict[str, Any]:
        """
        Compress all .snp files in the landing zone and clear it.

        Returns
        -------
        dict[str, Any]
            ``status``, ``site``, ``destination``, ``archive_path``,
            ``blob_path``, ``files_archived``, ``bytes_archived``.

        Raises
        ------
        ArchiveError
            If any file operation fails.
        """
        logger = get_logger(__name__)
        now = datetime.now(tz=timezone.utc)
        logger.info(
            "Archive started — site: %s  destination: %s  dry_run=%s",
            self.site, self.destination, self.dry_run,
        )

        if self.destination not in ("network", "blob", "both"):
            raise ArchiveError(
                f"Invalid destination '{self.destination}'. Choose: network, blob, both"
            )

        if self.destination in ("blob", "both") and not self.dry_run:
            self._validate_blob_credentials()

        try:
            landing = Path(self.landing_zone)
            snp_files = sorted(landing.glob("*.snp"))

            if not snp_files:
                logger.info("No .snp files found in %s — nothing to archive", landing)
                return {
                    "status": "SUCCESS",
                    "site": self.site,
                    "destination": self.destination,
                    "archive_path": None,
                    "blob_path": None,
                    "files_archived": 0,
                    "bytes_archived": 0,
                }

            total_bytes = sum(f.stat().st_size for f in snp_files)
            logger.info(
                "Found %d .snp files (%.2f MB) to archive",
                len(snp_files), total_bytes / 1_048_576,
            )

            year_str      = now.strftime("%Y")
            month_str     = now.strftime("%m")
            timestamp_str = now.strftime("%Y%m%d_%H%M%S")
            archive_name  = f"{self.site}_snippets_{timestamp_str}"

            network_zip: Path | None = None
            blob_path: str | None    = None

            if not self.dry_run:
                # Stage .snp files into a temp subfolder for zipping
                staging = landing / "_archive_staging"
                staging.mkdir(exist_ok=True)
                for snp in snp_files:
                    shutil.copy2(snp, staging / snp.name)

                try:
                    # ---- Network destination ----
                    if self.destination in ("network", "both"):
                        archive_dir = (
                            Path(self.archive_root)
                            / self.site
                            / year_str
                            / month_str
                        )
                        archive_dir.mkdir(parents=True, exist_ok=True)
                        archive_base = str(archive_dir / archive_name)
                        shutil.make_archive(archive_base, "zip", root_dir=str(staging))
                        network_zip = Path(archive_base + ".zip")
                        logger.info(
                            "Network archive created: %s (%.2f MB)",
                            network_zip, network_zip.stat().st_size / 1_048_576,
                        )

                    # ---- Blob destination ----
                    if self.destination in ("blob", "both"):
                        if self.destination == "blob":
                            # Zip to a temporary location, upload, then discard
                            with tempfile.TemporaryDirectory() as tmp_dir:
                                tmp_base = str(Path(tmp_dir) / archive_name)
                                shutil.make_archive(tmp_base, "zip", root_dir=str(staging))
                                tmp_zip = Path(tmp_base + ".zip")
                                blob_path = self._upload_zip_to_blob(
                                    tmp_zip, self.site, year_str, month_str
                                )
                        else:
                            # "both": upload the network zip that was just written
                            assert network_zip is not None
                            blob_path = self._upload_zip_to_blob(
                                network_zip, self.site, year_str, month_str
                            )

                finally:
                    shutil.rmtree(staging, ignore_errors=True)

                # Clear landing zone
                for snp in snp_files:
                    snp.unlink()
                logger.info("Landing zone cleared: %d files removed", len(snp_files))

            else:
                # Dry-run: log what would happen without touching anything
                if self.destination in ("network", "both"):
                    archive_dir = (
                        Path(self.archive_root) / self.site / year_str / month_str
                    )
                    network_zip = archive_dir / f"{archive_name}.zip"
                    logger.info("[DRY RUN] Would create network archive: %s", network_zip)

                if self.destination in ("blob", "both"):
                    blob_name = (
                        f"{self.blob_prefix}{self.site}/{year_str}/{month_str}/{archive_name}.zip"
                    )
                    logger.info("[DRY RUN] Would upload to blob: %s", blob_name)
                    blob_path = blob_name

                logger.info(
                    "[DRY RUN] Would remove %d .snp files from %s", len(snp_files), landing
                )

            return {
                "status": "SUCCESS",
                "site": self.site,
                "destination": self.destination,
                "archive_path": str(network_zip) if network_zip else None,
                "blob_path": blob_path,
                "files_archived": len(snp_files),
                "bytes_archived": total_bytes,
                "dry_run": self.dry_run,
            }

        except Exception as exc:
            logger.error("Archive failed", exc_info=True)
            raise ArchiveError(f"Archive failed for site {self.site}") from exc

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _validate_blob_credentials(self) -> None:
        import os
        conn_str = os.environ.get(self.blob_connection_string_env_var, "")
        if not conn_str:
            raise ArchiveError(
                f"Azure Storage connection string not found. "
                f"Set env var '{self.blob_connection_string_env_var}'."
            )
        try:
            from azure.storage.blob import BlobServiceClient  # noqa: F401
        except ImportError:
            raise ArchiveError(
                "azure-storage-blob is not installed. "
                "Run: pip install azure-storage-blob>=12.19.0"
            )

    def _upload_zip_to_blob(
        self, zip_path: Path, site: str, year: str, month: str
    ) -> str:
        import os
        from azure.storage.blob import BlobServiceClient

        conn_str = os.environ.get(self.blob_connection_string_env_var, "")
        logger = get_logger(__name__)
        client = BlobServiceClient.from_connection_string(conn_str)
        container = client.get_container_client(self.blob_container_name)

        blob_name = f"{self.blob_prefix}{site}/{year}/{month}/{zip_path.name}"
        logger.debug("Uploading → %s", blob_name)
        with open(zip_path, "rb") as fh:
            container.upload_blob(name=blob_name, data=fh, overwrite=True)
        logger.info(
            "Uploaded archive to blob: %s (%.2f MB)",
            blob_name, zip_path.stat().st_size / 1_048_576,
        )
        return blob_name
