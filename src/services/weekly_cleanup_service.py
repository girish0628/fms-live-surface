"""
Weekly cleanup service.

Walks the FMS output root for folders older than ``retention_days`` days,
uploads each folder to Azure Blob Storage, then deletes it from the local
file share.  Also purges old staging intermediate files.

Azure connection string is read from an OS environment variable so that
credentials never appear in config files or logs.

Design notes:
  - Each folder is uploaded atomically before local deletion; a failure
    during upload leaves the local folder intact (safe to retry).
  - ``dry_run=True`` logs all actions without writing or deleting anything.
  - Errors on individual folders are collected and reported in the result
    dict rather than aborting the entire run.
"""
from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from src.core.exceptions import WeeklyCleanupError
from src.core.logger import get_logger


@dataclass(frozen=True)
class WeeklyCleanupService:
    """
    Archive old FMS output folders to Azure Blob and purge locally.

    Parameters
    ----------
    output_root : str
        Root folder containing ``FMS_<timestamp>`` hourly output folders.
    staging_folder : str
        Staging folder for intermediate CSVs; older files are purged.
    blob_connection_string_env_var : str
        OS env var holding the Azure Storage connection string.
    blob_container_name : str
        Target Azure Blob container name.
    blob_prefix : str
        Virtual folder prefix inside the container (e.g. ``"fms/archive/"``).
    retention_days : int
        Archive and delete local folders older than this many days.
    staging_retention_days : int
        Delete staging files older than this many days (no blob upload).
    dry_run : bool
        Log all actions without uploading or deleting anything.
    """

    output_root: str
    staging_folder: str
    blob_connection_string_env_var: str = "AZURE_STORAGE_CONNECTION_STRING"
    blob_container_name: str = "fms-archive"
    blob_prefix: str = "fms-live-surface/"
    retention_days: int = 7
    staging_retention_days: int = 2
    dry_run: bool = False

    def cleanup(self) -> dict[str, Any]:
        """
        Run the weekly cleanup.

        Returns
        -------
        dict
            status (SUCCESS | PARTIAL), archived_count, deleted_count,
            staging_purged_count, error_count, errors, dry_run.

        Raises
        ------
        WeeklyCleanupError
            Only for non-recoverable setup failures (e.g. missing env var).
        """
        logger = get_logger(__name__)
        cutoff = datetime.now() - timedelta(days=self.retention_days)
        staging_cutoff = datetime.now() - timedelta(days=self.staging_retention_days)
        logger.info(
            "Weekly cleanup — output_root=%s  cutoff=%s  dry_run=%s",
            self.output_root, cutoff.strftime("%Y-%m-%d"), self.dry_run,
        )

        # Validate Azure creds early (fail fast, before any I/O)
        if not self.dry_run:
            self._validate_blob_credentials()

        archived: list[str] = []
        deleted: list[str] = []
        errors: list[str] = []

        # ----------------------------------------------------------------
        # Archive + delete old output folders
        # ----------------------------------------------------------------
        root = Path(self.output_root)
        for folder in sorted(root.glob("FMS_*")):
            if not folder.is_dir():
                continue
            mtime = datetime.fromtimestamp(folder.stat().st_mtime)
            if mtime >= cutoff:
                continue

            logger.info(
                "Archiving output folder: %s  (modified %s)",
                folder.name, mtime.strftime("%Y-%m-%d %H:%M"),
            )
            try:
                if not self.dry_run:
                    self._upload_folder_to_blob(folder)
                archived.append(folder.name)

                logger.info("Deleting local folder: %s", folder.name)
                if not self.dry_run:
                    shutil.rmtree(folder)
                deleted.append(folder.name)

            except Exception as exc:
                logger.error("Failed to archive/delete %s: %s", folder.name, exc)
                errors.append(f"{folder.name}: {exc}")

        # ----------------------------------------------------------------
        # Purge old staging files (no blob upload needed)
        # ----------------------------------------------------------------
        staging_purged = self._purge_staging(staging_cutoff)

        result: dict[str, Any] = {
            "status": "SUCCESS" if not errors else "PARTIAL",
            "archived_count": len(archived),
            "deleted_count": len(deleted),
            "staging_purged_count": staging_purged,
            "error_count": len(errors),
            "errors": errors,
            "dry_run": self.dry_run,
        }
        logger.info("Weekly cleanup complete: %s", result)
        return result

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _validate_blob_credentials(self) -> None:
        import os
        conn_str = os.environ.get(self.blob_connection_string_env_var, "")
        if not conn_str:
            raise WeeklyCleanupError(
                f"Azure Storage connection string not found. "
                f"Set env var '{self.blob_connection_string_env_var}'."
            )
        try:
            from azure.storage.blob import BlobServiceClient  # noqa: F401
        except ImportError:
            raise WeeklyCleanupError(
                "azure-storage-blob is not installed. "
                "Run: pip install azure-storage-blob>=12.19.0"
            )

    def _upload_folder_to_blob(self, folder: Path) -> None:
        import os
        from azure.storage.blob import BlobServiceClient

        conn_str = os.environ.get(self.blob_connection_string_env_var, "")
        logger = get_logger(__name__)
        client = BlobServiceClient.from_connection_string(conn_str)
        container = client.get_container_client(self.blob_container_name)

        files_uploaded = 0
        for file_path in sorted(folder.rglob("*")):
            if not file_path.is_file():
                continue
            relative = file_path.relative_to(folder)
            blob_name = f"{self.blob_prefix}{folder.name}/{relative}".replace("\\", "/")
            logger.debug("  Uploading → %s", blob_name)
            with open(file_path, "rb") as fh:
                container.upload_blob(name=blob_name, data=fh, overwrite=True)
            files_uploaded += 1

        logger.info(
            "Uploaded %d files from %s to blob container '%s'",
            files_uploaded, folder.name, self.blob_container_name,
        )

    def _purge_staging(self, cutoff: datetime) -> int:
        """Delete staging files older than cutoff. Returns count of items removed."""
        logger = get_logger(__name__)
        staging = Path(self.staging_folder)
        if not staging.exists():
            return 0

        purged = 0
        for item in sorted(staging.rglob("*")):
            if not item.is_file():
                continue
            mtime = datetime.fromtimestamp(item.stat().st_mtime)
            if mtime < cutoff:
                logger.debug("Purging staging file: %s", item)
                if not self.dry_run:
                    item.unlink(missing_ok=True)
                purged += 1

        logger.info("Staging purge: %d files removed (dry_run=%s)", purged, self.dry_run)
        return purged
