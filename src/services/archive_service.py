"""
Archive service — nightly input file compaction.

Compresses input files in the GIP landing zone for a site into one or more
dated ZIP archives.  Supports three destinations:

  network — write zip(s) to a network share (default)
  blob    — upload zip(s) to Azure Blob Storage; no local file kept
  both    — write to network AND upload to blob

File patterns archived depend on source_type:
  minestar — *.snp
  modular  — *.csv
  both     — *.snp and *.csv

Performance notes
-----------------
- Uses zipfile directly (no shutil.make_archive) with ZIP_STORED by default —
  avoids compression overhead on binary files.
- Uses os.scandir for a single-pass directory scan that caches file size
  without extra stat() calls.
- Writes directly into the ZIP from the landing zone — no staging copy step.
- Supports chunked output: one ZIP per N files to keep individual archive
  sizes manageable.
"""
from __future__ import annotations

import os
import tempfile
import time
import zipfile as zf
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.core.exceptions import ArchiveError
from src.core.logger import get_logger

_FILE_TYPE_LABEL = {"minestar": "snippets", "modular": "csv", "both": "input"}
_EXTENSIONS: dict[str, set[str]] = {
    "minestar": {".snp"},
    "modular":  {".csv"},
    "both":     {".snp", ".csv"},
}


@dataclass(frozen=True)
class ArchiveService:
    """
    Compress and archive input files for a mine site.

    Parameters
    ----------
    site : str
        Mine site code (WB, ER, SF, YND, JB, NWW, MAC).
    landing_zone : str
        Path to the GIP landing zone folder containing input files.
    archive_root : str
        Root archive directory; archives are written under
        ``<archive_root>/<site>/<YYYYMMDD>/``.
        Required when destination is 'network' or 'both'.
    source_type : str
        Input file type: 'minestar' (*.snp), 'modular' (*.csv), or 'both'.
    force_date : str | None
        YYYYMMDD string to override today's date for file matching,
        archive folder, and filename. When None, uses current UTC date.
    destination : str
        Where to write the archive: 'network', 'blob', or 'both'.
    compression_method : str
        'stored' (no compression, maximum speed) or 'deflated'.
    compress_level : int
        Compression level 1–9 when compression_method='deflated'.
    enable_chunking : bool
        Split output into multiple ZIP files when file count is large.
    files_per_chunk : int
        Maximum files per ZIP part when enable_chunking=True.
    delete_local_zip_after_upload : bool
        When destination='both', delete the local network ZIP after a
        successful blob upload.
    blob_connection_string_env_var : str
        OS env var holding the Azure Storage connection string.
    blob_container_name : str
        Target Azure Blob container name.
    blob_prefix : str
        Virtual folder prefix inside the container.
        Zips land at: ``{blob_prefix}{site}/{YYYYMMDD}/{archive_name}.zip``
    dry_run : bool
        When True, log what would happen without modifying files.
    """

    site: str
    landing_zone: str
    archive_root: str
    source_type: str = "minestar"
    force_date: str | None = None
    destination: str = "network"
    compression_method: str = "stored"
    compress_level: int = 1
    enable_chunking: bool = False
    files_per_chunk: int = 5000
    delete_local_zip_after_upload: bool = False
    blob_connection_string_env_var: str = "AZURE_STORAGE_CONNECTION_STRING"
    blob_container_name: str = "fms-archive"
    blob_prefix: str = "fms-snippets/"
    dry_run: bool = False

    def archive(self) -> dict[str, Any]:
        """
        Compress all matching input files in the landing zone and clear it.

        Returns
        -------
        dict[str, Any]
            status, site, source_type, destination, archive_paths,
            blob_paths, files_archived, bytes_archived, duration_seconds.

        Raises
        ------
        ArchiveError
            If any archive or upload operation fails.
        """
        logger = get_logger(__name__)
        t_start = time.monotonic()

        if self.force_date:
            now = datetime(
                int(self.force_date[:4]),
                int(self.force_date[4:6]),
                int(self.force_date[6:8]),
                tzinfo=timezone.utc,
            )
        else:
            now = datetime.now(tz=timezone.utc)

        logger.info(
            "Archive started — site: %s  source_type: %s  destination: %s"
            "  chunking=%s(%d)  compression=%s  dry_run=%s",
            self.site, self.source_type, self.destination,
            self.enable_chunking, self.files_per_chunk,
            self.compression_method, self.dry_run,
        )

        if self.source_type not in _EXTENSIONS:
            raise ArchiveError(
                f"Invalid source_type '{self.source_type}'. Choose: minestar, modular, both"
            )
        if self.destination not in ("network", "blob", "both"):
            raise ArchiveError(
                f"Invalid destination '{self.destination}'. Choose: network, blob, both"
            )

        if self.destination in ("blob", "both") and not self.dry_run:
            self._validate_blob_credentials()

        try:
            landing   = Path(self.landing_zone)
            date_str  = now.strftime("%Y%m%d")
            ts_str    = now.strftime("%Y%m%d_%H%M%S")
            file_label   = _FILE_TYPE_LABEL.get(self.source_type, "files")
            archive_name = f"{self.site}_{file_label}_{ts_str}"

            # Single-pass scan — collects files and total size together
            input_files, total_bytes = self._scan_files(landing, date_str)

            if not input_files:
                logger.info(
                    "No %s files found matching *%s* in %s — nothing to archive",
                    file_label, date_str, landing,
                )
                return self._empty_result(date_str, t_start)

            logger.info(
                "Found %d %s files (%.2f MB) to archive",
                len(input_files), file_label, total_bytes / 1_048_576,
            )

            archive_paths: list[Path] = []
            blob_paths: list[str]     = []

            if not self.dry_run:
                archive_dir = Path(self.archive_root) / self.site / date_str

                if self.destination == "blob":
                    # Build ZIPs in a temp dir, upload each, then discard
                    with tempfile.TemporaryDirectory() as tmp_dir:
                        zip_parts = self._build_zips(
                            input_files, Path(tmp_dir), archive_name
                        )
                        for zp in zip_parts:
                            bp = self._upload_zip_to_blob(zp, self.site, date_str)
                            blob_paths.append(bp)

                else:
                    # network or both: write ZIPs to the dated archive folder
                    archive_dir.mkdir(parents=True, exist_ok=True)
                    zip_parts = self._build_zips(input_files, archive_dir, archive_name)
                    archive_paths = list(zip_parts)

                    if self.destination == "both":
                        for zp in zip_parts:
                            bp = self._upload_zip_to_blob(zp, self.site, date_str)
                            blob_paths.append(bp)
                            if self.delete_local_zip_after_upload:
                                zp.unlink(missing_ok=True)
                                logger.info(
                                    "Deleted local ZIP after upload: %s", zp.name
                                )

                # Clear landing zone — log individual failures but don't abort
                deleted = 0
                for f in input_files:
                    try:
                        f.unlink()
                        deleted += 1
                    except OSError as exc:
                        logger.warning("Could not delete %s: %s", f.name, exc)
                logger.info("Landing zone cleared: %d files removed", deleted)

            else:
                # Dry-run — show what would be created and uploaded
                archive_dir = Path(self.archive_root) / self.site / date_str
                chunks = self._chunk_list(input_files)
                multi  = len(chunks) > 1
                for idx, chunk in enumerate(chunks, start=1):
                    suffix   = f"_part_{idx:03d}" if multi else ""
                    zip_name = f"{archive_name}{suffix}.zip"
                    if self.destination in ("network", "both"):
                        p = archive_dir / zip_name
                        logger.info(
                            "[DRY RUN] Would create: %s  (%d files)", p, len(chunk)
                        )
                        archive_paths.append(p)
                    if self.destination in ("blob", "both"):
                        blob_name = (
                            f"{self.blob_prefix}{self.site}/{date_str}/{zip_name}"
                        )
                        logger.info("[DRY RUN] Would upload to blob: %s", blob_name)
                        blob_paths.append(blob_name)
                logger.info(
                    "[DRY RUN] Would remove %d %s files from %s",
                    len(input_files), file_label, landing,
                )

            duration = round(time.monotonic() - t_start, 1)
            logger.info(
                "Archive complete — site: %s  parts: %d  files: %d  duration: %.1fs",
                self.site,
                max(len(archive_paths), len(blob_paths), 1),
                len(input_files),
                duration,
            )

            return {
                "status": "SUCCESS",
                "site": self.site,
                "source_type": self.source_type,
                "destination": self.destination,
                "archive_paths": [str(p) for p in archive_paths],
                "blob_paths": blob_paths,
                "files_archived": len(input_files),
                "bytes_archived": total_bytes,
                "duration_seconds": duration,
                "dry_run": self.dry_run,
            }

        except ArchiveError:
            raise
        except Exception as exc:
            logger.error("Archive failed", exc_info=True)
            raise ArchiveError(f"Archive failed for site {self.site}") from exc

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _empty_result(self, date_str: str, t_start: float) -> dict[str, Any]:
        return {
            "status": "SUCCESS",
            "site": self.site,
            "source_type": self.source_type,
            "destination": self.destination,
            "archive_paths": [],
            "blob_paths": [],
            "files_archived": 0,
            "bytes_archived": 0,
            "duration_seconds": round(time.monotonic() - t_start, 1),
            "dry_run": self.dry_run,
        }

    def _scan_files(
        self, landing: Path, date_str: str
    ) -> tuple[list[Path], int]:
        """
        Single-pass os.scandir over the landing zone.

        Returns (sorted file paths, total bytes).  Using os.scandir avoids a
        second glob pass and reuses the DirEntry stat cache on Windows.
        """
        exts: set[str] = _EXTENSIONS[self.source_type]
        files: list[Path] = []
        total_bytes = 0
        try:
            with os.scandir(landing) as it:
                for entry in it:
                    name = entry.name
                    if (
                        entry.is_file(follow_symlinks=False)
                        and date_str in name
                        and os.path.splitext(name)[1].lower() in exts
                    ):
                        st = entry.stat()
                        files.append(Path(entry.path))
                        total_bytes += st.st_size
        except FileNotFoundError:
            pass
        return sorted(files), total_bytes

    def _chunk_list(self, files: list[Path]) -> list[list[Path]]:
        if not self.enable_chunking:
            return [files]
        n = self.files_per_chunk
        return [files[i: i + n] for i in range(0, len(files), n)]

    def _build_zips(
        self,
        input_files: list[Path],
        output_dir: Path,
        archive_name: str,
    ) -> list[Path]:
        """
        Write input_files into one or more ZIP files under output_dir.

        Files are written directly from the landing zone — no staging copy.
        Returns list of created ZIP paths.
        """
        compression  = zf.ZIP_DEFLATED if self.compression_method == "deflated" else zf.ZIP_STORED
        compresslevel = self.compress_level if self.compression_method == "deflated" else None

        chunks = self._chunk_list(input_files)
        multi  = len(chunks) > 1
        zip_paths: list[Path] = []

        for idx, chunk in enumerate(chunks, start=1):
            suffix   = f"_part_{idx:03d}" if multi else ""
            zip_path = output_dir / f"{archive_name}{suffix}.zip"
            self._write_zip(zip_path, chunk, compression, compresslevel)
            zip_paths.append(zip_path)

        return zip_paths

    def _write_zip(
        self,
        zip_path: Path,
        files: list[Path],
        compression: int,
        compresslevel: int | None,
    ) -> None:
        """Write files into zip_path using arcname=filename only (no path)."""
        logger = get_logger(__name__)
        kwargs: dict[str, Any] = {"compression": compression, "allowZip64": True}
        if compresslevel is not None:
            kwargs["compresslevel"] = compresslevel

        failed = 0
        with zf.ZipFile(zip_path, "w", **kwargs) as zfp:
            for f in files:
                try:
                    zfp.write(f, arcname=f.name)
                except Exception as exc:
                    logger.warning("Skipped %s: %s", f.name, exc)
                    failed += 1

        logger.info(
            "ZIP created: %s  files=%d  size=%.2f MB%s",
            zip_path.name,
            len(files) - failed,
            zip_path.stat().st_size / 1_048_576,
            f"  ({failed} skipped)" if failed else "",
        )

    def _validate_blob_credentials(self) -> None:
        import os as _os
        conn_str = _os.environ.get(self.blob_connection_string_env_var, "")
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
        self, zip_path: Path, site: str, date_str: str
    ) -> str:
        import os as _os
        from azure.storage.blob import BlobServiceClient

        conn_str  = _os.environ.get(self.blob_connection_string_env_var, "")
        logger    = get_logger(__name__)
        client    = BlobServiceClient.from_connection_string(conn_str)
        container = client.get_container_client(self.blob_container_name)

        blob_name = f"{self.blob_prefix}{site}/{date_str}/{zip_path.name}"
        logger.info("Uploading → %s", blob_name)
        with open(zip_path, "rb") as fh:
            container.upload_blob(name=blob_name, data=fh, overwrite=True)
        logger.info(
            "Uploaded: %s  (%.2f MB)",
            blob_name, zip_path.stat().st_size / 1_048_576,
        )
        return blob_name
