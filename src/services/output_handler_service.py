"""
Output handler service.

Manages the FMS output folder structure and writes the standardised
metadata.json and ready.flag trigger file consumed by the publishing solution.

Output layout:
    FMS_Output/
    └── <site>/
        └── <YYYYMMDD_HHMM>/
            ├── <site>_elevation.tif
            ├── <site>_boundary.shp  (+ .dbf .shx .prj .geojson)
            ├── metadata.json
            └── ready.flag
"""
from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.core.exceptions import OutputHandlerError
from src.core.logger import get_logger


@dataclass(frozen=True)
class OutputHandlerService:
    """
    Copy raster/boundary outputs to the FMS output folder and write metadata.

    Parameters
    ----------
    site : str
        Mine site code (WB, ER, TG, JB, NM).
    output_root : str
        Root of the FMS output tree (e.g. ``//server/FMS_Output``).
    raster_path : str
        Source raster GeoTIFF produced by RasterGenerationService.
    boundary_path : str
        Source boundary shapefile produced by RasterGenerationService.
    processing_metadata : dict[str, Any]
        Metadata dict from the conversion and raster services to embed
        in metadata.json (snippet counts, processing parameters, etc.).
    retention_hours : int
        How many hours of timestamped output folders to retain.
        Older folders are removed by :meth:`cleanup_old_outputs`.
    """

    site: str
    output_root: str
    raster_path: str
    boundary_path: str
    processing_metadata: dict[str, Any]
    retention_hours: int = 48

    def publish_outputs(self) -> dict[str, Any]:
        """
        Copy outputs to the site/timestamp folder and write metadata.

        Returns
        -------
        dict[str, Any]
            ``status``, ``site``, ``output_dir``, ``raster``, ``boundary``,
            ``metadata_path``, ``flag_path``.

        Raises
        ------
        OutputHandlerError
            If any file operation fails.
        """
        logger = get_logger(__name__)
        timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M")
        output_dir = Path(self.output_root) / self.site / timestamp

        logger.info("Writing FMS outputs — site: %s  folder: %s", self.site, output_dir)

        try:
            output_dir.mkdir(parents=True, exist_ok=True)

            # Copy raster
            dest_raster = output_dir / f"{self.site}_elevation.tif"
            shutil.copy2(self.raster_path, dest_raster)
            logger.info("Raster copied: %s", dest_raster)

            # Copy boundary shapefile components (.shp .dbf .shx .prj .geojson)
            src_boundary = Path(self.boundary_path)
            boundary_dest = None
            for ext in (".shp", ".dbf", ".shx", ".prj", ".geojson"):
                src = src_boundary.with_suffix(ext)
                if src.exists():
                    dest = output_dir / f"{self.site}_boundary{ext}"
                    shutil.copy2(src, dest)
                    if ext == ".shp":
                        boundary_dest = str(dest)
            logger.info("Boundary files copied to %s", output_dir)

            # Write metadata.json
            metadata = self._build_metadata(
                str(dest_raster),
                boundary_dest or "",
                timestamp,
            )
            metadata_path = output_dir / "metadata.json"
            metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
            logger.info("Metadata written: %s", metadata_path)

            # Write ready.flag — triggers the publishing solution
            flag_path = output_dir / "ready.flag"
            flag_path.write_text(
                f"ready\ntimestamp={timestamp}\nsite={self.site}\n",
                encoding="utf-8",
            )
            logger.info("Trigger flag written: %s", flag_path)

            return {
                "status": "SUCCESS",
                "site": self.site,
                "output_dir": str(output_dir),
                "raster": str(dest_raster),
                "boundary": boundary_dest,
                "metadata_path": str(metadata_path),
                "flag_path": str(flag_path),
            }

        except Exception as exc:
            logger.error("Output handler failed", exc_info=True)
            raise OutputHandlerError(
                f"Failed to write outputs for site {self.site}"
            ) from exc

    def cleanup_old_outputs(self) -> int:
        """
        Delete timestamped output folders older than *retention_hours*.

        Returns
        -------
        int
            Number of folders removed.
        """
        logger = get_logger(__name__)
        site_dir = Path(self.output_root) / self.site
        if not site_dir.exists():
            return 0

        now = datetime.now(tz=timezone.utc)
        removed = 0
        for folder in sorted(site_dir.iterdir()):
            if not folder.is_dir():
                continue
            try:
                # Folder name format: YYYYMMDD_HHMM
                folder_dt = datetime.strptime(folder.name, "%Y%m%d_%H%M").replace(
                    tzinfo=timezone.utc
                )
                age_hours = (now - folder_dt).total_seconds() / 3600
                if age_hours > self.retention_hours:
                    shutil.rmtree(folder)
                    logger.info("Removed old output folder: %s (%.1f h old)", folder, age_hours)
                    removed += 1
            except ValueError:
                continue  # Skip non-timestamped folders

        logger.info("Cleanup complete — removed %d old output folders", removed)
        return removed

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_metadata(
        self,
        raster_path: str,
        boundary_path: str,
        timestamp: str,
    ) -> dict[str, Any]:
        """Compose the metadata.json payload."""
        return {
            "site": self.site,
            "timestamp": timestamp,
            "output": {
                "rasterPath": raster_path,
                "boundaryPath": boundary_path,
                "format": "GeoTIFF",
                "cellSize": self.processing_metadata.get("cell_size", 2),
                "spatialReference": "MGA50",
            },
            "sourceFiles": self.processing_metadata.get("sourceFiles", {}),
            "processing": self.processing_metadata.get("processing", {}),
            "status": "ready_for_publish",
        }
