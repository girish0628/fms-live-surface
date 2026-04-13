"""
Modular Mining CSV reprojection service.

Reprojects Modular Mining CSV files from site-specific projections
(WB94 or ER94) to MGA50 for consistent downstream processing.
"""
from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from src.core.exceptions import ModularCsvError
from src.core.logger import get_logger

PointRecord = tuple[float, float, float, str]  # x, y, z, timestamp


@dataclass(frozen=True)
class ModularCsvService:
    """
    Reproject Modular Mining CSV files to MGA50.

    Modular files arrive in site-specific coordinate systems (WB94 or ER94)
    and must be standardised to MGA50 before raster generation.

    Parameters
    ----------
    site : str
        Mine site code (WB, ER, TG, JB, NM).
    input_folder : str
        Folder containing Modular .csv files from the GIP landing zone.
    output_folder : str
        Staging folder root; output written to
        ``<output_folder>/<site>/<timestamp>/``.
    input_spatial_ref : str
        Path to .prj file for the Modular source CRS.
    output_spatial_ref : str
        Path to .prj file for the target CRS (MGA50 / GDA2020).
    x_field : str
        Column name for easting in the source CSV.
    y_field : str
        Column name for northing in the source CSV.
    z_field : str
        Column name for elevation in the source CSV.
    timestamp_field : str
        Column name for timestamp; empty string if not present.
    """

    site: str
    input_folder: str
    output_folder: str
    input_spatial_ref: str = ""
    output_spatial_ref: str = ""
    x_field: str = "EASTING"
    y_field: str = "NORTHING"
    z_field: str = "ELEVATION"
    timestamp_field: str = "TIMESTAMP"

    def process(self) -> dict[str, Any]:
        """
        Reproject all CSV files in input_folder and write a merged output CSV.

        Returns
        -------
        dict[str, Any]
            ``status``, ``site``, ``file_count``, ``total_points``,
            ``csv_path``, ``config_path``, ``output_dir``.

        Raises
        ------
        ModularCsvError
            If reprojection fails.
        """
        logger = get_logger(__name__)
        logger.info("Modular CSV processing started — site: %s", self.site)

        try:
            csv_files = sorted(Path(self.input_folder).glob("*.csv"))
            logger.info("Found %d Modular CSV files", len(csv_files))

            all_points: list[PointRecord] = []
            for csv_file in csv_files:
                pts = self._read_csv(csv_file)
                logger.debug("  %s → %d points", csv_file.name, len(pts))
                all_points.extend(pts)

            logger.info("Total points read: %d", len(all_points))
            all_points = self._reproject(all_points)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            output_dir = Path(self.output_folder) / self.site / timestamp
            output_dir.mkdir(parents=True, exist_ok=True)

            out_csv = output_dir / f"{self.site}_modular_points.csv"
            self._write_csv(all_points, out_csv)
            logger.info("Reprojected CSV written: %s", out_csv)

            config = {
                "site": self.site,
                "timestamp": timestamp,
                "source": "Modular",
                "csvPath": str(out_csv),
                "sourceFiles": {"csvCount": len(csv_files), "totalPoints": len(all_points)},
                "processing": {
                    "inputSpatialReference": self.input_spatial_ref,
                    "outputSpatialReference": self.output_spatial_ref,
                },
            }
            json_path = output_dir / "config.json"
            json_path.write_text(json.dumps(config, indent=2), encoding="utf-8")

            return {
                "status": "SUCCESS",
                "site": self.site,
                "file_count": len(csv_files),
                "total_points": len(all_points),
                "csv_path": str(out_csv),
                "config_path": str(json_path),
                "output_dir": str(output_dir),
            }

        except ModularCsvError:
            raise
        except Exception as exc:
            logger.error("Modular CSV processing failed", exc_info=True)
            raise ModularCsvError(f"Processing failed for site {self.site}") from exc

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _read_csv(self, path: Path) -> list[PointRecord]:
        """Read a single Modular CSV and return raw point records."""
        points: list[PointRecord] = []
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    x = float(row[self.x_field])
                    y = float(row[self.y_field])
                    z = float(row[self.z_field])
                    ts = row.get(self.timestamp_field, datetime.now().isoformat())
                    points.append((x, y, z, ts))
                except (KeyError, ValueError):
                    continue
        return points

    def _reproject(self, points: list[PointRecord]) -> list[PointRecord]:
        """Reproject from *input_spatial_ref* to *output_spatial_ref* via arcpy."""
        logger = get_logger(__name__)
        try:
            import arcpy  # noqa: PLC0415

            in_sr = arcpy.SpatialReference(self.input_spatial_ref)
            out_sr = arcpy.SpatialReference(self.output_spatial_ref)

            if in_sr.factoryCode == out_sr.factoryCode:
                return points

            reprojected: list[PointRecord] = []
            for x, y, z, ts in points:
                geom = arcpy.PointGeometry(arcpy.Point(x, y, z), in_sr)
                proj = geom.projectAs(out_sr)
                fp = proj.firstPoint
                reprojected.append((fp.X, fp.Y, z, ts))
            logger.info("Reprojected %d Modular points to MGA50", len(reprojected))
            return reprojected

        except ImportError:
            logger.warning("arcpy unavailable — reprojection skipped (non-production)")
            return points

    def _write_csv(self, points: list[PointRecord], path: Path) -> None:
        """Write merged, reprojected points to an output CSV."""
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["X", "Y", "Z", "TIMESTAMP"])
            writer.writerows(points)
