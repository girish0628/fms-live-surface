"""
Minestar snippet file (.snp) conversion service.

Converts proprietary Minestar .snp files to MGA50-projected CSV files
suitable for downstream raster generation.

Processing pipeline:
    Parse .snp → Z adjustment → Noise filter → Despike → Reproject → AOI filter → CSV
"""
from __future__ import annotations

import csv
import json
import struct
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from src.core.exceptions import SnippetConversionError
from src.core.logger import get_logger

# Type alias for a 4-tuple point record
PointRecord = tuple[float, float, float, str]  # x, y, z, timestamp


@dataclass(frozen=True)
class SnippetConversionService:
    """
    Convert Minestar .snp files to a single MGA50 CSV for a mine site.

    Parameters
    ----------
    site : str
        Mine site code (WB, ER, TG, JB, NM).
    input_folder : str
        Folder containing .snp files from GIP landing zone.
    output_folder : str
        Staging folder root; output is written to
        ``<output_folder>/<site>/<timestamp>/``.
    z_adjustment : float
        Additive Z datum adjustment converting ADPH to AHD (metres).
    min_neighbours : int
        Minimum number of neighbours required; used in noise validation.
    max_z : float
        Maximum valid Z value; points above this are discarded.
    input_spatial_ref : str
        Path to .prj file for the input CRS of .snp coordinates.
    output_spatial_ref : str
        Path to .prj file for the target CRS (MGA50 / GDA2020).
    aoi_feature_class : str
        Optional path to AOI polygon feature class for spatial filtering.
    aoi_where_clause : str
        SQL WHERE clause applied when querying *aoi_feature_class*.
    despike : bool
        Whether to apply the despiking (spike-smoothing) algorithm.
    """

    site: str
    input_folder: str
    output_folder: str
    z_adjustment: float = 3.155
    min_neighbours: int = 2
    max_z: float = 4000.0
    input_spatial_ref: str = ""
    output_spatial_ref: str = ""
    aoi_feature_class: str = ""
    aoi_where_clause: str = ""
    despike: bool = True

    def convert(self) -> dict[str, Any]:
        """
        Run the full snippet conversion pipeline.

        Returns
        -------
        dict[str, Any]
            ``status``, ``site``, ``snippet_count``, ``total_points``,
            ``valid_points``, ``csv_path``, ``config_path``, ``output_dir``.

        Raises
        ------
        SnippetConversionError
            If any stage of the pipeline fails.
        """
        logger = get_logger(__name__)
        logger.info("=" * 60)
        logger.info("Snippet conversion started — site: %s", self.site)

        try:
            snp_files = sorted(Path(self.input_folder).glob("*.snp"))
            logger.info("Found %d snippet files in %s", len(snp_files), self.input_folder)

            all_points: list[PointRecord] = []
            for snp_file in snp_files:
                pts = self._parse_snippet_file(snp_file)
                logger.debug("  %s → %d points", snp_file.name, len(pts))
                all_points.extend(pts)

            raw_count = len(all_points)
            logger.info("Total raw points parsed: %d", raw_count)

            all_points = self._apply_z_adjustment(all_points)
            all_points = self._filter_noise(all_points)
            if self.despike:
                all_points = self._despike(all_points)
            all_points = self._reproject(all_points)
            if self.aoi_feature_class:
                all_points = self._filter_to_aoi(all_points)

            valid_count = len(all_points)
            logger.info("Valid points after all filters: %d / %d", valid_count, raw_count)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            output_dir = Path(self.output_folder) / self.site / timestamp
            output_dir.mkdir(parents=True, exist_ok=True)

            csv_path = output_dir / f"{self.site}_points.csv"
            self._write_csv(all_points, csv_path)
            logger.info("CSV written: %s", csv_path)

            config = self._build_config(str(csv_path), len(snp_files), raw_count, valid_count, timestamp)
            json_path = output_dir / "config.json"
            json_path.write_text(json.dumps(config, indent=2), encoding="utf-8")
            logger.info("Config JSON written: %s", json_path)

            return {
                "status": "SUCCESS",
                "site": self.site,
                "snippet_count": len(snp_files),
                "total_points": raw_count,
                "valid_points": valid_count,
                "csv_path": str(csv_path),
                "config_path": str(json_path),
                "output_dir": str(output_dir),
            }

        except SnippetConversionError:
            raise
        except Exception as exc:
            logger.error("Snippet conversion failed", exc_info=True)
            raise SnippetConversionError(
                f"Conversion pipeline failed for site {self.site}"
            ) from exc

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _parse_snippet_file(self, snp_file: Path) -> list[PointRecord]:
        """
        Parse a single Minestar .snp binary file.

        Binary record layout (per Minestar specification):
          - 16-byte file header (skipped)
          - Repeating 32-byte records:
              bytes 0-23  : x, y, z  (three IEEE 754 doubles, little-endian)
              bytes 24-31 : timestamp (IEEE 754 double, seconds since epoch)

        NOTE: Adjust struct offsets/format if the site uses a different .snp
              variant (some sites write ASCII-delimited snippet files instead).
        """
        logger = get_logger(__name__)
        points: list[PointRecord] = []
        header_bytes = 16
        record_size = 32  # 3×8 (xyz) + 8 (timestamp)

        try:
            data = snp_file.read_bytes()
            offset = header_bytes
            while offset + record_size <= len(data):
                x, y, z = struct.unpack_from("<ddd", data, offset)
                ts_epoch = struct.unpack_from("<d", data, offset + 24)[0]
                try:
                    ts = datetime.fromtimestamp(ts_epoch).isoformat()
                except (OSError, OverflowError, ValueError):
                    ts = datetime.now().isoformat()
                points.append((x, y, z, ts))
                offset += record_size
        except Exception as exc:
            logger.warning("Could not fully parse %s: %s", snp_file.name, exc)

        return points

    def _apply_z_adjustment(self, points: list[PointRecord]) -> list[PointRecord]:
        """Add z_adjustment to every point's Z value (ADPH → AHD datum shift)."""
        return [(x, y, z + self.z_adjustment, ts) for x, y, z, ts in points]

    def _filter_noise(self, points: list[PointRecord]) -> list[PointRecord]:
        """Remove points with Z values exceeding max_z."""
        before = len(points)
        filtered = [(x, y, z, ts) for x, y, z, ts in points if z <= self.max_z]
        get_logger(__name__).debug(
            "Noise filter: removed %d points (Z > %.1f)", before - len(filtered), self.max_z
        )
        return filtered

    def _despike(self, points: list[PointRecord]) -> list[PointRecord]:
        """
        Clamp Z-spike outliers to the local median Z.

        Uses a global median as the reference surface and a fixed 10 m
        deviation threshold. Replace with a windowed approach for production.
        """
        if len(points) < 3:
            return points

        zs = sorted(p[2] for p in points)
        median_z = zs[len(zs) // 2]
        spike_threshold = 10.0  # metres

        result: list[PointRecord] = []
        clamped = 0
        for x, y, z, ts in points:
            if abs(z - median_z) > spike_threshold:
                result.append((x, y, median_z, ts))
                clamped += 1
            else:
                result.append((x, y, z, ts))

        get_logger(__name__).debug("Despike: clamped %d spike points", clamped)
        return result

    def _reproject(self, points: list[PointRecord]) -> list[PointRecord]:
        """
        Reproject points from input_spatial_ref to output_spatial_ref via arcpy.

        Falls back to returning points unchanged when arcpy is unavailable
        (e.g. during unit testing without an ArcGIS licence).
        """
        logger = get_logger(__name__)
        try:
            import arcpy  # noqa: PLC0415

            in_sr = arcpy.SpatialReference(self.input_spatial_ref)
            out_sr = arcpy.SpatialReference(self.output_spatial_ref)

            if in_sr.factoryCode == out_sr.factoryCode:
                logger.debug("Input and output SRS match — skipping reprojection")
                return points

            reprojected: list[PointRecord] = []
            for x, y, z, ts in points:
                geom = arcpy.PointGeometry(arcpy.Point(x, y, z), in_sr)
                proj = geom.projectAs(out_sr)
                fp = proj.firstPoint
                reprojected.append((fp.X, fp.Y, z, ts))  # Z preserved as-is
            logger.info("Reprojected %d points to %s", len(reprojected), self.output_spatial_ref)
            return reprojected

        except ImportError:
            logger.warning("arcpy unavailable — reprojection skipped (non-production)")
            return points

    def _filter_to_aoi(self, points: list[PointRecord]) -> list[PointRecord]:
        """
        Remove points that fall outside the configured AOI polygon.

        Queries *aoi_feature_class* using *aoi_where_clause* to obtain the
        boundary geometry, then tests each point for containment.
        """
        logger = get_logger(__name__)
        try:
            import arcpy  # noqa: PLC0415

            out_sr = arcpy.SpatialReference(self.output_spatial_ref)
            aoi_geom = None

            with arcpy.da.SearchCursor(
                self.aoi_feature_class,
                ["SHAPE@"],
                where_clause=self.aoi_where_clause or None,
            ) as cursor:
                for (shape,) in cursor:
                    aoi_geom = shape
                    break

            if aoi_geom is None:
                logger.warning("No AOI geometry found — skipping AOI filter")
                return points

            before = len(points)
            filtered = [
                (x, y, z, ts)
                for x, y, z, ts in points
                if aoi_geom.contains(arcpy.PointGeometry(arcpy.Point(x, y), out_sr))
            ]
            logger.info(
                "AOI filter: kept %d / %d points inside %s",
                len(filtered), before, self.aoi_where_clause,
            )
            return filtered

        except ImportError:
            logger.warning("arcpy unavailable — AOI filter skipped (non-production)")
            return points

    def _write_csv(self, points: list[PointRecord], path: Path) -> None:
        """Write point records to a CSV file with X, Y, Z, TIMESTAMP columns."""
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["X", "Y", "Z", "TIMESTAMP"])
            writer.writerows(points)

    def _build_config(
        self,
        csv_path: str,
        snippet_count: int,
        total_points: int,
        valid_points: int,
        timestamp: str,
    ) -> dict[str, Any]:
        """Return the processing metadata dict written to config.json."""
        return {
            "site": self.site,
            "timestamp": timestamp,
            "csvPath": csv_path,
            "sourceFiles": {
                "snippetCount": snippet_count,
                "totalPoints": total_points,
                "validPoints": valid_points,
            },
            "processing": {
                "zAdjustment": self.z_adjustment,
                "maxZ": self.max_z,
                "minNeighbours": self.min_neighbours,
                "inputSpatialReference": self.input_spatial_ref,
                "outputSpatialReference": self.output_spatial_ref,
                "despike": self.despike,
            },
        }
