"""
Modular Mining CSV conversion service.

Reads Modular Mining equipment CSV files, deduplicates points by XY grid key,
applies Z adjustment, despikes (1 pass), reprojects to MGA50, and writes a
merged output CSV for downstream raster generation.

Processing pipeline:
    Read CSVs → Deduplicate by XY → Z adjustment → Max-Z filter →
    Despike (1 pass) → Reproject → AOI filter → CSV

Column layout (positional, 0-based):
    Configurable per site via csv_col_x / csv_col_y / csv_col_z /
    csv_col_timestamp in app_config.yaml. Defaults match the reference layout:
        col 0: identifier (ignored)
        col 1: X (Easting)
        col 2: Y (Northing)
        col 3: Z (Elevation)
        col 4: Timestamp (datetime string 'YYYY-MM-DD HH:MM:SS.ffffff')
"""
from __future__ import annotations

import datetime
import json
from dataclasses import dataclass
from math import ceil, floor
from pathlib import Path
from typing import Any

import numpy

from src.core.exceptions import ModularCsvError
from src.core.logger import get_logger

_SCALE_FACTOR = 1.0          # Modular CSVs store coordinates in real metres (no scaling needed)

# Type alias: key = "X_Y", value = point attribute dict
_PointsDict = dict[str, dict[str, Any]]

# Type alias for projected point list
PointRecord = tuple[float, float, float, int]  # x, y, z, timestamp


def _float_round(num: float, places: int = 0, direction=floor) -> float:
    return direction(num * (10 ** places)) / float(10 ** places)


def _datetime_to_int(datestring: str) -> int:
    """Convert a datetime string to integer seconds since epoch."""
    try:
        t = datetime.datetime.strptime(str(datestring).strip(), "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        try:
            t = datetime.datetime.strptime(str(datestring).strip(), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return 0
    return int((t - datetime.datetime(1970, 1, 1)).total_seconds())


@dataclass(frozen=True)
class ModularCsvService:
    """
    Convert Modular Mining CSV files to a single MGA50 CSV for a mine site.

    Parameters
    ----------
    site : str
        Mine site code (SF, JB, NWW, etc.).
    input_folder : str
        Folder containing Modular .csv files from the GIP landing zone.
    output_folder : str
        Staging folder root; output written to
        ``<output_folder>/<site>/<timestamp>/``.
    z_adjustment : float
        Additive Z datum adjustment (metres).
    min_neighbours : int
        Minimum grid neighbours required to retain a point during despike.
    max_z : float
        Maximum valid Z value; points above this are discarded.
    decimal_digits : int
        Number of decimal places to round XYZ coordinates to.
    grid_size : int
        Grid cell size in metres; used to locate neighbours in despike.
    csv_col_x : int
        0-based column index for Easting (X) in source CSV.
    csv_col_y : int
        0-based column index for Northing (Y) in source CSV.
    csv_col_z : int
        0-based column index for Elevation (Z) in source CSV.
    csv_col_timestamp : int
        0-based column index for Timestamp in source CSV.
    input_spatial_ref : str
        Path to .prj file for the Modular source CRS.
    output_spatial_ref : str
        Path to .prj file for the target CRS (MGA50 / GDA2020).
    aoi_feature_class : str
        Optional path to AOI polygon feature class for spatial filtering.
    aoi_where_clause : str
        SQL WHERE clause applied when querying *aoi_feature_class*.
    despike : bool
        Whether to apply the grid-based despiking algorithm (1 pass).
    """

    site: str
    input_folder: str
    output_folder: str
    z_adjustment: float = 0.0
    min_neighbours: int = 3
    max_z: float = 4000.0
    decimal_digits: int = 2
    grid_size: int = 2
    csv_col_x: int = 1
    csv_col_y: int = 2
    csv_col_z: int = 3
    csv_col_timestamp: int = 4
    input_spatial_ref: str = ""
    output_spatial_ref: str = ""
    aoi_feature_class: str = ""
    aoi_where_clause: str = ""
    despike: bool = True

    def process(self) -> dict[str, Any]:
        """
        Run the full Modular CSV conversion pipeline.

        Returns
        -------
        dict[str, Any]
            ``status``, ``site``, ``file_count``, ``total_points``,
            ``valid_points``, ``csv_path``, ``config_path``, ``output_dir``.

        Raises
        ------
        ModularCsvError
            If processing fails.
        """
        logger = get_logger(__name__)
        logger.info("=" * 60)
        logger.info("Modular CSV conversion started — site: %s", self.site)

        try:
            csv_files = sorted(Path(self.input_folder).glob("*.csv"))
            logger.info("Found %d Modular CSV files in %s", len(csv_files), self.input_folder)

            # Points dict: key="X_Y", value={'X', 'Y', 'Z', 'Timestamp'}
            points: _PointsDict = {}

            for csv_file in csv_files:
                try:
                    self._process_csv_file(csv_file, points)
                except Exception as exc:
                    logger.warning("Error processing %s: %s", csv_file.name, exc)
                    # Swallow per-file errors — one bad file must not abort the run

            raw_count = len(points)
            logger.info("Total unique XY points parsed: %d", raw_count)

            # Apply Z datum adjustment
            for pt in points.values():
                pt["Z"] += self.z_adjustment

            # Filter points above max_z
            before = len(points)
            points = {k: v for k, v in points.items() if v["Z"] <= self.max_z}
            logger.debug(
                "Noise filter: removed %d points (Z > %.1f)", before - len(points), self.max_z
            )

            # Grid-based despike — 1 pass for Modular (reference runs 1 pass, minestar runs 3)
            if self.despike:
                logger.info("Despiking ...")
                inadequate = self._despike_pass(points)
                for key in inadequate:
                    points.pop(key, None)
                logger.info("Despike complete — %d points remain", len(points))

            # Convert dict to flat list for reprojection
            point_list: list[PointRecord] = [
                (v["X"], v["Y"], v["Z"], v["Timestamp"]) for v in points.values()
            ]

            point_list = self._reproject(point_list)

            if self.aoi_feature_class:
                point_list = self._filter_to_aoi(point_list)

            valid_count = len(point_list)
            logger.info("Valid points after all filters: %d / %d", valid_count, raw_count)

            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
            output_dir = Path(self.output_folder) / self.site / timestamp
            output_dir.mkdir(parents=True, exist_ok=True)

            out_csv = output_dir / f"{self.site}_points.csv"
            self._write_csv(point_list, out_csv)
            logger.info("CSV written: %s", out_csv)

            config = {
                "site": self.site,
                "timestamp": timestamp,
                "source": "Modular",
                "csvPath": str(out_csv),
                "sourceFiles": {
                    "csvCount": len(csv_files),
                    "totalPoints": raw_count,
                    "validPoints": valid_count,
                },
                "processing": {
                    "zAdjustment": self.z_adjustment,
                    "maxZ": self.max_z,
                    "minNeighbours": self.min_neighbours,
                    "gridSize": self.grid_size,
                    "inputSpatialReference": self.input_spatial_ref,
                    "outputSpatialReference": self.output_spatial_ref,
                    "despike": self.despike,
                },
            }
            json_path = output_dir / "config.json"
            json_path.write_text(json.dumps(config, indent=2), encoding="utf-8")
            logger.info("Config JSON written: %s", json_path)

            return {
                "status": "SUCCESS",
                "site": self.site,
                "file_count": len(csv_files),
                "total_points": raw_count,
                "valid_points": valid_count,
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

    def _process_csv_file(self, csv_file: Path, points: _PointsDict) -> None:
        """
        Read one Modular CSV and merge into *points* dict.

        Uses pandas for reading (matches reference implementation).
        Deduplicates on XY key — when the same position is seen again with a
        later timestamp the Z is averaged and the entry is updated.
        """
        import pandas  # noqa: PLC0415

        if csv_file.stat().st_size < 10:
            return

        df = pandas.read_csv(str(csv_file), sep=",")
        data = df.values

        dd = self.decimal_digits
        cx, cy, cz, ct = self.csv_col_x, self.csv_col_y, self.csv_col_z, self.csv_col_timestamp

        for row in data:
            try:
                x = _float_round(float(row[cx]), dd, ceil)
                y = _float_round(float(row[cy]), dd, ceil)
                z = _float_round(float(row[cz]), dd, ceil)
                ts = _datetime_to_int(row[ct])

                if x <= 0 or y <= 0 or z == 0 or z >= self.max_z:
                    continue

                key = f"{x}_{y}"
                if key in points:
                    if ts > points[key]["Timestamp"]:
                        z = float(numpy.average([points[key]["Z"], z]))
                        points[key] = {"X": x, "Y": y, "Z": z, "Timestamp": ts}
                else:
                    points[key] = {"X": x, "Y": y, "Z": z, "Timestamp": ts}

            except (IndexError, ValueError, TypeError):
                continue

    def _despike_pass(self, points: _PointsDict) -> list[str]:
        """
        One pass of the grid-based despike algorithm (same as snippet service).

        Flags points with fewer than min_neighbours for removal.
        Replaces Z with neighbour median when deviation exceeds neighbour std-dev.
        Returns list of keys flagged for removal.
        """
        gs = self.grid_size
        inadequate: list[str] = []

        def neighbour_coords(x: float, y: float) -> list[tuple[float, float]]:
            return [
                (x + gs, y - gs), (x + gs, y), (x + gs, y + gs),
                (x - gs, y - gs), (x - gs, y), (x - gs, y + gs),
                (x,      y + gs), (x,      y - gs),
            ]

        def estimate_z(x: float, y: float, z: float) -> float:
            z_values = []
            for nx, ny in neighbour_coords(x, y):
                nkey = f"{nx}_{ny}"
                if nkey in points:
                    z_values.append(points[nkey]["Z"])

            if len(z_values) < self.min_neighbours:
                inadequate.append(f"{x}_{y}")
            if z_values:
                np_arr = numpy.array(z_values)
                percentile = float(numpy.percentile(np_arr, 50))
                if abs(percentile - z) > float(numpy.std(np_arr)):
                    return percentile
            return z

        for key, pt in points.items():
            pt["Z"] = estimate_z(pt["X"], pt["Y"], pt["Z"])

        return inadequate

    @staticmethod
    def _load_spatial_ref(prj_path: str):
        """Load arcpy SpatialReference via WKT to avoid createFromFile UNC path issues."""
        import arcpy  # noqa: PLC0415

        wkt = Path(prj_path).read_text(encoding="utf-8").strip()
        sr = arcpy.SpatialReference()
        sr.loadFromString(wkt)
        return sr

    def _reproject(self, points: list[PointRecord]) -> list[PointRecord]:
        """Reproject points from input_spatial_ref to output_spatial_ref via arcpy."""
        logger = get_logger(__name__)
        if not self.input_spatial_ref or not self.output_spatial_ref:
            logger.warning("Spatial reference paths not configured — skipping reprojection")
            return points

        try:
            import arcpy  # noqa: PLC0415

            in_sr = self._load_spatial_ref(self.input_spatial_ref)
            out_sr = self._load_spatial_ref(self.output_spatial_ref)

            if in_sr.factoryCode == out_sr.factoryCode:
                logger.debug("Input and output SRS match — skipping reprojection")
                return points

            transformations = arcpy.ListTransformations(in_sr, out_sr)
            transform = transformations[0] if transformations else None
            if transform:
                logger.info("Using datum transformation: %s", transform)
            else:
                logger.warning(
                    "No datum transformation found between '%s' and '%s' — "
                    "projecting without transformation",
                    in_sr.name, out_sr.name,
                )

            null_count = 0
            reprojected: list[PointRecord] = []
            for x, y, z, ts in points:
                try:
                    geom = arcpy.PointGeometry(arcpy.Point(x, y), in_sr)
                    proj = geom.projectAs(out_sr, transform) if transform else geom.projectAs(out_sr)
                    fp = proj.firstPoint if proj else None
                    if fp is None:
                        null_count += 1
                        continue
                    reprojected.append((fp.X, fp.Y, z, ts))
                except Exception:
                    null_count += 1
                    continue

            if null_count:
                logger.warning(
                    "Reprojection: %d / %d points dropped (projection returned null)",
                    null_count, len(points),
                )
            logger.info("Reprojected %d Modular points to %s", len(reprojected), self.output_spatial_ref)
            return reprojected

        except ImportError:
            logger.warning("arcpy unavailable — reprojection skipped (non-production)")
            return points

    def _filter_to_aoi(self, points: list[PointRecord]) -> list[PointRecord]:
        """Remove points that fall outside the configured AOI polygon."""
        logger = get_logger(__name__)
        try:
            import arcpy  # noqa: PLC0415

            out_sr = self._load_spatial_ref(self.output_spatial_ref)
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
        """Write merged, reprojected points to an output CSV."""
        import csv as _csv

        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = _csv.writer(f)
            writer.writerow(["X", "Y", "Z", "TIMESTAMP"])
            writer.writerows(points)
