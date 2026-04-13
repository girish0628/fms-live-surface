"""
Minestar snippet file (.snp) conversion service.

Converts proprietary Minestar .snp files to MGA50-projected CSV files
suitable for downstream raster generation.

Processing pipeline:
    Parse .snp → Z adjustment → Noise filter → Despike → Reproject → AOI filter → CSV

Binary format (per Minestar specification):
    - Magic number at byte 0 (uint32 = 201339251)
    - Records located by scanning for marker byte 0x0B (11) starting at offset 20
    - Each record: 10 × uint32 (little-endian) = 40 bytes
        [0] X, [1] Y, [2] Z1, [3] T1, [4] Z2, [5] T2, [6] Z3, [7] T3, [8] Z4, [9] T4
    - All XYZ values scaled by cScaleFactor (0.01) to convert from integer to metres
    - Each record yields up to 4 point readings at the same XY location
"""
from __future__ import annotations

import csv
import json
import struct
from dataclasses import dataclass
from math import ceil, floor
from pathlib import Path
from typing import Any

import numpy

from src.core.exceptions import SnippetConversionError
from src.core.logger import get_logger

# Minestar binary format constants
_MAGIC_NUMBER = 201339251
_MARKER_BYTE = 11
_SCALE_FACTOR = 0.01
_RECORD_FORMAT = "LLLLLLLLLL"   # 10 × uint32, 40 bytes
_RECORD_SIZE = 40
_SCAN_START = 20                # first possible marker byte offset

# Type alias: key = "X_Y", value = point attribute dict
_PointsDict = dict[str, dict[str, Any]]

# Type alias for projected point list
PointRecord = tuple[float, float, float, int]  # x, y, z, timestamp


def _float_round(num: float, places: int = 0, direction=floor) -> float:
    return direction(num * (10 ** places)) / float(10 ** places)


@dataclass(frozen=True)
class SnippetConversionService:
    """
    Convert Minestar .snp files to a single MGA50 CSV for a mine site.

    Parameters
    ----------
    site : str
        Mine site code (WB, ER, TG, MAC, etc.).
    input_folder : str
        Folder containing .snp files from GIP landing zone.
    output_folder : str
        Staging folder root; output is written to
        ``<output_folder>/<site>/<timestamp>/``.
    z_adjustment : float
        Additive Z datum adjustment converting ADPH to AHD (metres).
    min_neighbours : int
        Minimum number of grid neighbours required to retain a point.
    max_z : float
        Maximum valid Z value; points above this are discarded.
    decimal_digits : int
        Number of decimal places to round XYZ coordinates to.
    grid_size : int
        Grid cell size in metres; used to locate neighbouring cells in despike.
    input_spatial_ref : str
        Path to .prj file for the input CRS of .snp coordinates.
    output_spatial_ref : str
        Path to .prj file for the target CRS (MGA50 / GDA2020).
    aoi_feature_class : str
        Optional path to AOI polygon feature class for spatial filtering.
    aoi_where_clause : str
        SQL WHERE clause applied when querying *aoi_feature_class*.
    despike : bool
        Whether to apply the grid-based despiking algorithm (3 passes).
    """

    site: str
    input_folder: str
    output_folder: str
    z_adjustment: float = 3.155
    min_neighbours: int = 3
    max_z: float = 4000.0
    decimal_digits: int = 2
    grid_size: int = 2
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
        import datetime

        logger = get_logger(__name__)
        logger.info("=" * 60)
        logger.info("Snippet conversion started — site: %s", self.site)

        try:
            snp_files = sorted(Path(self.input_folder).glob("*.snp"))
            logger.info("Found %d snippet files in %s", len(snp_files), self.input_folder)

            # Points dict: key="X_Y", value={'X', 'Y', 'Z', 'Timestamp'}
            points: _PointsDict = {}

            for snp_file in snp_files:
                try:
                    self._process_snippet_file(snp_file, points)
                except Exception as exc:
                    logger.warning("Error processing %s: %s", snp_file.name, exc)
                    # Swallow per-file errors — invalid files must not abort the run

            raw_count = len(points)
            logger.info("Total unique XY points parsed: %d", raw_count)

            # Apply Z datum adjustment
            for pt in points.values():
                pt["Z"] += self.z_adjustment

            # Filter points above max_z
            before = len(points)
            points = {k: v for k, v in points.items() if v["Z"] <= self.max_z}
            logger.debug("Noise filter: removed %d points (Z > %.1f)", before - len(points), self.max_z)

            # Grid-based despike (3 passes, matching reference implementation)
            if self.despike:
                logger.info("Despiking ...")
                for _ in range(3):
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

            csv_path = output_dir / f"{self.site}_points.csv"
            self._write_csv(point_list, csv_path)
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

    def _validate_snippet_file(self, buffer: bytes) -> bool:
        """Return True if the buffer has the expected Minestar magic number and marker byte."""
        if len(buffer) <= 36:
            return False
        magic_ok = struct.unpack_from("L", buffer, 0)[0] == _MAGIC_NUMBER
        marker_ok = (
            struct.unpack_from("B", buffer, 31)[0] == _MARKER_BYTE
            or struct.unpack_from("B", buffer, 36)[0] == _MARKER_BYTE
        )
        return magic_ok and marker_ok

    def _process_snippet_file(self, snp_file: Path, points: _PointsDict) -> None:
        """
        Parse one .snp binary file and merge results into *points*.

        Scans the file byte-by-byte from offset 20 looking for the 0x0B
        marker byte. Each marker introduces a 40-byte record of 10 uint32
        values: X, Y, Z1, T1, Z2, T2, Z3, T3, Z4, T4.
        Coordinates are scaled by 0.01 to convert from integer to metres.
        When the same XY already exists, the Z is averaged and the entry
        is kept if the new timestamp is >= the existing one.
        """
        if snp_file.stat().st_size < 36:
            return

        buffer = snp_file.read_bytes()
        if not self._validate_snippet_file(buffer):
            get_logger(__name__).debug("Not a valid snippet file, skipping: %s", snp_file.name)
            return

        buf_len = len(buffer)
        index = _SCAN_START

        while index < buf_len:
            if struct.unpack_from("b", buffer, index)[0] == _MARKER_BYTE:
                offset = index + 1
                if offset + _RECORD_SIZE > buf_len:
                    break

                item = struct.unpack_from(_RECORD_FORMAT, buffer, offset)
                parsed = self._parse_record(item, snp_file.name)

                for pt in parsed:
                    key = f"{pt['X']}_{pt['Y']}"
                    if key in points:
                        if points[key]["Timestamp"] <= pt["Timestamp"]:
                            pt["Z"] = float(numpy.average([pt["Z"], points[key]["Z"]]))
                            points[key] = pt
                    else:
                        points[key] = pt

                index = offset + _RECORD_SIZE - 1  # jump past the record

            index += 1

    def _parse_record(self, item: tuple, filename: str) -> list[dict[str, Any]]:
        """
        Parse one 10-uint32 record into up to 4 point dicts.

        Layout: X, Y, Z1, T1, Z2, T2, Z3, T3, Z4, T4
        Points with X<=0, Y<=0, Z==0, or T==0 are dropped.
        """
        dd = self.decimal_digits
        x = _float_round(item[0] * _SCALE_FACTOR, dd, ceil)
        y = _float_round(item[1] * _SCALE_FACTOR, dd, ceil)
        if x <= 0 or y <= 0:
            return []

        result = []
        for z_idx, t_idx in ((2, 3), (4, 5), (6, 7), (8, 9)):
            z = _float_round(item[z_idx] * _SCALE_FACTOR, dd, ceil)
            t = item[t_idx]
            if z < self.max_z and t != 0 and z != 0:
                # Dedup within the same record by (Z, Timestamp)
                if not any(p["Z"] == z and p["Timestamp"] == t for p in result):
                    result.append({"X": x, "Y": y, "Z": z, "Timestamp": t, "Filename": filename})
        return result

    def _despike_pass(self, points: _PointsDict) -> list[str]:
        """
        One pass of the grid-based despike algorithm.

        For each point, finds up to 8 neighbouring cells at ±grid_size
        distance. Points with fewer than min_neighbours are flagged for
        removal. Points whose Z deviates from the neighbour median by more
        than the neighbour std-dev have their Z replaced with the median.

        Returns list of keys flagged for removal (inadequate neighbours).
        """
        gs = self.grid_size
        inadequate: list[str] = []

        def neighbour_coords(x: float, y: float) -> list[tuple[float, float]]:
            return [
                (x + gs, y - gs), (x + gs, y), (x + gs, y + gs),
                (x - gs, y - gs), (x - gs, y), (x - gs, y + gs),
                (x,      y + gs), (x,      y - gs),
            ]

        def estimate_z(x: float, y: float, z: float) -> float | None:
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
            new_z = estimate_z(pt["X"], pt["Y"], pt["Z"])
            if new_z is not None:
                pt["Z"] = new_z

        return inadequate

    @staticmethod
    def _load_spatial_ref(prj_path: str):
        """
        Load an arcpy SpatialReference from a .prj file path.

        Reads the WKT directly and uses loadFromString to avoid arcpy's
        createFromFile, which rejects forward-slash UNC paths on Windows.
        """
        import arcpy  # noqa: PLC0415

        wkt = Path(prj_path).read_text(encoding="utf-8").strip()
        sr = arcpy.SpatialReference()
        sr.loadFromString(wkt)
        return sr

    def _reproject(self, points: list[PointRecord]) -> list[PointRecord]:
        """
        Reproject points from input_spatial_ref to output_spatial_ref via arcpy.

        Falls back to returning points unchanged when arcpy is unavailable
        (e.g. during unit testing without an ArcGIS licence).
        """
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
            logger.info("Reprojected %d points to %s", len(reprojected), self.output_spatial_ref)
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
                "gridSize": self.grid_size,
                "inputSpatialReference": self.input_spatial_ref,
                "outputSpatialReference": self.output_spatial_ref,
                "despike": self.despike,
            },
        }
