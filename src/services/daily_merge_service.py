"""
Daily merge service.

Finds all hourly output folders for a given date (``FMS_<date>HH0000/``),
mosaics per-site TIFFs into per-site daily TIFFs, merges all per-site
boundary shapefiles into a single dissolved daily boundary, and writes the
results to a ``FMS_<date>/`` folder under ``output_root``.

Output layout:
    <output_root>/
    └── FMS_<YYYYMMDD>/
        ├── FMS_<YYYYMMDD>_<SITE>.tif   ← one per mine site
        └── FMS_<YYYYMMDD>_boundary.shp ← dissolved union of all site boundaries
"""
from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from src.core.exceptions import DailyMergeError
from src.core.logger import get_logger
from src.utils.naming_utils import current_date_str, daily_folder_name, daily_survey_name


@dataclass(frozen=True)
class DailyMergeService:
    """
    Mosaic hourly TIFFs per site into daily TIFFs and merge all boundaries.

    Parameters
    ----------
    output_root : str
        Root folder containing both hourly ``FMS_<YYYYMMDDHH0000>`` folders
        and the daily ``FMS_<YYYYMMDD>`` output folder.
    run_date : str
        YYYYMMDD date to process.  Defaults to today if empty.
    coordinate_system_wkt : str
        WKT passed to MosaicToNewRaster.  If empty, inherited from first raster.
    cell_size : int
        Output raster cell size in metres (should match hourly cell size).
    """

    output_root: str
    run_date: str = ""
    coordinate_system_wkt: str = ""
    cell_size: int = 2

    def merge(self) -> dict[str, Any]:
        """
        Run the daily merge.

        Returns
        -------
        dict
            status, survey_name, date, daily_folder, sites_merged,
            tiffs_merged, daily_boundary, acquisition_date.

        Raises
        ------
        DailyMergeError
            If no hourly TIFFs are found or an arcpy operation fails.
        """
        logger = get_logger(__name__)
        date = self.run_date or current_date_str()
        survey = daily_survey_name(date)          # FMS_YYYYMMDD
        folder_name = daily_folder_name(date)     # FMS_YYYYMMDD (same)
        daily_folder = Path(self.output_root) / folder_name

        logger.info("=" * 60)
        logger.info("Daily Merge — date=%s  survey=%s", date, survey)
        logger.info("=" * 60)

        # Collect per-site hourly TIFFs
        site_tiffs = self._collect_site_tiffs(date)
        if not site_tiffs:
            raise DailyMergeError(
                f"No hourly TIFFs found for date {date} under {self.output_root}. "
                "Ensure all hourly jobs completed before running the daily merge."
            )

        for site, tiffs in site_tiffs.items():
            logger.info("Site %s: %d hourly TIFFs to merge", site, len(tiffs))
            for t in tiffs:
                logger.info("  %s", t)

        daily_folder.mkdir(parents=True, exist_ok=True)

        # Mosaic per-site TIFFs
        daily_tiff_paths: dict[str, str] = {}
        for site, tiffs in sorted(site_tiffs.items()):
            output_tiff = str(daily_folder / f"FMS_{date}_{site}.tif")
            if Path(output_tiff).exists():
                logger.warning("Daily TIFF already exists — skipping: %s", output_tiff)
            else:
                self._mosaic_tiffs(tiffs, output_tiff)
                logger.info("Daily TIFF written: %s", output_tiff)
            daily_tiff_paths[site] = output_tiff

        # Merge all per-site boundary shapefiles into one dissolved daily boundary
        boundary_shps = self._collect_boundary_shps(date)
        daily_boundary = str(daily_folder / f"FMS_{date}_boundary.shp")
        if boundary_shps:
            if Path(daily_boundary).exists():
                logger.warning("Daily boundary already exists — skipping: %s", daily_boundary)
            else:
                self._merge_boundaries(boundary_shps, daily_boundary)
                logger.info("Daily boundary written: %s", daily_boundary)
        else:
            logger.warning("No per-site boundary SHPs found — daily boundary skipped")
            daily_boundary = ""

        tiffs_merged = sum(len(t) for t in site_tiffs.values())
        result: dict[str, Any] = {
            "status": "SUCCESS",
            "survey_name": survey,
            "date": date,
            "daily_folder": str(daily_folder),
            "sites_merged": sorted(site_tiffs.keys()),
            "tiffs_merged": tiffs_merged,
            "daily_boundary": daily_boundary,
            "acquisition_date": f"{date}000000",
        }
        logger.info("Daily Merge complete: %s", result)
        return result

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _collect_site_tiffs(self, date: str) -> dict[str, list[Path]]:
        """
        Scan hourly folders ``FMS_<date>HH0000/`` for per-site TIF files.

        Returns {site_code: [sorted tiff paths]}.
        """
        root = Path(self.output_root)
        site_tiffs: dict[str, list[Path]] = {}
        # Pattern matches FMS_YYYYMMDDHH0000 (6 trailing chars for HH0000)
        for folder in sorted(root.glob(f"FMS_{date}??????")):
            if not folder.is_dir():
                continue
            for tiff in sorted(folder.glob("*.tif")):
                # Filename: FMS_YYYYMMDDHH0000_SITE.tif
                parts = tiff.stem.split("_")
                if len(parts) >= 3:
                    site = parts[-1]
                    site_tiffs.setdefault(site, []).append(tiff)
        return site_tiffs

    def _collect_boundary_shps(self, date: str) -> list[Path]:
        """
        Collect all per-site boundary SHPs from Source/ subfolders of
        hourly ``FMS_<date>HH0000/`` folders.
        """
        root = Path(self.output_root)
        shps: list[Path] = []
        for folder in sorted(root.glob(f"FMS_{date}??????")):
            if not folder.is_dir():
                continue
            source = folder / "Source"
            if source.is_dir():
                shps.extend(sorted(source.glob("FMS_*_boundary_*.shp")))
        return shps

    def _mosaic_tiffs(self, tiff_files: list[Path], output_tiff: str) -> None:
        try:
            import arcpy
        except ImportError:
            raise DailyMergeError(
                "arcpy is required for DailyMergeService. "
                "Run inside ArcGIS Pro Python environment."
            )

        logger = get_logger(__name__)
        out_path = Path(output_tiff)

        try:
            arcpy.env.overwriteOutput = True
            arcpy.management.MosaicToNewRaster(
                input_rasters=";".join(str(t) for t in tiff_files),
                output_location=str(out_path.parent),
                raster_dataset_name_with_extension=out_path.name,
                coordinate_system_for_the_raster=self.coordinate_system_wkt or None,
                pixel_type="32_BIT_FLOAT",
                cellsize=self.cell_size,
                number_of_bands=1,
                mosaic_method="MEAN",
                mosaic_colormap_mode="FIRST",
            )
            logger.debug("MosaicToNewRaster completed → %s", output_tiff)
        except Exception as exc:
            raise DailyMergeError(f"arcpy MosaicToNewRaster failed: {exc}") from exc

    def _merge_boundaries(self, boundary_shps: list[Path], output_shp: str) -> None:
        try:
            import arcpy
        except ImportError:
            raise DailyMergeError(
                "arcpy is required for boundary merge. "
                "Run inside ArcGIS Pro Python environment."
            )

        logger = get_logger(__name__)
        out_path = Path(output_shp)

        arcpy.env.overwriteOutput = True

        merged_tmp = str(out_path.parent / f"{out_path.stem}_tmp.shp")
        logger.info("Merging %d boundary SHPs → %s", len(boundary_shps), merged_tmp)
        arcpy.Merge_management([str(p) for p in boundary_shps], merged_tmp)

        logger.info("Dissolving → %s", output_shp)
        arcpy.Dissolve_management(merged_tmp, output_shp)

        for ext in (".shp", ".dbf", ".shx", ".prj", ".cpg"):
            p = Path(merged_tmp.replace(".shp", ext))
            if p.exists():
                p.unlink()
