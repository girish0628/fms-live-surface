"""
Daily merge service.

Finds all hourly TIFFs written to ``output_root`` for a given date
(folders matching ``FMS_<date>*``), mosaics them into a single daily
GeoTIFF using ``arcpy.management.MosaicToNewRaster``, and returns the
output metadata ready for the FME INGEST webhook call.

Output layout:
    <daily_output_root>/
    └── YYYYMMDD_FMS_Daily/
        ├── YYYYMMDD_FMS_Daily.tif
        └── ready.flag
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from src.core.exceptions import DailyMergeError
from src.core.logger import get_logger
from src.utils.naming_utils import current_date_str, daily_survey_name


@dataclass(frozen=True)
class DailyMergeService:
    """
    Mosaic all hourly TIFFs for one date into a single daily TIFF.

    Parameters
    ----------
    output_root : str
        Root folder where hourly ``FMS_<timestamp>`` folders are written.
    daily_output_root : str
        Destination folder for the daily merged TIFF.
    run_date : str
        YYYYMMDD date to process.  Defaults to today if empty.
    coordinate_system_wkt : str
        WKT of the output spatial reference passed to MosaicToNewRaster.
        If empty, arcpy inherits from the first input raster.
    cell_size : int
        Output raster cell size in metres (should match hourly cell size).
    """

    output_root: str
    daily_output_root: str
    run_date: str = ""
    coordinate_system_wkt: str = ""
    cell_size: int = 2

    def merge(self) -> dict[str, Any]:
        """
        Run the daily merge.

        Returns
        -------
        dict
            status, survey_name, date, daily_tiff_path, daily_folder,
            tiffs_merged, acquisition_date (YYYYMMDDHHMMSS).

        Raises
        ------
        DailyMergeError
            If no hourly TIFFs are found or the arcpy mosaic fails.
        """
        logger = get_logger(__name__)
        date = self.run_date or current_date_str()
        survey = daily_survey_name(date)
        daily_folder = Path(self.daily_output_root) / survey

        logger.info("=" * 60)
        logger.info("Daily Merge — date=%s  survey=%s", date, survey)
        logger.info("=" * 60)

        tiff_files = self._collect_hourly_tiffs(date)
        if not tiff_files:
            raise DailyMergeError(
                f"No hourly TIFFs found for date {date} under {self.output_root}. "
                "Ensure all hourly jobs completed before running the daily merge."
            )
        logger.info("Collected %d hourly TIFFs to merge:", len(tiff_files))
        for t in tiff_files:
            logger.info("  %s", t)

        daily_folder.mkdir(parents=True, exist_ok=True)
        output_tiff = str(daily_folder / f"{survey}.tif")

        # Check idempotency — skip if already merged
        if Path(output_tiff).exists():
            logger.warning(
                "Daily TIFF already exists — skipping mosaic: %s", output_tiff
            )
        else:
            self._mosaic_tiffs(tiff_files, output_tiff)
            logger.info("Daily TIFF written: %s", output_tiff)

        flag = daily_folder / "ready.flag"
        flag.write_text(
            f"ready\nsurvey={survey}\ndate={date}\ntiffs_merged={len(tiff_files)}\n",
            encoding="utf-8",
        )

        # acquisition_date = midnight of the run date (YYYYMMDDHHMMSS)
        acquisition_date = f"{date}000000"

        result: dict[str, Any] = {
            "status": "SUCCESS",
            "survey_name": survey,
            "date": date,
            "daily_tiff_path": output_tiff,
            "daily_folder": str(daily_folder),
            "tiffs_merged": len(tiff_files),
            "acquisition_date": acquisition_date,
        }
        logger.info("Daily Merge complete: %s", result)
        return result

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _collect_hourly_tiffs(self, date: str) -> list[Path]:
        """
        Return all *.tif files inside ``FMS_<date>*`` sub-folders of
        ``output_root``, sorted for deterministic mosaic ordering.
        """
        root = Path(self.output_root)
        tiffs: list[Path] = []
        for folder in sorted(root.glob(f"FMS_{date}*")):
            if folder.is_dir():
                # Exclude any previously generated daily TIFFs that were
                # accidentally written under the same root.
                if "Daily" not in folder.name:
                    tiffs.extend(sorted(folder.glob("*.tif")))
        return tiffs

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
            raise DailyMergeError(
                f"arcpy MosaicToNewRaster failed: {exc}"
            ) from exc
