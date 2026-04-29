"""
Daily cleanup service.

Queries the MTD mosaic dataset for items tagged ``SITE = 'Hourly'`` and
returns them as a list of survey descriptors ready to be passed to the
FME DELETE webhook.

This service only reads from the mosaic dataset; it does NOT delete
anything itself.  Deletion is performed by the FME workflow after the
DELETE webhook is called.

Typical call sequence (daily_cleanup_runner):
    1. DailyCleanupService.run()  → get list of hourly surveys
    2. FmeWebhookClient.delete()  → ask FME to remove them
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.core.exceptions import DailyCleanupError
from src.core.logger import get_logger


@dataclass(frozen=True)
class DailyCleanupService:
    """
    Identify hourly surveys in the mosaic dataset for FME deletion.

    Parameters
    ----------
    mosaic_dataset_path : str
        Full ArcGIS path to the mosaic dataset, e.g.
        ``//server/GDB/mtd.gdb/FMS_Mosaic`` or an SDE connection path.
    survey_name_field : str
        Field in the mosaic dataset that stores the survey name
        (used as-is in the FME DELETE payload).
    site_field : str
        Field used to distinguish hourly vs daily items.
    site_value : str
        Value that marks hourly surveys (default: ``"Hourly"``).
    """

    mosaic_dataset_path: str
    survey_name_field: str = "Name"
    site_field: str = "SITE"
    site_value: str = "Hourly"

    def get_hourly_surveys(self) -> list[dict[str, str]]:
        """
        Query the mosaic dataset and return all hourly survey entries.

        Returns
        -------
        list[dict[str, str]]
            Each dict: ``{"survey_name": "...", "capture_method": "FMS"}``

        Raises
        ------
        DailyCleanupError
            If arcpy is unavailable or the cursor query fails.
        """
        logger = get_logger(__name__)
        logger.info(
            "Querying mosaic dataset — %s = '%s': %s",
            self.site_field, self.site_value, self.mosaic_dataset_path,
        )

        try:
            import arcpy
        except ImportError:
            raise DailyCleanupError(
                "arcpy is required for DailyCleanupService. "
                "Run inside ArcGIS Pro Python environment."
            )

        where_clause = f"{self.site_field} = '{self.site_value}'"
        surveys: list[dict[str, str]] = []

        try:
            with arcpy.da.SearchCursor(
                self.mosaic_dataset_path,
                [self.survey_name_field],
                where_clause=where_clause,
            ) as cursor:
                for row in cursor:
                    name = row[0]
                    if name:
                        surveys.append({
                            "survey_name": str(name),
                            "capture_method": "FMS",
                        })
        except Exception as exc:
            raise DailyCleanupError(
                f"Mosaic dataset query failed ({self.mosaic_dataset_path}): {exc}"
            ) from exc

        logger.info(
            "Found %d hourly surveys in mosaic dataset", len(surveys)
        )
        for s in surveys:
            logger.debug("  %s", s["survey_name"])
        return surveys

    def run(self) -> dict[str, Any]:
        """
        Convenience wrapper — returns a result dict for runner consumption.

        Returns
        -------
        dict
            status (SUCCESS | NO_SURVEYS), surveys, count, mosaic_dataset.
        """
        surveys = self.get_hourly_surveys()
        return {
            "status": "SUCCESS" if surveys else "NO_SURVEYS",
            "surveys": surveys,
            "count": len(surveys),
            "mosaic_dataset": self.mosaic_dataset_path,
        }
