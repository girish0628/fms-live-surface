"""Survey naming conventions for FMS Live Surface."""
from __future__ import annotations

import os
from datetime import datetime

SURVEY_TAG = "FMS"


def hourly_survey_name(run_timestamp: str) -> str:
    """
    Canonical survey name for one hourly run (all sites merged).

    Format: YYYYMMDDHHMMSS_FMS  (e.g. 20260429130000_FMS)
    run_timestamp may be 12-char (YYYYMMDDHHMM) or 14-char (YYYYMMDDHHMMSS).
    """
    return f"{run_timestamp}_{SURVEY_TAG}"


def daily_survey_name(date: str | datetime | None = None) -> str:
    """
    Canonical survey name for a daily merged TIFF.

    Format: YYYYMMDD_FMS_Daily  (e.g. 20260429_FMS_Daily)
    """
    if date is None:
        date = datetime.now()
    if isinstance(date, datetime):
        date = date.strftime("%Y%m%d")
    return f"{date}_{SURVEY_TAG}_Daily"


def output_folder_name(run_timestamp: str) -> str:
    """Shared output folder name: FMS_<run_timestamp>."""
    return f"FMS_{run_timestamp}"


def current_run_timestamp() -> str:
    """Return YYYYMMDDHHMMSS for right now."""
    return datetime.now().strftime("%Y%m%d%H%M%S")


def current_date_str() -> str:
    """Return YYYYMMDD for today."""
    return datetime.now().strftime("%Y%m%d")


def run_timestamp_from_env() -> str:
    """
    Read FMS_RUN_TIMESTAMP from environment, or generate one.
    Jenkins sets this once before all parallel site stages fire.
    """
    return os.environ.get("FMS_RUN_TIMESTAMP") or current_run_timestamp()
