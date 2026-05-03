"""Survey naming conventions for FMS Live Surface."""
from __future__ import annotations

import os
from datetime import datetime

SURVEY_TAG = "FMS"


def to_hourly_ts(run_timestamp: str) -> str:
    """
    Normalize any run timestamp to YYYYMMDDHH0000 (hour-level granularity).

    Accepts 12-char (YYYYMMDDHHMM) or 14-char (YYYYMMDDHHMMSS) input.
    Minutes and seconds are zeroed so that all parallel site processes in one
    Jenkins hourly run share the same output folder name.
    """
    return run_timestamp[:10] + "0000"


def hourly_survey_name(run_timestamp: str) -> str:
    """
    Canonical survey name for one hourly run (all sites merged).

    Format: YYYYMMDDHH0000_FMS  (e.g. 20260503110000_FMS)
    Input is normalised to hour-level granularity via to_hourly_ts().
    """
    return f"{to_hourly_ts(run_timestamp)}_{SURVEY_TAG}"


def daily_survey_name(date: str | datetime | None = None) -> str:
    """
    Canonical survey name for a daily merged output.

    Format: FMS_YYYYMMDD  (e.g. FMS_20260503)
    """
    if date is None:
        date = datetime.now()
    if isinstance(date, datetime):
        date = date.strftime("%Y%m%d")
    return f"{SURVEY_TAG}_{date}"


def output_folder_name(run_timestamp: str) -> str:
    """Hourly output folder name: FMS_<YYYYMMDDHH0000>."""
    return f"FMS_{to_hourly_ts(run_timestamp)}"


def daily_folder_name(date: str | datetime | None = None) -> str:
    """Daily output folder name: FMS_<YYYYMMDD>."""
    if date is None:
        date = datetime.now()
    if isinstance(date, datetime):
        date = date.strftime("%Y%m%d")
    return f"FMS_{date}"


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
