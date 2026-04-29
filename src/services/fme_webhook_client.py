"""
FME Server webhook client for FMS Live Surface.

Handles two distinct webhook operations:

  INGEST  — uploads a processed TIFF survey into the MTD mosaic dataset.
             Called by fms_finalize_runner (hourly, SITE='Hourly') and
             daily_merge_runner (daily, SITE='Daily').

  DELETE  — removes one or more surveys from the mosaic dataset.
             Called by daily_cleanup_runner to purge the previous day's
             hourly surveys after the daily TIFF has been ingested.

Both operations POST to separate FME job submitter endpoints.  The FME
token is read from an OS environment variable (default: ``FME-TOKEN``).
Transient server errors are retried with exponential back-off.

Idempotency:
  The caller is responsible for writing a ``<survey_name>.ingested.flag``
  marker file after a successful call so that Jenkins re-runs do not
  double-ingest the same survey.  See FinalizeRunner / DailyMergeRunner
  for the guard logic.
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any

from src.core.exceptions import PublishingError
from src.core.logger import get_logger


# ---------------------------------------------------------------------------
# Parameter dataclasses
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class IngestParams:
    """
    Full set of FME INGEST webhook parameters (as documented in the
    MTD ingestion webhook spec).

    Parameters that are always NULL for FMS are pre-defaulted; callers
    only need to provide the fields marked with no default.
    """
    tiff_path: str                          # Folder containing the TIFFs
    survey_name: str                        # YYYYMMDDHHMMSS_FMS (or _FMS_Daily)
    acquisition_date: str                   # ISO 8601: YYYYMMDDHHMMSS
    project_extent: str                     # Path to merged boundary shapefile
    site: str                               # "Hourly" or "Daily"
    coordinate_system: str                  # WKT of output spatial reference
    user_email: str
    resolution: str = "2"
    type: str = "Terrain"                   # noqa: A003
    capture_method: str = "FMS"
    surveyor: str = "FMS"
    stages: str = "UPLOAD,PROCESS,INGEST"
    laz_path: str = "NULL"
    vertical_accuracy: str = "NULL"
    laz_classification: str = ""
    pixel_size: str = "NULL"
    no_data: str = "NULL"

    def to_payload(self) -> dict[str, Any]:
        return {
            "TYPE": self.type,
            "TIFF_PATH": self.tiff_path,
            "LAZ_PATH": self.laz_path,
            "SURVEY_NAME": self.survey_name,
            "ACQUISITION_DATE": self.acquisition_date,
            "CAPTURE_METHOD": self.capture_method,
            "SURVEYOR": self.surveyor,
            "RESOLUTION": self.resolution,
            "COORDINATE_SYSTEM": self.coordinate_system,
            "VERTICAL_ACCURACY": self.vertical_accuracy,
            "PROJECT_EXTENT": self.project_extent,
            "SITE": self.site,
            "LAZ_CLASSIFICATION": self.laz_classification,
            "PIXEL_SIZE": self.pixel_size,
            "NO_DATA": self.no_data,
            "STAGES": self.stages,
            "USER_EMAIL": self.user_email,
        }


@dataclass(frozen=True)
class DeleteParams:
    """
    Full set of FME DELETE webhook parameters.

    ``surveys`` is a list of dicts, each with ``survey_name`` and
    ``capture_method``.  Example::

        [{"survey_name": "20260429130000_FMS", "capture_method": "FMS"}]
    """
    surveys: list[dict[str, str]]
    user_email: str
    action: str = "DELETE"
    type: str = "Terrain"                   # noqa: A003
    delete_permanently: str = "FALSE"
    delete_cache: str = "TRUE"
    comments: str = "FMS scheduled deletion"

    def to_payload(self) -> dict[str, Any]:
        return {
            "ACTION": self.action,
            "TYPE": self.type,
            "SURVEYS": json.dumps(self.surveys),
            "DELETE_PERMANENTLY": self.delete_permanently,
            "DELETE_CACHE": self.delete_cache,
            "USER_EMAIL": self.user_email,
            "COMMENTS": self.comments,
        }


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FmeWebhookClient:
    """
    HTTP client for FME Server job submitter endpoints.

    Parameters
    ----------
    ingest_url : str
        Full URL of the FME ingest job submitter endpoint.
    delete_url : str
        Full URL of the FME delete job submitter endpoint.
    token_env_var : str
        Name of the OS environment variable holding the FME token.
    timeout : int
        HTTP request timeout in seconds.
    max_retries : int
        Total attempts (initial + retries) on transient failure.
    retry_delay : float
        Initial delay between retries (doubles on each attempt).
    """

    ingest_url: str
    delete_url: str
    token_env_var: str = "FME-TOKEN"
    timeout: int = 300
    max_retries: int = 3
    retry_delay: float = 5.0

    # ------------------------------------------------------------------
    # Public operations
    # ------------------------------------------------------------------

    def ingest(self, params: IngestParams) -> dict[str, Any]:
        """POST an INGEST request and return the parsed response."""
        logger = get_logger(__name__)
        logger.info(
            "FME INGEST → survey=%s  site=%s  tiff_path=%s",
            params.survey_name, params.site, params.tiff_path,
        )
        logger.debug("INGEST payload:\n%s", json.dumps(params.to_payload(), indent=2))

        if not self.ingest_url:
            raise PublishingError(
                "fme.ingest_url must be set in app_config.yaml for INGEST operations."
            )
        response = self._post_with_retry(self.ingest_url, params.to_payload())
        logger.info("FME INGEST accepted — survey=%s", params.survey_name)
        return {
            "status": "SUCCESS",
            "operation": "INGEST",
            "survey_name": params.survey_name,
            "site": params.site,
            "response": response,
        }

    def delete(self, params: DeleteParams) -> dict[str, Any]:
        """POST a DELETE request and return the parsed response."""
        logger = get_logger(__name__)
        names = [s["survey_name"] for s in params.surveys]
        logger.info("FME DELETE → %d surveys: %s", len(names), names)
        logger.debug("DELETE payload:\n%s", json.dumps(params.to_payload(), indent=2))

        if not self.delete_url:
            raise PublishingError(
                "fme.delete_url must be set in app_config.yaml for DELETE operations."
            )
        response = self._post_with_retry(self.delete_url, params.to_payload())
        logger.info("FME DELETE accepted — %d surveys", len(names))
        return {
            "status": "SUCCESS",
            "operation": "DELETE",
            "surveys_deleted": names,
            "response": response,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _post_with_retry(self, url: str, payload: dict[str, Any]) -> dict[str, Any]:
        """POST *payload* to *url*, retrying on transient errors."""
        token = os.environ.get(self.token_env_var, "")
        if not token:
            raise PublishingError(
                f"FME token not found — set the '{self.token_env_var}' environment variable."
            )

        logger = get_logger(__name__)
        encoded = json.dumps(payload).encode("utf-8")

        last_exc: Exception | None = None
        delay = self.retry_delay

        for attempt in range(1, self.max_retries + 1):
            req = urllib.request.Request(
                url,
                data=encoded,
                method="POST",
                headers={
                    "Authorization": f"fmetoken token={token}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            )
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    body = resp.read().decode("utf-8")
                    logger.info(
                        "FME webhook HTTP %s — attempt %d/%d",
                        resp.status, attempt, self.max_retries,
                    )
                    try:
                        return json.loads(body)
                    except json.JSONDecodeError:
                        return {"raw": body}

            except urllib.error.HTTPError as exc:
                if 400 <= exc.code < 500:
                    # Client errors (bad auth, bad params) are not retryable
                    raise PublishingError(
                        f"FME webhook client error HTTP {exc.code}: {exc.reason}"
                    ) from exc
                last_exc = exc
                logger.warning(
                    "FME webhook HTTP %s — attempt %d/%d, retry in %.0fs",
                    exc.code, attempt, self.max_retries, delay,
                )

            except (urllib.error.URLError, OSError) as exc:
                last_exc = exc
                logger.warning(
                    "FME webhook connection error — attempt %d/%d, retry in %.0fs: %s",
                    attempt, self.max_retries, delay, exc,
                )

            if attempt < self.max_retries:
                time.sleep(delay)
                delay *= 2  # Exponential back-off

        raise PublishingError(
            f"FME webhook at {url} failed after {self.max_retries} attempts: {last_exc}"
        ) from last_exc


# ---------------------------------------------------------------------------
# Factory helper
# ---------------------------------------------------------------------------

def fme_client_from_config(cfg: dict[str, Any]) -> FmeWebhookClient:
    """Construct a FmeWebhookClient from the ``fme`` block of app_config.yaml."""
    fme_cfg = cfg.get("fme", {})
    return FmeWebhookClient(
        ingest_url=fme_cfg.get("ingest_url", ""),
        delete_url=fme_cfg.get("delete_url", ""),
        token_env_var=fme_cfg.get("token_env_var", "FME-TOKEN"),
        timeout=int(fme_cfg.get("timeout", 300)),
        max_retries=int(fme_cfg.get("max_retries", 3)),
        retry_delay=float(fme_cfg.get("retry_delay_seconds", 5.0)),
    )
