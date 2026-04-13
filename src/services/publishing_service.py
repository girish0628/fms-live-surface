"""
Publishing integration service.

Hands off the processed FMS outputs (raster + boundary) to the existing
enterprise publishing solution.

Supports two integration modes (configured via ``integration_mode``):

  file_trigger (default)
    FMS writes outputs and a ready.flag; the publishing solution polls for
    the flag.  This service verifies the flag exists and optionally waits
    for a completion signal.

  direct_api
    FMS calls the existing publishing solution directly via a Python API
    (function call or REST endpoint).  Provides immediate feedback.

The SDE mosaic dataset operations (AddRastersToMosaicDataset etc.) are
performed by the *existing publishing solution*, not this service.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from src.core.exceptions import PublishingError
from src.core.logger import get_logger


@dataclass(frozen=True)
class PublishingService:
    """
    Trigger the existing enterprise publishing solution.

    Parameters
    ----------
    site : str
        Mine site code (WB, ER, TG, JB, NM).
    output_dir : str
        Timestamped output folder written by OutputHandlerService.
    integration_mode : str
        ``"file_trigger"`` or ``"direct_api"``.
    publishing_api_module : str
        Dotted Python module path of the existing publishing solution's
        entry point (used when *integration_mode* is ``"direct_api"``).
        E.g. ``"mtd.publishing.publish_to_mosaic"``.
    api_timeout : int
        Seconds to wait for the direct API call to complete.
    poll_interval : int
        Seconds between polls when waiting for a completion signal file
        (``done.flag``) in file-trigger mode.
    poll_timeout : int
        Maximum seconds to wait for ``done.flag`` before raising an error.
    """

    site: str
    output_dir: str
    integration_mode: str = "file_trigger"
    publishing_api_module: str = ""
    api_timeout: int = 300
    poll_interval: int = 30
    poll_timeout: int = 600

    def trigger(self) -> dict[str, Any]:
        """
        Trigger the downstream publishing solution.

        Returns
        -------
        dict[str, Any]
            ``status``, ``site``, ``integration_mode``, ``output_dir``.

        Raises
        ------
        PublishingError
            If the publishing handoff fails or times out.
        """
        logger = get_logger(__name__)
        logger.info(
            "Publishing trigger — site: %s  mode: %s", self.site, self.integration_mode
        )

        if self.integration_mode == "file_trigger":
            return self._file_trigger()
        elif self.integration_mode == "direct_api":
            return self._direct_api()
        else:
            raise PublishingError(
                f"Unknown integration_mode: {self.integration_mode!r}. "
                "Expected 'file_trigger' or 'direct_api'."
            )

    # ------------------------------------------------------------------
    # Option B: File-based trigger
    # ------------------------------------------------------------------

    def _file_trigger(self) -> dict[str, Any]:
        """
        Verify ready.flag exists and optionally wait for done.flag.

        The publishing solution is responsible for detecting ready.flag,
        processing the raster, and writing done.flag on completion.
        """
        logger = get_logger(__name__)
        output_path = Path(self.output_dir)
        flag_path = output_path / "ready.flag"
        done_path = output_path / "done.flag"
        metadata_path = output_path / "metadata.json"

        if not flag_path.exists():
            raise PublishingError(
                f"ready.flag not found in {self.output_dir}. "
                "Ensure OutputHandlerService ran successfully."
            )

        logger.info("ready.flag confirmed: %s", flag_path)

        # Read metadata to extract raster/boundary paths for logging
        metadata: dict[str, Any] = {}
        if metadata_path.exists():
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))

        raster = metadata.get("output", {}).get("rasterPath", "")
        boundary = metadata.get("output", {}).get("boundaryPath", "")
        logger.info("Raster available for publishing: %s", raster)
        logger.info("Boundary available for publishing: %s", boundary)

        # Optionally wait for the publishing solution to signal completion
        if self.poll_timeout > 0:
            logger.info(
                "Waiting up to %d s for done.flag (poll every %d s)",
                self.poll_timeout, self.poll_interval,
            )
            elapsed = 0
            while elapsed < self.poll_timeout:
                if done_path.exists():
                    logger.info("done.flag detected — publishing complete")
                    return self._success_result("file_trigger", published=True)
                time.sleep(self.poll_interval)
                elapsed += self.poll_interval
                logger.debug("Waiting for done.flag… elapsed %d s", elapsed)

            raise PublishingError(
                f"Timed out after {self.poll_timeout} s waiting for done.flag in {self.output_dir}"
            )

        # No wait configured — fire-and-forget
        logger.info(
            "File-trigger handoff complete (fire-and-forget, poll_timeout=0)"
        )
        return self._success_result("file_trigger", published=False)

    # ------------------------------------------------------------------
    # Option A: Direct Python API call
    # ------------------------------------------------------------------

    def _direct_api(self) -> dict[str, Any]:
        """
        Call the existing publishing solution's Python API directly.

        Dynamically imports *publishing_api_module* and calls
        ``publish_to_mosaic(raster_path, boundary_path, config)``.
        """
        logger = get_logger(__name__)

        if not self.publishing_api_module:
            raise PublishingError(
                "publishing_api_module must be set when integration_mode='direct_api'"
            )

        metadata_path = Path(self.output_dir) / "metadata.json"
        if not metadata_path.exists():
            raise PublishingError(f"metadata.json not found in {self.output_dir}")

        metadata: dict[str, Any] = json.loads(metadata_path.read_text(encoding="utf-8"))
        raster_path = metadata["output"]["rasterPath"]
        boundary_path = metadata["output"]["boundaryPath"]

        logger.info(
            "Calling publishing API: %s  raster=%s",
            self.publishing_api_module, raster_path,
        )

        try:
            import importlib  # noqa: PLC0415

            # Module path format: "package.module.function"
            parts = self.publishing_api_module.rsplit(".", 1)
            if len(parts) != 2:
                raise PublishingError(
                    f"publishing_api_module must be 'module.function', got: {self.publishing_api_module}"
                )
            module = importlib.import_module(parts[0])
            publish_fn = getattr(module, parts[1])

            result = publish_fn(
                raster_path=raster_path,
                boundary_path=boundary_path,
                config=metadata,
                timeout=self.api_timeout,
            )
            logger.info("Publishing API result: %s", result)
            return self._success_result("direct_api", published=True)

        except PublishingError:
            raise
        except Exception as exc:
            logger.error("Publishing API call failed", exc_info=True)
            raise PublishingError(
                f"Direct API call to {self.publishing_api_module} failed"
            ) from exc

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _success_result(self, mode: str, published: bool) -> dict[str, Any]:
        return {
            "status": "SUCCESS",
            "site": self.site,
            "integration_mode": mode,
            "output_dir": self.output_dir,
            "published": published,
        }
