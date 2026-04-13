"""
Monitoring service — snippet file delivery health check.

Checks that new .snp files have arrived in the GIP landing zone within
the configured threshold.  If files are stale or absent, an alert is
logged and an optional failover action is triggered (copy from PROD share).

Mirrors the existing ``Monitoring Job`` in the Jenkins multijob.
"""
from __future__ import annotations

import smtplib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any

from src.core.exceptions import MonitoringError
from src.core.logger import get_logger


@dataclass(frozen=True)
class MonitoringService:
    """
    Check snippet file delivery freshness for a mine site.

    Parameters
    ----------
    site : str
        Mine site code (WB, ER, TG, JB, NM).
    landing_zone : str
        Path to the GIP landing zone to monitor.
    threshold_minutes : int
        Maximum acceptable age (minutes) of the newest .snp file.
    alert_email : str
        Email address for delivery failure alerts.
    smtp_host : str
        SMTP relay host for outbound email.
    failover_share : str
        Optional path to a PROD file share to copy from on failure.
    """

    site: str
    landing_zone: str
    threshold_minutes: int = 10
    alert_email: str = ""
    smtp_host: str = ""
    failover_share: str = ""

    def check(self) -> dict[str, Any]:
        """
        Verify that recent snippet files are present in the landing zone.

        Returns
        -------
        dict[str, Any]
            ``status`` (``"OK"`` or ``"ALERT"``), ``site``,
            ``newest_file_age_minutes``, ``file_count``, ``alert_sent``.

        Raises
        ------
        MonitoringError
            If the monitoring check itself fails (not for delivery alerts).
        """
        logger = get_logger(__name__)
        logger.info("Monitoring check — site: %s", self.site)

        try:
            landing = Path(self.landing_zone)
            snp_files = list(landing.glob("*.snp"))

            if not snp_files:
                logger.warning("No .snp files found in %s", landing)
                self._handle_alert(
                    f"[FMS ALERT] Site {self.site}: No snippet files found in landing zone"
                )
                return {
                    "status": "ALERT",
                    "site": self.site,
                    "newest_file_age_minutes": None,
                    "file_count": 0,
                    "alert_sent": bool(self.alert_email),
                }

            # Find the newest file by modification time
            newest = max(snp_files, key=lambda p: p.stat().st_mtime)
            now_ts = datetime.now(tz=timezone.utc).timestamp()
            age_minutes = (now_ts - newest.stat().st_mtime) / 60

            logger.info(
                "Newest file: %s  Age: %.1f minutes  Threshold: %d minutes",
                newest.name, age_minutes, self.threshold_minutes,
            )

            if age_minutes > self.threshold_minutes:
                msg = (
                    f"[FMS ALERT] Site {self.site}: Newest snippet file is "
                    f"{age_minutes:.1f} minutes old (threshold {self.threshold_minutes} min). "
                    f"File: {newest.name}"
                )
                logger.warning(msg)
                self._handle_alert(msg)
                self._trigger_failover()
                return {
                    "status": "ALERT",
                    "site": self.site,
                    "newest_file_age_minutes": round(age_minutes, 1),
                    "file_count": len(snp_files),
                    "alert_sent": bool(self.alert_email),
                }

            logger.info(
                "Monitoring OK — %d files present, newest %.1f min old",
                len(snp_files), age_minutes,
            )
            return {
                "status": "OK",
                "site": self.site,
                "newest_file_age_minutes": round(age_minutes, 1),
                "file_count": len(snp_files),
                "alert_sent": False,
            }

        except MonitoringError:
            raise
        except Exception as exc:
            logger.error("Monitoring check failed", exc_info=True)
            raise MonitoringError(
                f"Monitoring check failed for site {self.site}"
            ) from exc

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _handle_alert(self, message: str) -> None:
        """Log the alert and send email if *smtp_host* and *alert_email* are set."""
        logger = get_logger(__name__)
        logger.error("ALERT: %s", message)

        if not (self.smtp_host and self.alert_email):
            logger.debug("No SMTP config — alert email not sent")
            return

        try:
            msg = EmailMessage()
            msg["Subject"] = f"[FMS ALERT] Site {self.site} — snippet file delivery issue"
            msg["From"] = f"fms-monitor@waio.bhp.com"
            msg["To"] = self.alert_email
            msg.set_content(message)

            with smtplib.SMTP(self.smtp_host) as smtp:
                smtp.send_message(msg)
            logger.info("Alert email sent to %s", self.alert_email)

        except Exception as exc:
            logger.error("Failed to send alert email: %s", exc)

    def _trigger_failover(self) -> None:
        """
        Copy snippet files from the PROD failover share to the landing zone.

        Only executed when *failover_share* is configured and the landing
        zone has stale/missing data.
        """
        logger = get_logger(__name__)
        if not self.failover_share:
            logger.debug("No failover_share configured — skipping failover")
            return

        import shutil  # noqa: PLC0415

        failover = Path(self.failover_share)
        landing = Path(self.landing_zone)

        if not failover.exists():
            logger.error("Failover share not accessible: %s", failover)
            return

        copied = 0
        for src in failover.glob("*.snp"):
            dest = landing / src.name
            if not dest.exists():
                shutil.copy2(src, dest)
                copied += 1

        logger.info("Failover: copied %d .snp files from %s", copied, failover)
