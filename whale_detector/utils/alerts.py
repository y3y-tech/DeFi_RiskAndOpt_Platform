"""
Alert delivery system.

Supports:
  - Console logging (always active)
  - Webhook delivery (Slack / Discord / custom)
  - In-memory queue for dashboard consumption
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from typing import Callable, Deque, List, Optional

import aiohttp

from whale_detector.config import get_api_config
from whale_detector.models import Alert, AlertSeverity

logger = logging.getLogger(__name__)


class AlertManager:
    """
    Central alert manager. Receives Alert objects, logs them,
    optionally sends to a webhook, and maintains an in-memory queue
    for the dashboard.

    Usage::

        mgr = AlertManager()
        mgr.handle(alert)           # synchronous
        await mgr.handle_async(alert)  # async with webhook delivery
    """

    # Severity → emoji prefix for webhook messages
    _SEVERITY_EMOJI = {
        AlertSeverity.INFO: ":information_source:",
        AlertSeverity.LOW: ":white_circle:",
        AlertSeverity.MEDIUM: ":large_yellow_circle:",
        AlertSeverity.HIGH: ":red_circle:",
        AlertSeverity.CRITICAL: ":rotating_light:",
    }

    def __init__(
        self,
        max_queue_size: int = 500,
        extra_handlers: Optional[List[Callable[[Alert], None]]] = None,
    ) -> None:
        self._cfg = get_api_config()
        self._queue: Deque[Alert] = deque(maxlen=max_queue_size)
        self._extra_handlers = extra_handlers or []
        self._stats: dict = {s.value: 0 for s in AlertSeverity}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def handle(self, alert: Alert) -> None:
        """Synchronously handle an alert (log + queue + extra handlers)."""
        self._log_alert(alert)
        self._queue.append(alert)
        self._stats[alert.severity.value] += 1
        for handler in self._extra_handlers:
            try:
                handler(alert)
            except Exception as exc:
                logger.error("Extra handler error: %s", exc)

    async def handle_async(self, alert: Alert) -> None:
        """Asynchronously handle an alert (log + queue + webhook)."""
        self.handle(alert)
        if self._cfg.alert_webhook_url:
            await self._send_webhook(alert)

    def get_recent(
        self,
        n: int = 50,
        min_severity: AlertSeverity = AlertSeverity.INFO,
    ) -> List[Alert]:
        """Return the most recent *n* alerts at or above *min_severity*."""
        severity_order = list(AlertSeverity)
        min_idx = severity_order.index(min_severity)
        filtered = [
            a for a in self._queue
            if severity_order.index(a.severity) >= min_idx
        ]
        return list(filtered)[-n:]

    def get_stats(self) -> dict:
        """Return alert counts by severity."""
        return dict(self._stats)

    def add_handler(self, handler: Callable[[Alert], None]) -> None:
        """Register an additional synchronous alert handler."""
        self._extra_handlers.append(handler)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _log_alert(alert: Alert) -> None:
        level_map = {
            AlertSeverity.INFO: logging.INFO,
            AlertSeverity.LOW: logging.INFO,
            AlertSeverity.MEDIUM: logging.WARNING,
            AlertSeverity.HIGH: logging.ERROR,
            AlertSeverity.CRITICAL: logging.CRITICAL,
        }
        level = level_map.get(alert.severity, logging.INFO)
        logger.log(
            level,
            "[%s] %s — %s",
            alert.severity.value,
            alert.title,
            alert.description,
        )

    async def _send_webhook(self, alert: Alert) -> None:
        emoji = self._SEVERITY_EMOJI.get(alert.severity, "")
        payload = self._build_webhook_payload(alert, emoji)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self._cfg.alert_webhook_url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status not in (200, 204):
                        logger.warning(
                            "Webhook delivery failed: HTTP %d", resp.status
                        )
        except Exception as exc:
            logger.error("Webhook delivery error: %s", exc)

    @staticmethod
    def _build_webhook_payload(alert: Alert, emoji: str) -> dict:
        """Build a Slack-compatible webhook payload."""
        ts_str = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(alert.timestamp))
        return {
            "text": f"{emoji} *[{alert.severity.value}] {alert.title}*",
            "attachments": [
                {
                    "color": _severity_color(alert.severity),
                    "fields": [
                        {"title": "Description", "value": alert.description, "short": False},
                        {"title": "Source", "value": alert.source, "short": True},
                        {"title": "Time", "value": ts_str, "short": True},
                    ],
                    "footer": "Whale Detection Platform",
                    "ts": int(alert.timestamp),
                }
            ],
        }


def _severity_color(severity: AlertSeverity) -> str:
    return {
        AlertSeverity.INFO: "#36a64f",
        AlertSeverity.LOW: "#439FE0",
        AlertSeverity.MEDIUM: "#FFCC00",
        AlertSeverity.HIGH: "#FF6600",
        AlertSeverity.CRITICAL: "#CC0000",
    }.get(severity, "#888888")
