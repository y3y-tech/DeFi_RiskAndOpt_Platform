"""Tests for the alert manager."""

import pytest

from whale_detector.models import Alert, AlertSeverity
from whale_detector.utils.alerts import AlertManager, _severity_color


def _alert(severity: AlertSeverity, title: str = "Test") -> Alert:
    return Alert(
        severity=severity,
        title=title,
        description="desc",
        source="test",
    )


class TestAlertManager:
    def setup_method(self):
        self.mgr = AlertManager()

    def test_handle_adds_to_queue(self):
        self.mgr.handle(_alert(AlertSeverity.HIGH))
        assert len(self.mgr.get_recent(n=10)) == 1

    def test_stats_updated(self):
        self.mgr.handle(_alert(AlertSeverity.CRITICAL))
        self.mgr.handle(_alert(AlertSeverity.MEDIUM))
        stats = self.mgr.get_stats()
        assert stats["CRITICAL"] == 1
        assert stats["MEDIUM"] == 1

    def test_get_recent_filters_by_severity(self):
        self.mgr.handle(_alert(AlertSeverity.INFO))
        self.mgr.handle(_alert(AlertSeverity.HIGH))
        high_plus = self.mgr.get_recent(n=10, min_severity=AlertSeverity.HIGH)
        assert all(
            a.severity in (AlertSeverity.HIGH, AlertSeverity.CRITICAL)
            for a in high_plus
        )

    def test_extra_handler_called(self):
        received = []
        self.mgr.add_handler(received.append)
        alert = _alert(AlertSeverity.MEDIUM, "custom")
        self.mgr.handle(alert)
        assert len(received) == 1
        assert received[0].title == "custom"

    def test_max_queue_size(self):
        mgr = AlertManager(max_queue_size=3)
        for i in range(5):
            mgr.handle(_alert(AlertSeverity.LOW, f"alert-{i}"))
        assert len(mgr.get_recent(n=100)) <= 3

    def test_severity_colors_defined(self):
        for severity in AlertSeverity:
            color = _severity_color(severity)
            assert color.startswith("#")
