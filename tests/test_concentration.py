"""Tests for the concentration tracker."""

import time

import pytest

from whale_detector.analytics.concentration import ConcentrationTracker
from whale_detector.models import Alert, AlertSeverity, TradeSource, WhalePosition


def _pos(symbol: str, wallet: str, notional: float) -> WhalePosition:
    return WhalePosition(
        source=TradeSource.BINANCE,
        symbol=symbol,
        side="BUY",
        quantity=notional / 65_000,
        price=65_000.0,
        notional_usd=notional,
        wallet_address=wallet,
    )


class TestConcentrationTracker:
    def setup_method(self):
        self.alerts = []
        self.tracker = ConcentrationTracker(
            window_secs=3600.0,
            on_alert=self.alerts.append,
        )

    def test_empty_report(self):
        report = self.tracker.get_concentration_report("BTCUSDT")
        assert report["hhi"] == 0.0
        assert report["entity_count"] == 0

    def test_single_entity_full_concentration(self):
        self.tracker.ingest(_pos("BTCUSDT", "whale-1", 1_000_000.0))
        report = self.tracker.get_concentration_report("BTCUSDT")
        assert report["hhi"] == pytest.approx(1.0)
        assert report["entity_count"] == 1

    def test_two_equal_entities_hhi(self):
        self.tracker.ingest(_pos("ETHUSDT", "whale-a", 500_000.0))
        self.tracker.ingest(_pos("ETHUSDT", "whale-b", 500_000.0))
        report = self.tracker.get_concentration_report("ETHUSDT")
        # Two equal shares: HHI = 0.5
        assert report["hhi"] == pytest.approx(0.5, abs=1e-6)

    def test_high_concentration_triggers_alert(self):
        # One wallet dominates (>40 % threshold for CRITICAL)
        self.tracker.ingest(_pos("SOLUSDT", "whale-x", 9_000_000.0))
        self.tracker.ingest(_pos("SOLUSDT", "whale-y", 1_000_000.0))
        # Alert should have fired
        assert len(self.alerts) >= 1

    def test_cross_pair_hhi(self):
        self.tracker.ingest(_pos("BTCUSDT", "w1", 1_000_000.0))
        self.tracker.ingest(_pos("ETHUSDT", "w2", 1_000_000.0))
        hhi = self.tracker.get_cross_pair_hhi()
        assert 0.0 < hhi <= 1.0

    def test_get_all_reports(self):
        self.tracker.ingest(_pos("BTCUSDT", "w1", 500_000.0))
        self.tracker.ingest(_pos("ETHUSDT", "w2", 700_000.0))
        reports = self.tracker.get_all_reports()
        assert "BTCUSDT" in reports
        assert "ETHUSDT" in reports
