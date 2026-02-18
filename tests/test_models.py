"""Tests for shared data models."""

import time

import pytest

from whale_detector.models import (
    Alert,
    AlertSeverity,
    AavePosition,
    CEXDepositEvent,
    LiquidationRiskReport,
    LiquiditySnapshot,
    TradeSource,
    WhalePosition,
)


class TestWhalePosition:
    def test_creation(self):
        pos = WhalePosition(
            source=TradeSource.BINANCE,
            symbol="BTCUSDT",
            side="BUY",
            quantity=1.5,
            price=65_000.0,
            notional_usd=97_500.0,
        )
        assert pos.source == TradeSource.BINANCE
        assert pos.symbol == "BTCUSDT"
        assert pos.notional_usd == 97_500.0
        assert pos.timestamp > 0

    def test_repr(self):
        pos = WhalePosition(
            source=TradeSource.SOLANA,
            symbol="SOL/USD",
            side="SELL",
            quantity=1000.0,
            price=150.0,
            notional_usd=150_000.0,
        )
        text = repr(pos)
        assert "SOLANA" in text
        assert "SELL" in text
        assert "SOL/USD" in text


class TestAavePosition:
    def test_ltv_calculation(self):
        pos = AavePosition(
            user="0xdeadbeef",
            collateral_usd=100_000.0,
            debt_usd=80_000.0,
            health_factor=1.15,
        )
        assert abs(pos.ltv - 0.80) < 1e-9

    def test_ltv_zero_collateral(self):
        pos = AavePosition(
            user="0xdeadbeef",
            collateral_usd=0.0,
            debt_usd=0.0,
            health_factor=0.0,
        )
        assert pos.ltv == 0.0


class TestAlert:
    def test_to_dict(self):
        alert = Alert(
            severity=AlertSeverity.HIGH,
            title="Test alert",
            description="Test description",
            source="test",
        )
        d = alert.to_dict()
        assert d["severity"] == "HIGH"
        assert d["title"] == "Test alert"
        assert isinstance(d["timestamp"], float)

    def test_timestamp_auto_set(self):
        before = time.time()
        alert = Alert(
            severity=AlertSeverity.INFO,
            title="x",
            description="y",
            source="z",
        )
        after = time.time()
        assert before <= alert.timestamp <= after


class TestLiquidationRiskReport:
    def test_defaults(self):
        report = LiquidationRiskReport(
            risk_score=0.5,
            positions_at_risk=10,
            total_debt_at_risk_usd=1_000_000.0,
            cascade_depth_estimate=5,
            concentration_score=0.3,
        )
        assert report.top_exposures == []
        assert report.risk_score == 0.5
