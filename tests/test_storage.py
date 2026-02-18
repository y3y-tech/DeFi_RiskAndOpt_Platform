"""Tests for the SQLite data store."""

import os
import tempfile
import time

import pytest

from whale_detector.models import (
    Alert,
    AlertSeverity,
    AavePosition,
    CEXDepositEvent,
    LiquiditySnapshot,
    TradeSource,
    WhalePosition,
)
from whale_detector.utils.storage import DataStore


@pytest.fixture
def store(tmp_path):
    db = DataStore(db_path=str(tmp_path / "test.db"))
    db.initialize()
    yield db
    db.close()


class TestDataStore:
    def test_save_and_retrieve_whale_position(self, store):
        pos = WhalePosition(
            source=TradeSource.BINANCE,
            symbol="BTCUSDT",
            side="BUY",
            quantity=0.5,
            price=65_000.0,
            notional_usd=32_500.0,
        )
        store.save_whale_position(pos)
        rows = store.get_recent_whale_positions(limit=10, since=time.time() - 60)
        assert len(rows) == 1
        assert rows[0]["symbol"] == "BTCUSDT"
        assert rows[0]["notional_usd"] == pytest.approx(32_500.0)

    def test_save_and_retrieve_cex_deposit(self, store):
        event = CEXDepositEvent(
            exchange="Binance",
            token="SOL",
            amount=1000.0,
            amount_usd=150_000.0,
            wallet_from="sender",
            wallet_to="binance-hot",
        )
        store.save_cex_deposit(event)
        rows = store.get_cex_deposits(hours=1)
        assert len(rows) == 1
        assert rows[0]["exchange"] == "Binance"

    def test_save_and_retrieve_alert(self, store):
        alert = Alert(
            severity=AlertSeverity.HIGH,
            title="Test alert",
            description="Something bad happened",
            source="test",
        )
        store.save_alert(alert)
        rows = store.get_recent_alerts(limit=10)
        assert len(rows) == 1
        assert rows[0]["severity"] == "HIGH"

    def test_save_liquidity_snapshot(self, store):
        snap = LiquiditySnapshot(
            protocol="Uniswap",
            pool_id="0xpool",
            token0="ETH",
            token1="USDC",
            tvl_usd=10_000_000.0,
            volume_24h_usd=500_000.0,
            fee_tier=0.003,
        )
        store.save_liquidity_snapshot(snap)
        # No retrieval method exposed yet, just verify no error
        assert True

    def test_whale_volume_by_pair(self, store):
        for side in ("BUY", "SELL"):
            pos = WhalePosition(
                source=TradeSource.BINANCE,
                symbol="ETHUSDT",
                side=side,
                quantity=10.0,
                price=3_000.0,
                notional_usd=30_000.0,
            )
            store.save_whale_position(pos)
        rows = store.get_whale_volume_by_pair(hours=1)
        symbols = [r["symbol"] for r in rows]
        assert "ETHUSDT" in symbols

    def test_multiple_positions(self, store):
        for i in range(5):
            pos = WhalePosition(
                source=TradeSource.SOLANA,
                symbol="SOL/USD",
                side="SELL",
                quantity=float(i + 1) * 100,
                price=150.0,
                notional_usd=float(i + 1) * 15_000.0,
            )
            store.save_whale_position(pos)
        rows = store.get_recent_whale_positions(limit=10, since=time.time() - 60)
        assert len(rows) == 5
