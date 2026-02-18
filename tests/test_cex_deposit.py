"""Tests for CEX deposit detector."""

import time

import pytest

from whale_detector.detectors.cex_deposit import CEXDepositDetector
from whale_detector.models import Alert, AlertSeverity, TradeSource, WhalePosition


def _make_whale(notional_usd: float, destination: str) -> WhalePosition:
    return WhalePosition(
        source=TradeSource.SOLANA,
        symbol="SOL/USD",
        side="SELL",
        quantity=notional_usd / 150.0,
        price=150.0,
        notional_usd=notional_usd,
        wallet_address="sender-wallet",
        raw={"destination": destination, "mint": "So11111111111111111111111111111111111111112"},
    )


class TestCEXDepositDetector:
    def test_no_alert_below_threshold(self):
        alerts = []
        detector = CEXDepositDetector(
            cex_wallets={"Binance": ["binance-deposit-addr"]},
            on_alert=alerts.append,
        )
        pos = _make_whale(100_000.0, "binance-deposit-addr")
        detector.ingest(pos)
        assert len(alerts) == 0

    def test_alert_above_threshold(self):
        alerts = []
        detector = CEXDepositDetector(
            cex_wallets={"Binance": ["binance-deposit-addr"]},
            on_alert=alerts.append,
        )
        # Deposit well above default 1M threshold
        pos = _make_whale(2_000_000.0, "binance-deposit-addr")
        detector.ingest(pos)
        assert len(alerts) == 1
        assert alerts[0].severity in (AlertSeverity.HIGH, AlertSeverity.CRITICAL)

    def test_ignores_non_cex_destination(self):
        alerts = []
        detector = CEXDepositDetector(
            cex_wallets={"Binance": ["binance-deposit-addr"]},
            on_alert=alerts.append,
        )
        pos = _make_whale(5_000_000.0, "random-dex-wallet")
        detector.ingest(pos)
        assert len(alerts) == 0

    def test_window_total(self):
        detector = CEXDepositDetector(
            cex_wallets={"Kraken": ["kraken-addr"]},
        )
        pos = _make_whale(300_000.0, "kraken-addr")
        detector.ingest(pos)
        total = detector.window_total("Kraken", "SOL")
        assert total == pytest.approx(300_000.0, rel=0.01)

    def test_add_cex_wallet(self):
        alerts = []
        detector = CEXDepositDetector(
            cex_wallets={},
            on_alert=alerts.append,
        )
        detector.add_cex_wallet("NewExchange", "new-wallet-123")
        pos = _make_whale(2_000_000.0, "new-wallet-123")
        detector.ingest(pos)
        assert len(alerts) == 1
