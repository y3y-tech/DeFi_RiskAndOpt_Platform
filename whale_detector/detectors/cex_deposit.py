"""
CEX deposit pattern detector.

Monitors Solana on-chain transfers to known centralised-exchange deposit
addresses and accumulates them in a sliding time window. When the
accumulated value exceeds the configured threshold it emits a
CEXDepositEvent and fires an early-warning Alert.

This is one of the most reliable on-chain signals for impending market
dumps: large holders moving assets to exchanges often precede selling.
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict, deque
from typing import Callable, Dict, List, Optional

from whale_detector.config import (
    KNOWN_CEX_DEPOSIT_WALLETS,
    get_whale_thresholds,
)
from whale_detector.models import Alert, AlertSeverity, CEXDepositEvent, WhalePosition

logger = logging.getLogger(__name__)


class CEXDepositDetector:
    """
    Receives on-chain transfer events and detects accumulation patterns
    into CEX deposit addresses.

    It maintains a per-(exchange, token) sliding window of recent transfers.
    When the window total exceeds the alert threshold, a warning is raised.

    Usage::

        detector = CEXDepositDetector(on_alert=alert_cb, on_event=event_cb)
        # Feed transfers from SolanaWhaleMonitor
        detector.ingest(whale_position)
    """

    def __init__(
        self,
        cex_wallets: Optional[Dict[str, List[str]]] = None,
        on_event: Optional[Callable[[CEXDepositEvent], None]] = None,
        on_alert: Optional[Callable[[Alert], None]] = None,
    ) -> None:
        self._cex_wallets = cex_wallets or KNOWN_CEX_DEPOSIT_WALLETS
        # Build reverse map: wallet_address -> exchange_name
        self._wallet_to_exchange: Dict[str, str] = {}
        for exchange, wallets in self._cex_wallets.items():
            for w in wallets:
                self._wallet_to_exchange[w.lower()] = exchange

        self.on_event = on_event
        self.on_alert = on_alert
        self._thresholds = get_whale_thresholds()

        # Sliding window: (exchange, token) -> deque of (timestamp, usd_value)
        self._windows: Dict[tuple, deque] = defaultdict(deque)
        self._last_alert_time: Dict[tuple, float] = {}
        # Minimum seconds between repeated alerts for the same pair
        self._alert_cooldown = 600.0  # 10 minutes

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ingest(self, position: WhalePosition) -> None:
        """
        Ingest a WhalePosition (from Solana monitor) and check if the
        destination wallet belongs to a CEX.
        """
        dest_wallet = (position.raw or {}).get("destination", "")
        if not dest_wallet:
            return

        exchange = self._wallet_to_exchange.get(dest_wallet.lower())
        if exchange is None:
            return

        token = position.symbol.split("/")[0]
        event = CEXDepositEvent(
            exchange=exchange,
            token=token,
            amount=position.quantity,
            amount_usd=position.notional_usd,
            wallet_from=position.wallet_address or "",
            wallet_to=dest_wallet,
            timestamp=position.timestamp,
            tx_hash=position.tx_hash,
        )

        logger.info(
            "CEX deposit detected: %s → %s: %.4f %s ($%.0f)",
            event.wallet_from, event.exchange, event.amount, event.token, event.amount_usd,
        )

        if self.on_event:
            self.on_event(event)

        self._update_window(exchange, token, event)

    def add_cex_wallet(self, exchange: str, wallet_address: str) -> None:
        """Dynamically add a new CEX deposit address."""
        self._wallet_to_exchange[wallet_address.lower()] = exchange
        if exchange not in self._cex_wallets:
            self._cex_wallets[exchange] = []
        self._cex_wallets[exchange].append(wallet_address)

    def window_total(self, exchange: str, token: str) -> float:
        """Return the current sliding-window total (USD) for (exchange, token)."""
        return self._window_sum(exchange, token)

    def get_all_window_totals(self) -> Dict[str, Dict[str, float]]:
        """Return all current sliding-window totals grouped by exchange and token."""
        result: Dict[str, Dict[str, float]] = defaultdict(dict)
        for (exchange, token), _ in self._windows.items():
            result[exchange][token] = self._window_sum(exchange, token)
        return dict(result)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _update_window(
        self, exchange: str, token: str, event: CEXDepositEvent
    ) -> None:
        key = (exchange, token)
        window = self._windows[key]
        now = event.timestamp
        cutoff = now - self._thresholds.cex_deposit_window_secs

        # Append new entry
        window.append((now, event.amount_usd))

        # Evict stale entries
        while window and window[0][0] < cutoff:
            window.popleft()

        total_usd = sum(v for _, v in window)

        if total_usd >= self._thresholds.cex_deposit_alert_usd:
            self._maybe_fire_alert(exchange, token, total_usd, event)

    def _window_sum(self, exchange: str, token: str) -> float:
        key = (exchange, token)
        window = self._windows.get(key)
        if not window:
            return 0.0
        now = time.time()
        cutoff = now - self._thresholds.cex_deposit_window_secs
        return sum(v for ts, v in window if ts >= cutoff)

    def _maybe_fire_alert(
        self, exchange: str, token: str, total_usd: float, last_event: CEXDepositEvent
    ) -> None:
        key = (exchange, token)
        last_alert = self._last_alert_time.get(key, 0.0)
        now = time.time()
        if now - last_alert < self._alert_cooldown:
            return
        self._last_alert_time[key] = now

        window_mins = self._thresholds.cex_deposit_window_secs // 60
        severity = (
            AlertSeverity.CRITICAL
            if total_usd >= self._thresholds.cex_deposit_alert_usd * 3
            else AlertSeverity.HIGH
        )

        alert = Alert(
            severity=severity,
            title=f"CEX deposit surge: {token} → {exchange}",
            description=(
                f"${total_usd:,.0f} of {token} deposited to {exchange} "
                f"in the last {window_mins} minutes. "
                f"Potential market dump signal."
            ),
            source="CEXDepositDetector",
            metadata={
                "exchange": exchange,
                "token": token,
                "total_usd": total_usd,
                "window_secs": self._thresholds.cex_deposit_window_secs,
                "last_tx": last_event.tx_hash,
            },
        )
        logger.warning("ALERT: %s – %s", alert.severity.value, alert.title)
        if self.on_alert:
            self.on_alert(alert)
