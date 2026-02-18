"""
Concentration exposure tracker.

Tracks whale concentration across trading pairs and liquidity pools.
Computes per-pair and cross-pair concentration metrics to identify
systemic risk from a small number of large participants dominating
markets.
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from whale_detector.config import get_risk_thresholds
from whale_detector.models import Alert, AlertSeverity, WhalePosition

logger = logging.getLogger(__name__)


class ConcentrationTracker:
    """
    Maintains rolling concentration metrics for whale positions.

    For each symbol/pair, tracks:
      - Volume contributed by each unique wallet/entity.
      - Herfindahl-Hirschman Index (HHI) of volume concentration.
      - Top entities by volume share.

    Fires alerts when a single entity's share exceeds the threshold.

    Usage::

        tracker = ConcentrationTracker(on_alert=alert_cb)
        tracker.ingest(whale_position)
        report = tracker.get_concentration_report("BTCUSDT")
    """

    def __init__(
        self,
        window_secs: float = 3600.0,
        on_alert: Optional[callable] = None,
    ) -> None:
        self._window_secs = window_secs
        self.on_alert = on_alert
        self._thresholds = get_risk_thresholds()
        # symbol -> list of (timestamp, wallet, notional_usd)
        self._events: Dict[str, List[Tuple[float, str, float]]] = defaultdict(list)
        self._last_alert_times: Dict[str, float] = {}
        self._alert_cooldown = 1800.0  # 30 minutes

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ingest(self, position: WhalePosition) -> None:
        """Ingest a whale position event."""
        wallet = position.wallet_address or position.exchange or "unknown"
        symbol = position.symbol
        ts = position.timestamp or time.time()

        self._events[symbol].append((ts, wallet, position.notional_usd))
        self._evict_stale(symbol)
        self._check_concentration(symbol)

    def get_concentration_report(self, symbol: str) -> Dict:
        """
        Return concentration metrics for a symbol.

        Returns::

            {
              "symbol": str,
              "hhi": float,              # 0-1
              "top_entities": [...],     # [(wallet, share_pct), ...]
              "total_volume_usd": float,
              "entity_count": int,
              "window_secs": float,
            }
        """
        self._evict_stale(symbol)
        events = self._events.get(symbol, [])
        wallet_volumes: Dict[str, float] = defaultdict(float)
        for _, wallet, notional in events:
            wallet_volumes[wallet] += notional

        total = sum(wallet_volumes.values())
        if total == 0:
            return self._empty_report(symbol)

        shares = {w: v / total for w, v in wallet_volumes.items()}
        hhi = sum(s ** 2 for s in shares.values())

        top = sorted(shares.items(), key=lambda x: x[1], reverse=True)[:10]

        return {
            "symbol": symbol,
            "hhi": hhi,
            "top_entities": [
                {"wallet": w, "share_pct": round(s * 100, 2)} for w, s in top
            ],
            "total_volume_usd": total,
            "entity_count": len(wallet_volumes),
            "window_secs": self._window_secs,
        }

    def get_all_reports(self) -> Dict[str, Dict]:
        """Return concentration reports for all tracked symbols."""
        return {sym: self.get_concentration_report(sym) for sym in self._events}

    def get_cross_pair_hhi(self) -> float:
        """
        Compute a cross-pair HHI: how concentrated is overall whale volume
        across all symbols?
        """
        symbol_totals: Dict[str, float] = {}
        for symbol, events in self._events.items():
            self._evict_stale(symbol)
            symbol_totals[symbol] = sum(n for _, _, n in self._events[symbol])

        grand_total = sum(symbol_totals.values())
        if grand_total == 0:
            return 0.0
        return sum((v / grand_total) ** 2 for v in symbol_totals.values())

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _evict_stale(self, symbol: str) -> None:
        cutoff = time.time() - self._window_secs
        self._events[symbol] = [
            e for e in self._events[symbol] if e[0] >= cutoff
        ]

    def _check_concentration(self, symbol: str) -> None:
        events = self._events.get(symbol, [])
        wallet_volumes: Dict[str, float] = defaultdict(float)
        for _, wallet, notional in events:
            wallet_volumes[wallet] += notional

        total = sum(wallet_volumes.values())
        if total == 0:
            return

        max_wallet, max_vol = max(wallet_volumes.items(), key=lambda x: x[1])
        share = max_vol / total

        if share >= self._thresholds.concentration_critical_pct:
            severity = AlertSeverity.HIGH
        elif share >= self._thresholds.concentration_warning_pct:
            severity = AlertSeverity.MEDIUM
        else:
            return

        # Cooldown
        now = time.time()
        last = self._last_alert_times.get(symbol, 0.0)
        if now - last < self._alert_cooldown:
            return
        self._last_alert_times[symbol] = now

        alert = Alert(
            severity=severity,
            title=f"High concentration on {symbol}: {share:.1%} by single entity",
            description=(
                f"Wallet {max_wallet[:8]}...{max_wallet[-4:]} accounts for "
                f"{share:.1%} of ${total:,.0f} whale volume on {symbol} "
                f"in the last {self._window_secs / 3600:.1f}h."
            ),
            source="ConcentrationTracker",
            metadata={
                "symbol": symbol,
                "wallet": max_wallet,
                "share_pct": share * 100,
                "total_volume_usd": total,
            },
        )
        if self.on_alert:
            self.on_alert(alert)

    def _empty_report(self, symbol: str) -> Dict:
        return {
            "symbol": symbol,
            "hhi": 0.0,
            "top_entities": [],
            "total_volume_usd": 0.0,
            "entity_count": 0,
            "window_secs": self._window_secs,
        }
