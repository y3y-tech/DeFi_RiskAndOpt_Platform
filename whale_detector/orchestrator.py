"""
Platform orchestrator.

Wires together all detectors and analytics engines, manages the shared
event loop, and provides clean startup/shutdown.

Usage::

    from whale_detector.orchestrator import Platform
    import asyncio

    platform = Platform()
    asyncio.run(platform.run())
"""

from __future__ import annotations

import asyncio
import logging
import signal
import time
from typing import Optional

from whale_detector.analytics.aave import AaveAnalytics
from whale_detector.analytics.concentration import ConcentrationTracker
from whale_detector.analytics.liquidation_risk import LiquidationRiskModel
from whale_detector.analytics.uniswap import UniswapAnalytics
from whale_detector.config import get_api_config
from whale_detector.detectors.binance_ws import BinanceWhaleMonitor
from whale_detector.detectors.cex_deposit import CEXDepositDetector
from whale_detector.detectors.solana_onchain import SolanaWhaleMonitor
from whale_detector.models import Alert, CEXDepositEvent, WhalePosition
from whale_detector.utils.alerts import AlertManager
from whale_detector.utils.storage import DataStore

logger = logging.getLogger(__name__)


class Platform:
    """
    Top-level orchestrator that runs all platform components.

    Components:
      - BinanceWhaleMonitor   — Binance WebSocket position stream
      - SolanaWhaleMonitor    — Solana on-chain transfer polling
      - CEXDepositDetector    — deposit pattern accumulator
      - AaveAnalytics         — at-risk position fetcher (scheduled)
      - UniswapAnalytics      — pool liquidity fetcher (scheduled)
      - LiquidationRiskModel  — cascade risk calculator
      - ConcentrationTracker  — per-pair concentration
      - AlertManager          — alert routing and delivery
      - DataStore             — SQLite persistence

    Usage::

        platform = Platform()
        await platform.run()
    """

    def __init__(self) -> None:
        self._cfg = get_api_config()
        self._alert_mgr = AlertManager()
        self._store = DataStore()
        self._concentration = ConcentrationTracker(on_alert=self._handle_alert)
        self._cex_detector = CEXDepositDetector(
            on_event=self._on_cex_deposit,
            on_alert=self._handle_alert,
        )
        self._liq_model = LiquidationRiskModel(on_alert=self._handle_alert)
        self._aave = AaveAnalytics(on_alert=self._handle_alert)
        self._uniswap = UniswapAnalytics(on_alert=self._handle_alert)

        self._binance_monitor = BinanceWhaleMonitor(
            on_whale=self._on_whale,
            on_alert=self._handle_alert,
        )
        self._solana_monitor = SolanaWhaleMonitor(
            on_whale=self._on_whale_solana,
            on_alert=self._handle_alert,
        )
        self._running = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Start all platform components. Blocks until cancelled."""
        self._running = True
        self._store.initialize()
        logger.info("Platform starting…")

        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.ensure_future(self.stop()))

        tasks = [
            asyncio.create_task(self._binance_monitor.run(), name="binance-ws"),
            asyncio.create_task(self._solana_monitor.run(), name="solana-onchain"),
            asyncio.create_task(self._risk_refresh_loop(), name="risk-refresh"),
        ]
        logger.info("All platform tasks started.")
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        finally:
            self._store.close()
            logger.info("Platform stopped.")

    async def stop(self) -> None:
        self._running = False
        await self._binance_monitor.stop()
        await self._solana_monitor.stop()
        for task in asyncio.all_tasks():
            if task.get_name() in ("binance-ws", "solana-onchain", "risk-refresh"):
                task.cancel()

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------

    def _on_whale(self, pos: WhalePosition) -> None:
        """Handle a whale position event from Binance."""
        self._store.save_whale_position(pos)
        self._concentration.ingest(pos)

    def _on_whale_solana(self, pos: WhalePosition) -> None:
        """Handle a whale position event from Solana (also feed CEX detector)."""
        self._store.save_whale_position(pos)
        self._concentration.ingest(pos)
        self._cex_detector.ingest(pos)

    def _on_cex_deposit(self, event: CEXDepositEvent) -> None:
        self._store.save_cex_deposit(event)
        logger.info("CEX deposit recorded: %s %s $%.0f", event.exchange, event.token, event.amount_usd)

    def _handle_alert(self, alert: Alert) -> None:
        self._store.save_alert(alert)
        self._alert_mgr.handle(alert)

    # ------------------------------------------------------------------
    # Periodic analytics refresh
    # ------------------------------------------------------------------

    async def _risk_refresh_loop(self) -> None:
        """Fetch DeFi analytics every 60 seconds and recompute risk."""
        refresh_interval = 60  # seconds
        while self._running:
            try:
                await self._refresh_risk()
            except Exception as exc:
                logger.error("Risk refresh error: %s", exc)
            await asyncio.sleep(refresh_interval)

    async def _refresh_risk(self) -> None:
        """Fetch Aave + Uniswap data and compute cascade liquidation risk."""
        positions = await self._aave.fetch_at_risk_positions(max_health_factor=1.2)
        pools = await self._uniswap.fetch_top_pools()

        # Persist pool snapshots
        for pool in pools:
            self._store.save_liquidity_snapshot(pool)

        # Compute risk
        prices: dict = {}  # production: pull from price oracle
        report = self._liq_model.compute(positions, pools, prices)

        logger.info(
            "Risk refresh: score=%.2f, positions=%d, cascade_depth=%d",
            report.risk_score,
            report.positions_at_risk,
            report.cascade_depth_estimate,
        )
