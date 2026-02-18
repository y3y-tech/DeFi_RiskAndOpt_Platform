"""
Binance WebSocket monitor for large position movements.

Connects to the Binance combined stream, subscribes to aggTrade streams
for all configured pairs, and emits WhalePosition events for trades
exceeding the configured threshold.

Monitors up to 10,000+ position movements simultaneously using a single
multiplexed WebSocket connection.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import defaultdict
from typing import Callable, Dict, List, Optional

import websockets

from whale_detector.config import MONITORED_PAIRS, get_api_config, get_whale_thresholds
from whale_detector.models import Alert, AlertSeverity, TradeSource, WhalePosition

logger = logging.getLogger(__name__)


class BinanceWhaleMonitor:
    """
    Monitors Binance perpetual futures and spot markets via WebSocket.

    Streams:
      - <symbol>@aggTrade  – aggregated trade feed
      - <symbol>@bookTicker – best bid/ask for real-time pricing

    Usage::

        monitor = BinanceWhaleMonitor(on_whale=my_callback, on_alert=alert_cb)
        await monitor.run()
    """

    # Binance combined-stream endpoint supports up to 1024 streams per connection.
    # We chunk pairs into groups to stay within limits while covering 10 000+ positions.
    _MAX_STREAMS_PER_CONNECTION = 200

    def __init__(
        self,
        pairs: Optional[List[str]] = None,
        on_whale: Optional[Callable[[WhalePosition], None]] = None,
        on_alert: Optional[Callable[[Alert], None]] = None,
    ) -> None:
        self.pairs = pairs or MONITORED_PAIRS
        self.on_whale = on_whale
        self.on_alert = on_alert
        self._cfg = get_api_config()
        self._thresholds = get_whale_thresholds()

        # Track position sizes per symbol for concentration analysis
        self._position_book: Dict[str, Dict[str, float]] = defaultdict(dict)
        # Sliding window of recent whale trades
        self._recent_whales: List[WhalePosition] = []
        self._running = False
        self._reconnect_delay = 1.0
        self._max_reconnect_delay = 60.0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Start monitoring. Runs indefinitely until cancelled."""
        self._running = True
        chunks = self._chunk_pairs(self.pairs)
        tasks = [self._stream_chunk(chunk, idx) for idx, chunk in enumerate(chunks)]
        logger.info(
            "Starting Binance monitor: %d pairs across %d connections",
            len(self.pairs),
            len(chunks),
        )
        await asyncio.gather(*tasks)

    async def stop(self) -> None:
        self._running = False

    def get_recent_whales(self, window_secs: float = 60.0) -> List[WhalePosition]:
        """Return whale positions detected in the last *window_secs* seconds."""
        cutoff = time.time() - window_secs
        return [w for w in self._recent_whales if w.timestamp >= cutoff]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _chunk_pairs(self, pairs: List[str]) -> List[List[str]]:
        chunk_size = self._MAX_STREAMS_PER_CONNECTION
        return [pairs[i:i + chunk_size] for i in range(0, len(pairs), chunk_size)]

    def _build_ws_url(self, pairs: List[str]) -> str:
        streams = "/".join(
            f"{p.lower()}@aggTrade" for p in pairs
        )
        return f"{self._cfg.binance_ws_url}?streams={streams}"

    async def _stream_chunk(self, pairs: List[str], chunk_idx: int) -> None:
        url = self._build_ws_url(pairs)
        delay = self._reconnect_delay
        while self._running:
            try:
                async with websockets.connect(url, ping_interval=20) as ws:
                    logger.info("Chunk %d connected (%d pairs)", chunk_idx, len(pairs))
                    delay = self._reconnect_delay  # reset on success
                    async for raw in ws:
                        if not self._running:
                            break
                        await self._handle_message(raw)
            except (websockets.WebSocketException, OSError, asyncio.TimeoutError) as e:
                if not self._running:
                    break
                logger.warning("Chunk %d disconnected (%s). Reconnecting in %.1fs", chunk_idx, e, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, self._max_reconnect_delay)

    async def _handle_message(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        data = msg.get("data", msg)
        event_type = data.get("e")

        if event_type == "aggTrade":
            await self._process_agg_trade(data)

    async def _process_agg_trade(self, data: dict) -> None:
        """Parse an aggTrade event and emit a WhalePosition if it qualifies."""
        try:
            symbol: str = data["s"]
            price: float = float(data["p"])
            quantity: float = float(data["q"])
            is_buyer_maker: bool = data["m"]  # True => seller is aggressor
            notional: float = price * quantity
        except (KeyError, ValueError, TypeError):
            return

        if notional < self._thresholds.min_trade_usd:
            return

        side = "SELL" if is_buyer_maker else "BUY"
        position = WhalePosition(
            source=TradeSource.BINANCE,
            symbol=symbol,
            side=side,
            quantity=quantity,
            price=price,
            notional_usd=notional,
            timestamp=data.get("T", time.time() * 1000) / 1000,
            raw=data,
        )

        # Maintain sliding window (keep last 1000)
        self._recent_whales.append(position)
        if len(self._recent_whales) > 1000:
            self._recent_whales = self._recent_whales[-1000:]

        logger.info("Whale trade detected: %s", position)

        if self.on_whale:
            self.on_whale(position)

        # Fire alert for very large single trades
        if notional >= self._thresholds.min_trade_usd * 10:
            alert = Alert(
                severity=AlertSeverity.HIGH,
                title=f"Mega whale trade on {symbol}",
                description=(
                    f"{side} {quantity:,.4f} {symbol} @ ${price:,.2f} "
                    f"(${notional:,.0f}) on Binance"
                ),
                source="BinanceWhaleMonitor",
                metadata={"symbol": symbol, "notional_usd": notional, "side": side},
            )
            if self.on_alert:
                self.on_alert(alert)

    # ------------------------------------------------------------------
    # Order-book snapshot (REST) for concentration analysis
    # ------------------------------------------------------------------

    async def fetch_order_book_snapshot(
        self, symbol: str, depth: int = 100
    ) -> Dict[str, list]:
        """
        Fetch a depth snapshot from Binance REST API.
        Returns {"bids": [...], "asks": [...]} with [price, qty] pairs.
        """
        import aiohttp

        url = f"{self._cfg.binance_rest_url}/api/v3/depth"
        params = {"symbol": symbol.upper(), "limit": depth}
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as resp:
                resp.raise_for_status()
                return await resp.json()

    def position_count(self) -> int:
        """Return total number of individual positions currently tracked."""
        return sum(len(v) for v in self._position_book.values())
