"""
Solana on-chain monitor for large token transfers and program interactions.

Uses the Solana JSON-RPC and WebSocket API to:
  1. Subscribe to account/log notifications for known whale wallets.
  2. Poll recent transaction signatures for large-value transfers.
  3. Decode token transfer instructions to extract amount & participants.

Emits WhalePosition events for transfers above the configured threshold.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Callable, Dict, List, Optional

import aiohttp

from whale_detector.config import (
    SOLANA_TOKENS,
    get_api_config,
    get_whale_thresholds,
)
from whale_detector.models import Alert, AlertSeverity, TradeSource, WhalePosition

logger = logging.getLogger(__name__)

# SPL Token program ID
SPL_TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
# SPL Token 2022 program ID
SPL_TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"


class SolanaWhaleMonitor:
    """
    Monitors Solana on-chain data for large token transfers.

    Strategy:
      - Poll ``getSignaturesForAddress`` for known token program accounts.
      - Decode each transaction to identify token transfers.
      - Enrich with price data to compute USD notional.
      - Emit events and alerts for transfers above threshold.

    Usage::

        monitor = SolanaWhaleMonitor(price_feed=my_price_fn, on_whale=cb)
        await monitor.run()
    """

    def __init__(
        self,
        tracked_wallets: Optional[List[str]] = None,
        price_feed: Optional[Callable[[str], float]] = None,
        on_whale: Optional[Callable[[WhalePosition], None]] = None,
        on_alert: Optional[Callable[[Alert], None]] = None,
        poll_interval: float = 2.0,
    ) -> None:
        self.tracked_wallets = tracked_wallets or []
        self._price_feed = price_feed or self._default_price
        self.on_whale = on_whale
        self.on_alert = on_alert
        self.poll_interval = poll_interval
        self._cfg = get_api_config()
        self._thresholds = get_whale_thresholds()
        self._seen_signatures: set = set()
        self._running = False
        # Reverse map: mint -> symbol
        self._mint_to_symbol: Dict[str, str] = {v: k for k, v in SOLANA_TOKENS.items()}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Start polling. Runs indefinitely until cancelled."""
        self._running = True
        logger.info("Starting Solana on-chain monitor (poll interval: %.1fs)", self.poll_interval)
        await asyncio.gather(
            self._poll_token_program(),
            self._poll_tracked_wallets(),
        )

    async def stop(self) -> None:
        self._running = False

    # ------------------------------------------------------------------
    # Polling loops
    # ------------------------------------------------------------------

    async def _poll_token_program(self) -> None:
        """Poll recent signatures for the SPL Token program."""
        program_ids = [SPL_TOKEN_PROGRAM, SPL_TOKEN_2022_PROGRAM]
        while self._running:
            for program_id in program_ids:
                try:
                    sigs = await self._get_signatures(program_id, limit=100)
                    new_sigs = [s for s in sigs if s not in self._seen_signatures]
                    for sig in new_sigs:
                        self._seen_signatures.add(sig)
                        asyncio.ensure_future(self._process_signature(sig))
                    # Prevent unbounded growth
                    if len(self._seen_signatures) > 50_000:
                        self._seen_signatures = set(list(self._seen_signatures)[-25_000:])
                except Exception as exc:
                    logger.warning("Error polling token program %s: %s", program_id, exc)
            await asyncio.sleep(self.poll_interval)

    async def _poll_tracked_wallets(self) -> None:
        """Poll recent transactions for explicitly tracked whale wallets."""
        if not self.tracked_wallets:
            return
        while self._running:
            for wallet in self.tracked_wallets:
                try:
                    sigs = await self._get_signatures(wallet, limit=20)
                    new_sigs = [s for s in sigs if s not in self._seen_signatures]
                    for sig in new_sigs:
                        self._seen_signatures.add(sig)
                        asyncio.ensure_future(self._process_signature(sig, wallet_hint=wallet))
                except Exception as exc:
                    logger.warning("Error polling wallet %s: %s", wallet, exc)
            await asyncio.sleep(self.poll_interval * 2)

    # ------------------------------------------------------------------
    # RPC helpers
    # ------------------------------------------------------------------

    async def _rpc(self, method: str, params: list) -> dict:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(
                self._cfg.solana_rpc_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                resp.raise_for_status()
                return await resp.json()

    async def _get_signatures(self, address: str, limit: int = 100) -> List[str]:
        result = await self._rpc(
            "getSignaturesForAddress",
            [address, {"limit": limit, "commitment": "confirmed"}],
        )
        sigs = result.get("result", []) or []
        return [s["signature"] for s in sigs if isinstance(s, dict)]

    async def _get_transaction(self, signature: str) -> Optional[dict]:
        result = await self._rpc(
            "getTransaction",
            [
                signature,
                {
                    "encoding": "jsonParsed",
                    "maxSupportedTransactionVersion": 0,
                    "commitment": "confirmed",
                },
            ],
        )
        return result.get("result")

    # ------------------------------------------------------------------
    # Transaction processing
    # ------------------------------------------------------------------

    async def _process_signature(
        self, signature: str, wallet_hint: Optional[str] = None
    ) -> None:
        try:
            tx = await self._get_transaction(signature)
            if tx is None or tx.get("meta", {}).get("err") is not None:
                return
            await self._parse_token_transfers(tx, signature, wallet_hint)
        except Exception as exc:
            logger.debug("Failed to process signature %s: %s", signature, exc)

    async def _parse_token_transfers(
        self, tx: dict, signature: str, wallet_hint: Optional[str]
    ) -> None:
        """
        Extract SPL token transfers from a parsed transaction and emit
        WhalePosition events for large ones.
        """
        instructions = (
            tx.get("transaction", {})
            .get("message", {})
            .get("instructions", [])
        )
        inner_instructions = tx.get("meta", {}).get("innerInstructions", [])

        all_instructions = list(instructions)
        for inner in inner_instructions:
            all_instructions.extend(inner.get("instructions", []))

        block_time = tx.get("blockTime", time.time())

        for ix in all_instructions:
            if not isinstance(ix, dict):
                continue
            parsed = ix.get("parsed")
            if not isinstance(parsed, dict):
                continue
            ix_type = parsed.get("type")
            if ix_type not in ("transfer", "transferChecked"):
                continue

            info = parsed.get("info", {})
            amount_raw = info.get("amount") or info.get("tokenAmount", {}).get("amount")
            decimals = info.get("decimals") or info.get("tokenAmount", {}).get("decimals")
            mint = info.get("mint")

            if amount_raw is None or mint is None:
                continue

            try:
                amount_raw = int(amount_raw)
                decimals = int(decimals) if decimals is not None else 0
                amount = amount_raw / (10 ** decimals)
            except (ValueError, TypeError):
                continue

            symbol = self._mint_to_symbol.get(mint)
            if symbol is None:
                continue

            price = await asyncio.get_event_loop().run_in_executor(
                None, self._price_feed, symbol
            )
            notional_usd = amount * price

            if notional_usd < self._thresholds.min_solana_transfer_usd:
                continue

            source_wallet = info.get("authority") or info.get("source") or wallet_hint
            dest_wallet = info.get("destination")

            position = WhalePosition(
                source=TradeSource.SOLANA,
                symbol=f"{symbol}/USD",
                side="SELL",  # on-chain transfer direction is ambiguous; mark SELL (moving)
                quantity=amount,
                price=price,
                notional_usd=notional_usd,
                timestamp=float(block_time) if block_time else time.time(),
                tx_hash=signature,
                wallet_address=source_wallet,
                raw={"mint": mint, "destination": dest_wallet},
            )
            logger.info("Solana whale transfer: %s", position)

            if self.on_whale:
                self.on_whale(position)

            if notional_usd >= self._thresholds.min_solana_transfer_usd * 5:
                alert = Alert(
                    severity=AlertSeverity.HIGH,
                    title=f"Large Solana {symbol} transfer",
                    description=(
                        f"{amount:,.2f} {symbol} (${notional_usd:,.0f}) "
                        f"from {source_wallet} → {dest_wallet}"
                    ),
                    source="SolanaWhaleMonitor",
                    metadata={
                        "mint": mint,
                        "amount": amount,
                        "notional_usd": notional_usd,
                        "tx_hash": signature,
                    },
                )
                if self.on_alert:
                    self.on_alert(alert)

    @staticmethod
    def _default_price(symbol: str) -> float:
        """Stub price feed – replace with a real one in production."""
        prices = {
            "SOL": 150.0,
            "USDC": 1.0,
            "USDT": 1.0,
            "RAY": 2.5,
            "ORCA": 3.0,
            "JTO": 2.8,
        }
        return prices.get(symbol, 1.0)
