"""
Uniswap v3 order flow and liquidity analytics.

Fetches pool liquidity, volume, and large swap data from the Uniswap v3
subgraph to monitor CEX-like order flow patterns in DeFi.
"""

from __future__ import annotations

import logging
import time
from typing import Dict, List, Optional

import aiohttp

from whale_detector.config import MONITORED_PAIRS, get_api_config
from whale_detector.models import LiquiditySnapshot

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# GraphQL queries
# ---------------------------------------------------------------------------

_QUERY_TOP_POOLS = """
{
  pools(
    first: 50
    orderBy: totalValueLockedUSD
    orderDirection: desc
    where: { totalValueLockedUSD_gt: 1000000 }
  ) {
    id
    token0 { symbol id }
    token1 { symbol id }
    feeTier
    totalValueLockedUSD
    volumeUSD
    txCount
    liquidity
    sqrtPrice
    tick
  }
}
"""

_QUERY_LARGE_SWAPS = """
query LargeSwaps($minAmountUSD: String!, $since: Int!) {
  swaps(
    first: 100
    orderBy: amountUSD
    orderDirection: desc
    where: { amountUSD_gt: $minAmountUSD timestamp_gt: $since }
  ) {
    id
    timestamp
    pool { id token0 { symbol } token1 { symbol } feeTier }
    sender
    recipient
    amount0
    amount1
    amountUSD
    sqrtPriceX96
  }
}
"""

_QUERY_POOL_TICKS = """
query PoolTicks($poolId: String!) {
  ticks(
    first: 200
    where: { pool: $poolId }
    orderBy: tickIdx
    orderDirection: asc
  ) {
    tickIdx
    liquidityGross
    liquidityNet
  }
}
"""


class UniswapAnalytics:
    """
    Fetches and analyses Uniswap v3 liquidity and order flow data.

    Provides:
      - Top pool liquidity snapshots
      - Large swap detection (whale order flow)
      - Liquidity depth and concentration analysis
      - Tick-level liquidity distribution for slippage estimation

    Usage::

        uni = UniswapAnalytics(on_alert=alert_cb)
        pools = await uni.fetch_top_pools()
        swaps = await uni.fetch_large_swaps(min_usd=500_000)
    """

    def __init__(self, on_alert: Optional[callable] = None) -> None:
        self._cfg = get_api_config()
        self.on_alert = on_alert

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def fetch_top_pools(self) -> List[LiquiditySnapshot]:
        """Return liquidity snapshots for the top 50 Uniswap v3 pools by TVL."""
        raw = await self._graphql(self._cfg.uniswap_subgraph_url, _QUERY_TOP_POOLS)
        pools = (raw.get("data") or {}).get("pools", [])
        snapshots: List[LiquiditySnapshot] = []
        for p in pools:
            try:
                tvl = float(p.get("totalValueLockedUSD") or 0)
                vol = float(p.get("volumeUSD") or 0)
                fee = float(p.get("feeTier") or 0) / 1_000_000
            except (ValueError, TypeError):
                continue
            snap = LiquiditySnapshot(
                protocol="Uniswap",
                pool_id=p["id"],
                token0=p.get("token0", {}).get("symbol", ""),
                token1=p.get("token1", {}).get("symbol", ""),
                tvl_usd=tvl,
                volume_24h_usd=vol,
                fee_tier=fee,
            )
            snapshots.append(snap)
        logger.info("Fetched %d Uniswap pools", len(snapshots))
        return snapshots

    async def fetch_large_swaps(
        self,
        min_usd: float = 500_000.0,
        lookback_secs: int = 3600,
    ) -> List[dict]:
        """
        Return large swaps from the last *lookback_secs* seconds.
        Each item is the raw subgraph swap dict.
        """
        since = int(time.time()) - lookback_secs
        query = _QUERY_LARGE_SWAPS
        variables = {
            "minAmountUSD": str(min_usd),
            "since": since,
        }
        raw = await self._graphql(
            self._cfg.uniswap_subgraph_url,
            query,
            variables=variables,
        )
        swaps = (raw.get("data") or {}).get("swaps", [])
        logger.info("Fetched %d large Uniswap swaps (>$%.0f)", len(swaps), min_usd)
        return swaps

    async def fetch_pool_ticks(self, pool_id: str) -> List[dict]:
        """Fetch tick-level liquidity for a specific pool."""
        raw = await self._graphql(
            self._cfg.uniswap_subgraph_url,
            _QUERY_POOL_TICKS,
            variables={"poolId": pool_id},
        )
        return (raw.get("data") or {}).get("ticks", [])

    async def compute_liquidity_concentration(
        self, pools: List[LiquiditySnapshot]
    ) -> Dict[str, float]:
        """
        Compute Herfindahl-Hirschman Index (HHI) for liquidity concentration
        across pools for each token pair.

        Returns a dict mapping "TOKEN0/TOKEN1" → HHI (0-1, higher = more concentrated).
        """
        pair_tvls: Dict[str, List[float]] = {}
        for snap in pools:
            key = f"{snap.token0}/{snap.token1}"
            pair_tvls.setdefault(key, []).append(snap.tvl_usd)

        hhi: Dict[str, float] = {}
        for pair, tvls in pair_tvls.items():
            total = sum(tvls)
            if total == 0:
                hhi[pair] = 0.0
                continue
            shares = [v / total for v in tvls]
            hhi[pair] = sum(s ** 2 for s in shares)
        return hhi

    async def estimate_slippage(
        self, pool_id: str, trade_size_usd: float, token0_price: float
    ) -> float:
        """
        Estimate price impact (slippage %) for a given trade size in a pool.
        Uses a simplified constant-product approximation from tick liquidity.

        Returns estimated slippage as a fraction (e.g. 0.003 = 0.3 %).
        """
        ticks = await self.fetch_pool_ticks(pool_id)
        if not ticks:
            return 0.0

        total_liquidity = sum(
            abs(int(t.get("liquidityNet") or 0)) for t in ticks
        )
        if total_liquidity == 0:
            return 0.0

        # Simplified: slippage ≈ trade_size / (2 * pool_depth_usd)
        # pool_depth estimated from liquidity and token price
        depth_usd = total_liquidity * token0_price / 1e12  # scale factor
        if depth_usd == 0:
            return 0.0
        return min(trade_size_usd / (2 * depth_usd), 1.0)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @staticmethod
    async def _graphql(url: str, query: str, variables: Optional[dict] = None) -> dict:
        payload: dict = {"query": query}
        if variables:
            payload["variables"] = variables
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                resp.raise_for_status()
                return await resp.json()
