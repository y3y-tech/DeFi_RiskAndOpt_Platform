"""
Aave v3 liquidity and position analytics.

Fetches borrower positions, health factors, and aggregate liquidity metrics
from the Aave v3 subgraph to quantify near-liquidation exposure and
cascade liquidation risk.
"""

from __future__ import annotations

import logging
import time
from typing import Dict, List, Optional

import aiohttp

from whale_detector.config import get_api_config, get_risk_thresholds
from whale_detector.models import Alert, AlertSeverity, AavePosition, LiquiditySnapshot

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# GraphQL queries
# ---------------------------------------------------------------------------

_QUERY_AT_RISK_POSITIONS = """
{
  users(
    first: 1000
    where: { borrowedReservesCount_gt: 0 }
    orderBy: healthFactor
    orderDirection: asc
  ) {
    id
    healthFactor
    totalCollateralUSD
    totalDebtUSD
    collateralReserve: reserves(where: { currentATokenBalance_gt: 0 }) {
      reserve { symbol underlyingAsset }
      currentATokenBalance
    }
    borrowReserve: reserves(where: { currentTotalDebt_gt: 0 }) {
      reserve { symbol underlyingAsset }
      currentTotalDebt
    }
  }
}
"""

_QUERY_RESERVE_LIQUIDITY = """
{
  reserves(first: 50 orderBy: totalLiquidity orderDirection: desc) {
    symbol
    underlyingAsset
    totalLiquidity
    totalLiquidityUSD: totalDeposits
    availableLiquidity
    utilizationRate
    liquidityRate
    variableBorrowRate
    totalCurrentVariableDebt
    price { priceInEth }
  }
}
"""


class AaveAnalytics:
    """
    Fetches and analyses Aave v3 position data.

    Provides:
      - Per-user health factor monitoring with alerts
      - Aggregate liquidation risk across all positions
      - Reserve liquidity snapshots

    Usage::

        aave = AaveAnalytics(on_alert=alert_cb)
        positions = await aave.fetch_at_risk_positions(max_health_factor=1.2)
        reserves  = await aave.fetch_reserve_liquidity()
    """

    def __init__(
        self,
        on_alert: Optional[callable] = None,
    ) -> None:
        self._cfg = get_api_config()
        self._thresholds = get_risk_thresholds()
        self.on_alert = on_alert

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def fetch_at_risk_positions(
        self, max_health_factor: float = 1.2
    ) -> List[AavePosition]:
        """
        Return Aave positions with health factor below *max_health_factor*.
        Fires alerts for positions near the liquidation threshold.
        """
        raw_users = await self._graphql(self._cfg.aave_subgraph_url, _QUERY_AT_RISK_POSITIONS)
        users = (raw_users.get("data") or {}).get("users", [])

        positions: List[AavePosition] = []
        for u in users:
            try:
                hf = float(u.get("healthFactor") or 1e18)
                if hf > max_health_factor:
                    continue
                collateral = float(u.get("totalCollateralUSD") or 0)
                debt = float(u.get("totalDebtUSD") or 0)
            except (ValueError, TypeError):
                continue

            pos = AavePosition(
                user=u["id"],
                collateral_usd=collateral,
                debt_usd=debt,
                health_factor=hf,
                collateral_assets=self._parse_reserves(u.get("collateralReserve", [])),
                debt_assets=self._parse_reserves(u.get("borrowReserve", [])),
            )
            positions.append(pos)
            self._check_hf_alert(pos)

        logger.info("Fetched %d at-risk Aave positions (hf < %.2f)", len(positions), max_health_factor)
        return positions

    async def fetch_reserve_liquidity(self) -> List[LiquiditySnapshot]:
        """Return current liquidity snapshots for all Aave reserves."""
        raw = await self._graphql(self._cfg.aave_subgraph_url, _QUERY_RESERVE_LIQUIDITY)
        reserves = (raw.get("data") or {}).get("reserves", [])
        snapshots: List[LiquiditySnapshot] = []
        for r in reserves:
            try:
                tvl = float(r.get("totalLiquidityUSD") or 0)
            except (ValueError, TypeError):
                tvl = 0.0
            snap = LiquiditySnapshot(
                protocol="Aave",
                pool_id=r.get("underlyingAsset", ""),
                token0=r.get("symbol", ""),
                token1="USD",
                tvl_usd=tvl,
                volume_24h_usd=0.0,  # not available in this query
            )
            snapshots.append(snap)
        return snapshots

    async def compute_aggregate_risk(self, positions: List[AavePosition]) -> Dict:
        """
        Compute aggregate risk metrics across a list of at-risk positions.

        Returns a dict with:
          - total_debt_at_risk_usd
          - positions_at_risk
          - weighted_avg_health_factor
          - asset_concentration  (per-asset share of at-risk debt)
        """
        if not positions:
            return {
                "total_debt_at_risk_usd": 0.0,
                "positions_at_risk": 0,
                "weighted_avg_health_factor": 0.0,
                "asset_concentration": {},
            }

        total_debt = sum(p.debt_usd for p in positions)
        asset_debt: Dict[str, float] = {}
        for p in positions:
            for asset, amount in p.debt_assets.items():
                asset_debt[asset] = asset_debt.get(asset, 0.0) + float(amount)

        weighted_hf = (
            sum(p.health_factor * p.debt_usd for p in positions) / total_debt
            if total_debt > 0 else 0.0
        )

        concentration = {
            k: v / total_debt for k, v in asset_debt.items()
        } if total_debt > 0 else {}

        return {
            "total_debt_at_risk_usd": total_debt,
            "positions_at_risk": len(positions),
            "weighted_avg_health_factor": weighted_hf,
            "asset_concentration": concentration,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _check_hf_alert(self, pos: AavePosition) -> None:
        if pos.health_factor <= self._thresholds.aave_health_factor_critical:
            severity = AlertSeverity.CRITICAL
        elif pos.health_factor <= self._thresholds.aave_health_factor_warning:
            severity = AlertSeverity.HIGH
        else:
            return

        alert = Alert(
            severity=severity,
            title=f"Aave position near liquidation (HF={pos.health_factor:.4f})",
            description=(
                f"User {pos.user[:8]}...{pos.user[-4:]} has "
                f"${pos.collateral_usd:,.0f} collateral / ${pos.debt_usd:,.0f} debt "
                f"(HF={pos.health_factor:.4f}, LTV={pos.ltv:.2%})"
            ),
            source="AaveAnalytics",
            metadata={
                "user": pos.user,
                "health_factor": pos.health_factor,
                "collateral_usd": pos.collateral_usd,
                "debt_usd": pos.debt_usd,
            },
        )
        if self.on_alert:
            self.on_alert(alert)

    @staticmethod
    def _parse_reserves(reserves: list) -> Dict[str, str]:
        result: Dict[str, str] = {}
        for r in reserves:
            reserve = r.get("reserve", {})
            symbol = reserve.get("symbol", "UNKNOWN")
            balance = r.get("currentATokenBalance") or r.get("currentTotalDebt") or "0"
            result[symbol] = balance
        return result

    @staticmethod
    async def _graphql(url: str, query: str) -> dict:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                json={"query": query},
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                resp.raise_for_status()
                return await resp.json()
