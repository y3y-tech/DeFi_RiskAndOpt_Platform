"""
Cascade liquidation risk model.

Combines Aave position data with Uniswap liquidity depth to estimate:
  1. The probability of a cascade liquidation event.
  2. The depth of the cascade (how many positions liquidated).
  3. Price impact if all at-risk positions were liquidated simultaneously.

Algorithm:
  - Rank positions by health factor (lowest first = most at risk).
  - Simulate price drops step by step (1 % increments).
  - At each step, determine which positions become eligible for liquidation.
  - Apply the liquidation to the pool depth; re-price; repeat.
  - Stop when no new liquidations occur or price drop exceeds 50 %.
"""

from __future__ import annotations

import logging
import math
from typing import Dict, List, Optional, Tuple

from whale_detector.config import get_risk_thresholds
from whale_detector.models import (
    Alert,
    AlertSeverity,
    AavePosition,
    LiquidationRiskReport,
    LiquiditySnapshot,
)

logger = logging.getLogger(__name__)


class LiquidationRiskModel:
    """
    Cascade liquidation risk calculator.

    Usage::

        model = LiquidationRiskModel(on_alert=alert_cb)
        report = model.compute(positions, pools, prices)
    """

    def __init__(self, on_alert: Optional[callable] = None) -> None:
        self._thresholds = get_risk_thresholds()
        self.on_alert = on_alert

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def compute(
        self,
        positions: List[AavePosition],
        pools: List[LiquiditySnapshot],
        prices: Dict[str, float],
    ) -> LiquidationRiskReport:
        """
        Compute cascade liquidation risk.

        Args:
            positions: Aave positions (ideally those with HF < 1.5).
            pools: Uniswap pool liquidity snapshots.
            prices: Current token prices {symbol: usd_price}.

        Returns:
            A :class:`LiquidationRiskReport`.
        """
        if not positions:
            return LiquidationRiskReport(
                risk_score=0.0,
                positions_at_risk=0,
                total_debt_at_risk_usd=0.0,
                cascade_depth_estimate=0,
                concentration_score=0.0,
            )

        pool_depth = self._aggregate_pool_depth(pools)
        cascade_depth, total_debt, price_impact = self._simulate_cascade(
            positions, pool_depth, prices
        )
        concentration = self._compute_concentration(positions)
        risk_score = self._compute_risk_score(
            cascade_depth, total_debt, price_impact, concentration
        )

        top_exposures = self._top_exposures(positions)

        report = LiquidationRiskReport(
            risk_score=risk_score,
            positions_at_risk=len(positions),
            total_debt_at_risk_usd=total_debt,
            cascade_depth_estimate=cascade_depth,
            concentration_score=concentration,
            top_exposures=top_exposures,
        )

        self._check_and_alert(report)
        return report

    # ------------------------------------------------------------------
    # Cascade simulation
    # ------------------------------------------------------------------

    def _simulate_cascade(
        self,
        positions: List[AavePosition],
        pool_depth_usd: float,
        prices: Dict[str, float],
    ) -> Tuple[int, float, float]:
        """
        Simulate a cascade liquidation.

        Returns:
            (cascade_depth, total_debt_liquidated_usd, price_impact_fraction)
        """
        if pool_depth_usd <= 0:
            pool_depth_usd = 1e6  # fallback if no liquidity data

        # Sort by health factor ascending (most at-risk first)
        sorted_positions = sorted(positions, key=lambda p: p.health_factor)

        # Current price multiplier (1.0 = no change)
        price_mult = 1.0
        liquidated = 0
        total_liquidated_usd = 0.0
        step_pct = 0.01  # 1 % price drop per simulation step
        max_steps = 50   # max 50 % price drop modelled

        remaining = list(sorted_positions)

        for _step in range(max_steps):
            newly_liquidated: List[AavePosition] = []
            still_healthy: List[AavePosition] = []

            for pos in remaining:
                # Adjust health factor for price movement
                # HF_new ≈ HF_old * price_mult (collateral falls, debt stays)
                adjusted_hf = pos.health_factor * price_mult
                if adjusted_hf < 1.0:
                    newly_liquidated.append(pos)
                else:
                    still_healthy.append(pos)

            if not newly_liquidated:
                break

            # Compute sell pressure from liquidations
            sell_pressure = sum(p.collateral_usd * price_mult for p in newly_liquidated)
            liquidated += len(newly_liquidated)
            total_liquidated_usd += sum(p.debt_usd for p in newly_liquidated)

            # Price impact: simplified constant-product model
            impact = sell_pressure / (pool_depth_usd + sell_pressure)
            price_mult *= (1 - impact)

            remaining = still_healthy

        price_impact = 1.0 - price_mult
        return liquidated, total_liquidated_usd, price_impact

    # ------------------------------------------------------------------
    # Concentration (HHI)
    # ------------------------------------------------------------------

    def _compute_concentration(self, positions: List[AavePosition]) -> float:
        """
        Compute a normalised HHI for debt concentration by asset.
        Returns 0-1 (1 = fully concentrated in one asset).
        """
        asset_debt: Dict[str, float] = {}
        for pos in positions:
            for asset, amount in pos.debt_assets.items():
                try:
                    asset_debt[asset] = asset_debt.get(asset, 0.0) + float(amount)
                except (ValueError, TypeError):
                    pass

        total = sum(asset_debt.values())
        if total == 0:
            return 0.0

        n = len(asset_debt)
        if n == 0:
            return 0.0

        hhi = sum((v / total) ** 2 for v in asset_debt.values())
        # Normalise: HHI ranges from 1/n (perfectly distributed) to 1 (monopoly)
        hhi_min = 1.0 / n
        if hhi_min >= 1.0:
            return 0.0
        return (hhi - hhi_min) / (1.0 - hhi_min)

    # ------------------------------------------------------------------
    # Risk scoring
    # ------------------------------------------------------------------

    def _compute_risk_score(
        self,
        cascade_depth: int,
        total_debt_usd: float,
        price_impact: float,
        concentration: float,
    ) -> float:
        """
        Combine sub-scores into a single 0-1 risk score.

        Sub-scores:
          - depth_score: log-scaled cascade depth
          - debt_score:  log-scaled total debt at risk
          - impact_score: estimated price impact
          - concentration_score: HHI concentration
        """
        # Depth: 0 → 0, 100 → ~0.7, 1000 → ~1.0
        depth_score = math.log1p(cascade_depth) / math.log1p(1000)
        # Debt: $0 → 0, $100M → ~0.7, $1B → ~1.0
        debt_score = math.log1p(total_debt_usd / 1e6) / math.log1p(1000)
        # Impact already 0-1
        impact_score = min(price_impact * 5, 1.0)  # scale: 20 % impact → score of 1.0

        composite = (
            0.30 * depth_score
            + 0.30 * debt_score
            + 0.25 * impact_score
            + 0.15 * concentration
        )
        return min(composite, 1.0)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _aggregate_pool_depth(pools: List[LiquiditySnapshot]) -> float:
        """Sum TVL across all pools as a proxy for available market depth."""
        return sum(p.tvl_usd for p in pools)

    @staticmethod
    def _top_exposures(positions: List[AavePosition], n: int = 10) -> List[dict]:
        """Return the top-n most exposed positions."""
        sorted_pos = sorted(positions, key=lambda p: p.debt_usd, reverse=True)
        return [
            {
                "user": p.user[:8] + "..." + p.user[-4:],
                "debt_usd": p.debt_usd,
                "health_factor": p.health_factor,
                "ltv": p.ltv,
            }
            for p in sorted_pos[:n]
        ]

    def _check_and_alert(self, report: LiquidationRiskReport) -> None:
        if report.risk_score >= self._thresholds.cascade_risk_high:
            severity = AlertSeverity.CRITICAL
        elif report.risk_score >= self._thresholds.cascade_risk_medium:
            severity = AlertSeverity.HIGH
        else:
            return

        alert = Alert(
            severity=severity,
            title=f"Cascade liquidation risk: {report.risk_score:.1%}",
            description=(
                f"{report.positions_at_risk} positions at risk, "
                f"${report.total_debt_at_risk_usd:,.0f} total debt. "
                f"Estimated cascade depth: {report.cascade_depth_estimate} positions."
            ),
            source="LiquidationRiskModel",
            metadata={
                "risk_score": report.risk_score,
                "positions_at_risk": report.positions_at_risk,
                "total_debt_usd": report.total_debt_at_risk_usd,
                "cascade_depth": report.cascade_depth_estimate,
                "concentration": report.concentration_score,
            },
        )
        if self.on_alert:
            self.on_alert(alert)
