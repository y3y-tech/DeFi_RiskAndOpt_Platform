"""
Shared data models (dataclasses) used across the platform.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class AlertSeverity(str, Enum):
    INFO = "INFO"
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class TradeSource(str, Enum):
    BINANCE = "BINANCE"
    SOLANA = "SOLANA"
    CEX_DEPOSIT = "CEX_DEPOSIT"


@dataclass
class WhalePosition:
    """A large position or trade detected on any chain/exchange."""

    source: TradeSource
    symbol: str                    # e.g. "BTCUSDT" or "SOL/USDC"
    side: str                      # "BUY" | "SELL"
    quantity: float
    price: float
    notional_usd: float
    timestamp: float = field(default_factory=time.time)
    tx_hash: Optional[str] = None  # Solana tx signature
    wallet_address: Optional[str] = None
    exchange: Optional[str] = None
    raw: Optional[dict] = None     # original payload

    def __repr__(self) -> str:
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(self.timestamp))
        return (
            f"WhalePosition({self.source.value} {self.side} "
            f"{self.quantity:.4f} {self.symbol} @ ${self.price:,.2f} "
            f"(${self.notional_usd:,.0f}) [{ts}])"
        )


@dataclass
class CEXDepositEvent:
    """Accumulation of token deposits into a centralised exchange wallet."""

    exchange: str
    token: str
    amount: float
    amount_usd: float
    wallet_from: str
    wallet_to: str                 # CEX deposit address
    timestamp: float = field(default_factory=time.time)
    tx_hash: Optional[str] = None
    chain: str = "SOLANA"


@dataclass
class Alert:
    """Platform alert emitted when a threshold is crossed."""

    severity: AlertSeverity
    title: str
    description: str
    source: str                    # module that raised the alert
    timestamp: float = field(default_factory=time.time)
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "severity": self.severity.value,
            "title": self.title,
            "description": self.description,
            "source": self.source,
            "timestamp": self.timestamp,
            "metadata": self.metadata,
        }


@dataclass
class LiquiditySnapshot:
    """Point-in-time liquidity data for a DeFi protocol pool."""

    protocol: str                  # "Aave" | "Uniswap"
    pool_id: str
    token0: str
    token1: str
    tvl_usd: float
    volume_24h_usd: float
    fee_tier: Optional[float] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class AavePosition:
    """A single borrower's position on Aave."""

    user: str
    collateral_usd: float
    debt_usd: float
    health_factor: float
    collateral_assets: dict = field(default_factory=dict)
    debt_assets: dict = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)

    @property
    def ltv(self) -> float:
        if self.collateral_usd == 0:
            return 0.0
        return self.debt_usd / self.collateral_usd


@dataclass
class LiquidationRiskReport:
    """Cascade liquidation risk assessment for a set of positions."""

    risk_score: float              # 0.0 – 1.0
    positions_at_risk: int
    total_debt_at_risk_usd: float
    cascade_depth_estimate: int    # estimated number of cascading liquidations
    concentration_score: float     # Herfindahl-Hirschman Index normalised 0-1
    top_exposures: list = field(default_factory=list)
    timestamp: float = field(default_factory=time.time)
