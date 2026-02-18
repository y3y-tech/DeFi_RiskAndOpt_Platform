"""
Configuration and constants for the Whale Detection & Risk Analytics Platform.
"""

import os
from dataclasses import dataclass, field
from typing import List, Optional


# ---------------------------------------------------------------------------
# Trading pairs monitored (15+ pairs as required)
# ---------------------------------------------------------------------------
MONITORED_PAIRS: List[str] = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "ADAUSDT",
    "XRPUSDT", "DOTUSDT", "AVAXUSDT", "MATICUSDT", "LINKUSDT",
    "UNIUSDT", "AAVEUSDT", "LDOUSDT", "ARBUSDT", "OPUSDT",
    "INJUSDT", "SUIUSDT", "APTUSDT",
]

# Solana token mints to track
SOLANA_TOKENS = {
    "SOL":  "So11111111111111111111111111111111111111112",
    "USDC": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    "USDT": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
    "RAY":  "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",
    "ORCA": "orcaEKTdK7LKz57vaAYr9QeNsVEPfiu6QeMU1kektZE",
    "JTO":  "jtojtomepa8beP8AuQc6eXt5FriJwfFMwjx2v2matFi",
}

# CEX deposit address prefixes / known exchange wallets (Solana)
KNOWN_CEX_DEPOSIT_WALLETS = {
    "Binance":  ["binance-hot-wallet-placeholder"],
    "Coinbase": ["coinbase-hot-wallet-placeholder"],
    "Kraken":   ["kraken-hot-wallet-placeholder"],
    "OKX":      ["okx-hot-wallet-placeholder"],
}


# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------
@dataclass
class WhaleThresholds:
    # Minimum USD notional to classify a trade as a whale trade
    min_trade_usd: float = 500_000.0
    # Minimum USD for a Solana on-chain transfer to be flagged
    min_solana_transfer_usd: float = 250_000.0
    # Minimum USD deposited to a CEX in a sliding window to trigger alert
    cex_deposit_alert_usd: float = 1_000_000.0
    # Sliding window (seconds) for CEX deposit accumulation
    cex_deposit_window_secs: int = 300  # 5 minutes
    # Max positions tracked simultaneously
    max_tracked_positions: int = 10_000


# ---------------------------------------------------------------------------
# Risk thresholds
# ---------------------------------------------------------------------------
@dataclass
class RiskThresholds:
    # Cascade liquidation risk score (0-1) above which alert fires
    cascade_risk_high: float = 0.75
    cascade_risk_medium: float = 0.50
    # Concentration: single-entity share of pool that triggers warning
    concentration_warning_pct: float = 0.20   # 20 %
    concentration_critical_pct: float = 0.40  # 40 %
    # Health-factor threshold for Aave positions near liquidation
    aave_health_factor_warning: float = 1.10
    aave_health_factor_critical: float = 1.05


# ---------------------------------------------------------------------------
# API / connection settings (override with environment variables)
# ---------------------------------------------------------------------------
@dataclass
class APIConfig:
    # Binance
    binance_ws_url: str = "wss://stream.binance.com:9443/stream"
    binance_rest_url: str = "https://api.binance.com"
    binance_api_key: str = field(
        default_factory=lambda: os.getenv("BINANCE_API_KEY", "")
    )
    binance_api_secret: str = field(
        default_factory=lambda: os.getenv("BINANCE_API_SECRET", "")
    )

    # Solana
    solana_rpc_url: str = field(
        default_factory=lambda: os.getenv(
            "SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com"
        )
    )
    solana_ws_url: str = field(
        default_factory=lambda: os.getenv(
            "SOLANA_WS_URL", "wss://api.mainnet-beta.solana.com"
        )
    )

    # DeFi protocol subgraph endpoints
    aave_subgraph_url: str = (
        "https://api.thegraph.com/subgraphs/name/aave/protocol-v3"
    )
    uniswap_subgraph_url: str = (
        "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3"
    )

    # Alert webhook (e.g. Slack / Discord)
    alert_webhook_url: Optional[str] = field(
        default_factory=lambda: os.getenv("ALERT_WEBHOOK_URL")
    )

    # Dashboard
    dashboard_host: str = "0.0.0.0"
    dashboard_port: int = 8050

    # Storage
    db_path: str = field(
        default_factory=lambda: os.getenv("DB_PATH", "whale_data.db")
    )


# ---------------------------------------------------------------------------
# Singleton accessors
# ---------------------------------------------------------------------------
_whale_thresholds: Optional[WhaleThresholds] = None
_risk_thresholds: Optional[RiskThresholds] = None
_api_config: Optional[APIConfig] = None


def get_whale_thresholds() -> WhaleThresholds:
    global _whale_thresholds
    if _whale_thresholds is None:
        _whale_thresholds = WhaleThresholds()
    return _whale_thresholds


def get_risk_thresholds() -> RiskThresholds:
    global _risk_thresholds
    if _risk_thresholds is None:
        _risk_thresholds = RiskThresholds()
    return _risk_thresholds


def get_api_config() -> APIConfig:
    global _api_config
    if _api_config is None:
        _api_config = APIConfig()
    return _api_config
