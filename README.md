# Crypto Whale Detection & Risk Analytics Platform

A real-time system for detecting large position movements and quantifying DeFi risk exposure across centralised and decentralised exchanges.

## Features

### 1. Real-Time Whale Detection
- **Binance WebSocket Monitor** — streams `aggTrade` data across 18+ pairs, multiplexed over multiple connections (supports 10,000+ simultaneous position streams)
- **Solana On-Chain Monitor** — polls the Solana JSON-RPC for large SPL token transfers; decodes transfer instructions and computes USD notional
- **CEX Deposit Pattern Detector** — accumulates on-chain transfers to known exchange hot wallets in a sliding window; fires early-warning alerts when inflows surge (a leading indicator for market dumps)

### 2. Risk Analytics Dashboard
- **Aave v3 Analytics** — fetches at-risk borrower positions (health factor < 1.2), monitors individual HF thresholds, and computes aggregate at-risk debt by asset
- **Uniswap v3 Analytics** — fetches top-50 pool TVL/volume snapshots, detects large swaps, estimates slippage, and computes pool liquidity HHI
- **Cascade Liquidation Risk Model** — simulates price-drop scenarios step-by-step to estimate cascade depth, total debt liquidated, and price impact; produces a 0-1 composite risk score
- **Concentration Exposure Tracker** — maintains per-pair rolling HHI of whale volume concentration; fires alerts when a single entity dominates

### 3. Plotly Dash Dashboard
Live dashboard at `http://localhost:8050` showing:
- KPI row: whale trade count, total volume, CEX inflow, liquidation risk score, alert count
- Whale volume bar chart (buy vs. sell by pair)
- CEX deposit accumulation chart
- Liquidation risk gauge (Aave)
- Uniswap pool TVL chart
- Concentration exposure table (15+ pairs)
- Rolling alert feed

## Architecture

```
whale_detector/
├── config.py              Configuration, thresholds, API endpoints
├── models.py              Shared data models (dataclasses)
├── orchestrator.py        Top-level async orchestrator
├── detectors/
│   ├── binance_ws.py      Binance WebSocket position monitor
│   ├── solana_onchain.py  Solana JSON-RPC polling monitor
│   └── cex_deposit.py     CEX deposit pattern detector
├── analytics/
│   ├── aave.py            Aave v3 subgraph analytics
│   ├── uniswap.py         Uniswap v3 subgraph analytics
│   ├── liquidation_risk.py Cascade liquidation risk model
│   └── concentration.py   Whale concentration tracker
├── dashboard/
│   └── app.py             Plotly Dash dashboard
└── utils/
    ├── alerts.py          Alert manager (logging + webhook)
    └── storage.py         SQLite persistence layer
tests/
├── test_models.py
├── test_cex_deposit.py
├── test_liquidation_risk.py
├── test_concentration.py
├── test_storage.py
└── test_alerts.py
```

## Quick Start

### Installation

```bash
# Using uv (recommended)
uv sync

# Or pip
pip install -e ".[dev]"
```

### Configuration

Set environment variables (or use defaults):

```bash
export BINANCE_API_KEY=your_key
export BINANCE_API_SECRET=your_secret
export SOLANA_RPC_URL=https://api.mainnet-beta.solana.com
export ALERT_WEBHOOK_URL=https://hooks.slack.com/services/...  # optional
export DB_PATH=whale_data.db
```

### Running

```bash
# Run everything (monitor + dashboard)
python main.py

# Monitor only (no dashboard)
python main.py monitor

# Dashboard only (reads from existing DB)
python main.py dashboard
```

### Running Tests

```bash
pytest tests/ -v
```

## Configuration

Key thresholds can be overridden by subclassing `WhaleThresholds` or `RiskThresholds` in `config.py`:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `min_trade_usd` | $500,000 | Minimum trade size to classify as whale |
| `min_solana_transfer_usd` | $250,000 | Minimum Solana transfer to flag |
| `cex_deposit_alert_usd` | $1,000,000 | CEX inflow threshold (5-min window) |
| `cascade_risk_high` | 0.75 | Liquidation risk score for CRITICAL alert |
| `concentration_critical_pct` | 40% | Single-entity share triggering HIGH alert |
| `aave_health_factor_critical` | 1.05 | HF below which CRITICAL alert fires |

## Monitored Pairs (18)

`BTCUSDT ETHUSDT BNBUSDT SOLUSDT ADAUSDT XRPUSDT DOTUSDT AVAXUSDT MATICUSDT LINKUSDT UNIUSDT AAVEUSDT LDOUSDT ARBUSDT OPUSDT INJUSDT SUIUSDT APTUSDT`

## Data Sources

| Source | API | Notes |
|--------|-----|-------|
| Binance | WebSocket `aggTrade` stream | Free, no auth needed for public data |
| Solana | JSON-RPC `getSignaturesForAddress` | Free public endpoint; consider a paid RPC for production |
| Aave v3 | The Graph subgraph | Free |
| Uniswap v3 | The Graph subgraph | Free |

## Extending

- **Add a new CEX deposit address**: call `detector.add_cex_wallet("Exchange", "address")`
- **Add a new trading pair**: add to `MONITORED_PAIRS` in `config.py`
- **Add a Solana token**: add to `SOLANA_TOKENS` in `config.py`
- **Use a production database**: replace `DataStore` with a PostgreSQL/TimescaleDB implementation following the same interface
