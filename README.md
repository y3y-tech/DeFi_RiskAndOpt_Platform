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

## How It Works

### Whale Trade Detection

The platform detects "whale" trades — large position movements that can signal institutional activity or market-moving events.

#### Binance (CEX) Detection Flow
```
WebSocket Stream (wss://stream.binance.com)
         ↓
    aggTrade messages (price, quantity, side)
         ↓
    notional_usd = price × quantity
         ↓
    IF notional_usd ≥ $500,000:
        → Save as WhalePosition to database
        → Fire HIGH alert if ≥ $5M (10× threshold)
```

#### Solana (On-Chain) Detection Flow
```
Poll Solana RPC every 2 seconds
         ↓
    Fetch new transaction signatures for SPL Token Program
         ↓
    Decode token transfers (amount, mint address, destination)
         ↓
    notional_usd = amount × token_price
         ↓
    IF notional_usd ≥ $250,000:
        → Save as WhalePosition
        → Check if destination is a known CEX deposit wallet
        → Fire HIGH alert if ≥ $1.25M (5× threshold)
```

### CEX Deposit Detection (Early Dump Signal)

When tokens flow into exchange wallets, it often precedes selling. The system tracks this pattern:

```
Whale transfers tokens → Exchange hot wallet
         ↓
5-minute sliding window accumulates all transfers
         ↓
IF window_total ≥ $1M:  → HIGH alert
IF window_total ≥ $3M:  → CRITICAL alert ("potential dump incoming")
```

### Trading Pairs Explained

Trading pairs represent the exchange rate between two assets. For example:
- **BTCUSDT** = Bitcoin priced in USDT (Tether stablecoin)
- **ETHUSDT** = Ethereum priced in USDT

The platform monitors 18 pairs across different categories:

| Category | Pairs |
|----------|-------|
| **Majors** | BTCUSDT, ETHUSDT, BNBUSDT, XRPUSDT, ADAUSDT |
| **DeFi/L2** | UNIUSDT, AAVEUSDT, LDOUSDT, ARBUSDT, OPUSDT, INJUSDT |
| **Alt L1s** | SOLUSDT, AVAXUSDT, MATICUSDT, DOTUSDT, APTUSDT, SUIUSDT, LINKUSDT |

### Cascade Liquidation Risk Calculation

The **LiquidationRiskModel** simulates what happens during a market crash on Aave:

#### Step-by-Step Simulation:
1. **Fetch at-risk positions** — Aave borrowers with health factor < 1.5
2. **Simulate price drops** — For each 1% drop:
   - Identify newly liquidatable positions (health factor < 1.0)
   - Calculate sell pressure = Σ(collateral being liquidated)
   - Estimate price impact = sell_pressure / available_liquidity
   - Update prices and repeat (cascade effect)
3. **Stop** when no new liquidations or price drops > 50%

#### Composite Risk Score (0-1):
```
risk_score =
    0.30 × depth_score       (number of positions at risk, log-scaled)
  + 0.30 × debt_score        (total USD at risk, log-scaled)
  + 0.25 × price_impact      (simulated market impact)
  + 0.15 × concentration_hhi (debt concentration by asset)
```

#### Alert Thresholds:
| Score | Severity | Meaning |
|-------|----------|---------|
| ≥ 0.75 | CRITICAL | Cascade liquidation likely |
| ≥ 0.50 | HIGH | Elevated systemic risk |
| < 0.50 | MEDIUM/LOW | Normal conditions |

### Data Flow: Detection to Dashboard

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ Binance WS      │     │ Solana RPC      │     │ Aave/Uniswap    │
│ (real-time)     │     │ (2s poll)       │     │ (60s poll)      │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Platform Orchestrator                         │
│  • Filter by threshold ($500k Binance, $250k Solana)            │
│  • Detect CEX deposit patterns                                   │
│  • Calculate concentration (HHI)                                 │
│  • Run liquidation risk simulation                               │
└─────────────────────────────────┬───────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                     DataStore (SQLite)                           │
│  Tables: whale_positions, cex_deposits, alerts, liquidity_snaps │
└─────────────────────────────────┬───────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Dashboard (Dash/Plotly)                        │
│  • Reads from DB every 5 seconds                                 │
│  • Renders KPIs, charts, tables                                  │
│  • Displays alerts with severity coloring                        │
└─────────────────────────────────────────────────────────────────┘
```

### Why the Dashboard Might Show No Activity

If your dashboard appears empty, check:

1. **Monitor not running** — The dashboard only reads data; run the full platform:
   ```bash
   python main.py  # or python main.py --mode all
   ```

2. **High thresholds** — Default is $500k minimum. For testing, lower in `config.py`:
   ```python
   min_trade_usd: float = 50_000.0  # Reduced from 500k
   ```

3. **Placeholder CEX wallets** — The Solana CEX detection uses placeholder addresses. Replace with real exchange deposit addresses in `config.py`.

4. **Market conditions** — $500k+ trades happen regularly but not every minute. During low-volume periods, you may need to wait.

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
