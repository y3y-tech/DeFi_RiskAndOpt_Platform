"""
Risk Analytics Dashboard — Dash/Plotly application.

Integrates:
  - Real-time whale position feed (Binance + Solana)
  - CEX deposit accumulation heatmap
  - Aave liquidation risk gauge
  - Uniswap pool liquidity depth chart
  - Concentration exposure table (15+ pairs)
  - Rolling alert feed

Run with::

    python -m whale_detector.dashboard.app

Environment variables:
    DASHBOARD_HOST  (default: 0.0.0.0)
    DASHBOARD_PORT  (default: 8050)
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from typing import Optional

import dash
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import Input, Output, callback, dash_table, dcc, html

from whale_detector.config import MONITORED_PAIRS, get_api_config
from whale_detector.utils.storage import DataStore

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Global state (updated by background threads/tasks)
# ---------------------------------------------------------------------------
_store: Optional[DataStore] = None


def get_store() -> DataStore:
    global _store
    if _store is None:
        _store = DataStore()
        _store.initialize()
    return _store


# ---------------------------------------------------------------------------
# App layout
# ---------------------------------------------------------------------------

def create_app() -> dash.Dash:
    app = dash.Dash(
        __name__,
        external_stylesheets=[dbc.themes.DARKLY],
        title="Crypto Whale & Risk Dashboard",
        suppress_callback_exceptions=True,
    )

    app.layout = dbc.Container(
        fluid=True,
        children=[
            # Header
            dbc.Row([
                dbc.Col(html.H2("🐋 Crypto Whale Detection & Risk Analytics"), width=9),
                dbc.Col(
                    html.Div(id="live-clock", style={"textAlign": "right", "paddingTop": "12px"}),
                    width=3,
                ),
            ], className="my-3"),

            # KPI Row
            dbc.Row([
                dbc.Col(_kpi_card("whale-count", "Whale Trades (1h)"), width=2),
                dbc.Col(_kpi_card("total-volume", "Whale Volume (1h)"), width=2),
                dbc.Col(_kpi_card("cex-inflow", "CEX Inflow (5m)"), width=2),
                dbc.Col(_kpi_card("liq-risk", "Cascade Risk"), width=2),
                dbc.Col(_kpi_card("alert-count", "Active Alerts"), width=2),
                dbc.Col(_kpi_card("positions-monitored", "Positions Monitored"), width=2),
            ], className="mb-3"),

            # Main charts row
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("Whale Trades — Notional Volume by Pair"),
                        dbc.CardBody(dcc.Graph(id="whale-volume-chart", style={"height": "350px"})),
                    ])
                ], width=6),
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("CEX Deposit Accumulation (5-min rolling)"),
                        dbc.CardBody(dcc.Graph(id="cex-deposit-chart", style={"height": "350px"})),
                    ])
                ], width=6),
            ], className="mb-3"),

            # Risk row
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("Cascade Liquidation Risk — Aave Positions"),
                        dbc.CardBody(dcc.Graph(id="liq-risk-gauge", style={"height": "300px"})),
                    ])
                ], width=4),
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("Uniswap Pool Liquidity (Top 10 by TVL)"),
                        dbc.CardBody(dcc.Graph(id="uniswap-tvl-chart", style={"height": "300px"})),
                    ])
                ], width=8),
            ], className="mb-3"),

            # Concentration + Alerts row
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("Whale Volume Concentration by Pair"),
                        dbc.CardBody(
                            dash_table.DataTable(
                                id="concentration-table",
                                style_table={"overflowY": "auto", "maxHeight": "280px"},
                                style_header={"backgroundColor": "#2c3e50", "color": "white"},
                                style_cell={
                                    "backgroundColor": "#1a252f",
                                    "color": "white",
                                    "fontSize": "12px",
                                    "padding": "6px",
                                },
                                style_data_conditional=[
                                    {
                                        "if": {"filter_query": "{notional_usd} > 5000000"},
                                        "backgroundColor": "#c0392b",
                                        "color": "white",
                                    }
                                ],
                            )
                        ),
                    ])
                ], width=6),
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("Recent Alerts"),
                        dbc.CardBody(
                            dash_table.DataTable(
                                id="alerts-table",
                                style_table={"overflowY": "auto", "maxHeight": "280px"},
                                style_header={"backgroundColor": "#2c3e50", "color": "white"},
                                style_cell={
                                    "backgroundColor": "#1a252f",
                                    "color": "white",
                                    "fontSize": "12px",
                                    "padding": "6px",
                                },
                                style_data_conditional=[
                                    {
                                        "if": {"filter_query": "{severity} = 'CRITICAL'"},
                                        "backgroundColor": "#922b21",
                                    },
                                    {
                                        "if": {"filter_query": "{severity} = 'HIGH'"},
                                        "backgroundColor": "#784212",
                                    },
                                ],
                            )
                        ),
                    ])
                ], width=6),
            ], className="mb-3"),

            # Auto-refresh
            dcc.Interval(id="refresh-interval", interval=5_000, n_intervals=0),
        ],
    )

    _register_callbacks(app)
    return app


def _kpi_card(card_id: str, label: str) -> dbc.Card:
    return dbc.Card([
        dbc.CardBody([
            html.H5(label, className="card-title text-muted", style={"fontSize": "12px"}),
            html.H3("—", id=card_id, className="card-text text-white"),
        ])
    ], color="dark", outline=True)


# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------

def _register_callbacks(app: dash.Dash) -> None:

    @app.callback(
        Output("live-clock", "children"),
        Input("refresh-interval", "n_intervals"),
    )
    def update_clock(_):
        return time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())

    @app.callback(
        [
            Output("whale-count", "children"),
            Output("total-volume", "children"),
            Output("alert-count", "children"),
            Output("positions-monitored", "children"),
        ],
        Input("refresh-interval", "n_intervals"),
    )
    def update_kpis(_):
        store = get_store()
        whales = store.get_recent_whale_positions(limit=1000, since=time.time() - 3600)
        alerts = store.get_recent_alerts(limit=100)
        volume = sum(w.get("notional_usd", 0) for w in whales)
        return (
            str(len(whales)),
            f"${volume / 1e6:.1f}M",
            str(len(alerts)),
            f"{len(MONITORED_PAIRS):,}+",
        )

    @app.callback(
        Output("whale-volume-chart", "figure"),
        Input("refresh-interval", "n_intervals"),
    )
    def update_whale_chart(_):
        store = get_store()
        rows = store.get_whale_volume_by_pair(hours=1)
        if not rows:
            return _empty_fig("No whale trades in last hour")
        symbols = [r["symbol"] for r in rows[:20]]
        buys = [r["total_usd"] if r["side"] == "BUY" else 0 for r in rows[:20]]
        sells = [-r["total_usd"] if r["side"] == "SELL" else 0 for r in rows[:20]]
        fig = go.Figure()
        fig.add_trace(go.Bar(name="Buy", x=symbols, y=buys, marker_color="#27ae60"))
        fig.add_trace(go.Bar(name="Sell", x=symbols, y=sells, marker_color="#e74c3c"))
        fig.update_layout(**_dark_layout(), barmode="relative",
                          yaxis_title="USD Volume", xaxis_tickangle=-45)
        return fig

    @app.callback(
        Output("cex-deposit-chart", "figure"),
        [Output("cex-inflow", "children")],
        Input("refresh-interval", "n_intervals"),
    )
    def update_cex_chart(_):
        store = get_store()
        deposits = store.get_cex_deposits(hours=1)
        total_5m = sum(
            d["amount_usd"] for d in deposits
            if d["timestamp"] >= time.time() - 300
        )
        if not deposits:
            return _empty_fig("No CEX deposits detected"), f"${total_5m:,.0f}"

        # Group by exchange and token
        by_exchange: dict = {}
        for d in deposits:
            key = d["exchange"]
            by_exchange.setdefault(key, 0.0)
            by_exchange[key] += d["amount_usd"]

        fig = go.Figure(go.Bar(
            x=list(by_exchange.keys()),
            y=list(by_exchange.values()),
            marker_color="#e67e22",
        ))
        fig.update_layout(**_dark_layout(), yaxis_title="USD Inflow")
        return fig, f"${total_5m:,.0f}"

    @app.callback(
        Output("liq-risk-gauge", "figure"),
        [Output("liq-risk", "children")],
        Input("refresh-interval", "n_intervals"),
    )
    def update_risk_gauge(_):
        # In production this reads from the analytics engine.
        # Placeholder value shown here.
        risk_score = 0.0
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=risk_score * 100,
            number={"suffix": "%"},
            gauge={
                "axis": {"range": [0, 100]},
                "bar": {"color": "#e74c3c"},
                "steps": [
                    {"range": [0, 50], "color": "#27ae60"},
                    {"range": [50, 75], "color": "#f39c12"},
                    {"range": [75, 100], "color": "#c0392b"},
                ],
                "threshold": {
                    "line": {"color": "white", "width": 3},
                    "thickness": 0.75,
                    "value": 75,
                },
            },
            title={"text": "Cascade Liquidation Risk"},
        ))
        fig.update_layout(**_dark_layout())
        return fig, f"{risk_score:.1%}"

    @app.callback(
        Output("uniswap-tvl-chart", "figure"),
        Input("refresh-interval", "n_intervals"),
    )
    def update_uniswap_chart(_):
        # Placeholder — in production pulled from UniswapAnalytics
        return _empty_fig("Connect Uniswap subgraph for live data")

    @app.callback(
        [
            Output("concentration-table", "data"),
            Output("concentration-table", "columns"),
        ],
        Input("refresh-interval", "n_intervals"),
    )
    def update_concentration(_):
        store = get_store()
        rows = store.get_whale_volume_by_pair(hours=24)
        if not rows:
            cols = [{"name": c, "id": c} for c in ["symbol", "side", "total_usd", "trade_count"]]
            return [], cols
        for r in rows:
            r["total_usd"] = f"${r['total_usd']:,.0f}"
        cols = [{"name": c.replace("_", " ").title(), "id": c} for c in rows[0].keys()]
        return rows, cols

    @app.callback(
        [
            Output("alerts-table", "data"),
            Output("alerts-table", "columns"),
        ],
        Input("refresh-interval", "n_intervals"),
    )
    def update_alerts(_):
        store = get_store()
        alerts = store.get_recent_alerts(limit=20)
        if not alerts:
            cols = [{"name": c, "id": c} for c in ["severity", "title", "source", "timestamp"]]
            return [], cols
        for a in alerts:
            a["timestamp"] = time.strftime(
                "%H:%M:%S", time.gmtime(a.get("timestamp", 0))
            )
            a.pop("description", None)
            a.pop("metadata", None)
        cols = [{"name": c.title(), "id": c} for c in alerts[0].keys()]
        return alerts, cols


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _dark_layout() -> dict:
    return {
        "paper_bgcolor": "#1a252f",
        "plot_bgcolor": "#1a252f",
        "font": {"color": "white", "size": 11},
        "margin": {"l": 40, "r": 20, "t": 20, "b": 60},
    }


def _empty_fig(message: str) -> go.Figure:
    fig = go.Figure()
    fig.add_annotation(
        text=message,
        xref="paper", yref="paper",
        x=0.5, y=0.5, showarrow=False,
        font={"color": "#aaa", "size": 14},
    )
    fig.update_layout(**_dark_layout())
    return fig


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_dashboard() -> None:
    cfg = get_api_config()
    app = create_app()
    logger.info("Starting dashboard on %s:%d", cfg.dashboard_host, cfg.dashboard_port)
    app.run(
        host=cfg.dashboard_host,
        port=cfg.dashboard_port,
        debug=False,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_dashboard()
