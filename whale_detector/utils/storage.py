"""
SQLite-based data persistence layer.

Stores whale positions, CEX deposit events, alerts, and liquidity snapshots
for historical analysis and dashboard consumption.

Uses SQLite (via aiosqlite for async access) for zero-dependency local storage.
Can be swapped for PostgreSQL / TimescaleDB by overriding the connection factory.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from typing import List, Optional

from whale_detector.config import get_api_config
from whale_detector.models import Alert, CEXDepositEvent, LiquiditySnapshot, WhalePosition

logger = logging.getLogger(__name__)

# DDL statements ----------------------------------------------------------------

_CREATE_WHALE_POSITIONS = """
CREATE TABLE IF NOT EXISTS whale_positions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source      TEXT    NOT NULL,
    symbol      TEXT    NOT NULL,
    side        TEXT    NOT NULL,
    quantity    REAL    NOT NULL,
    price       REAL    NOT NULL,
    notional_usd REAL   NOT NULL,
    timestamp   REAL    NOT NULL,
    tx_hash     TEXT,
    wallet_address TEXT,
    exchange    TEXT,
    raw         TEXT
);
"""

_CREATE_CEX_DEPOSITS = """
CREATE TABLE IF NOT EXISTS cex_deposits (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    exchange    TEXT    NOT NULL,
    token       TEXT    NOT NULL,
    amount      REAL    NOT NULL,
    amount_usd  REAL    NOT NULL,
    wallet_from TEXT    NOT NULL,
    wallet_to   TEXT    NOT NULL,
    timestamp   REAL    NOT NULL,
    tx_hash     TEXT,
    chain       TEXT
);
"""

_CREATE_ALERTS = """
CREATE TABLE IF NOT EXISTS alerts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    severity    TEXT    NOT NULL,
    title       TEXT    NOT NULL,
    description TEXT    NOT NULL,
    source      TEXT    NOT NULL,
    timestamp   REAL    NOT NULL,
    metadata    TEXT
);
"""

_CREATE_LIQUIDITY_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS liquidity_snapshots (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    protocol    TEXT    NOT NULL,
    pool_id     TEXT    NOT NULL,
    token0      TEXT    NOT NULL,
    token1      TEXT    NOT NULL,
    tvl_usd     REAL    NOT NULL,
    volume_24h_usd REAL,
    fee_tier    REAL,
    timestamp   REAL    NOT NULL
);
"""


class DataStore:
    """
    Synchronous SQLite data store.

    Initialise once at startup; thread-safe for single-writer use.

    Usage::

        store = DataStore()
        store.initialize()
        store.save_whale_position(pos)
    """

    def __init__(self, db_path: Optional[str] = None) -> None:
        self._db_path = db_path or get_api_config().db_path
        self._conn: Optional[sqlite3.Connection] = None

    def initialize(self) -> None:
        """Create tables if they don't exist."""
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        for ddl in (
            _CREATE_WHALE_POSITIONS,
            _CREATE_CEX_DEPOSITS,
            _CREATE_ALERTS,
            _CREATE_LIQUIDITY_SNAPSHOTS,
        ):
            self._conn.execute(ddl)
        self._conn.commit()
        logger.info("DataStore initialised at %s", self._db_path)

    def close(self) -> None:
        if self._conn:
            self._conn.close()

    # ------------------------------------------------------------------
    # Write methods
    # ------------------------------------------------------------------

    def save_whale_position(self, pos: WhalePosition) -> None:
        self._conn.execute(
            """
            INSERT INTO whale_positions
              (source, symbol, side, quantity, price, notional_usd,
               timestamp, tx_hash, wallet_address, exchange, raw)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                pos.source.value, pos.symbol, pos.side, pos.quantity,
                pos.price, pos.notional_usd, pos.timestamp,
                pos.tx_hash, pos.wallet_address, pos.exchange,
                json.dumps(pos.raw) if pos.raw else None,
            ),
        )
        self._conn.commit()

    def save_cex_deposit(self, event: CEXDepositEvent) -> None:
        self._conn.execute(
            """
            INSERT INTO cex_deposits
              (exchange, token, amount, amount_usd, wallet_from, wallet_to,
               timestamp, tx_hash, chain)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.exchange, event.token, event.amount, event.amount_usd,
                event.wallet_from, event.wallet_to, event.timestamp,
                event.tx_hash, event.chain,
            ),
        )
        self._conn.commit()

    def save_alert(self, alert: Alert) -> None:
        self._conn.execute(
            """
            INSERT INTO alerts (severity, title, description, source, timestamp, metadata)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                alert.severity.value, alert.title, alert.description,
                alert.source, alert.timestamp, json.dumps(alert.metadata),
            ),
        )
        self._conn.commit()

    def save_liquidity_snapshot(self, snap: LiquiditySnapshot) -> None:
        self._conn.execute(
            """
            INSERT INTO liquidity_snapshots
              (protocol, pool_id, token0, token1, tvl_usd, volume_24h_usd, fee_tier, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snap.protocol, snap.pool_id, snap.token0, snap.token1,
                snap.tvl_usd, snap.volume_24h_usd, snap.fee_tier, snap.timestamp,
            ),
        )
        self._conn.commit()

    # ------------------------------------------------------------------
    # Read methods
    # ------------------------------------------------------------------

    def get_recent_whale_positions(
        self, limit: int = 100, since: Optional[float] = None
    ) -> List[dict]:
        since = since or (time.time() - 3600)
        cursor = self._conn.execute(
            """
            SELECT source, symbol, side, quantity, price, notional_usd, timestamp,
                   tx_hash, wallet_address, exchange
            FROM whale_positions
            WHERE timestamp >= ?
            ORDER BY timestamp DESC LIMIT ?
            """,
            (since, limit),
        )
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]

    def get_recent_alerts(self, limit: int = 50) -> List[dict]:
        cursor = self._conn.execute(
            """
            SELECT severity, title, description, source, timestamp, metadata
            FROM alerts ORDER BY timestamp DESC LIMIT ?
            """,
            (limit,),
        )
        cols = [d[0] for d in cursor.description]
        rows = []
        for row in cursor.fetchall():
            d = dict(zip(cols, row))
            d["metadata"] = json.loads(d["metadata"]) if d["metadata"] else {}
            rows.append(d)
        return rows

    def get_cex_deposits(
        self, hours: float = 24, exchange: Optional[str] = None
    ) -> List[dict]:
        since = time.time() - hours * 3600
        if exchange:
            cursor = self._conn.execute(
                """
                SELECT exchange, token, amount, amount_usd, wallet_from, wallet_to, timestamp
                FROM cex_deposits WHERE timestamp >= ? AND exchange = ?
                ORDER BY timestamp DESC
                """,
                (since, exchange),
            )
        else:
            cursor = self._conn.execute(
                """
                SELECT exchange, token, amount, amount_usd, wallet_from, wallet_to, timestamp
                FROM cex_deposits WHERE timestamp >= ?
                ORDER BY timestamp DESC
                """,
                (since,),
            )
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]

    def get_whale_volume_by_pair(self, hours: float = 24) -> List[dict]:
        since = time.time() - hours * 3600
        cursor = self._conn.execute(
            """
            SELECT symbol, side, SUM(notional_usd) as total_usd, COUNT(*) as trade_count
            FROM whale_positions WHERE timestamp >= ?
            GROUP BY symbol, side ORDER BY total_usd DESC
            """,
            (since,),
        )
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]
