"""SQLite persistence for bot trades, markets, and sessions."""
from __future__ import annotations

import logging
import sqlite3
import threading
import time
from typing import Optional

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id TEXT,
    order_id TEXT,
    token_id TEXT NOT NULL,
    market_slug TEXT NOT NULL,
    side TEXT NOT NULL,
    size REAL NOT NULL,
    price REAL NOT NULL,
    usdc_value REAL NOT NULL,
    outcome TEXT,
    order_type TEXT,
    status TEXT DEFAULT 'MATCHED',
    created_at REAL NOT NULL,
    transaction_hash TEXT,
    UNIQUE(trade_id, order_id)
);

CREATE TABLE IF NOT EXISTS markets (
    slug TEXT PRIMARY KEY,
    question TEXT,
    up_token_id TEXT,
    down_token_id TEXT,
    start_ts REAL,
    end_ts REAL,
    first_seen_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at REAL NOT NULL,
    ended_at REAL,
    realized_pnl REAL DEFAULT 0,
    total_trades INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_trades_market_slug ON trades(market_slug);
CREATE INDEX IF NOT EXISTS idx_trades_created_at ON trades(created_at);
"""


class TradeDatabase:
    """Thread-safe SQLite persistence for bot data.

    Uses WAL mode so the dashboard can read concurrently while
    the bot writes.
    """

    def __init__(self, db_path: str = "bot_data.db"):
        self._db_path = db_path
        self._write_lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(_SCHEMA)
        self._migrate()
        self._conn.commit()
        logger.info("TradeDatabase initialized at %s", db_path)

    def _migrate(self) -> None:
        """Apply schema migrations for older databases."""
        cur = self._conn.execute("PRAGMA table_info(trades)")
        columns = {row[1] for row in cur.fetchall()}
        if "transaction_hash" not in columns:
            self._conn.execute(
                "ALTER TABLE trades ADD COLUMN transaction_hash TEXT"
            )
            logger.info("Migrated: added transaction_hash column to trades")

        # Migrate from UNIQUE(trade_id) to UNIQUE(trade_id, order_id)
        # so partial fills with different order_ids are recorded separately.
        cur = self._conn.execute("PRAGMA index_list(trades)")
        for row in cur.fetchall():
            idx_name = row[1]
            idx_info = self._conn.execute(
                f"PRAGMA index_info({idx_name})"
            ).fetchall()
            col_names = [c[2] for c in idx_info]
            if col_names == ["trade_id"] and row[2]:  # row[2] = unique flag
                # Old single-column unique index — rebuild table with composite key.
                # SQLite doesn't support DROP INDEX on auto-created unique constraints,
                # so we recreate the table.
                self._conn.executescript("""
                    CREATE TABLE IF NOT EXISTS trades_new (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        trade_id TEXT,
                        order_id TEXT,
                        token_id TEXT NOT NULL,
                        market_slug TEXT NOT NULL,
                        side TEXT NOT NULL,
                        size REAL NOT NULL,
                        price REAL NOT NULL,
                        usdc_value REAL NOT NULL,
                        outcome TEXT,
                        order_type TEXT,
                        status TEXT DEFAULT 'MATCHED',
                        created_at REAL NOT NULL,
                        transaction_hash TEXT,
                        UNIQUE(trade_id, order_id)
                    );
                    INSERT OR IGNORE INTO trades_new
                        SELECT id, trade_id, order_id, token_id, market_slug,
                               side, size, price, usdc_value, outcome,
                               order_type, status, created_at, transaction_hash
                        FROM trades;
                    DROP TABLE trades;
                    ALTER TABLE trades_new RENAME TO trades;
                """)
                # Recreate indexes lost during table rebuild
                self._conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_trades_market_slug ON trades(market_slug)"
                )
                self._conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_trades_created_at ON trades(created_at)"
                )
                logger.info("Migrated: changed unique constraint from (trade_id) to (trade_id, order_id)")
                break

    # ── write methods (called from bot threads) ──────────────

    def record_trade(
        self,
        trade_id: str,
        order_id: str,
        token_id: str,
        market_slug: str,
        side: str,
        size: float,
        price: float,
        outcome: Optional[str] = None,
        order_type: Optional[str] = None,
        status: str = "MATCHED",
        transaction_hash: Optional[str] = None,
    ) -> None:
        """Insert a trade, ignoring duplicates by trade_id."""
        with self._write_lock:
            try:
                self._conn.execute(
                    """INSERT OR IGNORE INTO trades
                       (trade_id, order_id, token_id, market_slug,
                        side, size, price, usdc_value, outcome,
                        order_type, status, created_at, transaction_hash)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        trade_id, order_id, token_id, market_slug,
                        side, size, price, round(size * price, 6),
                        outcome, order_type, status, time.time(),
                        transaction_hash,
                    ),
                )
                self._conn.commit()
            except Exception:
                logger.exception("Failed to record trade %s", trade_id)

    def update_trade_status(
        self, trade_id: str, status: str, transaction_hash: Optional[str] = None,
    ) -> None:
        """Update the status of an existing trade, optionally setting the tx hash."""
        with self._write_lock:
            try:
                if transaction_hash:
                    self._conn.execute(
                        "UPDATE trades SET status = ?, transaction_hash = ? WHERE trade_id = ?",
                        (status, transaction_hash, trade_id),
                    )
                else:
                    self._conn.execute(
                        "UPDATE trades SET status = ? WHERE trade_id = ?",
                        (status, trade_id),
                    )
                self._conn.commit()
            except Exception:
                logger.exception("Failed to update trade status %s", trade_id)

    def record_market(self, market) -> None:
        """Insert or ignore a market record."""
        with self._write_lock:
            try:
                self._conn.execute(
                    """INSERT OR IGNORE INTO markets
                       (slug, question, up_token_id, down_token_id,
                        start_ts, end_ts, first_seen_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        market.slug, market.question,
                        market.up_token_id, market.down_token_id,
                        market.start_ts, market.end_ts,
                        time.time(),
                    ),
                )
                self._conn.commit()
            except Exception:
                logger.exception("Failed to record market %s", market.slug)

    def start_session(self) -> int:
        """Create a new session row and return its id."""
        with self._write_lock:
            cur = self._conn.execute(
                "INSERT INTO sessions (started_at) VALUES (?)",
                (time.time(),),
            )
            self._conn.commit()
            session_id = cur.lastrowid
            logger.info("Session %d started", session_id)
            return session_id

    def end_session(
        self, session_id: int, realized_pnl: float, total_trades: int
    ) -> None:
        """Finalize a session with summary stats."""
        with self._write_lock:
            try:
                self._conn.execute(
                    """UPDATE sessions
                       SET ended_at = ?, realized_pnl = ?, total_trades = ?
                       WHERE id = ?""",
                    (time.time(), realized_pnl, total_trades, session_id),
                )
                self._conn.commit()
            except Exception:
                logger.exception("Failed to end session %d", session_id)

    # ── read methods (called from dashboard) ─────────────────

    def get_recent_trades(self, limit: int = 100) -> list[dict]:
        cur = self._conn.execute(
            """SELECT trade_id, order_id, token_id, market_slug,
                      side, size, price, usdc_value, outcome,
                      order_type, status, created_at, transaction_hash
               FROM trades ORDER BY created_at DESC LIMIT ?""",
            (limit,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def get_pnl_by_market(self) -> list[dict]:
        """Aggregate trade volume and estimated PnL per market slug.

        PnL estimation: for each market, sum the USDC spent on initial
        buys and USDC received from reaction buys (which settle at $1
        if the market resolves in our favor).
        """
        cur = self._conn.execute(
            """SELECT market_slug,
                      COUNT(*) as trade_count,
                      SUM(usdc_value) as total_volume,
                      SUM(CASE WHEN order_type = 'initial_buy' THEN -usdc_value ELSE 0 END) +
                      SUM(CASE WHEN order_type = 'fill_reaction_buy' THEN size - usdc_value ELSE 0 END)
                        as estimated_pnl
               FROM trades
               WHERE status != 'FAILED'
               GROUP BY market_slug
               ORDER BY MAX(created_at) DESC""",
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def get_all_time_stats(self) -> dict:
        """Return aggregate stats across all trades."""
        cur = self._conn.execute(
            """SELECT COUNT(*) as total_trades,
                      COALESCE(SUM(usdc_value), 0) as total_volume,
                      COUNT(DISTINCT market_slug) as markets_traded,
                      COALESCE(
                        SUM(CASE WHEN order_type = 'initial_buy' THEN -usdc_value ELSE 0 END) +
                        SUM(CASE WHEN order_type = 'fill_reaction_buy' THEN size - usdc_value ELSE 0 END),
                        0
                      ) as estimated_pnl
               FROM trades WHERE status != 'FAILED'""",
        )
        row = cur.fetchone()
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row)) if row else {
            "total_trades": 0, "total_volume": 0,
            "markets_traded": 0, "estimated_pnl": 0,
        }

    def get_markets_traded(self) -> list[dict]:
        cur = self._conn.execute(
            """SELECT slug, question, up_token_id, down_token_id,
                      start_ts, end_ts, first_seen_at
               FROM markets ORDER BY first_seen_at DESC""",
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def get_session_history(self, limit: int = 20) -> list[dict]:
        cur = self._conn.execute(
            """SELECT id, started_at, ended_at, realized_pnl, total_trades
               FROM sessions ORDER BY started_at DESC LIMIT ?""",
            (limit,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def close(self) -> None:
        self._conn.close()
