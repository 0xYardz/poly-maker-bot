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
    strategy TEXT,
    UNIQUE(trade_id, order_id)
);

CREATE TABLE IF NOT EXISTS markets (
    slug TEXT PRIMARY KEY,
    question TEXT,
    up_token_id TEXT,
    down_token_id TEXT,
    start_ts REAL,
    end_ts REAL,
    first_seen_at REAL NOT NULL,
    resolved_winner TEXT
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
        if "strategy" not in columns:
            self._conn.execute(
                "ALTER TABLE trades ADD COLUMN strategy TEXT"
            )
            logger.info("Migrated: added strategy column to trades")
        # Backfill existing trades that have no strategy tag
        self._conn.execute(
            "UPDATE trades SET strategy = 'simple_mm' WHERE strategy IS NULL"
        )

        # Add resolved_winner column to markets table
        mcur = self._conn.execute("PRAGMA table_info(markets)")
        mcols = {row[1] for row in mcur.fetchall()}
        if "resolved_winner" not in mcols:
            self._conn.execute(
                "ALTER TABLE markets ADD COLUMN resolved_winner TEXT"
            )
            logger.info("Migrated: added resolved_winner column to markets")

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
        strategy: Optional[str] = None,
    ) -> None:
        """Insert a trade, ignoring duplicates by trade_id."""
        with self._write_lock:
            try:
                self._conn.execute(
                    """INSERT OR IGNORE INTO trades
                       (trade_id, order_id, token_id, market_slug,
                        side, size, price, usdc_value, outcome,
                        order_type, status, created_at, transaction_hash,
                        strategy)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        trade_id, order_id, token_id, market_slug,
                        side, size, price, round(size * price, 6),
                        outcome, order_type, status, time.time(),
                        transaction_hash, strategy,
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

    def set_market_resolution(self, slug: str, winner: str) -> None:
        """Record which side won: 'UP' or 'DOWN'."""
        with self._write_lock:
            try:
                self._conn.execute(
                    "UPDATE markets SET resolved_winner = ? WHERE slug = ?",
                    (winner, slug),
                )
                self._conn.commit()
                logger.info("Market %s resolved: winner=%s", slug, winner)
            except Exception:
                logger.exception("Failed to set resolution for %s", slug)

    def get_unresolved_markets(self) -> list[dict]:
        """Return markets that ended but have no resolution recorded yet."""
        cur = self._conn.execute(
            """SELECT slug, question, up_token_id, down_token_id,
                      start_ts, end_ts, first_seen_at, resolved_winner
               FROM markets
               WHERE resolved_winner IS NULL AND end_ts < ?
               ORDER BY end_ts DESC""",
            (time.time(),),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    # ── read methods (called from dashboard) ─────────────────

    def get_recent_trades(self, limit: int = 100) -> list[dict]:
        cur = self._conn.execute(
            """SELECT trade_id, order_id, token_id, market_slug,
                      side, size, price, usdc_value, outcome,
                      order_type, status, created_at, transaction_hash,
                      strategy
               FROM trades ORDER BY created_at DESC LIMIT ?""",
            (limit,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def get_pnl_by_market(self) -> list[dict]:
        """Aggregate trade volume and estimated PnL per market slug.

        PnL estimation: pairs up UP and DOWN shares bought for each market.
        Each paired share is worth exactly $1.00 at resolution (one side pays out).
        Paired PnL = paired_shares * (1.00 - avg_up_price - avg_down_price).
        Unmatched shares (excess on one side) are shown separately.
        """
        cur = self._conn.execute(
            """SELECT t.market_slug,
                      COUNT(*) as trade_count,
                      SUM(t.usdc_value) as total_volume,
                      -- UP side totals
                      COALESCE(SUM(CASE WHEN t.token_id = m.up_token_id THEN t.size ELSE 0 END), 0) as up_shares,
                      COALESCE(SUM(CASE WHEN t.token_id = m.up_token_id THEN t.usdc_value ELSE 0 END), 0) as up_cost,
                      -- DOWN side totals
                      COALESCE(SUM(CASE WHEN t.token_id = m.down_token_id THEN t.size ELSE 0 END), 0) as down_shares,
                      COALESCE(SUM(CASE WHEN t.token_id = m.down_token_id THEN t.usdc_value ELSE 0 END), 0) as down_cost,
                      MAX(t.strategy) as strategy,
                      m.resolved_winner
               FROM trades t
               JOIN markets m ON t.market_slug = m.slug
               WHERE t.status != 'FAILED'
               GROUP BY t.market_slug
               ORDER BY MAX(t.created_at) DESC""",
        )
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]

        # Compute paired PnL for each market
        for r in rows:
            up_s, dn_s = r["up_shares"], r["down_shares"]
            up_c, dn_c = r["up_cost"], r["down_cost"]
            paired = min(up_s, dn_s)

            if paired > 0:
                # Average prices for each side
                up_avg = up_c / up_s if up_s > 0 else 0
                dn_avg = dn_c / dn_s if dn_s > 0 else 0
                # Each pair pays out $1.00 at resolution
                r["paired_pnl"] = round(paired * (1.00 - up_avg - dn_avg), 6)
            else:
                r["paired_pnl"] = 0.0

            r["paired_shares"] = paired
            r["unmatched_shares"] = abs(up_s - dn_s)
            r["unmatched_side"] = "UP" if up_s > dn_s else "DOWN" if dn_s > up_s else None
            # Total cost of unmatched shares (worth $1 or $0 depending on resolution)
            if up_s > dn_s and up_s > 0:
                r["unmatched_cost"] = round((up_s - dn_s) * (up_c / up_s), 4)
            elif dn_s > up_s and dn_s > 0:
                r["unmatched_cost"] = round((dn_s - up_s) * (dn_c / dn_s), 4)
            else:
                r["unmatched_cost"] = 0.0

            # Unmatched PnL: if market resolved, we know the outcome
            winner = r.get("resolved_winner")
            if winner and r["unmatched_shares"] > 0:
                # Winner side pays $1.00 per share, loser pays $0.00
                if r["unmatched_side"] == winner:
                    # We hold excess winning shares: profit = shares * (1.00 - avg_price)
                    r["unmatched_pnl"] = round(
                        r["unmatched_shares"] * (1.00 - r["unmatched_cost"] / r["unmatched_shares"]), 4
                    )
                else:
                    # We hold excess losing shares: loss = -cost
                    r["unmatched_pnl"] = round(-r["unmatched_cost"], 4)
                r["resolved"] = True
            else:
                r["unmatched_pnl"] = 0.0
                r["resolved"] = winner is not None

            r["estimated_pnl"] = round(r["paired_pnl"] + r["unmatched_pnl"], 6)

        return rows

    def get_all_time_stats(self) -> dict:
        """Return aggregate stats across all trades.

        Uses the same paired-shares PnL formula as get_pnl_by_market():
        sum of paired_pnl across all markets.
        """
        pnl_rows = self.get_pnl_by_market()
        total_trades = sum(r["trade_count"] for r in pnl_rows)
        total_volume = sum(r["total_volume"] for r in pnl_rows)
        markets_traded = len(pnl_rows)
        estimated_pnl = sum(r["estimated_pnl"] for r in pnl_rows)

        return {
            "total_trades": total_trades,
            "total_volume": total_volume,
            "markets_traded": markets_traded,
            "estimated_pnl": round(estimated_pnl, 6),
        }

    def get_markets_traded(self) -> list[dict]:
        cur = self._conn.execute(
            """SELECT slug, question, up_token_id, down_token_id,
                      start_ts, end_ts, first_seen_at, resolved_winner
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
