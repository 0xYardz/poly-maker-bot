"""Assembles dashboard data snapshots from bot state and SQLite."""
from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from poly_maker_bot.maker_bot import LiquidityProviderBot
    from poly_maker_bot.db import TradeDatabase

logger = logging.getLogger(__name__)


class DashboardDataCollector:
    """Reads from in-memory bot state (fast) and SQLite (historical)."""

    DB_CACHE_TTL = 30.0  # refresh DB queries every 30s

    def __init__(self, bot: "LiquidityProviderBot", db: "TradeDatabase"):
        self._bot = bot
        self._db = db
        self._start_time = time.time()

        # DB query cache
        self._db_cache: dict = {}
        self._db_cache_ts: float = 0.0
        self._db_lock = threading.Lock()

    # ── public API ───────────────────────────────────────────

    def collect_full_snapshot(self) -> dict:
        """Full snapshot for initial page load (same as realtime now)."""
        return self.collect_realtime_snapshot()

    def collect_realtime_snapshot(self) -> dict:
        """Snapshot from in-memory state + cached DB data (for SSE stream)."""
        bot = self._bot
        return {
            "timestamp": time.time(),
            "uptime_sec": time.time() - self._start_time,
            "bot_running": bot.running,
            "current_market": self._collect_market(),
            "positions": self._collect_positions(),
            "pnl": self._collect_pnl(),
            "orderbook": self._collect_orderbook(),
            "open_orders": self._collect_open_orders(),
            "strategy": self._collect_strategy(),
            "metrics": self._collect_metrics(),
            "historical": self._get_db_data(),
        }

    # ── in-memory readers (thread-safe via existing locks) ───

    def _collect_market(self) -> Optional[dict]:
        m = self._bot._current_market
        if m is None:
            return None
        now = time.time()
        return {
            "slug": m.slug,
            "question": m.question,
            "up_token_id": m.up_token_id,
            "down_token_id": m.down_token_id,
            "start_ts": m.start_ts,
            "end_ts": m.end_ts,
            "time_remaining_sec": max(0, m.end_ts - now),
            "window_duration_sec": m.end_ts - m.start_ts,
        }

    def _collect_positions(self) -> dict:
        return self._bot.position_tracker.get_all_positions()

    def _collect_pnl(self) -> dict:
        bot = self._bot
        positions = bot.position_tracker.get_all_positions()

        def _get_exit_price(token_id: str):
            store = bot.orderbook_stores.get(token_id)
            if store is None:
                return None
            try:
                tob = store.top()
                # Prefer best_bid (realistic sell price), fall back to best_ask
                return tob.best_bid if tob.best_bid is not None else tob.best_ask
            except Exception:
                return None

        from poly_maker_bot.position.pnl_calculator import calculate_unrealized_pnl
        pnl_by_token, pnl_pct_by_token, total_unrealized = calculate_unrealized_pnl(
            positions, _get_exit_price,
        )

        return {
            "unrealized_by_token": pnl_by_token,
            "unrealized_pct_by_token": pnl_pct_by_token,
            "total_unrealized": total_unrealized,
            "realized": bot.realized_pnl,
        }

    def _collect_orderbook(self) -> dict:
        result = {}
        stores = self._bot.orderbook_stores
        if not stores:
            return result
        for token_id, store in dict(stores).items():
            try:
                tob = store.top()
                spread = None
                if tob.best_bid is not None and tob.best_ask is not None:
                    spread = round(tob.best_ask - tob.best_bid, 4)
                result[token_id] = {
                    "best_bid": tob.best_bid,
                    "best_bid_size": tob.best_bid_size,
                    "best_ask": tob.best_ask,
                    "best_ask_size": tob.best_ask_size,
                    "spread": spread,
                    "last_update": store.last_update_ts,
                }
            except Exception:
                pass
        return result

    def _collect_open_orders(self) -> dict:
        return self._bot.order_manager.get_all_orders()

    def _collect_strategy(self) -> Optional[dict]:
        s = self._bot._strategy
        if s is None:
            return None
        from poly_maker_bot.strategy.simple_mm import SimpleMarketMaker
        from poly_maker_bot.strategy.volatile_mm import VolatileMarketMaker

        base = {
            "running": s.is_running,
            "market_slug": s.market.slug,
        }

        if isinstance(s, VolatileMarketMaker):
            base.update({
                "type": "volatile_mm",
                "pending_reactions": len(s._pending_reactions),
                "pending_fill_size": dict(s._pending_fill_size),
                "total_bought": dict(s._total_bought),
                "layered_orders": len(s._layered_orders),
            })
        elif isinstance(s, SimpleMarketMaker):
            base.update({
                "type": "simple_mm",
                "pending_reactions": len(s._pending_reactions),
                "pending_fill_size": {
                    f"pair{k[0]}_{k[1][:12]}": v
                    for k, v in s._pending_fill_size.items()
                },
                "buy_pairs": len(s._buy_pairs),
            })

        return base

    def _collect_metrics(self) -> dict:
        client = self._bot.client
        if client is None:
            return {}
        return {
            "post_order_latency": client._lat_post_order.snapshot(),
            "delete_order_latency": client._lat_delete_order.snapshot(),
            "post_order_rate": client._rate_post_order.get_rates(),
            "delete_order_rate": client._rate_delete_order.get_rates(),
        }

    # ── SQLite readers (cached) ──────────────────────────────

    def _get_db_data(self) -> dict:
        now = time.time()
        with self._db_lock:
            if now - self._db_cache_ts > self.DB_CACHE_TTL:
                try:
                    self._db_cache = {
                        "all_time_stats": self._db.get_all_time_stats(),
                        "pnl_by_market": self._db.get_pnl_by_market(),
                        "recent_trades": self._db.get_recent_trades(limit=100),
                        "markets_traded": self._db.get_markets_traded(),
                        "sessions": self._db.get_session_history(limit=20),
                    }
                    self._db_cache_ts = now
                except Exception:
                    logger.exception("Failed to refresh DB cache")
            return self._db_cache
