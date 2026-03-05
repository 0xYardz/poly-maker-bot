#!/usr/bin/env python3
"""
Poly-Maker-Bot: Market Maker Bot for Polymarket

Usage:
    # Run with default settings
    python -m poly_maker_bot.maker_bot

    # Run with custom position size
    python -m poly_maker_bot.maker_bot --position-size 10 --max-position 50
"""
from __future__ import annotations

import argparse
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import logging
import os
import queue
import random
import signal
import sys
import threading
import time
from datetime import datetime, time as dt_time
from zoneinfo import ZoneInfo
from typing import Optional

# Load environment variables FIRST, before any imports that read environment
# This ensures all config values are available when modules are imported
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

# Now import everything else
from poly_maker_bot.config import load_config
from poly_maker_bot.config import (
    BotConfig,
)
from poly_maker_bot.exchange.polymarket_client import MarketReward, PolymarketClient, Position, Token
from poly_maker_bot.logging_utils import setup_logging
from poly_maker_bot.market_data.ws_orderbook_client import WsOrderbookClient, WsOrderbookConfig
from poly_maker_bot.market_data.orderbook_store import OrderBookStore, TopOfBook
from poly_maker_bot.market_data.ws_trade_client import WsTradeClient, ClobApiKeyCreds
from poly_maker_bot.metrics.latency import RollingLatency, RateTracker

from poly_maker_bot.position import (
    PositionTracker,
    calculate_unrealized_pnl,
)
from poly_maker_bot.orders import (
    OrderManager,
    OrderPlacer,
)

from poly_maker_bot.utils import (
    round_to_tick,
)
from poly_maker_bot.market.discovery import discover_market, Market
from poly_maker_bot.strategy import StrategyEngine, SimpleMarketMaker, VolatileMarketMaker
from poly_maker_bot.utils.slug_helpers import Underlying, Timespan, slug_for_window, window_sec_for_timespan
from poly_maker_bot.db import TradeDatabase
from poly_maker_bot.dashboard import DashboardServer

# Create default bot config from environment variables
DEFAULT_BOT_CONFIG = BotConfig()
logger = logging.getLogger(__name__)

# Market targeting from .env (defaults: btc / 15m)
UNDERLYING: Underlying = os.environ.get("POLY_UNDERLYING", "btc")  # type: ignore[assignment]
TIMESPAN: Timespan = os.environ.get("POLY_TIMESPAN", "15m")  # type: ignore[assignment]
STRATEGY: str = os.environ.get("POLY_STRATEGY", "simple_mm")  # "simple_mm" or "volatile_mm"


class LiquidityProviderBot:
    """
    Main bot class for Polymarket liquidity provision.

    Responsibilities:
    - Connect to exchange and data feeds
    - Monitor active markets
    - Place and manage limit orders within spread
    - Track inventory and rewards
    - Handle graceful shutdown
    """

    def __init__(
        self,
    ):
        """
        Initialize the bot.
        """

        # State
        self.running = False
        self.shutdown_requested = False

        # Components (to be initialized)
        self.config = None
        self.client = None
        self.ws_orderbook_client = None
        self.ws_trade_client = None  # Handles user trade subscriptions

        # WebSocket trade client state
        self._ws_trade_connected = False  # Trade client connection state

        # Orderbook tracking
        self.orderbook_stores = {}   # {token_id: OrderBookStore} - managed by WsOrderbookClient

        # Phase 4: Position tracking module
        self.position_tracker = PositionTracker()

        # Phase 5: Order management module
        self.order_manager = OrderManager()

        # Trading state

        # PnL tracking
        self.unrealized_pnl = {}         # {token_id: unrealized_pnl}
        self.unrealized_pnl_pct = {}     # {token_id: unrealized_pnl_pct}
        self.total_unrealized_pnl = 0.0  # Sum of all unrealized PnL
        self.realized_pnl = 0.0          # Cumulative realized PnL from hedge fills

        # Trade confirmation tracking
        self.pending_confirmations = {}                  # {trade_id: {"timestamp": float, "token_id": str, "size": float, "price": float, "market_slug": str}}

        # Thread synchronization
        # DEPRECATED: Order manager has its own lock
        self._orders_lock = self.order_manager._lock  # Protects open_orders, known_order_ids, order_types
        self._positions_lock = threading.RLock()  # Protects positions dict
        self._markets_lock = threading.RLock()  # Protects active_markets, available_markets

        # Executor metrics
        self._market_update_latency = RollingLatency(maxlen=2000)
        self._sell_order_latency = RollingLatency(maxlen=2000)

        # Market discovery timer state (legacy, kept for compatibility)
        self._market_discovery_stop = threading.Event()
        self._market_discovery_thread: Optional[threading.Thread] = None

        # Market lifecycle management
        self._current_market: Optional[Market] = None
        self._next_market: Optional[Market] = None
        self._strategies: dict[str, StrategyEngine] = {}  # {market_slug: strategy}
        self._strategies_lock = threading.Lock()
        self._active_strategy_slug: Optional[str] = None
        self._lifecycle_stop = threading.Event()
        self._lifecycle_thread: Optional[threading.Thread] = None

        # Periodic redeem management
        self._redeem_stop = threading.Event()
        self._redeem_thread: Optional[threading.Thread] = None

        # Database and dashboard
        self.db = TradeDatabase()
        self.session_id = self.db.start_session()
        self._dashboard: Optional[DashboardServer] = None

    @property
    def _strategy(self) -> Optional[StrategyEngine]:
        """The active strategy (tick loop running). For backward compatibility."""
        slug = self._active_strategy_slug
        if slug:
            return self._strategies.get(slug)
        return None

    def setup(self):
        """Initialize all bot components."""
        logger.info("==========================================")
        logger.info("⚙️ Setting up bot components... ")
        logger.info("==========================================")

        # Load configuration
        self.config = load_config()
        logger.info("Configuration loaded")
        logger.info("Market targeting: underlying=%s timespan=%s", UNDERLYING, TIMESPAN)

        # Initialize exchange client
        self.client = PolymarketClient(self.config.polymarket)
        logger.info("Exchange client initialized")

        self.order_placer = OrderPlacer(client=self.client)
        logger.info("Order management components initialized")

        # Create and start trade client
        logger.info("Initializing WebSocket trade client...")
        try:
            # Get CLOB API credentials from PolymarketClient
            api_creds = self.client.get_clob_api_creds()
            print("Derived CLOB API creds for WS trade client:", api_creds)

            clob_creds = ClobApiKeyCreds(
                api_key=api_creds.api_key,
                secret=api_creds.api_secret,
                passphrase=api_creds.api_passphrase
            )

            # Create WsTradeClient with callbacks (using new CLOB WebSocket API)
            self.ws_trade_client = WsTradeClient(
                ws_url=self.config.polymarket.ws_user_url,
                clob_creds=clob_creds,
                on_trade=self._handle_trade_message,
                on_status_change=self._handle_trade_status_change,
                on_connection_unhealthy=self._handle_ws_connection_unhealthy,
                on_connection_healthy=self._handle_ws_connection_healthy,
                on_order=self.order_placer.handle_order_event,
            )

            # Start trade client (connects and subscribes)
            self.ws_trade_client.start()
            logger.info("WebSocket trade client started successfully")

        except Exception as e:
            logger.error(f"Failed to start trade client: {e}", exc_info=True)
            logger.warning("Trade WebSocket unavailable - fill detection will not work")
            self.ws_trade_client = None

        # Start dashboard if port configured
        dashboard_port = int(os.environ.get("POLY_DASHBOARD_PORT", "0"))
        if dashboard_port > 0:
            self._dashboard = DashboardServer(self, self.db, port=dashboard_port)
            self._dashboard.start()

        logger.info("==========================================")
        logger.info("⚙️ Bot setup complete ✅")
        logger.info("==========================================")

    def start_data_feeds(self):
        """
        Initialize WsOrderbookClient and subscribe to active markets.

        Creates WsOrderbookClient that:
        - Handles orderbook updates via WebSocket
        - Auto-resyncs on staleness or crossed books
        - Supports dynamic token addition via add_tokens()
        """
        logger.info("Starting data feeds for active markets...")
        if self.ws_orderbook_client is not None:
            logger.warning("WebSocket client already running")
            return

        if not self.active_markets:
            logger.info("No active markets to subscribe to")
            return

        logger.info(f"Starting data feeds for {len(self.active_markets)} markets...")

        # Collect token IDs from active markets
        token_ids = []
        for market in self.active_markets:
            for token in market.tokens:
                token_ids.append(token.token_id)

        logger.info(f"Initializing WsOrderbookClient for {len(token_ids)} tokens...")

        cfg = WsOrderbookConfig(
            rest_seed_on_start=True,        # Seed from REST before WS
            staleness_sec=120.0,              # Resync if no update for 120s
            resync_on_crossed=True,         # Resync if bid > ask
            resync_on_reconnect=True,       # Resync after reconnect
        )

        self.ws_orderbook_client = WsOrderbookClient(
            ws_url=self.config.polymarket.ws_url,
            token_ids=token_ids,
            stores=self.orderbook_stores,
            rest_client=self.client,
            cfg=cfg,
            on_connection_unhealthy=self._handle_ws_connection_unhealthy,
            on_connection_healthy=self._handle_ws_connection_healthy
        )

        self.ws_orderbook_client.start()
        logger.info(f"WsOrderbookClient started successfully")

    def subscribe_to_markets(self, markets):
        """
        Subscribe to orderbook updates for new markets dynamically.

        Args:
            markets: List of MarketReward objects to subscribe to
        """
        if self.ws_orderbook_client is None:
            logger.error("WebSocket client not initialized - call start_data_feeds() first")
            return

        # Collect token IDs from markets
        token_ids = []
        for market in markets:
            for token in market.tokens:
                token_ids.append(token.token_id)

        # Add tokens to existing subscription
        self.ws_orderbook_client.add_tokens(token_ids)

    def run(self):
        """Main bot loop."""
        logger.info("Starting bot...")
        self.running = True

        try:
            self.setup()
            self._start_market_lifecycle()
            self._start_redeem_loop()

            logger.info("Bot started successfully")

            # Main thread: keep alive while lifecycle + strategy threads run
            while self.running and not self.shutdown_requested:
                time.sleep(1.0)

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
        finally:
            self.shutdown()

    def get_orderbook(self, token_id: str) -> Optional[OrderBookStore]:
        """
        Get the orderbook store for a specific token.

        Args:
            token_id: The token ID to get orderbook for

        Returns:
            OrderBookStore if token is subscribed, None otherwise

        Usage:
            store = self.get_orderbook(token_id)
            if store:
                bid_price, bid_size = store.best_bid()
                ask_price, ask_size = store.best_ask()
        """
        return self.orderbook_stores.get(token_id)

    # ── Market lifecycle management ────────────────────────────────

    def _start_market_lifecycle(self) -> None:
        """Spawn the market lifecycle daemon thread."""
        self._lifecycle_stop.clear()
        self._lifecycle_thread = threading.Thread(
            target=self._market_lifecycle_loop,
            name="market-lifecycle",
            daemon=True,
        )
        self._lifecycle_thread.start()
        logger.info("Market lifecycle thread started")

    def _market_lifecycle_loop(self) -> None:
        """
        Background loop that manages market window transitions.

        Timeline for each 900s window:
            t=0        : discover current market, start strategy, warm up next 3
            t=840 (T-60): refresh — discover any new future markets not yet warmed
            t=900      : transition — stop old strategy, promote next warmed-up strategy
        """
        PREFETCH_LEAD_SEC = 60  # refresh future markets 60s before window end
        FUTURE_MARKETS_COUNT = 3  # always keep next N markets warmed up

        while not self._lifecycle_stop.is_set():
            try:
                # ── Phase 1: Discover + activate current market ──
                if self._current_market is None:
                    self._current_market = self._discover_current_market()
                    if self._current_market is None:
                        logger.error("Failed to discover current market, retrying in 10s")
                        self._lifecycle_stop.wait(timeout=10.0)
                        continue
                    self._activate_market(self._current_market)

                # ── Phase 2: Immediately warm up future markets ──
                self._prefetch_and_warm_up_future_markets(
                    self._current_market, FUTURE_MARKETS_COUNT,
                )

                # ── Phase 3: Wait until T-60, then refresh (pick up any missing) ──
                now = time.time()
                prefetch_time = self._current_market.end_ts - PREFETCH_LEAD_SEC
                wait_for_prefetch = max(0, prefetch_time - now)

                if wait_for_prefetch > 0:
                    logger.info(
                        "Waiting %.1fs to refresh future markets (current ends at %.0f)",
                        wait_for_prefetch,
                        self._current_market.end_ts,
                    )
                    if self._lifecycle_stop.wait(timeout=wait_for_prefetch):
                        break  # shutdown requested

                # Refresh: discover any future markets that failed earlier
                self._prefetch_and_warm_up_future_markets(
                    self._current_market, FUTURE_MARKETS_COUNT,
                )

                # ── Phase 4: Wait for window end, then transition ──
                now = time.time()
                wait_for_end = max(0, self._current_market.end_ts - now)
                if wait_for_end > 0:
                    logger.info("Waiting %.1fs for current window to end", wait_for_end)
                    if self._lifecycle_stop.wait(timeout=wait_for_end):
                        break  # shutdown requested

                # ── Phase 5: Transition (deactivates old, promotes next) ──
                self._transition_to_next_market()

            except Exception:
                logger.exception("Error in market lifecycle loop")
                self._lifecycle_stop.wait(timeout=5.0)

    def _discover_current_market(self) -> Optional[Market]:
        """Discover the market for the current 15m window."""
        try:
            market = discover_market(
                client=self.client,
                underlying=UNDERLYING,
                timespan=TIMESPAN,
            )
            logger.info(
                "Discovered current market: %s (ends at %.0f)",
                market.slug, market.end_ts,
            )
            return market
        except Exception:
            logger.exception("Failed to discover current market")
            return None

    def _prefetch_and_warm_up_future_markets(
        self, current: Market, count: int,
    ) -> None:
        """Discover the next *count* markets and place initial orders on each."""
        ws = window_sec_for_timespan(TIMESPAN)

        for i in range(1, count + 1):
            future_start_ts = int(current.end_ts) + (i - 1) * ws

            # Skip if we already have a strategy for this window
            expected_slug = slug_for_window(UNDERLYING, future_start_ts, TIMESPAN)
            with self._strategies_lock:
                if expected_slug in self._strategies:
                    logger.debug("Already have strategy for %s, skipping", expected_slug)
                    continue

            try:
                future_market = discover_market(
                    client=self.client,
                    window_start_unix=future_start_ts,
                    underlying=UNDERLYING,
                    timespan=TIMESPAN,
                )
                logger.info("Discovered future market [+%d]: %s", i, future_market.slug)
                self._warm_up_market(future_market)
            except Exception:
                logger.exception("Failed to discover future market +%d", i)

        # Set _next_market for transition (first future market)
        next_slug = slug_for_window(UNDERLYING, int(current.end_ts), TIMESPAN)
        with self._strategies_lock:
            next_strategy = self._strategies.get(next_slug)
        self._next_market = next_strategy.market if next_strategy else None

    def _warm_up_market(self, market: Market) -> None:
        """
        Subscribe to orderbook feeds and create a strategy in warm-up mode.
        Places initial orders immediately but does NOT start the tick loop.
        """
        token_ids = [market.up_token_id, market.down_token_id]

        # Subscribe to orderbook data
        if self.ws_orderbook_client is not None:
            self.ws_orderbook_client.add_tokens(token_ids)
        else:
            self._init_orderbook_client(token_ids)

        # Record market in DB
        self.db.record_market(market)

        # volatile_mm doesn't use pre-placed orders; skip warm-up
        if STRATEGY == "volatile_mm":
            logger.info("VolatileMarketMaker does not support warm-up; skipping %s", market.slug)
            return

        strategy = SimpleMarketMaker(
            market=market,
            client=self.client,
            order_placer=self.order_placer,
            order_manager=self.order_manager,
            position_tracker=self.position_tracker,
            orderbook_stores=self.orderbook_stores,
        )
        strategy.warm_up()

        with self._strategies_lock:
            self._strategies[market.slug] = strategy

        logger.info("Market warmed up (initial orders placed): %s", market.slug)

    def _activate_market(self, market: Market) -> None:
        """
        Activate a market: subscribe orderbook + start strategy tick loop.

        If the market was already warmed up, promote it (start tick loop only).
        Otherwise create a fresh strategy and start it.
        """
        token_ids = [market.up_token_id, market.down_token_id]

        # Subscribe to orderbook data (idempotent if already subscribed)
        if self.ws_orderbook_client is not None:
            self.ws_orderbook_client.add_tokens(token_ids)
        else:
            self._init_orderbook_client(token_ids)

        # Record market in DB
        self.db.record_market(market)

        with self._strategies_lock:
            existing = self._strategies.get(market.slug)

        if existing and existing._warmed_up:
            # Promote warmed-up strategy to active (tick loop only, skip on_start)
            existing.start()
            self._active_strategy_slug = market.slug
            logger.info("Promoted warmed-up strategy to active: %s", market.slug)
        else:
            # Fresh activation (no warm-up occurred)
            strategy_kwargs = dict(
                market=market,
                client=self.client,
                order_placer=self.order_placer,
                order_manager=self.order_manager,
                position_tracker=self.position_tracker,
                orderbook_stores=self.orderbook_stores,
            )
            if STRATEGY == "volatile_mm":
                strategy = VolatileMarketMaker(**strategy_kwargs)
                logger.info("Using VolatileMarketMaker strategy")
            else:
                strategy = SimpleMarketMaker(**strategy_kwargs)
                logger.info("Using SimpleMarketMaker strategy")

            with self._strategies_lock:
                self._strategies[market.slug] = strategy
            strategy.start()
            self._active_strategy_slug = market.slug
            logger.info("Market activated (fresh): %s", market.slug)

    def _deactivate_market(self, market: Market) -> None:
        """Stop strategy, cancel orders, and unsubscribe from orderbook feeds."""
        with self._strategies_lock:
            strategy = self._strategies.pop(market.slug, None)

        if strategy and strategy.can_receive_fills:
            strategy.stop(cancel_orders=True)

        if self._active_strategy_slug == market.slug:
            self._active_strategy_slug = None

        token_ids = [market.up_token_id, market.down_token_id]
        if self.ws_orderbook_client is not None:
            self.ws_orderbook_client.remove_tokens(token_ids)

        logger.info("Market deactivated: %s", market.slug)

    def _cleanup_expired_strategies(self) -> None:
        """Remove strategies for markets whose windows have already ended."""
        now = time.time()
        with self._strategies_lock:
            expired_slugs = [
                slug for slug, strategy in self._strategies.items()
                if strategy.market.end_ts < now
                and slug != self._active_strategy_slug
            ]
        for slug in expired_slugs:
            with self._strategies_lock:
                strategy = self._strategies.pop(slug, None)
            if strategy:
                strategy.stop(cancel_orders=True)
                # Also unsubscribe and clean positions
                m = strategy.market
                token_ids = [m.up_token_id, m.down_token_id]
                if self.ws_orderbook_client is not None:
                    self.ws_orderbook_client.remove_tokens(token_ids)
                removed = self.position_tracker.remove_tokens(token_ids)
                if removed:
                    logger.info("Cleared %d position(s) from expired market %s", removed, slug)
                logger.info("Cleaned up expired strategy: %s", slug)

    def _transition_to_next_market(self) -> None:
        """
        Seamless transition from current market to next market.

        1. Stop strategy on old market (cancel orders)
        2. Unsubscribe old tokens from orderbook WS
        3. Clean up expired strategies
        4. Promote or start strategy on new market
        """
        old_market = self._current_market
        new_market = self._next_market

        if old_market:
            self._deactivate_market(old_market)
            # Clear stale positions from ended market so dashboard stays clean
            old_tokens = [old_market.up_token_id, old_market.down_token_id]
            removed = self.position_tracker.remove_tokens(old_tokens)
            if removed:
                logger.info("Cleared %d position(s) from ended market %s", removed, old_market.slug)

        # Clean up any other strategies for markets that have ended
        self._cleanup_expired_strategies()

        if new_market is None:
            # Pre-fetch failed earlier — try again synchronously
            logger.warning("Next market was not pre-fetched, discovering now...")
            new_market = self._discover_current_market()

        if new_market is None:
            logger.error("Cannot transition — no next market available")
            self._current_market = None
            self._next_market = None
            return

        self._current_market = new_market
        self._next_market = None
        self._activate_market(new_market)

        logger.info(
            "Transitioned: %s -> %s",
            old_market.slug if old_market else "none",
            new_market.slug,
        )

    def _init_orderbook_client(self, token_ids: list[str]) -> None:
        """Initialize WsOrderbookClient for the first time."""
        cfg = WsOrderbookConfig(
            rest_seed_on_start=True,
            staleness_sec=120.0,
            resync_on_crossed=True,
            resync_on_reconnect=True,
        )
        self.ws_orderbook_client = WsOrderbookClient(
            ws_url=self.config.polymarket.ws_url,
            token_ids=token_ids,
            stores=self.orderbook_stores,
            rest_client=self.client,
            cfg=cfg,
            on_connection_unhealthy=self._handle_ws_connection_unhealthy,
            on_connection_healthy=self._handle_ws_connection_healthy,
        )
        self.ws_orderbook_client.start()

    # ── Periodic redeem management ──────────────────────────────

    REDEEM_INTERVAL_SEC = 7 * 60  # 7 minutes

    def _start_redeem_loop(self) -> None:
        """Spawn the periodic redeem daemon thread."""
        self._redeem_stop.clear()
        self._redeem_thread = threading.Thread(
            target=self._redeem_loop,
            name="redeem-positions",
            daemon=True,
        )
        self._redeem_thread.start()
        logger.info("Redeem loop thread started (interval=%ds)", self.REDEEM_INTERVAL_SEC)

    def _redeem_loop(self) -> None:
        """Background loop that redeems all resolved positions and checks market resolutions."""
        while not self._redeem_stop.is_set():
            try:
                redeemed = self.client.redeem_all_positions()
                if redeemed > 0:
                    logger.info("[REDEEM] Redeemed %d position(s) this cycle", redeemed)
            except Exception:
                logger.exception("Error in redeem loop")

            # Check resolution for ended markets that we haven't recorded yet
            try:
                self._check_market_resolutions()
            except Exception:
                logger.exception("Error checking market resolutions")

            self._redeem_stop.wait(timeout=self.REDEEM_INTERVAL_SEC)

    def _check_market_resolutions(self) -> None:
        """Query Gamma API for unresolved ended markets and record winners."""
        import json as _json

        unresolved = self.db.get_unresolved_markets()
        if not unresolved:
            return

        for mkt in unresolved:
            slug = mkt["slug"]
            try:
                raw = self.client.get_market_by_slug(slug)
                if not raw:
                    continue

                closed = raw.get("closed", False)
                if not closed:
                    continue

                # Parse outcome_prices and clobTokenIds to find winner
                prices_raw = raw.get("outcomePrices", raw.get("outcome_prices", ""))
                tokens_raw = raw.get("clobTokenIds", raw.get("clob_token_ids", ""))

                prices = _json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
                tokens = _json.loads(tokens_raw) if isinstance(tokens_raw, str) else tokens_raw

                if not prices or not tokens or len(prices) != len(tokens):
                    continue

                # Find which outcome resolved to 1.0
                up_tid = mkt["up_token_id"]
                down_tid = mkt["down_token_id"]
                winner = None

                for tid, price_str in zip(tokens, prices):
                    p = float(price_str)
                    if p >= 0.99:  # resolved to $1.00
                        if tid == up_tid:
                            winner = "UP"
                        elif tid == down_tid:
                            winner = "DOWN"
                        break

                if winner:
                    self.db.set_market_resolution(slug, winner)
                    logger.info("[RESOLUTION] %s resolved: %s won", slug, winner)

            except Exception:
                logger.exception("Error checking resolution for %s", slug)

    def _get_market_for_token(self, token_id: str) -> Optional[Market]:
        """Return the market that owns this token, checking all tracked strategies."""
        with self._strategies_lock:
            for strategy in self._strategies.values():
                m = strategy.market
                if token_id in (m.up_token_id, m.down_token_id):
                    return m
        # Fallback: check _current_market (in case no strategy exists yet)
        m = self._current_market
        if m and token_id in (m.up_token_id, m.down_token_id):
            return m
        return None

    def _route_fill_to_strategy(
        self,
        asset_id: str,
        side: str,
        price: float,
        size: float,
        order_id: str = "",
        trade_id: str = "",
        status: str = "CONFIRMED",
    ) -> None:
        """Forward a fill event to whichever strategy owns this token."""
        with self._strategies_lock:
            strategies = list(self._strategies.values())

        for strategy in strategies:
            if not strategy.can_receive_fills:
                continue
            market_tokens = {
                strategy.market.up_token_id,
                strategy.market.down_token_id,
            }
            if asset_id in market_tokens:
                strategy.on_fill(
                    token_id=asset_id,
                    side=side,
                    price=price,
                    size=size,
                    order_id=order_id,
                    trade_id=trade_id,
                    status=status,
                )
                return  # tokens are unique across markets

    # ── WebSocket callbacks ──────────────────────────────────────

    def _handle_trade_message(self, message):
        """
        Handle incoming trade messages from WebSocket clob_user stream.

        Processes fills in real-time with <1s latency.

        Args:
            message: Message dataclass from RealTimeDataClient with trade payload
        """
        try:
            # Log less verbosely than Phase 1 (only key info)
            logger.debug(f"[WS-TRADE] Received trade: {message.payload.get('id', 'unknown')[:12]}...")

            # Extract trade data
            trade_id = message.payload.get("id")
            status = message.payload.get("status")
            match_time = message.payload.get("match_time")
            transaction_hash = message.payload.get("transaction_hash")
            maker_orders = message.payload.get("maker_orders", [])
            taker_order_id = message.payload.get("taker_order_id")
            taker_address = message.payload.get("maker_address")
            asset_id = message.payload.get("asset_id")

            # Only process statuses the strategy cares about
            if status not in ("MATCHED", "MINED", "CONFIRMED", "FAILED", "RETRYING"):
                return

            # Check if we were the taker (our order executed as taker due to market movement)
            if taker_address and str(taker_address).lower() == str(self.client._funder).lower():
                # Extract taker order details from trade-level fields
                asset_id = message.payload.get("asset_id")
                side = message.payload.get("side", "").upper()
                size = float(message.payload.get("size", 0))
                price = float(message.payload.get("price", 0))
                outcome = message.payload.get("outcome", "Unknown")

                # Find market_slug for this token
                market = self._get_market_for_token(asset_id)
                market_slug = market.slug if market else "unknown"

                logger.info(
                    f"[WS-FILL-TAKER-{status}] {market_slug} {outcome} {side} "
                    f"size={size} @ ${price:.4f} "
                    f"order={taker_order_id[:12]}... (WebSocket TAKER)"
                )

                self._route_fill_to_strategy(asset_id, side, price, size, order_id=taker_order_id, trade_id=trade_id, status=status)

                # Record to DB
                if status == "MATCHED" and trade_id:
                    self.db.record_trade(
                        trade_id=trade_id, order_id=taker_order_id or "",
                        token_id=asset_id, market_slug=market_slug,
                        side=side, size=size, price=price,
                        outcome=outcome,
                        order_type=self.order_manager.get_order_type(taker_order_id) if taker_order_id else None,
                        status=status,
                        transaction_hash=transaction_hash,
                        strategy=STRATEGY,
                    )
                elif status in ("CONFIRMED", "FAILED") and trade_id:
                    self.db.update_trade_status(trade_id, status, transaction_hash=transaction_hash)
            else:
                # Process maker orders if we weren't the taker
                # Process each maker order fill
                for order_data in maker_orders:
                    order_id = order_data.get("order_id")
                    maker_address = order_data.get("maker_address")

                    # Verify this is our order (by maker_address)
                    if maker_address != self.client._funder:
                        logger.debug(f"[WS-TRADE] Skipping maker order {order_id[:12]}... (wrong address: {maker_address[:10]}...)")
                        continue

                    # # Verify we know this order_id (ownership check)
                    # if order_id not in self.known_order_ids:
                    #     logger.warning(
                    #         f"[WS-TRADE] Skipping unknown order {order_id[:12]}... "
                    #         f"(not in known_order_ids, address={maker_address[:10]}...)"
                    #     )
                    #     logger.warning(f"Known order IDs: {list(self.known_order_ids)}")
                    #     continue

                    # # Verify the market is active
                    # if market_id and not self._is_market_active(market_id):
                    #     logger.debug(f"[WS-TRADE] Skipping fill for inactive market {market_id}")
                    #     continue

                    # Extract fill details
                    asset_id = order_data.get("asset_id")
                    side = order_data.get("side", "").upper()
                    size = float(order_data.get("matched_amount", 0))
                    price = float(order_data.get("price", 0))
                    outcome = order_data.get("outcome", "Unknown")

                    # Find market_slug for this token
                    market = self._get_market_for_token(asset_id)
                    market_slug = market.slug if market else "unknown"

                    logger.info(
                        f"[WS-FILL-{status}] {market_slug} {outcome} {side} "
                        f"size={size} @ ${price:.4f} "
                        f"order={order_id[:12]}... (WebSocket)"
                    )

                    self._route_fill_to_strategy(asset_id, side, price, size, order_id=order_id, trade_id=trade_id, status=status)

                    # Record to DB
                    if status == "MATCHED" and trade_id:
                        self.db.record_trade(
                            trade_id=trade_id, order_id=order_id or "",
                            token_id=asset_id, market_slug=market_slug,
                            side=side, size=size, price=price,
                            outcome=outcome,
                            order_type=self.order_manager.get_order_type(order_id) if order_id else None,
                            status=status,
                            transaction_hash=transaction_hash,
                            strategy=STRATEGY,
                        )
                    elif status in ("CONFIRMED", "FAILED") and trade_id:
                        self.db.update_trade_status(trade_id, status, transaction_hash=transaction_hash)

        except Exception as e:
            logger.error(f"[WS-TRADE] Error processing trade message: {e}", exc_info=True)

    def _handle_trade_status_change(self, status):
        """
        Handle WebSocket trade client connection status changes.

        Updates connection health tracking.

        Args:
            status: ConnectionStatus from RealTimeDataClient
        """
        from poly_maker_bot.market_data.rtds_client import ConnectionStatus

        if status == ConnectionStatus.CONNECTED:
            self._ws_trade_connected = True
            logger.info("[WS-TRADE-HEALTH] Trade WebSocket connected")
        elif status == ConnectionStatus.DISCONNECTED:
            self._ws_trade_connected = False
            logger.warning("[WS-TRADE-HEALTH] Trade WebSocket disconnected")

    def _handle_ws_connection_unhealthy(self):
        """
        Handle WebSocket connection becoming unhealthy (PONG timeout).

        Called when either orderbook or trade WebSocket fails to receive PONG
        within the configured timeout. This indicates the connection may be dead
        even though it's still technically "connected".

        Thread safety: This is called from the WebSocket watchdog thread, so we must
        acquire locks when accessing shared state.
        """
        logger.error(
            "[WS-HEALTH] WebSocket connection unhealthy - PONG timeout detected. "
        )

    def _handle_ws_connection_healthy(self):
        """
        Handle WebSocket connection recovery (PONG received after timeout).

        Called when PONG is received after a timeout period. Connection has recovered
        and orderbook data should be reliable again.

        Action: Log recovery.
        """
        logger.info(
            "[WS-HEALTH] WebSocket connection recovered - PONG received. "
        )


    def _record_trade(self, market_id: str, token_id: str, price: float, size: float, timestamp: int, side: str) -> None:
        """
        Record a trade observation for a market.
        Used for emergency trade cascade detection and undercut validation.

        Called from WsOrderbookClient when last_trade_price messages arrive.

        Args:
            market_id: The market (condition_id) where the trade occurred
            token_id: The token that traded
            price: Trade price
            size: Trade size
            timestamp: Trade timestamp (milliseconds)
            side: "BUY" or "SELL"
        """
        print("_record_trade pass")

        pass

    def shutdown(self):
        """Graceful shutdown: cancel orders, close connections."""
        if not self.running:
            return

        logger.info("Shutting down bot...")
        self.running = False

        try:
            # Stop redeem thread
            self._redeem_stop.set()
            if self._redeem_thread and self._redeem_thread.is_alive():
                logger.info("Stopping redeem thread...")
                self._redeem_thread.join(timeout=5.0)
                logger.info("Redeem thread stopped")

            # Stop market lifecycle thread
            self._lifecycle_stop.set()
            if self._lifecycle_thread and self._lifecycle_thread.is_alive():
                logger.info("Stopping market lifecycle thread...")
                self._lifecycle_thread.join(timeout=5.0)
                logger.info("Market lifecycle thread stopped")

            # Stop market discovery timer thread (legacy)
            if self._market_discovery_thread is not None:
                logger.info("Stopping market discovery timer thread...")
                self._market_discovery_stop.set()
                self._market_discovery_thread.join(timeout=5.0)
                logger.info("Market discovery timer thread stopped")

            # Stop all strategies (active + warmed up)
            with self._strategies_lock:
                all_strategies = list(self._strategies.items())
                self._strategies.clear()
            for slug, strategy in all_strategies:
                logger.info("Stopping strategy for %s...", slug)
                strategy.stop(cancel_orders=True)
            self._active_strategy_slug = None
            logger.info("All strategies stopped")

            # Cancel any remaining open orders
            if self.order_manager.open_orders:
                logger.info(f"Cancelling {len(self.order_manager.open_orders)} remaining open orders...")
                for order_id in list(self.order_manager.open_orders.keys()):
                    self.order_placer.cancel_order(order_id)

            # Stop WebSocket orderbook client
            if self.ws_orderbook_client is not None:
                logger.info("Stopping WebSocket orderbook client...")
                self.ws_orderbook_client.stop()
                logger.info("WebSocket client stopped")

            # Stop WebSocket trade client
            if self.ws_trade_client is not None:
                logger.info("Stopping WebSocket trade client...")
                self.ws_trade_client.stop()
                logger.info("WebSocket trade client stopped")

            # Stop dashboard and finalize DB session
            if self._dashboard:
                self._dashboard.stop()
            trade_count = len(self.db.get_recent_trades(limit=10000))
            self.db.end_session(self.session_id, self.realized_pnl, trade_count)
            self.db.close()

            logger.info("Shutdown complete")
        except Exception as e:
            logger.error(f"Error during shutdown: {e}", exc_info=True)


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Polymarket Liquidity Provider Bot",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
            Examples:
            # Run with default settings (from .env or fallbacks)
            python -m poly_maker_bot.maker_bot

            # Verbose logging
            python -m poly_maker_bot.maker_bot -v
        """
    )

    # Logging
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose logging (DEBUG level)"
    )

    args = parser.parse_args()

    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    setup_logging(log_level)

    # Create and run bot
    bot = LiquidityProviderBot()

    # Signal handlers for graceful shutdown
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, shutting down...")
        bot.shutdown_requested = True

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Run bot
    try:
        bot.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
