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
from poly_maker_bot.strategy import StrategyEngine, SimpleMarketMaker
from poly_maker_bot.utils.slug_helpers import Underlying, Timespan

# Create default bot config from environment variables
DEFAULT_BOT_CONFIG = BotConfig()
logger = logging.getLogger(__name__)

# Market targeting from .env (defaults: btc / 15m)
UNDERLYING: Underlying = os.environ.get("POLY_UNDERLYING", "btc")  # type: ignore[assignment]
TIMESPAN: Timespan = os.environ.get("POLY_TIMESPAN", "15m")  # type: ignore[assignment]


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
        self._strategy: Optional[StrategyEngine] = None
        self._lifecycle_stop = threading.Event()
        self._lifecycle_thread: Optional[threading.Thread] = None

        # Periodic redeem management
        self._redeem_stop = threading.Event()
        self._redeem_thread: Optional[threading.Thread] = None

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
            t=0        : discover current market, start strategy, subscribe orderbook
            t=840 (T-60): pre-fetch next market via discover_market(window_start_unix=end_ts)
            t=900      : transition — stop old strategy, start new strategy
        """
        PREFETCH_LEAD_SEC = 60  # fetch next market 60s before window end

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

                # ── Phase 2: Wait, then pre-fetch next market ──
                now = time.time()
                prefetch_time = self._current_market.end_ts - PREFETCH_LEAD_SEC
                wait_for_prefetch = max(0, prefetch_time - now)

                if wait_for_prefetch > 0:
                    logger.info(
                        "Waiting %.1fs to pre-fetch next market (current ends at %.0f)",
                        wait_for_prefetch,
                        self._current_market.end_ts,
                    )
                    if self._lifecycle_stop.wait(timeout=wait_for_prefetch):
                        break  # shutdown requested

                self._next_market = self._discover_next_market(self._current_market)

                # ── Phase 3: Wait for window end, then transition ──
                now = time.time()
                wait_for_end = max(0, self._current_market.end_ts - now)
                if wait_for_end > 0:
                    logger.info("Waiting %.1fs for current window to end", wait_for_end)
                    if self._lifecycle_stop.wait(timeout=wait_for_end):
                        break  # shutdown requested

                # ── Phase 4: Transition (deactivates old, activates new) ──
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

    def _discover_next_market(self, current: Market) -> Optional[Market]:
        """Pre-fetch the market for the next 15m window."""
        try:
            next_market = discover_market(
                client=self.client,
                window_start_unix=int(current.end_ts),
                underlying=UNDERLYING,
                timespan=TIMESPAN,
            )
            logger.info("Pre-fetched next market: %s", next_market.slug)
            return next_market
        except Exception:
            logger.exception("Failed to pre-fetch next market")
            return None

    def _activate_market(self, market: Market) -> None:
        """
        Subscribe to orderbook feeds and start strategy for a market.

        1. Subscribe WS orderbook to up_token_id and down_token_id
        2. Create a StrategyEngine instance
        3. Start the strategy
        """
        token_ids = [market.up_token_id, market.down_token_id]

        # Subscribe to orderbook data
        if self.ws_orderbook_client is not None:
            self.ws_orderbook_client.add_tokens(token_ids)
        else:
            self._init_orderbook_client(token_ids)

        # Create and start strategy
        self._strategy = SimpleMarketMaker(
            market=market,
            client=self.client,
            order_placer=self.order_placer,
            order_manager=self.order_manager,
            position_tracker=self.position_tracker,
            orderbook_stores=self.orderbook_stores,
        )
        self._strategy.start()

        logger.info("Market activated: %s", market.slug)

    def _deactivate_market(self, market: Market) -> None:
        """Stop strategy and unsubscribe from orderbook feeds for a market."""
        if self._strategy and self._strategy.is_running:
            self._strategy.stop(cancel_orders=True)
            self._strategy = None

        token_ids = [market.up_token_id, market.down_token_id]
        if self.ws_orderbook_client is not None:
            self.ws_orderbook_client.remove_tokens(token_ids)

        logger.info("Market deactivated: %s", market.slug)

    def _transition_to_next_market(self) -> None:
        """
        Seamless transition from current market to next market.

        1. Stop strategy on old market (cancel orders)
        2. Unsubscribe old tokens from orderbook WS
        3. Subscribe new tokens to orderbook WS
        4. Start strategy on new market
        """
        old_market = self._current_market
        new_market = self._next_market

        if old_market:
            self._deactivate_market(old_market)

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

    REDEEM_INTERVAL_SEC = 30 * 60  # 30 minutes

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
        """Background loop that redeems all resolved positions every 30 minutes."""
        while not self._redeem_stop.is_set():
            try:
                redeemed = self.client.redeem_all_positions()
                if redeemed > 0:
                    logger.info("[REDEEM] Redeemed %d position(s) this cycle", redeemed)
            except Exception:
                logger.exception("Error in redeem loop")

            self._redeem_stop.wait(timeout=self.REDEEM_INTERVAL_SEC)

    def _get_market_for_token(self, token_id: str) -> Optional[Market]:
        """Return the current market if it owns this token, else None."""
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
        """Forward a fill event to the active strategy if it owns this token."""
        if not self._strategy or not self._strategy.is_running:
            return

        market_tokens = {
            self._strategy.market.up_token_id,
            self._strategy.market.down_token_id,
        }
        if asset_id in market_tokens:
            self._strategy.on_fill(
                token_id=asset_id,
                side=side,
                price=price,
                size=size,
                order_id=order_id,
                trade_id=trade_id,
                status=status,
            )

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

            # Stop active strategy (cancels its orders)
            if self._strategy and self._strategy.is_running:
                logger.info("Stopping active strategy...")
                self._strategy.stop(cancel_orders=True)
                logger.info("Active strategy stopped")

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
