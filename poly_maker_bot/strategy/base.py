"""Abstract base class for market-making strategies."""
from __future__ import annotations

import abc
import logging
import threading
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from poly_maker_bot.market.discovery import Market
    from poly_maker_bot.exchange.polymarket_client import PolymarketClient
    from poly_maker_bot.orders.manager import OrderManager
    from poly_maker_bot.orders.placer import OrderPlacer
    from poly_maker_bot.position.tracker import PositionTracker
    from poly_maker_bot.market_data.orderbook_store import OrderBookStore

logger = logging.getLogger(__name__)


class StrategyEngine(abc.ABC):
    """
    Abstract base for a single-market market-making strategy.

    Lifecycle:
        1. __init__()  — receives market + shared components
        2. start()     — begins placing/managing orders (runs strategy loop in a thread)
        3. on_fill()   — called by the bot when a fill is received for this market
        4. stop()      — cancels all orders for this market, cleans up

    Each instance is bound to exactly one Market. When the 15m window
    transitions, the bot stops the old engine and starts a new one.
    """

    def __init__(
        self,
        market: "Market",
        client: "PolymarketClient",
        order_placer: "OrderPlacer",
        order_manager: "OrderManager",
        position_tracker: "PositionTracker",
        orderbook_stores: dict[str, "OrderBookStore"],
    ):
        self.market = market
        self.client = client
        self.order_placer = order_placer
        self.order_manager = order_manager
        self.position_tracker = position_tracker
        self.orderbook_stores = orderbook_stores

        self._running = False
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # ── public interface ──────────────────────────────────────────

    def start(self) -> None:
        """Start the strategy loop in a background daemon thread."""
        if self._running:
            logger.warning("Strategy already running for %s", self.market.slug)
            return

        logger.info("Starting strategy for market %s", self.market.slug)
        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name=f"strategy-{self.market.slug}",
            daemon=True,
        )
        self._thread.start()

    def stop(self, cancel_orders: bool = True) -> None:
        """
        Stop the strategy loop and optionally cancel all outstanding orders
        for this market.
        """
        if not self._running:
            return

        logger.info("Stopping strategy for market %s", self.market.slug)
        self._stop_event.set()
        self._running = False

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)

        if cancel_orders:
            self._cancel_all_market_orders()

    @property
    def is_running(self) -> bool:
        return self._running

    # ── methods subclasses MUST implement ────────────────────────

    @abc.abstractmethod
    def on_tick(self) -> None:
        """
        Called once per strategy loop iteration.

        This is the core of the strategy. Implementations should:
        - Read orderbook state from self.orderbook_stores
        - Decide on quotes (prices, sizes) for up_token and down_token
        - Place/amend/cancel orders via self.order_placer / self.order_manager
        - Respect position limits via self.position_tracker
        """
        ...

    @abc.abstractmethod
    def on_fill(
        self,
        token_id: str,
        side: str,
        price: float,
        size: float,
        order_id: str = "",
        trade_id: str = "",
        status: str = "CONFIRMED",
    ) -> None:
        """
        Called by the bot when a fill is received for this market.

        Args:
            token_id: The token that was filled
            side: "BUY" or "SELL"
            price: Fill price
            size: Fill size
            order_id: The order ID that was filled
            trade_id: Unique trade ID (same across MATCHED/MINED/CONFIRMED events)
            status: Trade status — MATCHED, MINED, CONFIRMED, RETRYING, or FAILED
        """
        ...

    # ── optional hooks subclasses MAY override ───────────────────

    def on_start(self) -> None:
        """Called once when the strategy loop begins (inside the thread)."""
        pass

    def on_stop(self) -> None:
        """Called once when the strategy loop ends (inside the thread)."""
        pass

    def tick_interval_sec(self) -> float:
        """How often on_tick() is called. Override to change cadence."""
        return 1.0

    # ── internal machinery ───────────────────────────────────────

    def _run_loop(self) -> None:
        """Main loop: calls on_tick() at tick_interval_sec() cadence."""
        try:
            self.on_start()
            while not self._stop_event.is_set():
                try:
                    self.on_tick()
                except Exception:
                    logger.exception(
                        "Error in strategy tick for %s", self.market.slug
                    )
                self._stop_event.wait(timeout=self.tick_interval_sec())
        finally:
            self.on_stop()

    def _cancel_all_market_orders(self) -> None:
        """Cancel every open order belonging to this market's tokens."""
        token_ids = {self.market.up_token_id, self.market.down_token_id}
        for order_id, order in list(self.order_manager.get_all_orders().items()):
            if order.get("token_id") in token_ids:
                self.order_placer.cancel_order(order_id)
                self.order_manager.remove_order(order_id)

    # ── orderbook helpers ────────────────────────────────────────

    def get_orderbook(self, token_id: str) -> Optional["OrderBookStore"]:
        """Convenience: get the OrderBookStore for a token."""
        return self.orderbook_stores.get(token_id)
