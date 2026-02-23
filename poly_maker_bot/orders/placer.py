"""Order placement logic."""
from __future__ import annotations

import logging
import threading
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from poly_maker_bot.exchange.polymarket_client import PolymarketClient

logger = logging.getLogger(__name__)

# How long to wait for a WS PLACEMENT confirmation before giving up (seconds)
_WS_CONFIRM_TIMEOUT = 5.0


class OrderPlacer:
    """
    Order placement with exchange integration.

    Responsibilities:
    - Place limit orders on exchange
    - Cancel orders on exchange
    - Handle API errors gracefully
    - Verify order placement via WebSocket PLACEMENT events
    """

    def __init__(self, client: "PolymarketClient"):
        self.client = client

        # Pending placement confirmations: {(token_id, price, size): threading.Event}
        # When a WS PLACEMENT event arrives, we set the matching event and store the order_id.
        self._pending_lock = threading.Lock()
        self._pending: dict[tuple[str, float, float], threading.Event] = {}
        self._confirmed_ids: dict[tuple[str, float, float], str] = {}

    # ── WS order event handler ────────────────────────────────

    def handle_order_event(self, msg: dict) -> None:
        """
        Called by the WsTradeClient on_order callback for every
        order/order_update event.

        We only care about PLACEMENT events — they confirm that an order
        we attempted to place actually landed on the book.
        """
        if msg.get("type") != "PLACEMENT":
            return

        asset_id = msg.get("asset_id", "")
        try:
            price = float(msg.get("price", 0))
            size = float(msg.get("original_size", 0))
        except (TypeError, ValueError):
            return

        order_id = msg.get("id", "")
        key = (asset_id, price, size)

        with self._pending_lock:
            evt = self._pending.get(key)
            if evt is not None:
                self._confirmed_ids[key] = order_id
                evt.set()
                logger.info(
                    "WS confirmed order placement: id=%s token=%s price=%s size=%s",
                    order_id[:12], asset_id[:12], price, size,
                )

    # ── order placement ───────────────────────────────────────

    def place_limit_order(
        self,
        token_id: str,
        side: str,
        price: float,
        size: float,
        market_slug: str,
        is_sell: bool = False,
    ) -> Optional[str]:
        """
        Place a limit order on the exchange.

        If the API call fails but the order may have been placed (network error,
        timeout, etc.), we wait for a WebSocket PLACEMENT confirmation instead
        of polling the REST API.

        Args:
            token_id: Token to trade
            side: "UP" or "DOWN" (outcome side)
            price: Limit price
            size: Order size
            market_slug: Market identifier for logging
            is_sell: True to place a sell order, False for buy order

        Returns:
            Order ID if successful, None if failed
        """
        # Register a pending confirmation *before* the API call so we don't
        # miss a fast WS event that arrives before the except block.
        key = (token_id, price, size)
        evt = threading.Event()
        with self._pending_lock:
            self._pending[key] = evt

        try:
            order_type = "GTC"  # Good-til-canceled
            order_type_str = "SELL" if is_sell else "BUY"
            logger.info(
                f"[{market_slug}] Placing {order_type_str} order token={token_id[:12]}... "
                f"size={size} price={price} type={order_type}"
            )

            if is_sell:
                response = self.client.place_limit_sell(
                    token_id=token_id,
                    size=size,
                    price=price,
                    order_type=order_type,
                    market_slug=market_slug
                )
            else:
                response = self.client.place_limit_buy(
                    token_id=token_id,
                    size=size,
                    price=price,
                    order_type=order_type,
                    market_slug=market_slug
                )

            logger.info(
                f"Order placed: {order_type_str} {side} {response.filled_size}/{response.requested_size} "
                f"@ ${response.avg_price:.3f} id={response.order_id[:12]}... market={market_slug}"
            )

            return response.order_id

        except Exception as e:
            logger.error(f"Failed to place order: {e}")

            # Wait for WS PLACEMENT confirmation instead of polling REST
            logger.info("Waiting for WS confirmation of order placement...")
            if evt.wait(timeout=_WS_CONFIRM_TIMEOUT):
                with self._pending_lock:
                    order_id = self._confirmed_ids.pop(key, None)
                if order_id:
                    logger.warning(
                        f"Order WAS placed despite error (confirmed via WS)! "
                        f"id={order_id[:12]}... token={token_id[:12]}... size={size} price={price}"
                    )
                    return order_id

            logger.info("No WS confirmation received — order was NOT placed")
            return None

        finally:
            with self._pending_lock:
                self._pending.pop(key, None)
                self._confirmed_ids.pop(key, None)

    def cancel_order(self, order_id: str, caller_info: str = "") -> bool:
        """
        Cancel an order on the exchange.

        Args:
            order_id: Order ID to cancel
            caller_info: Optional caller information for debugging

        Returns:
            True if successful, False if failed
        """
        try:
            logger.info(f"Cancelling order {order_id[:12]}...{caller_info}")

            resp = self.client.cancel_order(order_id)
            logger.debug(f"Cancel order RESPONSE for {order_id[:12]}... : {resp}")

            # Check if order was successfully canceled
            if len(resp.get('canceled', [])) > 0:
                logger.info(f"Order cancelled: {order_id[:12]}...")
                return True

            # Check if order was already canceled or matched - treat as success
            not_canceled = resp.get('not_canceled', {})
            if order_id in not_canceled:
                reason = not_canceled[order_id]
                if "already canceled" in reason.lower() or "matched" in reason.lower() or "can't be found" in reason.lower():
                    logger.info(f"Order {order_id[:12]}... already canceled/matched: {reason}")
                    return True

            # Order couldn't be canceled for other reasons
            logger.warning(
                f"Order cancel response for order {order_id[:12]}... indicates no orders were cancelled: {resp}"
            )
            return False

        except Exception as e:
            logger.error(f"Failed to cancel order {order_id}: {e}")
            return False
