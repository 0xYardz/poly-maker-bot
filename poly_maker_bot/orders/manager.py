"""Order state management and tracking."""
from __future__ import annotations

import threading
import time
from collections import deque
from typing import Optional

import logging

logger = logging.getLogger(__name__)

class OrderManager:
    """
    Thread-safe order state management.

    Responsibilities:
    - Track open orders
    - Track recently placed order IDs
    - Track recently cancelled orders
    - Map order IDs to order types
    - Thread-safe read/write operations
    """

    def __init__(
        self,
        known_order_ids_maxlen: int = 500,
        recently_cancelled_grace_sec: float = 30.0,
    ):
        self.open_orders = {}  # {order_id: order_details}
        self.known_order_ids = deque(maxlen=known_order_ids_maxlen)
        self.order_types = {}  # {order_id: order_type}
        self.recently_cancelled = {}  # {order_id: timestamp}
        self.recently_cancelled_grace_sec = recently_cancelled_grace_sec

        self._lock = threading.RLock()

    def add_order(
        self,
        order_id: str,
        order_details: dict,
        order_type: str,
    ) -> None:
        """Add a new order to tracking."""
        with self._lock:
            self.open_orders[order_id] = order_details
            self.known_order_ids.append(order_id)
            self.order_types[order_id] = order_type

    def remove_order(self, order_id: str) -> Optional[dict]:
        """Remove order from tracking."""
        with self._lock:
            order = self.open_orders.pop(order_id, None)
            self.order_types.pop(order_id, None)
            return order

    def mark_cancelled(self, order_id: str, timestamp: float) -> None:
        """Mark order as recently cancelled."""
        with self._lock:
            self.recently_cancelled[order_id] = timestamp

    def was_recently_cancelled(self, order_id: str, current_time: float) -> bool:
        """Check if order was recently cancelled (within grace period)."""
        with self._lock:
            if order_id not in self.recently_cancelled:
                return False
            return current_time - self.recently_cancelled[order_id] < self.recently_cancelled_grace_sec

    def cleanup_expired_cancelled(self, current_time: float, grace_period_sec: float) -> None:
        """Remove old entries from recently_cancelled to prevent memory growth."""
        with self._lock:
            expired = [
                order_id for order_id, ts in self.recently_cancelled.items()
                if current_time - ts > grace_period_sec
            ]
            for order_id in expired:
                del self.recently_cancelled[order_id]
            if expired:
                logger.debug(f"Cleaned up {len(expired)} expired recently_cancelled entries")

    def get_orders_by_token(self, token_id: str) -> list[tuple[str, dict]]:
        """Get all orders for a specific token."""
        with self._lock:
            return [
                (oid, order) for oid, order in self.open_orders.items()
                if order.get("token_id") == token_id
            ]

    def get_orders_by_type(self, order_type: str) -> list[tuple[str, dict]]:
        """Get all orders of a specific type."""
        with self._lock:
            return [
                (oid, order) for oid, order in self.open_orders.items()
                if self.order_types.get(oid) == order_type
            ]

    def get_order(self, order_id: str) -> Optional[dict]:
        """Get a specific order by ID."""
        with self._lock:
            return self.open_orders.get(order_id)

    def has_order(self, order_id: str) -> bool:
        """Check if order exists in tracking."""
        with self._lock:
            return order_id in self.open_orders

    def get_all_orders(self) -> dict[str, dict]:
        """Get snapshot of all open orders."""
        with self._lock:
            return dict(self.open_orders)

    def get_order_count(self) -> int:
        """Get count of open orders."""
        with self._lock:
            return len(self.open_orders)

    def get_order_type(self, order_id: str) -> Optional[str]:
        """Get order type for an order ID."""
        with self._lock:
            return self.order_types.get(order_id)

    def update_order_size(self, order_id: str, new_size: float) -> None:
        """Update the size of an order."""
        with self._lock:
            if order_id in self.open_orders:
                self.open_orders[order_id]["size"] = new_size

    def is_known_order(self, order_id: str) -> bool:
        """Check if order ID is in known_order_ids."""
        with self._lock:
            return order_id in self.known_order_ids
