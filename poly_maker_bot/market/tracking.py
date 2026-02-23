"""Active market tracking and management."""
from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from poly_lp.exchange.polymarket_client import MarketReward

logger = logging.getLogger(__name__)


class MarketTracker:
    """
    Thread-safe tracker for active and available markets.

    Responsibilities:
    - Maintain list of active markets (currently providing liquidity)
    - Maintain list of available markets (discovered from exchange)
    - Thread-safe add/remove operations
    """

    def __init__(self):
        self.active_markets: list["MarketReward"] = []
        self.available_markets: list["MarketReward"] = []
        self._lock = threading.RLock()

    def add_market(self, market: "MarketReward") -> None:
        """
        Add market to active list (thread-safe).

        Args:
            market: Market to add to active list
        """
        with self._lock:
            if market not in self.active_markets:
                self.active_markets.append(market)
                logger.info(f"Market activated: {market.question[:60]}...")
            else:
                logger.debug(f"Market already active: {market.question[:60]}...")

    def remove_market(self, market: "MarketReward") -> None:
        """
        Remove market from active list (thread-safe).

        Args:
            market: Market to remove from active list
        """
        with self._lock:
            if market in self.active_markets:
                self.active_markets.remove(market)
                logger.info(f"Market deactivated: {market.question[:60]}...")
            else:
                logger.debug(f"Market not in active_markets: {market.question[:60]}...")

    def get_active_markets(self) -> list["MarketReward"]:
        """
        Get snapshot of active markets (thread-safe).

        Returns:
            Copy of active markets list
        """
        with self._lock:
            return list(self.active_markets)

    def update_available_markets(self, markets: list["MarketReward"]) -> None:
        """
        Update available markets list (thread-safe).

        Args:
            markets: New list of available markets
        """
        with self._lock:
            self.available_markets = markets

    def get_available_markets(self) -> list["MarketReward"]:
        """
        Get snapshot of available markets (thread-safe).

        Returns:
            Copy of available markets list
        """
        with self._lock:
            return list(self.available_markets)

    def update_active_markets(self, markets: list["MarketReward"]) -> None:
        """
        Update active markets list (thread-safe).

        Args:
            markets: New list of active markets
        """
        with self._lock:
            self.active_markets = markets

    def is_market_active(self, market_id: str) -> bool:
        """
        Check if market is in active list.

        Args:
            market_id: Market condition ID to check

        Returns:
            True if market is active, False otherwise
        """
        with self._lock:
            return any(m.condition_id == market_id for m in self.active_markets)
