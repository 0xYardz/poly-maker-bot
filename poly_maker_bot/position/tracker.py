"""Position state tracking and management."""
from __future__ import annotations

import logging
import threading
from typing import Optional

logger = logging.getLogger(__name__)


class PositionTracker:
    """
    Thread-safe position state management.

    Responsibilities:
    - Track positions by token_id
    - Update positions on fills
    - Remove dust positions
    - Thread-safe read/write operations

    Position structure:
    {
        "size": float,
        "avg_price": float,
        "cost_basis": float,
        "market_slug": str,
    }
    """

    def __init__(self, dust_threshold: float = 0.1):
        self.positions = {}  # {token_id: position_dict}
        self.dust_threshold = dust_threshold
        self._lock = threading.RLock()
        # Track recently sold positions to prevent backfill race conditions
        # Format: {token_id: timestamp_when_sold}
        self._recently_sold = {}  # type: dict[str, float]
        # Track recently bought positions to prevent backfill overwriting fresh buys
        # Format: {token_id: timestamp_when_bought}
        self._recently_bought = {}  # type: dict[str, float]

    def update_on_buy_fill(
        self,
        token_id: str,
        fill_size: float,
        fill_price: float,
        market_slug: str,
    ) -> tuple[float, float]:
        """
        Update position when buy order fills.

        Args:
            token_id: The token that was filled
            fill_size: Size of the fill (positive for buys)
            fill_price: Price at which the fill occurred
            market_slug: The market slug for this token

        Returns:
            Tuple of (old_size, new_size) for caller's use
        """
        logger.info(
            f"Updating position for fill: {market_slug} {token_id[:12]}... "
            f"fill_size={fill_size} fill_price=${fill_price:.4f}"
        )

        with self._lock:
            if token_id not in self.positions:
                self.positions[token_id] = {
                    "size": 0.0,
                    "avg_price": 0.0,
                    "cost_basis": 0.0,
                    "market_slug": market_slug,
                }

            pos = self.positions[token_id]
            old_size = pos["size"]
            old_cost_basis = pos["cost_basis"]

            # Update size and cost basis
            new_size = old_size + fill_size
            new_cost_basis = old_cost_basis + (fill_size * fill_price)

            # Calculate new average price
            if new_size > 0:
                new_avg_price = new_cost_basis / new_size
            else:
                new_avg_price = 0.0
                new_cost_basis = 0.0  # Reset if position closed

            pos["size"] = new_size
            pos["avg_price"] = new_avg_price
            pos["cost_basis"] = new_cost_basis

            # Track this as a recent buy to prevent backfill race conditions
            import time
            self._recently_bought[token_id] = time.time()

            logger.info(
                f"Position updated: {market_slug} {token_id[:12]}... "
                f"size={old_size} -> {new_size} "
                f"avg_price=${new_avg_price:.4f}"
            )

            return old_size, new_size

    def update_on_sell_fill(
        self,
        token_id: str,
        sell_size: float,
        sell_price: float,
        market_slug: str,
    ) -> tuple[float, float, float, float]:
        """
        Update position when sell order fills.

        Args:
            token_id: The token that was sold
            sell_size: Size of the sell fill (positive)
            sell_price: Price at which the sell occurred
            market_slug: The market slug for this token

        Returns:
            Tuple of (realized_pnl, old_size, new_size, old_avg_price)
        """
        logger.info(
            f"[SELL FILL] Updating position: {market_slug} {token_id[:12]}... "
            f"sell_size={sell_size:.2f} sell_price=${sell_price:.4f}"
        )

        with self._lock:
            if token_id not in self.positions:
                logger.warning(
                    f"[SELL FILL] No position found for {token_id[:12]}... "
                    f"Creating empty position to track sell."
                )
                self.positions[token_id] = {
                    "size": 0.0,
                    "avg_price": 0.0,
                    "cost_basis": 0.0,
                    "market_slug": market_slug,
                }

            pos = self.positions[token_id]
            old_size = pos["size"]
            old_avg_price = pos["avg_price"]
            old_cost_basis = pos["cost_basis"]

            # Calculate realized PnL: (sell_price - avg_price) * sell_size
            realized_pnl = (sell_price - old_avg_price) * sell_size

            # Decrease position size
            new_size = max(0.0, old_size - sell_size)

            # Update cost basis proportionally
            if old_size > 0 and new_size > 0:
                ratio = new_size / old_size
                new_cost_basis = old_cost_basis * ratio
            else:
                new_cost_basis = 0.0

            # Recalculate average price (should stay the same unless position closed)
            if new_size > 0:
                new_avg_price = new_cost_basis / new_size
            else:
                new_avg_price = 0.0

            pos["size"] = new_size
            pos["avg_price"] = new_avg_price
            pos["cost_basis"] = new_cost_basis

            # Clean up dust positions - remove entirely if below threshold
            if new_size < self.dust_threshold:
                del self.positions[token_id]
                # Track this as a recent sell to prevent backfill race conditions
                import time
                self._recently_sold[token_id] = time.time()
                logger.info(
                    f"[SELL FILL] Removed dust position {token_id[:12]}... "
                    f"(size={new_size:.4f} < {self.dust_threshold})"
                )
                logger.debug(self.positions)

            logger.info(
                f"[SELL FILL] Position updated: {market_slug} {token_id[:12]}... "
                f"size={old_size:.2f} -> {new_size:.2f} "
                f"avg_price=${old_avg_price:.4f} -> ${new_avg_price:.4f} "
                f"realized_pnl=${realized_pnl:+.2f}"
            )

            return realized_pnl, old_size, new_size, old_avg_price

    def get_position(self, token_id: str) -> Optional[dict]:
        """Get position for token (thread-safe)."""
        with self._lock:
            return self.positions.get(token_id)

    def get_all_positions(self) -> dict[str, dict]:
        """Get snapshot of all positions (thread-safe)."""
        with self._lock:
            return dict(self.positions)

    def has_position(self, token_id: str) -> bool:
        """Check if position exists for token."""
        with self._lock:
            pos = self.positions.get(token_id)
            return pos is not None and pos["size"] >= self.dust_threshold

    def remove_dust_positions(self) -> int:
        """
        Remove positions below dust threshold.

        Returns:
            Number of positions removed
        """
        with self._lock:
            to_remove = [
                tid
                for tid, pos in self.positions.items()
                if pos["size"] < self.dust_threshold
            ]
            for tid in to_remove:
                del self.positions[tid]
            return len(to_remove)

    def was_recently_sold(self, token_id: str, current_time: float, grace_sec: float = 60.0) -> bool:
        """
        Check if position was recently sold (within grace period).

        This prevents backfilling positions that were just sold but haven't
        been removed from the REST API yet.

        Args:
            token_id: Token to check
            current_time: Current timestamp
            grace_sec: Grace period in seconds (default 60s)

        Returns:
            True if position was sold within grace period
        """
        with self._lock:
            sell_time = self._recently_sold.get(token_id)
            if sell_time is None:
                return False
            return (current_time - sell_time) < grace_sec

    def cleanup_expired_recent_sells(self, current_time: float, grace_sec: float = 60.0) -> int:
        """
        Remove expired entries from recently sold tracking.

        Args:
            current_time: Current timestamp
            grace_sec: Grace period in seconds

        Returns:
            Number of entries removed
        """
        with self._lock:
            to_remove = [
                tid
                for tid, sell_time in self._recently_sold.items()
                if (current_time - sell_time) >= grace_sec
            ]
            for tid in to_remove:
                del self._recently_sold[tid]
            return len(to_remove)

    def was_recently_bought(self, token_id: str, current_time: float, grace_sec: float = 60.0) -> bool:
        """
        Check if position was recently bought (within grace period).

        This prevents backfill from overwriting a position whose size was just
        updated locally by a buy fill but hasn't propagated to the REST API yet.
        """
        with self._lock:
            buy_time = self._recently_bought.get(token_id)
            if buy_time is None:
                return False
            return (current_time - buy_time) < grace_sec

    def cleanup_expired_recent_buys(self, current_time: float, grace_sec: float = 60.0) -> int:
        """Remove expired entries from recently bought tracking."""
        with self._lock:
            to_remove = [
                tid
                for tid, buy_time in self._recently_bought.items()
                if (current_time - buy_time) >= grace_sec
            ]
            for tid in to_remove:
                del self._recently_bought[tid]
            return len(to_remove)
