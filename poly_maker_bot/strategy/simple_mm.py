"""Simple market-making strategy for 15m up/down markets."""
from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from poly_maker_bot.strategy.base import StrategyEngine

if TYPE_CHECKING:
    from poly_maker_bot.market.discovery import Market
    from poly_maker_bot.exchange.polymarket_client import PolymarketClient
    from poly_maker_bot.orders.manager import OrderManager
    from poly_maker_bot.orders.placer import OrderPlacer
    from poly_maker_bot.position.tracker import PositionTracker
    from poly_maker_bot.market_data.orderbook_store import OrderBookStore

logger = logging.getLogger(__name__)

# Order type tags for OrderManager tracking
_OT_INITIAL = "initial_buy"
_OT_FILL_REACTION = "fill_reaction_buy"

# Strategy constants
INITIAL_BUY_PRICE = float(os.environ.get("POLY_INITIAL_BUY_PRICE", "0.03"))   # 3c
REACTION_BUY_PRICE = float(os.environ.get("POLY_REACTION_BUY_PRICE", "0.96"))  # 96c
ORDER_SIZE = float(os.environ.get("POLY_ORDER_SIZE", "100.0"))  # shares
MIN_ORDER_SIZE = 5.0        # exchange minimum order size
CANCEL_BEFORE_END_SEC = int(os.environ.get("POLY_CANCEL_BEFORE_END_SEC", "60"))  # cancel unfilled initial orders with <1 min left
CONFIRM_TIMEOUT_SEC = 30.0  # cancel reaction if trade not MINED/CONFIRMED within this window

# Statuses that confirm a trade is on-chain
_CONFIRMED_STATUSES = frozenset({"MINED", "CONFIRMED"})


@dataclass
class _PendingReaction:
    """Tracks a reaction order that is waiting for its triggering trade to confirm."""
    reaction_order_id: str
    matched_at: float          # time.time() when MATCHED arrived
    opposite_token: str        # token_id for the reaction order
    opposite_label: str        # "UP" or "DOWN"
    reaction_size: float       # size of the reaction order
    trade_ids: set[str] = field(default_factory=set)
    confirmed: bool = False
    cancelled_for_retry: bool = False  # True if cancelled due to RETRYING


class SimpleMarketMaker(StrategyEngine):
    """
    Two-sided market maker for a 15m up/down binary market.

    Strategy:
    - On start, place a 3c BUY order on both the Up and Down tokens.
    - On MATCHED fill of an initial order, immediately place a 96c BUY
      on the opposite outcome token for speed.
    - If the trade is not MINED or CONFIRMED within CONFIRM_TIMEOUT_SEC,
      cancel the reaction order (the initial fill didn't actually go through).
    - On RETRYING, cancel the reaction order but keep tracking — if CONFIRMED
      arrives after the retry, re-place the reaction order.
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
        super().__init__(
            market=market,
            client=client,
            order_placer=order_placer,
            order_manager=order_manager,
            position_tracker=position_tracker,
            orderbook_stores=orderbook_stores,
        )
        # Track order IDs for the initial resting orders
        self._up_initial_order_id: str | None = None
        self._down_initial_order_id: str | None = None

        # Accumulate partial fills per token before placing reaction orders.
        # Keyed by token_id that was filled (the reaction goes to the *opposite* token).
        self._pending_fill_size: dict[str, float] = {}
        self._fill_lock = threading.Lock()

        # Pending reaction confirmations: reaction_order_id -> _PendingReaction
        self._pending_reactions: dict[str, _PendingReaction] = {}
        self._pending_reactions_lock = threading.Lock()

    # ── order helpers ──────────────────────────────────────────

    def _place_and_track(
        self,
        token_id: str,
        side_label: str,
        price: float,
        size: float,
        order_type: str,
    ) -> str | None:
        """Place a BUY order and register it with the OrderManager."""
        order_id = self.order_placer.place_limit_order(
            token_id=token_id,
            side=side_label,
            price=price,
            size=size,
            market_slug=self.market.slug,
            is_sell=False,
        )
        if order_id:
            self.order_manager.add_order(
                order_id=order_id,
                order_details={
                    "token_id": token_id,
                    "side": "BUY",
                    "price": price,
                    "size": size,
                    "market_slug": self.market.slug,
                },
                order_type=order_type,
            )
        return order_id

    def _place_initial_order(self, token_id: str, side_label: str) -> str | None:
        """Place a 3c / ORDER_SIZE initial buy order."""
        logger.info(
            "[%s] Placing initial BUY: %s token=%s price=%.2f size=%.0f",
            self.market.slug, side_label, token_id[:12],
            INITIAL_BUY_PRICE, ORDER_SIZE,
        )
        return self._place_and_track(
            token_id=token_id,
            side_label=side_label,
            price=INITIAL_BUY_PRICE,
            size=ORDER_SIZE,
            order_type=_OT_INITIAL,
        )

    # ── lifecycle hooks ────────────────────────────────────────

    def on_start(self) -> None:
        logger.info(
            "[%s] SimpleMarketMaker started: buy_price=%.2f reaction_price=%.2f size=%.0f",
            self.market.slug, INITIAL_BUY_PRICE, REACTION_BUY_PRICE, ORDER_SIZE,
        )
        self._up_initial_order_id = self._place_initial_order(
            self.market.up_token_id, "UP",
        )
        self._down_initial_order_id = self._place_initial_order(
            self.market.down_token_id, "DOWN",
        )

    def on_stop(self) -> None:
        logger.info("[%s] SimpleMarketMaker stopped", self.market.slug)

    # ── core strategy methods ──────────────────────────────────

    def on_tick(self) -> None:
        """
        Periodic housekeeping:
        1. Cancel reaction orders whose triggering trades timed out (no MINED/CONFIRMED).
        2. Cancel unfilled initial orders when <1 min remains in the market.
        """
        now = time.time()

        # ── Check for timed-out pending reactions ──
        with self._pending_reactions_lock:
            timed_out_ids = [
                rid for rid, pr in self._pending_reactions.items()
                if not pr.confirmed and not pr.cancelled_for_retry
                and (now - pr.matched_at) > CONFIRM_TIMEOUT_SEC
            ]
            for rid in timed_out_ids:
                del self._pending_reactions[rid]

        for rid in timed_out_ids:
            logger.warning(
                "[%s] Trade not confirmed within %.0fs — cancelling reaction order %s",
                self.market.slug, CONFIRM_TIMEOUT_SEC, rid[:12],
            )
            self.order_placer.cancel_order(rid)
            self.order_manager.remove_order(rid)

        # ── Cancel initial orders near market end ──
        remaining = self.market.end_ts - now
        if remaining < CANCEL_BEFORE_END_SEC:
            for label, order_id in [
                ("UP", self._up_initial_order_id),
                ("DOWN", self._down_initial_order_id),
            ]:
                if order_id is None or not self.order_manager.has_order(order_id):
                    continue
                logger.info(
                    "[%s] <1 min left (%.0fs) — cancelling unfilled %s initial order %s",
                    self.market.slug, remaining, label, order_id[:12],
                )
                self.order_placer.cancel_order(order_id)
                self.order_manager.remove_order(order_id)

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
        Handle a fill event (MATCHED, MINED, CONFIRMED, RETRYING, or FAILED).

        On MATCHED: react immediately (place 96c opposite order), but track
        the reaction so we can cancel it if the trade never confirms.
        On MINED/CONFIRMED: mark the pending reaction as confirmed, or re-place
        it if it was previously cancelled due to RETRYING.
        On RETRYING: cancel the reaction order but keep tracking for re-placement.
        On FAILED: cancel the reaction order we placed.
        """
        logger.info(
            "[%s] Fill: token=%s side=%s price=%.4f size=%.2f order=%s status=%s trade=%s",
            self.market.slug, token_id[:12], side, price, size,
            order_id[:12] if order_id else "?",
            status,
            trade_id[:12] if trade_id else "?",
        )

        # ── Handle non-MATCHED statuses (confirmation / failure updates) ──
        if status != "MATCHED":
            # Remove confirmed orders from open-orders tracking, but preserve
            # the order_type for initial orders — MATCHED partials can arrive
            # after CONFIRMED, and we need the type to decide whether to react.
            if status in _CONFIRMED_STATUSES and order_id:
                order_type = self.order_manager.get_order_type(order_id)
                self.order_manager.remove_order(order_id)
                if order_type == _OT_INITIAL:
                    self.order_manager.preserve_order_type(order_id, order_type)
            self._handle_trade_status_update(trade_id, status)
            return

        # ── MATCHED path — react for speed ──
        if side != "BUY":
            return

        # Update position tracker for every buy fill (initial and reaction)
        self.position_tracker.update_on_buy_fill(
            token_id=token_id,
            fill_size=size,
            fill_price=price,
            market_slug=self.market.slug,
        )

        # Only place reaction orders for initial order fills.
        # NOTE: Do NOT remove_order here — partial fills arrive as multiple
        # MATCHED events for the same order_id, and removing on the first
        # partial would cause subsequent partials to lose their order_type,
        # falling through to the reaction logic incorrectly.
        if order_id:
            order_type = self.order_manager.get_order_type(order_id)
            if order_type != _OT_INITIAL:
                logger.info(
                    "[%s] %s fill — position updated, no further action. order=%s",
                    self.market.slug,
                    order_type or "unknown",
                    order_id[:12],
                )
                return

        # Determine the opposite token
        if token_id == self.market.up_token_id:
            opposite_token = self.market.down_token_id
            opposite_label = "DOWN"
        elif token_id == self.market.down_token_id:
            opposite_token = self.market.up_token_id
            opposite_label = "UP"
        else:
            logger.warning(
                "[%s] Fill for unknown token %s — ignoring",
                self.market.slug, token_id[:12],
            )
            return

        # Accumulate fill size — exchange requires minimum order of 5 shares.
        # Lock prevents concurrent partial fills from each firing a reaction.
        trade_ids_for_reaction: set[str] = set()
        with self._fill_lock:
            accumulated = self._pending_fill_size.get(token_id, 0.0) + size
            if accumulated < MIN_ORDER_SIZE:
                self._pending_fill_size[token_id] = accumulated
                logger.info(
                    "[%s] Accumulated %.2f shares on %s (need %.0f) — waiting for more fills",
                    self.market.slug, accumulated, token_id[:12], MIN_ORDER_SIZE,
                )
                # Still need to track this trade_id for confirmation even though
                # we haven't placed the reaction yet. We'll associate it when we do.
                return

            # Claim the accumulated size and reset before releasing the lock
            reaction_size = accumulated
            self._pending_fill_size[token_id] = 0.0
            if trade_id:
                trade_ids_for_reaction.add(trade_id)

        # Place reaction order outside the lock (network I/O)
        logger.info(
            "[%s] Placing reaction BUY: %s token=%s price=%.2f size=%.2f",
            self.market.slug, opposite_label, opposite_token[:12],
            REACTION_BUY_PRICE, reaction_size,
        )
        reaction_order_id = self._place_and_track(
            token_id=opposite_token,
            side_label=opposite_label,
            price=REACTION_BUY_PRICE,
            size=reaction_size,
            order_type=_OT_FILL_REACTION,
        )

        # Track pending confirmation
        if reaction_order_id and trade_ids_for_reaction:
            with self._pending_reactions_lock:
                self._pending_reactions[reaction_order_id] = _PendingReaction(
                    reaction_order_id=reaction_order_id,
                    matched_at=time.time(),
                    opposite_token=opposite_token,
                    opposite_label=opposite_label,
                    reaction_size=reaction_size,
                    trade_ids=trade_ids_for_reaction,
                )

    # ── confirmation helpers ──────────────────────────────────

    def _handle_trade_status_update(self, trade_id: str, status: str) -> None:
        """Handle MINED/CONFIRMED/RETRYING/FAILED for a trade we previously reacted to."""
        if not trade_id:
            return

        with self._pending_reactions_lock:
            # Find the pending reaction(s) associated with this trade_id
            matching = [
                (rid, pr) for rid, pr in self._pending_reactions.items()
                if trade_id in pr.trade_ids
            ]

        if not matching:
            return

        if status == "CONFIRMED":
            for rid, pr in matching:
                if pr.cancelled_for_retry:
                    # Trade confirmed after a RETRYING — re-place the reaction order
                    logger.info(
                        "[%s] Trade %s %s after RETRYING — re-placing reaction BUY: "
                        "%s token=%s price=%.2f size=%.2f",
                        self.market.slug, trade_id[:12], status,
                        pr.opposite_label, pr.opposite_token[:12],
                        REACTION_BUY_PRICE, pr.reaction_size,
                    )
                    with self._pending_reactions_lock:
                        self._pending_reactions.pop(rid, None)
                    new_order_id = self._place_and_track(
                        token_id=pr.opposite_token,
                        side_label=pr.opposite_label,
                        price=REACTION_BUY_PRICE,
                        size=pr.reaction_size,
                        order_type=_OT_FILL_REACTION,
                    )
                    if new_order_id:
                        logger.info(
                            "[%s] Re-placed reaction order %s (was %s)",
                            self.market.slug, new_order_id[:12], rid[:12],
                        )
                else:
                    logger.info(
                        "[%s] Trade %s %s — reaction order %s confirmed",
                        self.market.slug, trade_id[:12], status, rid[:12],
                    )
                    with self._pending_reactions_lock:
                        pr.confirmed = True
                        self._pending_reactions.pop(rid, None)

        elif status == "RETRYING":
            for rid, pr in matching:
                logger.warning(
                    "[%s] Trade %s RETRYING — cancelling reaction order %s (will re-place if confirmed)",
                    self.market.slug, trade_id[:12], rid[:12],
                )
                self.order_placer.cancel_order(rid)
                self.order_manager.remove_order(rid)
                with self._pending_reactions_lock:
                    pr.cancelled_for_retry = True

        elif status == "FAILED":
            logger.info(
                "[%s] Trade %s FAILED — cancelling reaction orders",
                self.market.slug, trade_id[:12],
            )
            for rid, pr in matching:
                logger.warning(
                    "[%s] Trade %s FAILED — cancelling reaction order %s",
                    self.market.slug, trade_id[:12], rid[:12],
                )
                with self._pending_reactions_lock:
                    self._pending_reactions.pop(rid, None)
                if not pr.cancelled_for_retry:
                    self.order_placer.cancel_order(rid)
                    self.order_manager.remove_order(rid)
