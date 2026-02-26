"""Simple market-making strategy for 15m up/down markets."""
from __future__ import annotations

import json
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
MIN_ORDER_SIZE = 5.0        # exchange minimum order size


@dataclass(frozen=True)
class BuyPair:
    """One (initial_price, reaction_price, size) configuration tuple."""
    index: int
    initial_price: float
    reaction_price: float
    size: float


def _load_buy_pairs() -> list[BuyPair]:
    """Load buy pairs from POLY_BUY_PAIRS (JSON) or legacy scalar env vars."""
    raw = os.environ.get("POLY_BUY_PAIRS")
    if raw:
        entries = json.loads(raw)
        return [
            BuyPair(
                index=i,
                initial_price=float(e["initial"]),
                reaction_price=float(e["reaction"]),
                size=float(e["size"]),
            )
            for i, e in enumerate(entries)
        ]
    return [
        BuyPair(
            index=0,
            initial_price=float(os.environ.get("POLY_INITIAL_BUY_PRICE", "0.03")),
            reaction_price=float(os.environ.get("POLY_REACTION_BUY_PRICE", "0.96")),
            size=float(os.environ.get("POLY_ORDER_SIZE", "100.0")),
        )
    ]


BUY_PAIRS: list[BuyPair] = _load_buy_pairs()
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
    reaction_price: float      # price for the reaction order (from BuyPair)
    trade_ids: set[str] = field(default_factory=set)
    confirmed: bool = False
    cancelled_for_retry: bool = False  # True if cancelled due to RETRYING


class SimpleMarketMaker(StrategyEngine):
    """
    Two-sided market maker for a 15m up/down binary market.

    Supports multiple independent buy pairs, each with its own initial price,
    reaction price, and order size (configured via POLY_BUY_PAIRS env var).

    Strategy (per pair):
    - On start, place an initial BUY order on both the Up and Down tokens.
    - On MATCHED fill of an initial order, immediately place a reaction BUY
      at that pair's reaction price on the opposite outcome token.
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
        self._buy_pairs = BUY_PAIRS

        # Maps order_id -> BuyPair for every live initial order
        self._initial_order_to_pair: dict[str, BuyPair] = {}
        # Maps (pair_index, token_id) -> order_id for end-of-market cancellation
        self._initial_orders: dict[tuple[int, str], str] = {}

        # Accumulate partial fills per (pair, token) before placing reaction orders.
        self._pending_fill_size: dict[tuple[int, str], float] = {}
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

    def _place_initial_order(self, token_id: str, side_label: str, pair: BuyPair) -> str | None:
        """Place an initial buy order for the given pair."""
        logger.info(
            "[%s] Placing initial BUY: %s pair=%d token=%s price=%.2f size=%.0f",
            self.market.slug, side_label, pair.index, token_id[:12],
            pair.initial_price, pair.size,
        )
        order_id = self._place_and_track(
            token_id=token_id,
            side_label=side_label,
            price=pair.initial_price,
            size=pair.size,
            order_type=_OT_INITIAL,
        )
        if order_id:
            self._initial_order_to_pair[order_id] = pair
            self._initial_orders[(pair.index, token_id)] = order_id
        return order_id

    # ── lifecycle hooks ────────────────────────────────────────

    def on_start(self) -> None:
        logger.info(
            "[%s] SimpleMarketMaker started: %d buy pair(s)",
            self.market.slug, len(self._buy_pairs),
        )
        for pair in self._buy_pairs:
            logger.info(
                "[%s]   pair %d: initial=%.2f reaction=%.2f size=%.0f",
                self.market.slug, pair.index,
                pair.initial_price, pair.reaction_price, pair.size,
            )
            self._place_initial_order(self.market.up_token_id, "UP", pair)
            self._place_initial_order(self.market.down_token_id, "DOWN", pair)

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
            for (pair_idx, token_id), order_id in list(self._initial_orders.items()):
                if not self.order_manager.has_order(order_id):
                    continue
                label = "UP" if token_id == self.market.up_token_id else "DOWN"
                logger.info(
                    "[%s] <1 min left (%.0fs) — cancelling unfilled %s pair=%d initial order %s",
                    self.market.slug, remaining, label, pair_idx, order_id[:12],
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

        # Look up which buy pair this initial order belongs to
        pair = self._initial_order_to_pair.get(order_id) if order_id else None
        if pair is None:
            logger.warning(
                "[%s] Initial order %s has no associated pair — ignoring",
                self.market.slug, (order_id or "?")[:12],
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
        fill_key = (pair.index, token_id)
        trade_ids_for_reaction: set[str] = set()
        with self._fill_lock:
            accumulated = self._pending_fill_size.get(fill_key, 0.0) + size
            if accumulated < MIN_ORDER_SIZE:
                self._pending_fill_size[fill_key] = accumulated
                logger.info(
                    "[%s] Accumulated %.2f shares on %s pair=%d (need %.0f) — waiting for more fills",
                    self.market.slug, accumulated, token_id[:12], pair.index, MIN_ORDER_SIZE,
                )
                # Still need to track this trade_id for confirmation even though
                # we haven't placed the reaction yet. We'll associate it when we do.
                return

            # Claim the accumulated size and reset before releasing the lock
            reaction_size = accumulated
            self._pending_fill_size[fill_key] = 0.0
            if trade_id:
                trade_ids_for_reaction.add(trade_id)

        # Place reaction order outside the lock (network I/O)
        logger.info(
            "[%s] Placing reaction BUY: %s pair=%d token=%s price=%.2f size=%.2f",
            self.market.slug, opposite_label, pair.index, opposite_token[:12],
            pair.reaction_price, reaction_size,
        )
        reaction_order_id = self._place_and_track(
            token_id=opposite_token,
            side_label=opposite_label,
            price=pair.reaction_price,
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
                    reaction_price=pair.reaction_price,
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
                        pr.reaction_price, pr.reaction_size,
                    )
                    with self._pending_reactions_lock:
                        self._pending_reactions.pop(rid, None)
                    new_order_id = self._place_and_track(
                        token_id=pr.opposite_token,
                        side_label=pr.opposite_label,
                        price=pr.reaction_price,
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
