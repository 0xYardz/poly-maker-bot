"""Volatile market-making strategy for 5m up/down binary markets."""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from poly_maker_bot.strategy.base import StrategyEngine
from poly_maker_bot.utils import round_to_tick

if TYPE_CHECKING:
    from poly_maker_bot.market.discovery import Market
    from poly_maker_bot.exchange.polymarket_client import PolymarketClient
    from poly_maker_bot.orders.manager import OrderManager
    from poly_maker_bot.orders.placer import OrderPlacer
    from poly_maker_bot.position.tracker import PositionTracker
    from poly_maker_bot.market_data.orderbook_store import OrderBookStore, TopOfBook

logger = logging.getLogger(__name__)

# ── Order type tags ──────────────────────────────────────────
_OT_INITIAL = "initial_buy"
_OT_FILL_REACTION = "fill_reaction_buy"

# ── Strategy constants ───────────────────────────────────────
MAX_ORDER_SIZE = 10.0           # max shares per single order
MIN_ORDER_SIZE = 5.0            # exchange minimum
MAX_IMBALANCE = 14.0            # max net imbalance (this_side - opposite_side)
MAX_BOUGHT_PER_SIDE = 60.0     # max total shares bought per token across the session

NUM_LAYERS = 2                 # number of BUY orders per token
LAYER_SPACING = 0.02            # 1c between layers (before volatility adjustment)
TICK_SIZE = 0.01                # price tick size

CANCEL_BEFORE_END_SEC = 15      # cancel unfilled orders with <15s left
CONFIRM_TIMEOUT_SEC = 30.0      # cancel reaction if trade not confirmed

STALE_PRICE_THRESHOLD = 0.005   # cancel/replace if target price drifted >0.5c

# Volatility measurement windows (seconds ago)
VOL_WINDOW_SHORT = 5.0
VOL_WINDOW_MED = 15.0
VOL_WINDOW_LONG = 30.0

# Volatility regime thresholds (max price swing across windows)
VOL_HIGH_THRESHOLD = 0.05       # 5c swing = "high vol"
VOL_LOW_THRESHOLD = 0.02        # <2c swing = "low vol"

# Inventory skew: per share of inventory, shift quote by this amount
SKEW_SENSITIVITY = 0.005        # 0.5c per share

# Orderbook imbalance: max skew from imbalance signal
IMBALANCE_SKEW_MAX = 0.02       # max 2c skew

# ── Side selection constants ────────────────────────────────
SIDE_STICKINESS_TICKS = 6          # min ticks on a side before switching (3s at 0.5s tick)
SIDE_SCORE_PRICE_WEIGHT = 0.40     # prefer side with better risk/reward
SIDE_SCORE_INVENTORY_WEIGHT = 0.35 # prefer the lighter side
SIDE_SCORE_IMBALANCE_WEIGHT = 0.15 # prefer side with bullish book
SIDE_SCORE_BOUGHT_WEIGHT = 0.10    # prefer side with more remaining capacity
SIDE_SWITCH_THRESHOLD = 0.15       # score bonus for current side (hysteresis)
PRICE_AVOID_THRESHOLD = 0.10       # avoid quoting a side priced below 10c (likely loser)

# Statuses that confirm a trade is on-chain
_CONFIRMED_STATUSES = frozenset({"MINED", "CONFIRMED"})


@dataclass
class _PendingReaction:
    """Tracks a reaction order waiting for its triggering trade to confirm."""
    reaction_order_id: str
    matched_at: float           # time.time() when MATCHED arrived
    opposite_token: str         # token_id for the reaction order
    opposite_label: str         # "UP" or "DOWN"
    reaction_size: float
    reaction_price: float       # the price used for the reaction order
    trade_ids: set[str] = field(default_factory=set)
    confirmed: bool = False
    cancelled_for_retry: bool = False


@dataclass
class _LayeredOrder:
    """Tracks one layered initial order."""
    order_id: str
    token_id: str
    label: str          # "UP" or "DOWN"
    price: float
    size: float
    layer_index: int    # 0 = closest to best bid, 1 = next, etc.


class VolatileMarketMaker(StrategyEngine):
    """
    Volatile market maker for 5m up/down binary markets.

    Strategy:
    - Dynamically select ONE side (UP or DOWN) to quote each tick based on
      price, inventory, orderbook imbalance, and remaining capacity signals.
    - Place layered BUY orders on the chosen side around the current best bid
      (join best bid, then 1c below, 2c below).
    - On fill at price P, react by buying the opposite token at 1.00 - P - tick_size,
      locking in 1 tick of profit per share.
    - Adapts layer spacing and sizes to orderbook volatility.
    - Skews quotes based on current inventory to avoid one-sided accumulation.
    - Includes hysteresis to prevent rapid side-flipping.
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
        # Layered order tracking: {order_id: _LayeredOrder}
        self._layered_orders: dict[str, _LayeredOrder] = {}
        self._layered_orders_lock = threading.Lock()

        # Fill accumulation (same pattern as simple_mm)
        self._pending_fill_size: dict[str, float] = {}
        # Track the weighted average fill price for reaction price calculation
        self._pending_fill_cost: dict[str, float] = {}
        self._fill_lock = threading.Lock()

        # Pending reaction confirmations (same pattern as simple_mm)
        self._pending_reactions: dict[str, _PendingReaction] = {}
        self._pending_reactions_lock = threading.Lock()

        # Cumulative shares bought per token (never resets within a market window)
        self._total_bought: dict[str, float] = {}
        self._total_bought_lock = threading.Lock()

        # ── Side selection state ─────────────────────────────────
        self._active_side: str | None = None      # "UP" or "DOWN" or None
        self._side_locked_until_tick: int = 0      # tick count when lock expires
        self._tick_count: int = 0

    # ── order helpers ────────────────────────────────────────────

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

    # ── lifecycle hooks ──────────────────────────────────────────

    def on_start(self) -> None:
        logger.info(
            "[%s] VolatileMarketMaker started: max_size=%d max_imbalance=%d max_bought=%d layers=%d",
            self.market.slug, MAX_ORDER_SIZE, int(MAX_IMBALANCE),
            int(MAX_BOUGHT_PER_SIDE), NUM_LAYERS,
        )
        self._refresh_layered_orders()

    def on_stop(self) -> None:
        logger.info("[%s] VolatileMarketMaker stopped", self.market.slug)

    def tick_interval_sec(self) -> float:
        return 0.5

    # ── core strategy methods ────────────────────────────────────

    def on_tick(self) -> None:
        """
        Periodic housekeeping + adaptive order management:
        1. Check for timed-out pending reactions.
        2. Cancel all orders near market end.
        3. Refresh layered orders at updated prices.
        """
        now = time.time()
        self._tick_count += 1

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

        # ── Cancel all orders near market end ──
        remaining = self.market.end_ts - now
        if remaining < CANCEL_BEFORE_END_SEC:
            self._cancel_all_layered_orders()
            return

        # ── Refresh layered orders ──
        self._refresh_layered_orders()

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
        Handle fill events — same MATCHED/MINED/CONFIRMED/RETRYING/FAILED flow.

        On MATCHED: react by buying opposite token at 1.00 - fill_price - tick_size.
        On MINED/CONFIRMED: mark reaction as confirmed or re-place if retried.
        On RETRYING: cancel reaction, keep tracking for re-placement.
        On FAILED: cancel reaction, clean up.
        """
        logger.info(
            "[%s] Fill: token=%s side=%s price=%.4f size=%.2f order=%s status=%s trade=%s",
            self.market.slug, token_id[:12], side, price, size,
            order_id[:12] if order_id else "?",
            status,
            trade_id[:12] if trade_id else "?",
        )

        # ── Handle non-MATCHED statuses ──
        if status != "MATCHED":
            if status in _CONFIRMED_STATUSES and order_id:
                self.order_manager.remove_order(order_id)
            self._handle_trade_status_update(trade_id, status)
            return

        # ── MATCHED path ──
        if side != "BUY":
            return

        # Update position tracker
        self.position_tracker.update_on_buy_fill(
            token_id=token_id,
            fill_size=size,
            fill_price=price,
            market_slug=self.market.slug,
        )

        # Update layered order tracking (may be partial fill)
        with self._layered_orders_lock:
            if order_id in self._layered_orders:
                lo = self._layered_orders[order_id]
                lo.size -= size
                if lo.size <= 0:
                    del self._layered_orders[order_id]

        # Only place reaction orders for initial order fills
        if order_id:
            order_type = self.order_manager.get_order_type(order_id)
            if order_type and order_type != _OT_INITIAL:
                logger.info(
                    "[%s] Reaction fill — position updated, no further action. order=%s",
                    self.market.slug, order_id[:12],
                )
                return

        # Track cumulative initial shares bought (reactions don't count)
        with self._total_bought_lock:
            new_total = self._total_bought.get(token_id, 0.0) + size
            self._total_bought[token_id] = new_total
            if new_total >= MAX_BOUGHT_PER_SIDE:
                logger.info(
                    "[%s] Reached MAX_BOUGHT_PER_SIDE (%.0f) on %s — stopping initial quotes",
                    self.market.slug, MAX_BOUGHT_PER_SIDE, token_id[:12],
                )

        # Determine opposite token
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

        # Accumulate partial fills before placing reaction
        trade_ids_for_reaction: set[str] = set()
        with self._fill_lock:
            accumulated_size = self._pending_fill_size.get(token_id, 0.0) + size
            accumulated_cost = self._pending_fill_cost.get(token_id, 0.0) + (price * size)

            if accumulated_size < MIN_ORDER_SIZE:
                self._pending_fill_size[token_id] = accumulated_size
                self._pending_fill_cost[token_id] = accumulated_cost
                logger.info(
                    "[%s] Accumulated %.2f shares on %s (need %.0f) — waiting for more fills",
                    self.market.slug, accumulated_size, token_id[:12], MIN_ORDER_SIZE,
                )
                return

            # Claim accumulated fills
            reaction_size = min(accumulated_size, MAX_ORDER_SIZE)
            avg_fill_price = accumulated_cost / accumulated_size
            self._pending_fill_size[token_id] = 0.0
            self._pending_fill_cost[token_id] = 0.0
            if trade_id:
                trade_ids_for_reaction.add(trade_id)

        # Compute reaction price: 1.00 - avg_fill_price - tick_size
        reaction_price = round_to_tick(1.00 - avg_fill_price - 0.01, TICK_SIZE)
        reaction_price = max(TICK_SIZE, reaction_price)

        # Cap at best_ask - 0.01 so we don't cross the spread
        opp_store = self.get_orderbook(opposite_token)
        if opp_store:
            opp_top = opp_store.top()
            if opp_top.best_ask is not None:
                ask_cap = round_to_tick(opp_top.best_ask - 0.01, TICK_SIZE)
                if reaction_price >= ask_cap > 0:
                    logger.info(
                        "[%s] Capping reaction price %.2f -> %.2f (best_ask=%.2f)",
                        self.market.slug, reaction_price, ask_cap, opp_top.best_ask,
                    )
                    reaction_price = ask_cap - 0.01

        logger.info(
            "[%s] Placing reaction BUY: %s token=%s price=%.2f size=%.2f (fill_avg=%.4f)",
            self.market.slug, opposite_label, opposite_token[:12],
            reaction_price, reaction_size, avg_fill_price,
        )
        reaction_order_id = self._place_and_track(
            token_id=opposite_token,
            side_label=opposite_label,
            price=reaction_price,
            size=reaction_size,
            order_type=_OT_FILL_REACTION,
        )

        if reaction_order_id and trade_ids_for_reaction:
            with self._pending_reactions_lock:
                self._pending_reactions[reaction_order_id] = _PendingReaction(
                    reaction_order_id=reaction_order_id,
                    matched_at=time.time(),
                    opposite_token=opposite_token,
                    opposite_label=opposite_label,
                    reaction_size=reaction_size,
                    reaction_price=reaction_price,
                    trade_ids=trade_ids_for_reaction,
                )

    # ── confirmation helpers ─────────────────────────────────────

    def _handle_trade_status_update(self, trade_id: str, status: str) -> None:
        """Handle MINED/CONFIRMED/RETRYING/FAILED for a trade we previously reacted to."""
        if not trade_id:
            return

        with self._pending_reactions_lock:
            matching = [
                (rid, pr) for rid, pr in self._pending_reactions.items()
                if trade_id in pr.trade_ids
            ]

        if not matching:
            return

        if status in _CONFIRMED_STATUSES:
            for rid, pr in matching:
                if pr.cancelled_for_retry:
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

    # ── quoting engine ───────────────────────────────────────────

    def _compute_quotes(
        self,
    ) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
        """
        Compute target (price, size) layers for the dynamically chosen side.

        ONE-SIDED QUOTING: only the chosen side gets layers; the other returns
        empty (causing _refresh_layered_orders to cancel stale orders there).

        Returns:
            (up_layers, down_layers) — each a list of (price, size) tuples.
            Exactly one will be non-empty (or both empty if no data / limit hit).
        """
        up_store = self.get_orderbook(self.market.up_token_id)
        down_store = self.get_orderbook(self.market.down_token_id)
        if not up_store or not down_store:
            return [], []

        up_top = up_store.top()
        down_top = down_store.top()

        # Need best bid to anchor our quotes
        up_best_bid = up_top.best_bid
        down_best_bid = down_top.best_bid

        # Measure volatility
        volatility = self._measure_volatility(up_store, down_store)

        # Volatility regime → spread multiplier + size multiplier
        if volatility >= VOL_HIGH_THRESHOLD:
            spread_mult = 1.5
            size_mult = 0.5
        elif volatility <= VOL_LOW_THRESHOLD:
            spread_mult = 0.8
            size_mult = 1.0
        else:
            ratio = (volatility - VOL_LOW_THRESHOLD) / (VOL_HIGH_THRESHOLD - VOL_LOW_THRESHOLD)
            spread_mult = 0.8 + 0.7 * ratio
            size_mult = 1.0 - 0.5 * ratio

        effective_spacing = LAYER_SPACING * spread_mult
        base_size = max(MIN_ORDER_SIZE, round(MAX_ORDER_SIZE * size_mult))

        # Compute orderbook imbalance
        up_imbalance = self._compute_imbalance(up_store)
        down_imbalance = self._compute_imbalance(down_store)

        # Current inventory
        up_pos = self.position_tracker.get_position(self.market.up_token_id)
        down_pos = self.position_tracker.get_position(self.market.down_token_id)
        up_inventory = up_pos["size"] if up_pos else 0.0
        down_inventory = down_pos["size"] if down_pos else 0.0

        # Inventory skew based on net imbalance (pushes bids lower on the side we're overweight)
        up_inv_skew = -(up_inventory - down_inventory) * SKEW_SENSITIVITY
        down_inv_skew = -(down_inventory - up_inventory) * SKEW_SENSITIVITY

        # Imbalance skew (positive imbalance = bullish → raise bid slightly)
        up_imb_skew = min(max(up_imbalance * 0.01, -IMBALANCE_SKEW_MAX), IMBALANCE_SKEW_MAX)
        down_imb_skew = min(max(down_imbalance * 0.01, -IMBALANCE_SKEW_MAX), IMBALANCE_SKEW_MAX)

        # Cumulative shares bought
        with self._total_bought_lock:
            up_total_bought = self._total_bought.get(self.market.up_token_id, 0.0)
            down_total_bought = self._total_bought.get(self.market.down_token_id, 0.0)

        # ── Select which side to quote ──
        chosen_side = self._select_quoting_side(
            up_best_bid=up_best_bid,
            down_best_bid=down_best_bid,
            up_inventory=up_inventory,
            down_inventory=down_inventory,
            up_imbalance=up_imbalance,
            down_imbalance=down_imbalance,
            up_total_bought=up_total_bought,
            down_total_bought=down_total_bought,
        )

        # Build layers only for the chosen side
        up_layers: list[tuple[float, float]] = []
        down_layers: list[tuple[float, float]] = []

        if chosen_side == "UP":
            up_layers = self._build_layers(
                best_bid=up_best_bid,
                effective_spacing=effective_spacing,
                base_size=base_size,
                inventory=up_inventory,
                opposite_inventory=down_inventory,
                total_bought=up_total_bought,
                inv_skew=up_inv_skew,
                imb_skew=up_imb_skew,
            )
        else:
            down_layers = self._build_layers(
                best_bid=down_best_bid,
                effective_spacing=effective_spacing,
                base_size=base_size,
                inventory=down_inventory,
                opposite_inventory=up_inventory,
                total_bought=down_total_bought,
                inv_skew=down_inv_skew,
                imb_skew=down_imb_skew,
            )

        return up_layers, down_layers

    def _build_layers(
        self,
        best_bid: float | None,
        effective_spacing: float,
        base_size: float,
        inventory: float,
        opposite_inventory: float,
        total_bought: float,
        inv_skew: float,
        imb_skew: float,
    ) -> list[tuple[float, float]]:
        """Build layered orders for one token."""
        if best_bid is None:
            return []
        # Net imbalance: how much more we hold of this token vs the other
        net_imbalance = inventory - opposite_inventory
        if net_imbalance >= MAX_IMBALANCE:
            return []
        if total_bought >= MAX_BOUGHT_PER_SIDE:
            return []

        remaining_imbalance_cap = MAX_IMBALANCE - net_imbalance
        remaining_bought_cap = MAX_BOUGHT_PER_SIDE - total_bought
        remaining_capacity = min(remaining_imbalance_cap, remaining_bought_cap)
        layers = []

        for i in range(NUM_LAYERS):
            layer_price = best_bid - (i * effective_spacing) + inv_skew + imb_skew
            layer_price = round_to_tick(layer_price, TICK_SIZE)
            layer_price = max(TICK_SIZE, layer_price)  # floor at 1 tick

            layer_size = min(base_size, remaining_capacity)
            if layer_size < MIN_ORDER_SIZE:
                break

            layers.append((layer_price, layer_size))
            remaining_capacity -= layer_size
            if remaining_capacity < MIN_ORDER_SIZE:
                break

        return layers

    def _refresh_layered_orders(self) -> None:
        """
        Compare current layered orders to computed target quotes.
        Cancel stale orders, place new ones at updated prices.
        """
        up_layers, down_layers = self._compute_quotes()

        # Build desired state: {(token_id, layer_index): (price, size)}
        desired: dict[tuple[str, int], tuple[float, float]] = {}
        for i, (price, size) in enumerate(up_layers):
            desired[(self.market.up_token_id, i)] = (price, size)
        for i, (price, size) in enumerate(down_layers):
            desired[(self.market.down_token_id, i)] = (price, size)

        # Map existing orders by key
        with self._layered_orders_lock:
            existing_keys: dict[tuple[str, int], _LayeredOrder] = {}
            for lo in self._layered_orders.values():
                key = (lo.token_id, lo.layer_index)
                existing_keys[key] = lo

        # Determine which to cancel and which to keep
        to_cancel: list[_LayeredOrder] = []
        to_keep_keys: set[tuple[str, int]] = set()

        for key, lo in existing_keys.items():
            if not self.order_manager.has_order(lo.order_id):
                # Order was filled or already cancelled externally
                with self._layered_orders_lock:
                    self._layered_orders.pop(lo.order_id, None)
                continue

            if key not in desired:
                to_cancel.append(lo)
            else:
                target_price, _target_size = desired[key]
                if abs(lo.price - target_price) > STALE_PRICE_THRESHOLD:
                    to_cancel.append(lo)
                else:
                    to_keep_keys.add(key)

        # Execute cancellations
        for lo in to_cancel:
            logger.debug(
                "[%s] Cancelling stale layered order: %s token=%s layer=%d price=%.2f",
                self.market.slug, lo.label, lo.token_id[:12], lo.layer_index, lo.price,
            )
            self.order_placer.cancel_order(lo.order_id)
            self.order_manager.remove_order(lo.order_id)
            with self._layered_orders_lock:
                self._layered_orders.pop(lo.order_id, None)

        # Place new orders for layers that need them
        for key, (price, size) in desired.items():
            if key in to_keep_keys:
                continue

            token_id, layer_index = key
            label = "UP" if token_id == self.market.up_token_id else "DOWN"

            logger.info(
                "[%s] Placing layered BUY: %s token=%s layer=%d price=%.2f size=%.0f",
                self.market.slug, label, token_id[:12], layer_index, price, size,
            )
            order_id = self._place_and_track(
                token_id=token_id,
                side_label=label,
                price=price,
                size=size,
                order_type=_OT_INITIAL,
            )
            if order_id:
                with self._layered_orders_lock:
                    self._layered_orders[order_id] = _LayeredOrder(
                        order_id=order_id,
                        token_id=token_id,
                        label=label,
                        price=price,
                        size=size,
                        layer_index=layer_index,
                    )

    def _cancel_all_layered_orders(self) -> None:
        """Cancel all resting layered orders (used near market end)."""
        with self._layered_orders_lock:
            orders_to_cancel = list(self._layered_orders.values())
            self._layered_orders.clear()

        for lo in orders_to_cancel:
            if self.order_manager.has_order(lo.order_id):
                logger.info(
                    "[%s] <1 min left — cancelling layered order %s %s",
                    self.market.slug, lo.label, lo.order_id[:12],
                )
                self.order_placer.cancel_order(lo.order_id)
                self.order_manager.remove_order(lo.order_id)

    # ── market data helpers ──────────────────────────────────────

    @staticmethod
    def _safe_mid(top: "TopOfBook") -> float:
        """Compute midpoint, falling back to 0.50 if no data."""
        if top.best_bid is not None and top.best_ask is not None:
            return (top.best_bid + top.best_ask) / 2.0
        if top.best_bid is not None:
            return top.best_bid
        if top.best_ask is not None:
            return top.best_ask
        return 0.50

    def _measure_volatility(
        self,
        up_store: "OrderBookStore",
        down_store: "OrderBookStore",
    ) -> float:
        """
        Measure recent price volatility from orderbook history.

        Looks at mid-price at 5s, 15s, and 30s ago.
        Returns the maximum absolute price swing observed.
        """
        max_swing = 0.0
        for store in [up_store, down_store]:
            current_mid = self._safe_mid(store.top())
            for window in [VOL_WINDOW_SHORT, VOL_WINDOW_MED, VOL_WINDOW_LONG]:
                historical_top = store.get_top_at_time(window)
                if historical_top is None:
                    continue
                historical_mid = self._safe_mid(historical_top)
                swing = abs(current_mid - historical_mid)
                max_swing = max(max_swing, swing)
        return max_swing

    @staticmethod
    def _compute_imbalance(store: "OrderBookStore") -> float:
        """
        Compute bid/ask volume imbalance.

        Returns:
            Positive = more bid volume (bullish), negative = more ask volume.
            Range: -1.0 to +1.0.
        """
        bids = store.all_bid_levels()
        asks = store.all_ask_levels()

        total_bid_vol = sum(size for _, size in bids[:5])
        total_ask_vol = sum(size for _, size in asks[:5])

        total = total_bid_vol + total_ask_vol
        if total == 0:
            return 0.0
        return (total_bid_vol - total_ask_vol) / total

    # ── side selection ────────────────────────────────────────────

    def _select_quoting_side(
        self,
        up_best_bid: float | None,
        down_best_bid: float | None,
        up_inventory: float,
        down_inventory: float,
        up_imbalance: float,
        down_imbalance: float,
        up_total_bought: float,
        down_total_bought: float,
    ) -> str:
        """
        Choose which side ("UP" or "DOWN") to quote this tick.

        Uses a weighted composite score across price, inventory, imbalance,
        and remaining capacity.  Includes hysteresis (score bonus for current
        side) and tick-based stickiness to prevent rapid flip-flopping.
        """
        up_score = 0.0
        down_score = 0.0

        # ── Signal 1: Price (risk/reward) ──
        # Prefer the cheaper side for better reaction margins, BUT hard-avoid
        # any side priced below PRICE_AVOID_THRESHOLD — it's likely the loser
        # and accumulating it creates unhedged exposure.
        up_bid = up_best_bid if up_best_bid is not None else 0.50
        down_bid = down_best_bid if down_best_bid is not None else 0.50
        up_is_loser = up_bid < PRICE_AVOID_THRESHOLD
        down_is_loser = down_bid < PRICE_AVOID_THRESHOLD
        if up_is_loser and not down_is_loser:
            up_price_score = 0.0
            down_price_score = 1.0
        elif down_is_loser and not up_is_loser:
            up_price_score = 1.0
            down_price_score = 0.0
        elif up_is_loser and down_is_loser:
            # Both very cheap (shouldn't happen in a binary), neutral
            up_price_score = 0.5
            down_price_score = 0.5
        else:
            # Both in competitive range — prefer cheaper side for reaction margin
            bid_sum = up_bid + down_bid
            if bid_sum > 0:
                up_price_score = 1.0 - up_bid / bid_sum
                down_price_score = 1.0 - down_bid / bid_sum
            else:
                up_price_score = 0.5
                down_price_score = 0.5
        up_score += SIDE_SCORE_PRICE_WEIGHT * up_price_score
        down_score += SIDE_SCORE_PRICE_WEIGHT * down_price_score

        # ── Signal 2: Inventory (prefer lighter side) ──
        inv_total = up_inventory + down_inventory
        if inv_total > 0:
            up_inv_score = 1.0 - (up_inventory / inv_total)
            down_inv_score = 1.0 - (down_inventory / inv_total)
        else:
            up_inv_score = 0.5
            down_inv_score = 0.5
        up_score += SIDE_SCORE_INVENTORY_WEIGHT * up_inv_score
        down_score += SIDE_SCORE_INVENTORY_WEIGHT * down_inv_score

        # ── Signal 3: Orderbook imbalance (prefer bullish side) ──
        up_imb_score = (up_imbalance + 1.0) / 2.0
        down_imb_score = (down_imbalance + 1.0) / 2.0
        up_score += SIDE_SCORE_IMBALANCE_WEIGHT * up_imb_score
        down_score += SIDE_SCORE_IMBALANCE_WEIGHT * down_imb_score

        # ── Signal 4: Remaining capacity (prefer more room) ──
        up_remaining = max(0.0, MAX_BOUGHT_PER_SIDE - up_total_bought)
        down_remaining = max(0.0, MAX_BOUGHT_PER_SIDE - down_total_bought)
        cap_total = up_remaining + down_remaining
        if cap_total > 0:
            up_cap_score = up_remaining / cap_total
            down_cap_score = down_remaining / cap_total
        else:
            up_cap_score = 0.5
            down_cap_score = 0.5
        up_score += SIDE_SCORE_BOUGHT_WEIGHT * up_cap_score
        down_score += SIDE_SCORE_BOUGHT_WEIGHT * down_cap_score

        # ── Hysteresis: bonus for current active side ──
        if self._active_side == "UP":
            up_score += SIDE_SWITCH_THRESHOLD
        elif self._active_side == "DOWN":
            down_score += SIDE_SWITCH_THRESHOLD

        # ── Stickiness: honour lock period ──
        if self._active_side is not None and self._tick_count < self._side_locked_until_tick:
            return self._active_side

        chosen = "UP" if up_score >= down_score else "DOWN"

        if self._active_side is None:
            logger.info(
                "[%s] Initial side selection: %s (up_score=%.3f down_score=%.3f)",
                self.market.slug, chosen, up_score, down_score,
            )
            self._side_locked_until_tick = self._tick_count + SIDE_STICKINESS_TICKS
        elif chosen != self._active_side:
            logger.info(
                "[%s] Side switch: %s -> %s (up_score=%.3f down_score=%.3f)",
                self.market.slug, self._active_side, chosen, up_score, down_score,
            )
            self._side_locked_until_tick = self._tick_count + SIDE_STICKINESS_TICKS

        self._active_side = chosen
        return chosen
