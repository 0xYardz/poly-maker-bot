"""PnL calculation utilities."""
from __future__ import annotations

import logging
from typing import Callable, Optional

logger = logging.getLogger(__name__)


def calculate_unrealized_pnl(
    positions: dict[str, dict],
    get_mid_price_func: Callable[[str], Optional[float]],
) -> tuple[dict[str, float], dict[str, float], float]:
    """
    Calculate unrealized PnL for all positions.

    Args:
        positions: Dict of token_id -> position
        get_mid_price_func: Function to get current mid price for token (best_ask)

    Returns:
        Tuple of (
            unrealized_pnl_by_token,
            unrealized_pnl_pct_by_token,
            total_unrealized_pnl
        )
    """
    pnl_by_token = {}
    pnl_pct_by_token = {}
    total_unrealized_pnl = 0.0

    for token_id, pos in positions.items():
        size = pos["size"]
        cost_basis = pos["cost_basis"]

        if size <= 0:
            pnl_by_token[token_id] = 0.0
            pnl_pct_by_token[token_id] = 0.0
            continue

        # Get current exit price (best_ask)
        exit_price = get_mid_price_func(token_id)
        if exit_price is None:
            pnl_by_token[token_id] = 0.0
            pnl_pct_by_token[token_id] = 0.0
            continue

        # Calculate unrealized PnL
        market_value = size * exit_price
        unrealized_pnl = market_value - cost_basis
        pnl_by_token[token_id] = unrealized_pnl
        total_unrealized_pnl += unrealized_pnl

        # Calculate percentage
        if cost_basis > 0:
            pnl_pct = (unrealized_pnl / cost_basis) * 100.0
        else:
            pnl_pct = 0.0
        pnl_pct_by_token[token_id] = pnl_pct

    return pnl_by_token, pnl_pct_by_token, total_unrealized_pnl


def calculate_realized_pnl(
    sell_size: float,
    sell_price: float,
    avg_cost: float,
) -> float:
    """
    Calculate realized PnL from a sell.

    Args:
        sell_size: Size of sell
        sell_price: Price of sell
        avg_cost: Average cost basis

    Returns:
        Realized PnL (positive = profit, negative = loss)
    """
    return (sell_price - avg_cost) * sell_size
