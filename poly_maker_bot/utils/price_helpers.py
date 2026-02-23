"""Price normalization and formatting utilities."""
from __future__ import annotations

import decimal


def round_to_tick(price: float, tick_size: float) -> float:
    """
    Round price to nearest tick size.

    Args:
        price: Price to round
        tick_size: Tick size (e.g., 0.001, 0.01)

    Returns:
        Price rounded to nearest tick
    """
    # Get type of tick_size
    tick_size = float(tick_size)
    # Calculate decimal places needed for clean representation
    # e.g., 0.01 -> 2 decimal places, 0.001 -> 3 decimal places
    decimal_places = abs(decimal.Decimal(str(tick_size)).as_tuple().exponent)
    return round(round(price / tick_size) * tick_size, decimal_places)