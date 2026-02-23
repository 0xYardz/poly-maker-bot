"""Market discovery and filtering logic."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import timezone
import logging
import re
from typing import TYPE_CHECKING, Any, Optional, Tuple

from poly_maker_bot.utils.slug_helpers import (
    Timespan,
    Underlying,
    _parse_iso_dt,
    align_to_window_start,
    current_window_slug,
    slug_for_window,
    window_sec_for_timespan,
)

if TYPE_CHECKING:
    from poly_maker_bot.exchange.polymarket_client import PolymarketClient, MarketReward

logger = logging.getLogger(__name__)


@dataclass
class Market:
  """A discovered 15m up/down market with resolved token IDs and window boundaries."""
  market_id: str
  slug: str
  condition_id: str
  question: str
  up_token_id: str
  down_token_id: str
  start_ts: float
  end_ts: float
  s0: Optional[float]
  raw_market: dict[str, Any]


def discover_market(
  *,
  client: PolymarketClient,
  window_start_unix: int | None = None,
  now_unix: int | None = None,
  underlying: Underlying = "btc",
  timespan: Timespan = "15m",
) -> Market:
    """
    Discovers the current up/down market via Gamma:
    - Compute slug from window-aligned window_start
    - GET /events/slug/<slug>
    - Extract markets[0] and map Up/Down -> clobTokenIds
    - Build MarketContext (for strategy)
    """
    ws = window_sec_for_timespan(timespan)

    if window_start_unix is None:
        slug, t0 = current_window_slug(underlying, timespan, now_unix=now_unix)
    else:
        t0 = align_to_window_start(int(window_start_unix), ws)
        slug = slug_for_window(underlying, t0, timespan)

    market = client.get_market_by_slug(slug)
    if not market:
        raise RuntimeError(f"No market found for slug={slug}")

    up_token, down_token = _extract_up_down_token_ids(market)

    market_id = str(market.get("id", ""))
    condition_id = str(market.get("conditionId", ""))
    question = str(market.get("question", ""))

    # Window boundaries derived from the slug timestamp (t0),
    # because market startDateIso/endDateIso may be date-only or not aligned.
    start_ts = float(t0)
    end_ts = float(t0 + ws)

    end_date = market.get("endDate")
    if end_date:
        try:
            end_dt = _parse_iso_dt(str(end_date))
            api_end = end_dt.replace(tzinfo=timezone.utc).timestamp()
            if abs(api_end - end_ts) > 5:
                logger.warning("Gamma endDate mismatch vs slug window: api_end=%s slug_end=%s", api_end, end_ts)
        except Exception:
            logger.warning("Failed to parse Gamma endDate: %s", end_date)
            pass

    # s0 isn't provided directly here. We will fetch this later from the price feed, 
    # but we want to include it in the discovered market for completeness. Set a placeholder for now.
    s0 = None

    resultMarket = Market(
        market_id=market_id,
        slug=slug,
        condition_id=condition_id,
        question=question,
        up_token_id=up_token,
        down_token_id=down_token,
        start_ts=start_ts,
        end_ts=end_ts,
        s0=s0,
        raw_market=market,
    )

    return resultMarket


def _extract_up_down_token_ids(market: dict[str, Any]) -> Tuple[str, str]:
  """
  Extract Up and Down CLOB token IDs from a Gamma market dict.

  Gamma returns outcomes and clobTokenIds as positional parallel arrays
  (JSON-encoded strings or already-parsed lists):
    outcomes:     '["Up","Down"]'
    clobTokenIds: '["token_abc","token_xyz"]'

  Returns:
      (up_token_id, down_token_id)
  """
  import json as _json

  token_ids_raw = market.get("clobTokenIds")
  if token_ids_raw is None:
    raise KeyError("Gamma market missing clobTokenIds")

  outcomes_raw = market.get("outcomes")
  if outcomes_raw is None:
    raise KeyError("Gamma market missing outcomes")

  # Parse if JSON strings, otherwise use as-is
  token_ids = _json.loads(token_ids_raw) if isinstance(token_ids_raw, str) else token_ids_raw
  outcomes = _json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw

  if len(token_ids) != len(outcomes):
    raise ValueError(
      f"clobTokenIds length ({len(token_ids)}) != outcomes length ({len(outcomes)})"
    )

  # Build outcome -> token_id mapping
  outcome_to_token = {o.strip().lower(): tid for o, tid in zip(outcomes, token_ids)}

  up_token = outcome_to_token.get("up")
  down_token = outcome_to_token.get("down")

  if up_token is None or down_token is None:
    raise ValueError(
      f"Expected 'Up' and 'Down' outcomes, got: {outcomes}"
    )

  return up_token, down_token

def has_ticker_in_question(question: str) -> bool:
    """
    Check if a market question contains a stock ticker symbol.

    Detects patterns like:
    - "Google (GOOGL)"
    - "Apple Inc (AAPL)"
    - "Tesla [TSLA]"
    - "S&P 500 (SPX)"

    Uses a blacklist of known finance tickers from Polymarket's daily finance markets.

    Args:
        question: The market question text

    Returns:
        True if a ticker pattern is found, False otherwise
    """
    # Known finance tickers to exclude (extracted from Polymarket finance markets)
    # Updated from pagination_new.json on 2026-02-04
    finance_tickers = {
        'AAPL',   # Apple
        'AMZN',   # Amazon
        'CL',     # Crude Oil futures
        'COIN',   # Coinbase
        'DAX',    # German Stock Index (DAX)
        'DJI',    # Dow Jones Industrial Average
        'GC',     # Gold futures
        'GOOGL',  # Alphabet/Google
        'HOOD',   # Robinhood
        'HSI',    # Hang Seng Index
        'META',   # Meta/Facebook
        'MSFT',   # Microsoft
        'NDX',    # Nasdaq 100
        'NFLX',   # Netflix
        'NIK',    # Nikkei 225
        'NVDA',   # NVIDIA
        'NYA',    # NYSE Composite Index
        'OPEN',   # Opendoor
        'PLTR',   # Palantir
        'RUT',    # Russell 2000
        'SI',     # Silver futures
        'SPX',    # S&P 500
        'TSLA',   # Tesla
        'UKX',    # FTSE 100
    }

    # Pattern matches 2-5 uppercase letters in parentheses or brackets
    matches = re.findall(r'[\(\[]\s*([A-Z]{2,5})\s*[\)\]]', question)

    # Return True if any match IS in the finance tickers blacklist
    return any(match in finance_tickers for match in matches)


def is_excluded_by_pattern(market: "MarketReward", excluded_patterns: list[str]) -> bool:
    """
    Check if a market should be excluded based on slug patterns or ticker detection.

    Special handling for 'up-or-down' markets and stock ticker markets:
    - These are normally excluded, but allowed if the first token price is extreme (>= 0.9 or <= 0.1)
    - Extreme prices indicate near-certain outcomes, making them safer to LP

    Args:
        market: The market to check
        excluded_patterns: List of lowercase patterns to check against slug

    Returns:
        True if market should be excluded, False if it should be included
    """
    slug_lower = market.market_slug.lower()
    is_up_or_down = 'up-or-down' in slug_lower
    has_ticker = has_ticker_in_question(market.question)

    # Check if excluded by other patterns (not up-or-down since we handle that specially)
    other_patterns = [p for p in excluded_patterns if p != 'up-or-down']
    if any(pattern in slug_lower for pattern in other_patterns):
        return True

    # For up-or-down or ticker markets, allow if price is extreme
    if is_up_or_down or has_ticker:
        if market.tokens:
            token_price = market.tokens[0].price
            # Allow if price is extreme (near 0 or near 1) AND rate is high enough
            if (token_price > 0.9 or token_price < 0.1) and market.rewards_config[0].rate_per_day >= 150:
                return False  # Don't exclude - price is extreme
        # Price not extreme - exclude
        return True

    return False
