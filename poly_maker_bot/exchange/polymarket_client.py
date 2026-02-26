# This is a thin wrapper I'll fill in with actual API calls.
# poly_maker_bot/exchange/polymarket_client.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, List
import time
from typing import Any
import logging
import os
import requests

from py_clob_client import OpenOrderParams, TradeParams
from py_clob_client.client import ClobClient, ApiCreds, OrderArgs, OrderType  # from py-clob-client

from polymarket_apis import PolymarketDataClient, PolymarketGaslessWeb3Client, PolymarketWeb3Client

from poly_maker_bot.config import ExchangeConfig
from poly_maker_bot.metrics import RollingLatency, RateTracker

logger = logging.getLogger(__name__)

@dataclass
class OrderResponse:
  order_id: str
  status: str
  requested_size: float
  filled_size: float
  avg_price: float
  raw: dict

@dataclass
class Fill:
  """Represents a fill (trade) from our maker orders."""
  trade_id: str
  order_id: str  # Our maker order ID
  market_id: str  # condition_id
  asset_id: str  # token_id
  side: str  # "BUY" or "SELL"
  size: float
  price: float
  timestamp: int  # Unix timestamp
  outcome: str  # "Yes" or "No"

@dataclass
class Activity:
  """Represents a user activity event (trade, split, merge, etc.)."""
  timestamp: int           # Unix timestamp
  type: str                # TRADE, SPLIT, MERGE, REDEEM, REWARD, CONVERSION, MAKER_REBATE
  condition_id: str        # Market condition ID
  asset: str               # Asset/token identifier
  side: str                # BUY or SELL (for trades)
  size: float              # Token amount
  usdc_size: float         # USDC equivalent
  price: float             # Execution price
  outcome: str             # Outcome name (Yes/No)
  outcome_index: int       # Outcome position (0 or 1)
  transaction_hash: str    # On-chain tx hash
  title: str               # Market title
  slug: str                # Market slug

@dataclass
class Position:
  """Represents a user's position in a market outcome."""
  asset: str               # Token ID
  condition_id: str        # Market condition ID
  size: float              # Position size
  avg_price: float         # Average entry price
  initial_value: float     # Initial value (cost basis)
  current_value: float     # Current market value
  cash_pnl: float          # Realized + unrealized PnL in USDC
  percent_pnl: float       # PnL as percentage
  cur_price: float         # Current market price
  outcome: str             # Outcome name (Yes/No)
  outcome_index: int       # Outcome position (0 or 1)
  title: str               # Market title
  slug: str                # Market slug
  mergeable: bool          # Whether this position is mergeable
  negative_risk: bool      # Whether this is a negative risk position

@dataclass
class MarketPrice:
  """A single price data point from price history."""
  t: int    # Unix timestamp
  p: float  # Price

@dataclass
class OrderBookSnapshot:
  best_bid: Optional[float]
  best_bid_size: float
  best_ask: Optional[float]
  best_ask_size: float
  # extend with depth if needed
  ts: float  # timestamp (e.g. epoch seconds)

@dataclass
class OrderBookDepth:
  """Full orderbook depth with all price levels."""
  bids: list[tuple[float, float]]  # [(price, size), ...] sorted best first (highest price)
  asks: list[tuple[float, float]]  # [(price, size), ...] sorted best first (lowest price)
  ts: float  # timestamp (e.g. epoch seconds)

@dataclass
class Token:
  """Outcome token for a market."""
  token_id: str
  outcome: str  # "Yes" or "No"
  price: float

@dataclass
class RewardsConfig:
  """Reward configuration for a market."""
  asset_address: str
  start_date: str
  end_date: str
  rate_per_day: float
  total_rewards: float
  id: int

@dataclass
class Earnings:
  """Current earnings for a market."""
  asset_address: str
  earnings: float
  asset_rate: float

@dataclass
class MarketReward:
  """Represents a market eligible for LP rewards."""
  market_id: str
  condition_id: str
  question: str
  market_slug: str
  volume_24hr: float
  event_id: str
  event_slug: str
  image: str
  maker_address: str
  tokens: list[Token]
  rewards_config: list[RewardsConfig]
  earnings: list[Earnings]
  rewards_max_spread: float
  rewards_min_size: float
  earning_percentage: float
  spread: float
  market_competitiveness: float
  end_date: str = ""  # Market end date (YYYY-MM-DD format), populated during market selection
  competition_level: str = "low"  # "none", "low", "medium", "high", populated during market selection


@dataclass
class EventMarket:
  """A market within an event, as returned by the Gamma API."""
  id: str
  condition_id: str
  question: str
  slug: str
  outcomes: str                          # JSON-encoded list, e.g. '["Yes","No"]'
  outcome_prices: str                    # JSON-encoded list, e.g. '["0.55","0.45"]'
  active: bool
  closed: bool
  volume: float
  liquidity: float
  start_date: Optional[str]
  end_date: Optional[str]
  clob_token_ids: str                    # JSON-encoded list of token IDs
  neg_risk: Optional[bool]
  order_price_min_tick_size: Optional[float]
  raw: dict[str, Any]

@dataclass
class Event:
  """An event as returned by GET /events/slug/{slug} on the Gamma API."""
  id: str
  slug: str
  title: str
  description: str
  active: bool
  closed: bool
  neg_risk: bool
  liquidity: float
  volume: float
  start_date: Optional[str]
  end_date: Optional[str]
  markets: list[EventMarket]
  raw: dict[str, Any]

@dataclass
class UserRewardsEarning:
  """User-specific rewards earnings and market configuration for a given day."""
  condition_id: str
  question: str
  market_slug: str
  event_slug: str
  image: str
  rewards_max_spread: float
  rewards_min_size: float
  market_competitiveness: float
  tokens: list[Token]
  rewards_config: list[RewardsConfig]
  maker_address: str
  earning_percentage: float
  earnings: list[Earnings]


def _to_float(x: Any) -> float:
  try:
    return float(x)
  except Exception:
    return 0.0

class PolymarketClient:
  """
  Minimal client wrapper for Polymarket CLOB.

  IMPORTANT: we will only implement BUY orders (never sells).
  """

  def __init__(self, cfg: ExchangeConfig):
    host = cfg.api_url
    chain_id = 137  # Polygon mainnet

    funder = os.environ.get(cfg.funder_env)
    private_key = os.environ.get(cfg.private_key_env)
    if not private_key:
      raise RuntimeError(
          f"Private key env var {cfg.private_key_env} not set"
      )
    
    self._funder = funder

    self._client = ClobClient(
      host=host,
      chain_id=chain_id,
      signature_type=2,
      key=private_key,
      funder=funder
    )

    api_creds = self._client.create_or_derive_api_creds()
    # print("Derived CLOB API creds:", api_creds)
    self._client.set_api_creds(api_creds)

    # Cache for tick sizes (token_id -> (tick_size, last_fetch_time))
    # Tick sizes change based on price level (>0.96 or <0.04), so we cache with TTL
    self._tick_size_cache: dict[str, tuple[float, float]] = {}
    self._tick_size_cache_ttl = 10.0  # Refresh every 10 seconds

    # Cache for neg_risk flag (token_id -> (neg_risk, last_fetch_time))
    # Neg risk is a market property that doesn't change, so we use a long TTL
    self._neg_risk_cache: dict[str, tuple[bool, float]] = {}
    self._neg_risk_cache_ttl = 3600.0  # Refresh every hour (neg_risk doesn't change)
    self._lat_post_order = RollingLatency()
    self._lat_delete_order = RollingLatency()
    self._rate_post_order = RateTracker()
    self._rate_delete_order = RateTracker()

    # Rewards API configuration
    self._rewards_max_pages = int(os.environ.get("POLY_LP_REWARDS_MAX_PAGES", "4"))

    # Web3 + data clients for on-chain operations (redeem, merge, etc.)
    self._data_client = PolymarketDataClient()
    builder_key = os.environ.get(cfg.builder_api_key_env)
    builder_secret = os.environ.get(cfg.builder_secret_env)
    builder_passphrase = os.environ.get(cfg.builder_passphrase_env)
    builder_creds = None
    if builder_key and builder_secret and builder_passphrase:
      from polymarket_apis.types.clob_types import ApiCreds as BuilderApiCreds
      builder_creds = BuilderApiCreds(
        key=builder_key, secret=builder_secret, passphrase=builder_passphrase
      )
    self._web3_client = PolymarketGaslessWeb3Client(
      private_key=private_key,
      signature_type=2,  # Safe wallet
      builder_creds=builder_creds,
    )
    polygon_rpc_url = os.environ.get("POLYGON_RPC_URL", "https://polygon.drpc.org")
    self._web3_gas_client = PolymarketWeb3Client(
      private_key=private_key,
      signature_type=2,  # Safe wallet
      rpc_url=polygon_rpc_url,
    )

  def get_clob_api_creds(self) -> ApiCreds:
    """
    Get CLOB API credentials for WebSocket authentication.

    Returns:
        ApiCreds: CLOB API credentials (key, secret, passphrase)
    """
    # Credentials already derived in __init__ at line 165
    return self._client.create_or_derive_api_creds()

  # --- Market / book helpers ---
  
  # poly_maker_bot/exchange/polymarket_client.py (inside PolymarketClient)
  def get_orderbook(self, token_id: str) -> OrderBookSnapshot:
    """
    Fetch top-of-book for given outcome token via REST.
    Returns best bid/ask and sizes.
    """
    ts = time.time()

    # py-clob-client method names can vary. Try common ones.
    ob: dict[str, Any] | Any
    ob = self._client.get_order_book(token_id)
    if ob is None:
      raise RuntimeError(f"Failed to fetch orderbook for token {token_id}")
    
    # Normalize bids/asks
    bids = ob.get("bids") if isinstance(ob, dict) else getattr(ob, "bids", None)
    asks = ob.get("asks") if isinstance(ob, dict) else getattr(ob, "asks", None)
    bids = bids or []
    asks = asks or []

    def _level_price_size(lvl: Any) -> tuple[float, float]:
      # Supports dict-like {"price": "...", "size": "..."} and objects with .price/.size
      if isinstance(lvl, dict):
        return _to_float(lvl.get("price")), _to_float(lvl.get("size"))
      # OrderSummary(price='0.01', size='2298.8')
      p = getattr(lvl, "price", None)
      s = getattr(lvl, "size", None)
      return _to_float(p), _to_float(s)

    def best(levels: list[Any], side: str) -> tuple[float | None, float]:
      if not levels:
        return None, 0.0

      parsed = [_level_price_size(x) for x in levels]
      # filter out invalid
      parsed = [(p, s) for (p, s) in parsed if p > 0 and s >= 0]

      if not parsed:
        return None, 0.0

      if side == "bid":
        p, s = max(parsed, key=lambda t: t[0])
      else:
        p, s = min(parsed, key=lambda t: t[0])
      return p, s

    best_bid, best_bid_size = best(bids, "bid")
    best_ask, best_ask_size = best(asks, "ask")

    return OrderBookSnapshot(
      best_bid=best_bid,
      best_bid_size=best_bid_size,
      best_ask=best_ask,
      best_ask_size=best_ask_size,
      ts=ts,
    )

  def get_orderbook_depth(self, token_id: str) -> OrderBookDepth:
    """
    Fetch full orderbook depth for given outcome token via REST.
    Returns all bid/ask levels sorted by price.
    """
    ts = time.time()

    ob: dict[str, Any] | Any
    ob = self._client.get_order_book(token_id)
    if ob is None:
      raise RuntimeError(f"Failed to fetch orderbook for token {token_id}")

    # Normalize bids/asks
    bids_raw = ob.get("bids") if isinstance(ob, dict) else getattr(ob, "bids", None)
    asks_raw = ob.get("asks") if isinstance(ob, dict) else getattr(ob, "asks", None)
    bids_raw = bids_raw or []
    asks_raw = asks_raw or []

    def _level_price_size(lvl: Any) -> tuple[float, float]:
      if isinstance(lvl, dict):
        return _to_float(lvl.get("price")), _to_float(lvl.get("size"))
      p = getattr(lvl, "price", None)
      s = getattr(lvl, "size", None)
      return _to_float(p), _to_float(s)

    # Parse and filter invalid levels
    bids = [(p, s) for p, s in (_level_price_size(x) for x in bids_raw) if p > 0 and s > 0]
    asks = [(p, s) for p, s in (_level_price_size(x) for x in asks_raw) if p > 0 and s > 0]

    # Sort: bids highest first, asks lowest first
    bids.sort(key=lambda t: t[0], reverse=True)
    asks.sort(key=lambda t: t[0])

    return OrderBookDepth(bids=bids, asks=asks, ts=ts)

  def place_limit_buy(
    self,
    token_id: str,
    size: float,
    price: float,
    order_type: str = "GTC",
    market_slug: str = ""
  ) -> OrderResponse:
    """
    Place a limit buy order.
    BY DESIGN: there is no 'sell' API here.

    Args:
      token_id: The outcome token to buy.
      size: Number of shares.
      price: Limit price (0.01 - 0.99).
      order_type: "GTC" (Good-Til-Canceled), "FAK" (Fill-And-Kill), "FOK" (Fill-Or-Kill).
    """

    ot_map = {"GTC": OrderType.GTC, "FAK": OrderType.FAK, "FOK": OrderType.FOK}
    ot = ot_map.get(order_type.upper(), OrderType.GTC)
    
    order = self._client.create_order(
      OrderArgs(
        token_id=token_id,
        price=price,
        size=size,
        side="BUY",
      )
    )

    t0 = time.perf_counter()
    try:
      resp = self._client.post_order(order, orderType=ot)
      self._rate_post_order.record()  # Record successful request
    finally:
      latency = time.perf_counter() - t0
      self._lat_post_order.add(latency)
      # logger.info("[LATENCY] POST %s", latency)

    # logger.info(f"Order response: {resp}")

    # NOTE: response schema varies; keep raw + map conservatively
    requested = float(resp.get("original_size", resp.get("size", size)) or 0.0)
    filled = float(resp.get("filled_size", resp.get("size_filled", 0)) or 0.0)
    avg_px = float(resp.get("avg_price", resp.get("price", price)) or 0.0)

    return OrderResponse(
      order_id=resp.get("orderID", resp.get("order_id", "")) or "",
      status=resp.get("status", "") or "",
      requested_size=requested,
      filled_size=filled,
      avg_price=avg_px,
      raw=resp,
    )

  def place_limit_sell(
    self,
    token_id: str,
    size: float,
    price: float,
    order_type: str = "GTC",
    market_slug: str = ""
  ) -> OrderResponse:
    """
    Place a limit sell order.

    Args:
      token_id: The outcome token to sell.
      size: Number of shares.
      price: Limit price (0.01 - 0.99).
      order_type: "GTC" (Good-Til-Canceled), "FAK" (Fill-And-Kill), "FOK" (Fill-Or-Kill).
    """

    ot_map = {"GTC": OrderType.GTC, "FAK": OrderType.FAK, "FOK": OrderType.FOK}
    ot = ot_map.get(order_type.upper(), OrderType.GTC)

    order = self._client.create_order(
      OrderArgs(
        token_id=token_id,
        price=price,
        size=size,
        side="SELL",  # Key difference from place_limit_buy
      )
    )

    resp = self._client.post_order(order, orderType=ot, post_only=False)

    # NOTE: response schema varies; keep raw + map conservatively
    requested = float(resp.get("original_size", resp.get("size", size)) or 0.0)
    filled = float(resp.get("filled_size", resp.get("size_filled", 0)) or 0.0)
    avg_px = float(resp.get("avg_price", resp.get("price", price)) or 0.0)

    return OrderResponse(
      order_id=resp.get("orderID", resp.get("order_id", "")) or "",
      status=resp.get("status", "") or "",
      requested_size=requested,
      filled_size=filled,
      avg_price=avg_px,
      raw=resp,
    )

  def cancel_order(self, order_id: str) -> dict[str, Any]:
    """
    Cancel an open order by order_id.

    Returns the raw response from the CLOB API.
    """
    import inspect
    # Get caller information for debugging
    frame = inspect.currentframe()
    caller_frame = frame.f_back if frame else None
    caller_info = ""
    if caller_frame:
      caller_name = caller_frame.f_code.co_name
      caller_line = caller_frame.f_lineno
      caller_file = caller_frame.f_code.co_filename.split('/')[-1]
      caller_info = f" (called from {caller_file}:{caller_name}:{caller_line})"

    logger.info(f"Cancelling order order_id={order_id}{caller_info}")
    t0 = time.perf_counter()
    try:
      resp = self._client.cancel(order_id)
      self._rate_delete_order.record()  # Record successful request
    finally:
      latency = time.perf_counter() - t0
      self._lat_delete_order.add(latency)
      # logger.info("[LATENCY] DELETE %s", latency)
    return resp
  
  def get_tick_size(self, token_id: str) -> float:
    """
    Fetch the minimum tick size for a given outcome token.

    Tick sizes are cached with TTL since they change based on price level.
    When price > 0.96 or < 0.04, tick size may be different.

    Args:
      token_id: The outcome token ID

    Returns:
      Tick size (e.g., 0.001, 0.01)
    """
    current_time = time.time()

    # Check cache and TTL
    if token_id in self._tick_size_cache:
      cached_tick_size, cached_time = self._tick_size_cache[token_id]
      if current_time - cached_time < self._tick_size_cache_ttl:
        return cached_tick_size

    # Fetch from API (cache miss or expired)
    try:
      tick_size = self._client.get_tick_size(token_id)
      logger.debug(f"Fetched tick size for token {token_id[:12]}...: {tick_size}")
    except Exception as e:
      logger.warning(f"Failed to fetch tick size for {token_id[:12]}..., using default 0.01: {e}")
      tick_size = 0.01

    # Cache it with timestamp
    self._tick_size_cache[token_id] = (tick_size, current_time)
    return tick_size

  def get_neg_risk(self, token_id: str) -> bool:
    """
    Fetch the neg_risk flag for a given outcome token.

    Neg risk is a market property indicating whether the market uses
    the neg risk CTF framework. This is cached with a long TTL since
    it doesn't change for a market.

    Args:
      token_id: The outcome token ID

    Returns:
      True if neg risk market, False otherwise
    """
    current_time = time.time()

    # Check cache and TTL
    if token_id in self._neg_risk_cache:
      cached_neg_risk, cached_time = self._neg_risk_cache[token_id]
      if current_time - cached_time < self._neg_risk_cache_ttl:
        return cached_neg_risk

    # Fetch from API (cache miss or expired)
    try:
      neg_risk = self._client.get_neg_risk(token_id)
      logger.debug(f"Fetched neg_risk for token {token_id[:12]}...: {neg_risk}")
    except Exception as e:
      logger.warning(f"Failed to fetch neg_risk for {token_id[:12]}..., using default True: {e}")
      neg_risk = True  # Most markets are neg_risk

    # Cache it with timestamp
    self._neg_risk_cache[token_id] = (neg_risk, current_time)
    return neg_risk

  def get_current_rewards(self, max_pages: int | None = None, limit: int = 100) -> List[MarketReward]:
    """
    Fetch all markets currently eligible for LP rewards.

    This implements the GET /rewards/markets endpoint.
    Returns a list of MarketReward objects containing reward configuration.

    Args:
      max_pages: Maximum number of pages to fetch (default from POLY_LP_REWARDS_MAX_PAGES env var)
      limit: Number of markets per page (default 100)

    Note: This method is not yet in py-clob-client, so we implement it directly.
    """
    # Use env var default if not specified
    if max_pages is None:
      max_pages = self._rewards_max_pages

    start_time = time.time()
    logger.info(f"[REWARDS] Starting fetch: max_pages={max_pages}, limit={limit}")

    GET_REWARDS_MARKETS_CURRENT = "/rewards/markets"
    results: List[MarketReward] = []
    next_cursor = "MA=="  # Initial cursor

    try:
      url = f"https://polymarket.com/api{GET_REWARDS_MARKETS_CURRENT}"

      for page in range(max_pages):
        params = {
          "orderBy": "rate_per_day",
          "position": "DESC",
          "tagSlug": "all",
          "nextCursor": next_cursor,
          "onlyMergeable": "false",
          "limit": limit,
        }

        # Make request using requests library directly
        # (py-clob-client doesn't expose this endpoint yet)
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        markets_data = data if isinstance(data, list) else data.get("data", [])
        if not markets_data:
          # No more data
          break

        # Convert to MarketReward objects
        for market in markets_data:
          try:
            # Parse tokens
            tokens = []
            for token_data in market.get("tokens", []):
              tokens.append(Token(
                token_id=token_data.get("token_id", ""),
                outcome=token_data.get("outcome", ""),
                price=float(token_data.get("price", 0.0))
              ))

            # Parse rewards config
            rewards_config = []
            for config_data in market.get("rewards_config", []):
              rewards_config.append(RewardsConfig(
                asset_address=config_data.get("asset_address", ""),
                start_date=config_data.get("start_date", ""),
                end_date=config_data.get("end_date", ""),
                rate_per_day=float(config_data.get("rate_per_day", 0.0)),
                total_rewards=float(config_data.get("total_rewards", 0.0)),
                id=int(config_data.get("id", 0))
              ))

            # Parse earnings
            earnings = []
            for earnings_data in market.get("earnings", []):
              earnings.append(Earnings(
                asset_address=earnings_data.get("asset_address", ""),
                earnings=float(earnings_data.get("earnings", 0.0)),
                asset_rate=float(earnings_data.get("asset_rate", 0.0))
              ))

            reward = MarketReward(
              market_id=market.get("market_id", ""),
              condition_id=market.get("condition_id", ""),
              question=market.get("question", ""),
              market_slug=market.get("market_slug", ""),
              volume_24hr=float(market.get("volume_24hr", 0.0)),
              event_id=market.get("event_id", ""),
              event_slug=market.get("event_slug", ""),
              image=market.get("image", ""),
              maker_address=market.get("maker_address", ""),
              tokens=tokens,
              rewards_config=rewards_config,
              earnings=earnings,
              rewards_max_spread=float(market.get("rewards_max_spread", 3)) / 100.0, # Convert cents to dollars
              rewards_min_size=float(market.get("rewards_min_size", 10.0)),
              earning_percentage=float(market.get("earning_percentage", 0.0)),
              spread=float(market.get("spread", 0.0)),
              market_competitiveness=float(market.get("market_competitiveness", 0.0))
            )
            results.append(reward)
          except Exception as e:
            logger.warning(f"Failed to parse market reward: {e}")
            continue

        # Get next cursor for pagination
        next_cursor = data.get("next_cursor") if isinstance(data, dict) else None
        if not next_cursor:
          # No more pages
          break

        # Small delay between pages to avoid overwhelming HTTP/2 connection pool
        if page < max_pages - 1:  # Don't sleep after last page
          time.sleep(0.1)

    except requests.RequestException as e:
      elapsed = time.time() - start_time
      logger.error(f"[REWARDS] Failed to fetch rewards markets: {e} (elapsed={elapsed:.2f}s)")
      return []

    elapsed = time.time() - start_time
    logger.info(
      f"[REWARDS] Completed: fetched {len(results)} markets across {page + 1} pages "
      f"(elapsed={elapsed:.2f}s, avg={elapsed/(page+1):.2f}s/page)"
    )
    return results

  def get_user_earnings_and_markets_config(
    self,
    date: str,
    order_by: str = "",
    position: str = "",
    no_competition: bool = False,
  ) -> List[UserRewardsEarning]:
    """
    Fetch user-specific rewards earnings and market configurations for a given date.

    This implements the GET /rewards/user/markets endpoint (L2 authenticated).
    Ported from the TypeScript clob-client's getUserEarningsAndMarketsConfig.

    Args:
      date: Date string (e.g. "2026-02-06")
      order_by: Field to order by (default "")
      position: Sort direction e.g. "DESC" (default "")
      no_competition: Filter out competition markets (default False)

    Note: This method is not yet in py-clob-client, so we implement it directly.
    """
    from py_clob_client.clob_types import RequestArgs
    from py_clob_client.headers.headers import create_level_2_headers
    from py_clob_client.constants import END_CURSOR

    start_time = time.time()
    logger.info(f"[REWARDS] Starting user earnings fetch: date={date}")

    GET_REWARDS_EARNINGS_PERCENTAGES = "/rewards/user/markets"

    # Build L2 auth headers (signed against the endpoint path, not the full URL)
    request_args = RequestArgs(method="GET", request_path=GET_REWARDS_EARNINGS_PERCENTAGES)
    headers = create_level_2_headers(
      self._client.signer, self._client.creds, request_args
    )

    results: List[UserRewardsEarning] = []
    next_cursor = "MA=="  # INITIAL_CURSOR

    try:
      url = f"{self._client.host}{GET_REWARDS_EARNINGS_PERCENTAGES}"
      page = 0

      while next_cursor != END_CURSOR:
        params = {
          "date": date,
          "signature_type": self._client.builder.sig_type,
          "next_cursor": next_cursor,
          "order_by": order_by,
          "position": position,
          "no_competition": str(no_competition).lower(),
          "limit": 500,
        }

        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        entries = data.get("data", [])
        if not entries:
          break

        for entry in entries:
          if len(results) >= 1000:
            break
          try:
            tokens = []
            for t in entry.get("tokens", []):
              tokens.append(Token(
                token_id=t.get("token_id", ""),
                outcome=t.get("outcome", ""),
                price=float(t.get("price", 0.0))
              ))

            rewards_config = []
            for rc in entry.get("rewards_config", []):
              rewards_config.append(RewardsConfig(
                asset_address=rc.get("asset_address", ""),
                start_date=rc.get("start_date", ""),
                end_date=rc.get("end_date", ""),
                rate_per_day=float(rc.get("rate_per_day", 0.0)),
                total_rewards=float(rc.get("total_rewards", 0.0)),
                id=int(rc.get("id", 0))
              ))

            earnings = []
            for e in entry.get("earnings", []):
              earnings.append(Earnings(
                asset_address=e.get("asset_address", ""),
                earnings=float(e.get("earnings", 0.0)),
                asset_rate=float(e.get("asset_rate", 0.0))
              ))

            results.append(UserRewardsEarning(
              condition_id=entry.get("condition_id", ""),
              question=entry.get("question", ""),
              market_slug=entry.get("market_slug", ""),
              event_slug=entry.get("event_slug", ""),
              image=entry.get("image", ""),
              rewards_max_spread=float(entry.get("rewards_max_spread", 3)) / 100.0,
              rewards_min_size=float(entry.get("rewards_min_size", 10.0)),
              market_competitiveness=float(entry.get("market_competitiveness", 0.0)),
              tokens=tokens,
              rewards_config=rewards_config,
              maker_address=entry.get("maker_address", ""),
              earning_percentage=float(entry.get("earning_percentage", 0.0)),
              earnings=earnings,
            ))
          except Exception as e:
            logger.warning(f"Failed to parse user rewards earning: {e}")
            continue

        if len(results) >= 1000:
          break

        next_cursor = data.get("next_cursor", END_CURSOR)
        page += 1

        if next_cursor != END_CURSOR:
          time.sleep(0.1)

    except requests.RequestException as e:
      elapsed = time.time() - start_time
      logger.error(f"[REWARDS] Failed to fetch user earnings: {e} (elapsed={elapsed:.2f}s)")
      return []

    elapsed = time.time() - start_time
    logger.info(
      f"[REWARDS] User earnings completed: fetched {len(results)} entries across {page} pages "
      f"(elapsed={elapsed:.2f}s)"
    )
    return results

  def cancel_all(self) -> dict[str, Any]:
    """
    Cancel all open orders.

    Returns the raw response from the CLOB API.
    """
    logger.info("Cancelling all open orders")
    resp = self._client.cancel_all()
    logger.info(f"Cancel all response: {resp}")
    return resp

  def get_open_orders(self, params: OpenOrderParams = None) -> list[dict[str, Any]]:
    """
    Get all open orders for this account.
    """
    resp = self._client.get_orders(params)
    # py-clob-client returns list of OpenOrder objects or dicts
    if isinstance(resp, list):
      return [o if isinstance(o, dict) else vars(o) for o in resp]
    return []
  
  def get_market_by_condition_id(self, condition_id: str) -> dict[str, Any]:
    """
    Fetch market details by condition ID using the Gamma API REST endpoint.

    Uses https://gamma-api.polymarket.com/markets instead of the CLOB client
    because the CLOB client's get_market() does not return startDate.

    Retries on transient network errors.
    """
    import time

    url = "https://gamma-api.polymarket.com/markets"
    params = {"condition_ids": condition_id}
    max_retries = 3
    for attempt in range(max_retries):
      try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        markets = response.json()
        if markets and len(markets) > 0:
          return markets[0]
        raise ValueError(f"No market found for condition_id {condition_id}")
      except (requests.RequestException, ValueError) as e:
        if attempt < max_retries - 1:
          wait_time = 0.5 * (2 ** attempt)
          logger.warning(
            f"Error fetching market {condition_id[:12]}..., "
            f"retrying in {wait_time}s (attempt {attempt + 1}/{max_retries}): {e}"
          )
          time.sleep(wait_time)
          continue
        raise

  def get_market_by_slug(self, slug: str) -> dict[str, Any]:
    """
    Fetch market details by slug using the Gamma API REST endpoint.

    Uses GET https://gamma-api.polymarket.com/markets/slug/{slug}

    Retries on transient network errors.
    """
    import time

    url = f"https://gamma-api.polymarket.com/markets/slug/{slug}"
    max_retries = 3
    for attempt in range(max_retries):
      try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
      except requests.RequestException as e:
        if attempt < max_retries - 1:
          wait_time = 0.5 * (2 ** attempt)
          logger.warning(
            f"Error fetching market by slug '{slug}', "
            f"retrying in {wait_time}s (attempt {attempt + 1}/{max_retries}): {e}"
          )
          time.sleep(wait_time)
          continue
        raise

  def get_prices_history(
    self,
    market: Optional[str] = None,
    start_ts: Optional[int] = None,
    end_ts: Optional[int] = None,
    fidelity: Optional[int] = None,
    interval: Optional[str] = None,
  ) -> List[MarketPrice]:
    """
    Fetch historical price data for a market.

    This implements the GET /prices-history endpoint.

    Args:
      market: Token ID to get price history for
      start_ts: Start Unix timestamp
      end_ts: End Unix timestamp
      fidelity: Number of data points to return
      interval: Time interval - "max", "1w", "1d", "6h", or "1h"

    Returns:
      List of MarketPrice objects with timestamp and price
    """
    try:
      url = f"{self._client.host}/prices-history"
      params: dict[str, Any] = {}

      if market is not None:
        params["market"] = market
      if start_ts is not None:
        params["startTs"] = start_ts
      if end_ts is not None:
        params["endTs"] = end_ts
      if fidelity is not None:
        params["fidelity"] = fidelity
      if interval is not None:
        params["interval"] = interval

      response = requests.get(url, params=params, timeout=10)
      response.raise_for_status()
      data = response.json()

      if not isinstance(data, list):
        data = data.get("history", [])

      return [
        MarketPrice(t=int(item.get("t", 0)), p=float(item.get("p", 0.0)))
        for item in data
      ]

    except requests.RequestException as e:
      logger.error(f"Failed to fetch prices history: {e}")
      return []
    except Exception as e:
      logger.error(f"Failed to parse prices history: {e}", exc_info=True)
      return []

  def get_recent_fills(self, after_ts: int = 0) -> List[Fill]:
    """
    Get recent fills for our maker orders.

    Uses the /data/trades endpoint filtered by maker_address to get
    trades where our limit orders were filled.

    Args:
      after_ts: Only return fills after this Unix timestamp (seconds)

    Returns:
      List of Fill objects, sorted by timestamp ascending
    """
    try:
      print(f"Fetching recent fills for maker_address={self._funder} after_ts={after_ts}")
      params = TradeParams(maker_address=self._funder)
      # print(f"Fetching recent fills for maker_address={params.maker_address} after_ts={after_ts}")
      if after_ts > 0:
        params.after = str(after_ts)

      trades = self._client.get_trades(params)

      fills: List[Fill] = []
      for trade in trades:
        # Each trade has maker_orders which contains our fill details
        trade_dict = trade if isinstance(trade, dict) else vars(trade)

        # Only process CONFIRMED trades
        status = trade_dict.get("status", "")
        if status != "CONFIRMED":
          logger.debug(f"Skipping trade with status={status}: {trade_dict.get('id', '')}")
          continue

        trade_id = trade_dict.get("id", "")
        market_id = trade_dict.get("market", "")

        # Parse match_time to Unix timestamp
        match_time = trade_dict.get("match_time", "")
        try:
          # match_time is typically ISO format or Unix timestamp string
          if match_time.isdigit():
            ts = int(match_time)
          else:
            # Try parsing ISO format
            from datetime import datetime
            dt = datetime.fromisoformat(match_time.replace("Z", "+00:00"))
            ts = int(dt.timestamp())
        except Exception:
          ts = 0

        # Extract our maker order fills from maker_orders array
        # A single trade can have multiple makers, so filter to only our fills
        maker_orders = trade_dict.get("maker_orders", [])
        for maker_order in maker_orders:
          mo = maker_order if isinstance(maker_order, dict) else vars(maker_order)

          # Only process maker orders that belong to us
          mo_maker_address = mo.get("maker_address", "")
          if mo_maker_address.lower() != self._funder.lower():
            continue

          fill = Fill(
            trade_id=trade_id,
            order_id=mo.get("order_id", ""),
            market_id=market_id,
            asset_id=mo.get("asset_id", ""),
            side=mo.get("side", "").upper(),
            size=float(mo.get("matched_amount", 0)),
            price=float(mo.get("price", 0)),
            timestamp=ts,
            outcome=mo.get("outcome", ""),
          )
          fills.append(fill)

      # Sort by timestamp ascending
      fills.sort(key=lambda f: f.timestamp)
      return fills

    except Exception as e:
      logger.error(f"Failed to fetch recent fills: {e}", exc_info=True)
      return []

  def get_user_activity(
    self,
    activity_type: Optional[List[str]] = None,
    side: Optional[str] = None,
    start_ts: Optional[int] = None,
    end_ts: Optional[int] = None,
    market: Optional[List[str]] = None,
    limit: int = 100,
    offset: int = 0,
  ) -> List[Activity]:
    """
    Get user activity from the Polymarket data API.

    This provides a history of trades, splits, merges, redemptions, rewards, etc.

    Args:
      activity_type: Filter by type(s): TRADE, SPLIT, MERGE, REDEEM, REWARD, CONVERSION, MAKER_REBATE
      side: Filter by side: BUY or SELL
      start_ts: Only return activity after this Unix timestamp
      end_ts: Only return activity before this Unix timestamp
      market: Filter by condition ID(s)
      limit: Max results (default 100, max 500)
      offset: Pagination offset (default 0, max 10000)

    Returns:
      List of Activity objects, sorted by timestamp descending (newest first)
    """
    try:
      user_address = self._client.get_address()

      url = "https://data-api.polymarket.com/activity"
      params: dict[str, Any] = {
        "user": user_address,
        "limit": min(limit, 500),
        "offset": min(offset, 10000),
        "sortBy": "TIMESTAMP",
        "sortDirection": "DESC",
      }

      if activity_type:
        params["type"] = ",".join(activity_type)
      if side:
        params["side"] = side.upper()
      if start_ts is not None:
        params["start"] = start_ts
      if end_ts is not None:
        params["end"] = end_ts
      if market:
        params["market"] = ",".join(market)

      response = requests.get(url, params=params, timeout=10)
      response.raise_for_status()
      data = response.json()

      activities: List[Activity] = []
      for item in data:
        activity = Activity(
          timestamp=int(item.get("timestamp", 0)),
          type=item.get("type", ""),
          condition_id=item.get("conditionId", ""),
          asset=item.get("asset", ""),
          side=item.get("side", ""),
          size=float(item.get("size", 0)),
          usdc_size=float(item.get("usdcSize", 0)),
          price=float(item.get("price", 0)),
          outcome=item.get("outcome", ""),
          outcome_index=int(item.get("outcomeIndex", 0)),
          transaction_hash=item.get("transactionHash", ""),
          title=item.get("title", ""),
          slug=item.get("slug", ""),
        )
        activities.append(activity)

      return activities

    except requests.RequestException as e:
      logger.error(f"Failed to fetch user activity: {e}")
      return []
    except Exception as e:
      logger.error(f"Failed to parse user activity: {e}", exc_info=True)
      return []

  def get_positions(
    self,
    size_threshold: float = 0.0,
    limit: int = 500,
    mergeable: bool | None = None,
  ) -> List[Position]:
    """
    Get user's current positions from the Polymarket data API.

    Args:
      market_ids: Ignored - always fetches all positions (kept for API compatibility)
      size_threshold: Minimum position size (default 0)
      limit: Max results (default 500, max 500)

    Returns:
      List of Position objects
    """
    try:
      user_address = self._funder

      url = "https://data-api.polymarket.com/positions"
      params: dict[str, Any] = {
        "user": user_address,
        "sizeThreshold": size_threshold,
        "limit": min(limit, 500),
        "sortBy": "TOKENS",
        "sortDirection": "DESC",
      }

      if mergeable is not None:
        params["mergeable"] = mergeable

      response = requests.get(url, params=params, timeout=10)
      response.raise_for_status()
      data = response.json()

      positions: List[Position] = []
      for item in data:
        position = Position(
          asset=item.get("asset", ""),
          condition_id=item.get("conditionId", ""),
          size=float(item.get("size", 0)),
          avg_price=float(item.get("avgPrice", 0)),
          initial_value=float(item.get("initialValue", 0)),
          current_value=float(item.get("currentValue", 0)),
          cash_pnl=float(item.get("cashPnl", 0)),
          percent_pnl=float(item.get("percentPnl", 0)),
          cur_price=float(item.get("curPrice", 0)),
          outcome=item.get("outcome", ""),
          outcome_index=int(item.get("outcomeIndex", 0)),
          title=item.get("title", ""),
          slug=item.get("slug", ""),
          mergeable=item.get("mergeable", False),
          negative_risk=item.get("negativeRisk", True)
        )
        positions.append(position)

      return positions

    except requests.RequestException as e:
      logger.error(f"Failed to fetch positions: {e}")
      return []
    except Exception as e:
      logger.error(f"Failed to parse positions: {e}", exc_info=True)
      return []

  def redeem_all_positions(self) -> int:
    """
    Redeem all redeemable positions (resolved markets) into USDC.

    Uses the polymarket-apis PolymarketDataClient to discover redeemable
    positions, then PolymarketGaslessWeb3Client to execute on-chain redemptions.

    Returns:
      Number of positions successfully redeemed.
    """
    redeemed = 0
    try:
      user_address = self._funder

      # Query the data API directly (more reliable than the library client
      # which always sends mergeable=False alongside redeemable=True)
      url = "https://data-api.polymarket.com/positions"
      params: dict[str, Any] = {
        "user": user_address,
        "redeemable": "true",
        "sizeThreshold": 0,
        "limit": 500,
        "sortBy": "TOKENS",
        "sortDirection": "DESC",
      }
      response = requests.get(url, params=params, timeout=10)
      response.raise_for_status()
      raw_positions = response.json()

      if not raw_positions:
        logger.info("[REDEEM] No redeemable positions found")
        return 0

      logger.info(f"[REDEEM] Found {len(raw_positions)} redeemable position(s)")

      # Group by condition_id — each condition needs one redeem call
      # with amounts for both outcomes
      from collections import defaultdict
      by_condition: dict[str, dict[int, float]] = defaultdict(dict)
      neg_risk_map: dict[str, bool] = {}
      for pos in raw_positions:
        cid = pos.get("conditionId", "")
        by_condition[cid][int(pos.get("outcomeIndex", 0))] = float(pos.get("size", 0))
        neg_risk_map[cid] = pos.get("negativeRisk", True)

      for idx, (condition_id, outcomes) in enumerate(by_condition.items()):
        amounts = [outcomes.get(0, 0.0), outcomes.get(1, 0.0)]
        neg_risk = neg_risk_map[condition_id]

        # Avoid 429 rate-limits on the relayer
        if idx > 0:
          time.sleep(2)

        try:
          logger.info(
            f"[REDEEM] Redeeming condition={condition_id[:12]}... "
            f"amounts={amounts} neg_risk={neg_risk}"
          )
          try:
            receipt = self._web3_client.redeem_position(
              condition_id=condition_id,
              amounts=amounts,
              neg_risk=neg_risk,
            )
          except Exception as gasless_err:
            if "429" in str(gasless_err):
              logger.warning(
                f"[REDEEM] Gasless 429 for condition={condition_id[:12]}..., "
                f"falling back to gas client"
              )
              receipt = self._web3_gas_client.redeem_position(
                condition_id=condition_id,
                amounts=amounts,
                neg_risk=neg_risk,
              )
            else:
              raise
          if receipt.status == 1:
            redeemed += 1
            logger.info(
              f"[REDEEM] Success: condition={condition_id[:12]}... "
              f"tx={receipt.tx_hash}"
            )
          else:
            logger.warning(
              f"[REDEEM] Failed on-chain: condition={condition_id[:12]}... "
              f"tx={receipt.tx_hash}"
            )
        except Exception as e:
          logger.error(
            f"[REDEEM] Error redeeming condition={condition_id[:12]}...: {e}",
            exc_info=True,
          )

    except Exception as e:
      logger.error(f"[REDEEM] Error fetching redeemable positions: {e}", exc_info=True)

    logger.info(f"[REDEEM] Completed: redeemed {redeemed} position(s)")
    return redeemed

