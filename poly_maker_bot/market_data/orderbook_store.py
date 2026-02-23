from __future__ import annotations

from dataclasses import dataclass
import time
import threading
from collections import deque
from typing import Dict, Optional, Any
import logging

logger = logging.getLogger(__name__)


@dataclass
class TopOfBook:
  best_bid: Optional[float]
  best_bid_size: float
  best_ask: Optional[float]
  best_ask_size: float


def _f(x: Any) -> float:
  try:
    return float(x)
  except Exception:
    return 0.0


def _px_to_key(p: float) -> int:
  # store as integer "mils" (tenths of cents) to avoid float drift
  # and support tick_size of 0.001
  # 0.371 -> 371, 0.37 -> 370
  return int(round(p * 1000.0))


def _key_to_px(k: int) -> float:
  return k / 1000.0


class OrderBookStore:
  """
  In-memory order book for a single token_id.
  price_change semantics: size is TOTAL at that price level (set-to), not delta.
  size==0 => remove level.
  """

  def __init__(self, token_id: str):
    self.token_id = token_id
    self.bids: Dict[int, float] = {}  # price_key -> size
    self.asks: Dict[int, float] = {}
    self.last_seq: Optional[int] = None
    self.last_update_ts: Optional[float] = None
    self._lock = threading.RLock()

    # History tracking: keep snapshots for last 5 minutes, sampled at ~1s intervals
    self.history: deque = deque(maxlen=500)  # (timestamp, bids_snapshot, asks_snapshot)
    self.history_max_age = 300.0  # Keep 300 seconds (5 min) of history
    self._last_snapshot_ts: float = 0.0  # throttle snapshots to ~1/sec
    self._snapshot_min_interval: float = 1.0

  def apply_snapshot(self, snapshot: Any) -> None:
    with self._lock:
      if isinstance(snapshot, dict):
        bids = snapshot.get("bids") or []
        asks = snapshot.get("asks") or []
      else:
        bids = getattr(snapshot, "bids", []) or []
        asks = getattr(snapshot, "asks", []) or []

      self.bids.clear()
      self.asks.clear()

      def _get_ps(lvl):
        if isinstance(lvl, dict):
          return lvl.get("price", lvl.get("p")), lvl.get("size", lvl.get("s"))
        return getattr(lvl, "price", None), getattr(lvl, "size", None)

      for lvl in bids:
        price, size = _get_ps(lvl)
        p = _f(price)
        s = _f(size)
        if p > 0 and s > 0:
          self.bids[_px_to_key(p)] = s

      for lvl in asks:
        price, size = _get_ps(lvl)
        p = _f(price)
        s = _f(size)
        if p > 0 and s > 0:
          self.asks[_px_to_key(p)] = s

      self.last_update_ts = time.time()
      self._record_snapshot(force=True)

  def apply_price_change(self, side: str, price: Any, size: Any) -> None:
    with self._lock:
      p = _f(price)
      s = _f(size)
      if p <= 0:
        return

      k = _px_to_key(p)

      if side == "bid":
        book = self.bids
      elif side == "ask":
        book = self.asks
      else:
        return

      if s <= 0:
        book.pop(k, None)
      else:
        book[k] = s

      # print(f"Applied price_change: token_id={self.token_id} side={side} price={p} size={s}")
      # # Print Book for Token
      # print(f"token_id={self.token_id}: Bids={self.bids}")
      # print(f"token_id={self.token_id}: Asks={self.asks}")

      self.last_update_ts = time.time()
      self._record_snapshot()

  def top(self) -> TopOfBook:
    with self._lock:
      best_bid_k = max(self.bids.keys()) if self.bids else None
      best_ask_k = min(self.asks.keys()) if self.asks else None

      best_bid = _key_to_px(best_bid_k) if best_bid_k is not None else None
      best_ask = _key_to_px(best_ask_k) if best_ask_k is not None else None

      return TopOfBook(
        best_bid=best_bid,
        best_bid_size=(self.bids.get(best_bid_k, 0.0) if best_bid_k is not None else 0.0),
        best_ask=best_ask,
        best_ask_size=(self.asks.get(best_ask_k, 0.0) if best_ask_k is not None else 0.0),
      )

  def is_crossed(self) -> bool:
    with self._lock:
      if not self.bids or not self.asks:
        return False
      return max(self.bids.keys()) > min(self.asks.keys())

  def eligible_bid_levels(self, max_spread: float, mid_price: float) -> list[tuple[float, float]]:
    """
    Get all bid levels that are within max_spread from the midpoint.

    Args:
      max_spread: Maximum spread from midpoint (e.g., 0.035 for 3.5 cents)
      mid_price: Market midpoint price

    Returns:
      List of (price, size) tuples sorted by price descending (best first)
    """
    with self._lock:
      min_price = mid_price - max_spread
      eligible = []

      for price_key, size in self.bids.items():
        price = _key_to_px(price_key)
        if price >= min_price:
          eligible.append((price, size))

      # Sort by price descending (best bid first)
      eligible.sort(reverse=True)
      return eligible

  def eligible_ask_levels(self, max_spread: float, mid_price: float) -> list[tuple[float, float]]:
    """
    Get all ask levels that are within max_spread from the midpoint.

    Args:
      max_spread: Maximum spread from midpoint (e.g., 0.035 for 3.5 cents)
      mid_price: Market midpoint price

    Returns:
      List of (price, size) tuples sorted by price ascending (best first)
    """
    with self._lock:
      max_price = mid_price + max_spread
      eligible = []

      for price_key, size in self.asks.items():
        price = _key_to_px(price_key)
        if price <= max_price:
          eligible.append((price, size))

      # Sort by price ascending (best ask first)
      eligible.sort()
      return eligible

  def all_bid_levels(self) -> list[tuple[float, float]]:
    """
    Get all bid levels sorted by price descending (best first).

    Returns:
      List of (price, size) tuples sorted by price descending
    """
    with self._lock:
      levels = [(self._key_to_px(k), s) for k, s in self.bids.items()]
      levels.sort(reverse=True)
      return levels
    
  def all_ask_levels(self) -> list[tuple[float, float]]:
    """
    Get all ask levels sorted by price ascending (best first).

    Returns:
      List of (price, size) tuples sorted by price ascending
    """
    with self._lock:
      levels = [(self._key_to_px(k), s) for k, s in self.asks.items()]
      levels.sort()
      return levels

  def _key_to_px(self, k: int) -> float:
    """Convert internal price key back to float price."""
    return _key_to_px(k)

  def _record_snapshot(self, force: bool = False) -> None:
    """
    Record current orderbook state to history.
    Called after apply_snapshot() and apply_price_change().
    Thread-safe (already inside _lock from callers).
    Throttled to ~1 snapshot/sec to limit memory usage.
    """
    now = time.time()

    # Throttle: skip if we recorded recently (unless forced, e.g. full snapshot)
    if not force and (now - self._last_snapshot_ts) < self._snapshot_min_interval:
      return

    # Create shallow copies of current state
    bids_snapshot = dict(self.bids)
    asks_snapshot = dict(self.asks)

    # Append to history
    self.history.append((now, bids_snapshot, asks_snapshot))
    self._last_snapshot_ts = now

    # Prune old entries beyond max age
    cutoff = now - self.history_max_age
    while self.history and self.history[0][0] < cutoff:
      self.history.popleft()

  def get_snapshot_at_time(self, seconds_ago: float) -> Optional[tuple[Dict[int, float], Dict[int, float]]]:
    """
    Get orderbook snapshot from approximately seconds_ago in the past.

    Args:
      seconds_ago: How many seconds back to look

    Returns:
      Tuple of (bids_dict, asks_dict) or None if no data available
      Both dicts use price_key -> size format
    """
    with self._lock:
      if not self.history:
        return None

      target_ts = time.time() - seconds_ago

      # Find closest snapshot to target time
      best_snapshot = None
      best_diff = float('inf')

      for ts, bids, asks in self.history:
        diff = abs(ts - target_ts)
        if diff < best_diff:
          best_diff = diff
          best_snapshot = (bids, asks)

      return best_snapshot

  def get_top_at_time(self, seconds_ago: float) -> Optional[TopOfBook]:
    """
    Get top of book from approximately seconds_ago in the past.

    Args:
      seconds_ago: How many seconds back to look

    Returns:
      TopOfBook object or None if no data available
    """
    snapshot = self.get_snapshot_at_time(seconds_ago)
    if not snapshot:
      return None

    bids, asks = snapshot

    best_bid_k = max(bids.keys()) if bids else None
    best_ask_k = min(asks.keys()) if asks else None

    best_bid = _key_to_px(best_bid_k) if best_bid_k is not None else None
    best_ask = _key_to_px(best_ask_k) if best_ask_k is not None else None

    return TopOfBook(
      best_bid=best_bid,
      best_bid_size=(bids.get(best_bid_k, 0.0) if best_bid_k is not None else 0.0),
      best_ask=best_ask,
      best_ask_size=(asks.get(best_ask_k, 0.0) if best_ask_k is not None else 0.0),
    )
