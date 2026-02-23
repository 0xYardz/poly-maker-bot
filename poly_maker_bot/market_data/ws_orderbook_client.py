# poly_maker_bot/market_data/ws_orderbook_client.py
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
import time
from typing import Callable, Dict, Iterable, Optional
import threading

from poly_maker_bot.market_data.orderbook_store import OrderBookStore
from poly_maker_bot.exchange.polymarket_client import PolymarketClient

try:
    import websocket  # websocket-client library
except ImportError:
    raise ImportError(
        "websocket-client required. Install with: pip install websocket-client"
    )

logger = logging.getLogger(__name__)


@dataclass
class WsOrderbookConfig:
  rest_seed_on_start: bool = True
  staleness_sec: float = 120.0          # if no WS update for token in this long -> resync
  min_resync_interval_sec: float = 5.0 # per-token cooldown
  poll_interval_sec: float = 0.25      # how often we check staleness
  resync_on_crossed: bool = True
  reconnect_grace_sec: float = 5.0    # pause staleness checks after reconnect
  resync_on_reconnect: bool = True    # immediately REST resync after WS reconnects
  pong_timeout_sec: float = 30.0      # max time without PONG before connection considered unhealthy


class WsOrderbookClient:
  """
  WebSocket client for Polymarket CLOB market data.

  Uses the new WebSocket API: wss://ws-subscriptions-clob.polymarket.com
  Documentation: https://docs.polymarket.com/quickstart/websocket/WSS-Quickstart
  """

  DEFAULT_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
  PING_INTERVAL = 10.0  # Polymarket requires PING every 10 seconds

  def __init__(
    self,
    token_ids: Iterable[str],
    stores: Dict[str, OrderBookStore],
    rest_client: PolymarketClient,
    ws_url: str = DEFAULT_WS_URL,
    cfg: WsOrderbookConfig = WsOrderbookConfig(),
    on_connection_unhealthy: Optional[Callable[[], None]] = None,
    on_connection_healthy: Optional[Callable[[], None]] = None,
  ):
    self.ws_url = ws_url
    self.token_ids = [str(t) for t in token_ids]
    self.stores = stores
    self.rest = rest_client
    self.cfg = cfg
    self._stop = threading.Event()
    self._last_resync_ts: Dict[str, float] = {}   # per-token cooldown (used by _rest_resync_token)
    self._last_pair_resync_ts: float = 0.0        # pair-wide cooldown (watcher uses this)
    self._resync_lock = threading.Lock()          # prevent overlapping resync passes
    self._watcher_thread: threading.Thread | None = None
    self._last_reconnect_ts: float = 0.0          # track reconnect time for grace period
    self._on_connection_unhealthy = on_connection_unhealthy
    self._on_connection_healthy = on_connection_healthy

    # WebSocket state
    self.ws: Optional[websocket.WebSocketApp] = None
    self._ws_thread: Optional[threading.Thread] = None
    self._ping_timer: Optional[threading.Timer] = None
    self._connected = False
    self._shutdown_requested = False

    # Auto-reconnect
    self._reconnect_timer: Optional[threading.Timer] = None
    self._reconnect_attempts = 0
    self._max_reconnect_delay = 30.0

    # PONG timeout monitoring
    self._last_pong_ts: float = 0.0
    self._pong_watchdog_thread: Optional[threading.Thread] = None
    self._connection_healthy = True  # Track connection health status
    self._pong_lock = threading.Lock()  # Protects _last_pong_ts and _connection_healthy

  def _rest_resync_token(self, token_id: str, skip_cooldown: bool = False) -> None:
    now = time.time()
    last = self._last_resync_ts.get(token_id, 0.0)
    if not skip_cooldown and now - last < self.cfg.min_resync_interval_sec:
      return

    # Retry with exponential backoff
    max_retries = 3
    base_delay = 1.0
    for attempt in range(max_retries):
      try:
        full = self.rest._client.get_order_book(token_id)
        store = self._store(token_id)
        store.apply_snapshot(full)
        self._last_resync_ts[token_id] = now
        logger.debug(f"REST resync token={token_id[:16]}...")
        return  # Success, exit retry loop
      except Exception as e:
        if attempt < max_retries - 1:
          delay = base_delay * (2 ** attempt)
          logger.warning(f"REST resync failed token={token_id[:16]}... (attempt {attempt + 1}/{max_retries}), retrying in {delay}s: {e}")
          time.sleep(delay)
        else:
          logger.error(f"REST resync failed token={token_id[:16]}... after {max_retries} attempts: {e}")

  def _resync_all_tokens(self) -> None:
    """Resync all tokens from REST API (called on reconnect)."""
    if not self._resync_lock.acquire(blocking=False):
      logger.debug("Resync already in progress, skipping reconnect resync")
      return

    try:
      logger.info("Resyncing all tokens after reconnect...")
      for tid in self.token_ids:
        self._rest_resync_token(tid, skip_cooldown=True)
      self._last_pair_resync_ts = time.time()
      logger.info("Reconnect resync complete")
    finally:
      self._resync_lock.release()

  def _watch_pong_timeout_loop(self) -> None:
    """Monitor PONG timeout and trigger connection unhealthy callback."""
    while not self._stop.is_set():
      now = time.time()

      # Only check PONG timeout if connected and not shutting down
      if self._connected and not self._shutdown_requested:
        with self._pong_lock:
          last_pong = self._last_pong_ts
          is_healthy = self._connection_healthy

        if last_pong > 0:
          time_since_pong = now - last_pong

          if time_since_pong > self.cfg.pong_timeout_sec:
            # Connection is unhealthy
            if is_healthy:
              logger.warning(
                f"PONG timeout detected: no PONG received for {time_since_pong:.1f}s "
                f"(threshold: {self.cfg.pong_timeout_sec}s)"
              )
              with self._pong_lock:
                self._connection_healthy = False

              # Trigger unhealthy callback
              if self._on_connection_unhealthy:
                try:
                  self._on_connection_unhealthy()
                except Exception as e:
                  logger.error(f"Error in connection_unhealthy callback: {e}", exc_info=True)

      # Check every second
      self._stop.wait(1.0)

  def _watch_staleness_loop(self) -> None:
    while not self._stop.is_set():
      now = time.time()

      # Skip staleness checks during reconnect grace period
      if (now - self._last_reconnect_ts) < self.cfg.reconnect_grace_sec:
        self._stop.wait(self.cfg.poll_interval_sec)
        continue

      # Decide if ANY token is stale/corrupt, track reasons
      should_resync_pair = False
      resync_reasons: list[str] = []

      for tid in self.token_ids:
        store = self._store(tid)

        last_upd = store.last_update_ts
        stale = (last_upd is None) or ((now - last_upd) > self.cfg.staleness_sec)
        corrupt = self.cfg.resync_on_crossed and store.is_crossed()

        if stale:
          age = "never" if last_upd is None else f"{now - last_upd:.1f}s ago"
          resync_reasons.append(f"token {tid[:16]}... stale (last update: {age})")
          should_resync_pair = True
        if corrupt:
          resync_reasons.append(f"token {tid[:16]}... crossed book")
          should_resync_pair = True

      # Pair-wide throttle + lock so we never run overlapping resyncs
      if should_resync_pair:
        if (now - self._last_pair_resync_ts) >= self.cfg.min_resync_interval_sec:
          if self._resync_lock.acquire(blocking=False):
            try:
              reasons_str = "; ".join(resync_reasons)
              logger.debug(f"Watcher triggered resync: {reasons_str}")
              self._last_pair_resync_ts = now
              for tid in self.token_ids:
                self._rest_resync_token(tid)
              logger.debug("Completed staleness/corruption resync pass.")
            finally:
              self._resync_lock.release()

      self._stop.wait(self.cfg.poll_interval_sec)

  def _store(self, token_id: str) -> OrderBookStore:
    if token_id not in self.stores:
      self.stores[token_id] = OrderBookStore(token_id)
    return self.stores[token_id]

  def _on_open(self, ws_app):
    """Called when WebSocket connection is established."""
    logger.info("WebSocket connected to new CLOB API")
    self._connected = True
    self._reconnect_attempts = 0
    self._last_reconnect_ts = time.time()

    # Initialize PONG timestamp and health status on connect
    with self._pong_lock:
      self._last_pong_ts = time.time()
      self._connection_healthy = True

    # Subscribe to market channel for all tokens
    subscription_msg = {
      "assets_ids": self.token_ids,
      "type": "market",
      "custom_feature_enabled": True,
    }

    try:
      ws_app.send(json.dumps(subscription_msg))
      logger.info(f"Subscribed to {len(self.token_ids)} tokens on market channel")
    except Exception as e:
      logger.error(f"Failed to send subscription: {e}")

    # Start PING heartbeat
    self._schedule_ping()

    # Resync from REST after reconnect
    if self.cfg.resync_on_reconnect:
      threading.Thread(
        target=self._resync_all_tokens,
        daemon=True,
        name="reconnect-resync"
      ).start()

  def _on_message(self, _ws, raw_message: str):
    """Called when a message is received from WebSocket."""
    try:
      # Handle non-JSON messages (like PONG)
      if not raw_message.strip().startswith("{") and not raw_message.strip().startswith("["):
        if raw_message.strip().upper() == "PONG":
          trigger_recovery_callback = False

          with self._pong_lock:
            self._last_pong_ts = time.time()
            # Check if connection was previously unhealthy and now recovered
            if not self._connection_healthy:
              logger.info("Connection recovered - PONG received after timeout")
              self._connection_healthy = True
              trigger_recovery_callback = True

          logger.debug("Received PONG")

          # Trigger callback outside the lock to avoid potential deadlocks
          if trigger_recovery_callback and self._on_connection_healthy:
            try:
              self._on_connection_healthy()
            except Exception as e:
              logger.error(f"Error in connection_healthy callback: {e}", exc_info=True)
        else:
          logger.debug(f"Non-JSON message: {raw_message}")
        return

      data = json.loads(raw_message)

      # Handle both single objects and arrays
      messages = data if isinstance(data, list) else [data]

      for msg in messages:
        if not isinstance(msg, dict):
          continue

        # Route based on event_type field
        event_type = msg.get("event_type")

        if event_type == "book":
          self._handle_book_event(msg)
        elif event_type == "price_change":
          self._handle_price_change_event(msg)
        elif event_type == "tick_size_change":
          logger.debug(f"Tick size change: {msg}")
        elif event_type in ("new_market", "market_resolved"):
          logger.debug(f"Market lifecycle event: {event_type}")
        else:
          logger.debug(f"Unknown event_type: {event_type}, msg: {msg}")

    except json.JSONDecodeError as e:
      logger.warning(f"Failed to parse message: {e}")
    except Exception as e:
      logger.error(f"Error handling message: {e}", exc_info=True)

  def _handle_book_event(self, msg: dict):
    """
    Handle 'book' event - full orderbook snapshot.

    Structure: {event_type, asset_id, market, timestamp, hash, bids[], asks[]}
    """
    token_id = str(msg.get("asset_id", ""))
    if not token_id:
      return

    # logger.info(f"Book event for {token_id[:16]}...")
    # logger.info(f"Snapshot bids: {msg.get('bids', [])}")
    # logger.info(f"Snapshot asks: {msg.get('asks', [])}")
    store = self._store(token_id)
    store.apply_snapshot(msg)

  def _handle_price_change_event(self, msg: dict):
    """
    Handle 'price_change' event - incremental price level updates.

    Structure: {event_type, market, timestamp, price_changes: [{asset_id, price, size, side, ...}]}
    """
    market = msg.get("market", "")
    price_changes = msg.get("price_changes", [])
    if not isinstance(price_changes, list):
      return

    for change in price_changes:
      if not isinstance(change, dict):
        continue

      token_id = str(change.get("asset_id", ""))
      if not token_id:
        continue

      side_str = str(change.get("side", "")).upper()
      price = change.get("price")
      size = change.get("size")

      if not price or size is None:
        continue

      # Map side to bid/ask
      if side_str == "BUY":
        book_side = "bid"
      elif side_str == "SELL":
        book_side = "ask"
      else:
        logger.warning(f"Unknown side: {side_str}")
        continue

      # logger.info(f"Price change: {token_id[:16]}... {book_side} {price} @ {size}")
      store = self._store(token_id)
      store.apply_price_change(side=book_side, price=price, size=size)
 
  def _on_error(self, _ws, error):
    """Called when WebSocket error occurs."""
    logger.error(f"WebSocket error: {error}")

  def _on_close(self, _ws, close_status_code, close_msg):
    """Called when WebSocket connection is closed."""
    logger.info(f"WebSocket closed: {close_status_code} - {close_msg}")
    self._connected = False

    # Cancel ping timer
    if self._ping_timer:
      self._ping_timer.cancel()
      self._ping_timer = None

    # Auto-reconnect if not shutting down
    if not self._shutdown_requested:
      self._schedule_reconnect()

  def _send_ping(self):
    """Send PING message to keep connection alive."""
    if self.ws and self._connected and not self._shutdown_requested:
      try:
        self.ws.send("PING")
        logger.debug("Sent PING")
        self._schedule_ping()
      except Exception as e:
        logger.error(f"Failed to send PING: {e}")

  def _schedule_ping(self):
    """Schedule next PING message."""
    if self._shutdown_requested:
      return
    self._ping_timer = threading.Timer(self.PING_INTERVAL, self._send_ping)
    self._ping_timer.daemon = True
    self._ping_timer.start()

  def _schedule_reconnect(self):
    """Schedule reconnection with exponential backoff."""
    if self._reconnect_timer:
      self._reconnect_timer.cancel()

    delay = min(0.5 * (2 ** self._reconnect_attempts), self._max_reconnect_delay)
    self._reconnect_attempts += 1

    logger.info(f"Reconnecting in {delay:.1f}s (attempt {self._reconnect_attempts})...")

    def reconnect():
      if not self._shutdown_requested:
        self._connect_ws()

    self._reconnect_timer = threading.Timer(delay, reconnect)
    self._reconnect_timer.daemon = True
    self._reconnect_timer.start()

  def _connect_ws(self):
    """Establish WebSocket connection."""
    self.ws = websocket.WebSocketApp(
      self.ws_url,
      on_open=self._on_open,
      on_message=self._on_message,
      on_error=self._on_error,
      on_close=self._on_close
    )

    self._ws_thread = threading.Thread(
      target=self.ws.run_forever,
      kwargs={"ping_interval": 0, "ping_timeout": None},  # We handle PING manually
      daemon=True
    )
    self._ws_thread.start()

  def start(self) -> None:
    """Start the WebSocket client."""
    if self.cfg.rest_seed_on_start:
      logger.info("Seeding orderbooks from REST API...")
      for tid in self.token_ids:
        try:
          full = self.rest._client.get_order_book(tid)
          store = self._store(tid)
          store.apply_snapshot(full)
        except Exception as e:
          logger.error(f"Failed to seed token {tid}: {e}")

    # Set initial reconnect timestamp so grace period applies to first connect
    self._last_reconnect_ts = time.time()

    # Start watcher BEFORE connecting so we can resync even if WS is slow to deliver
    self._watcher_thread = threading.Thread(target=self._watch_staleness_loop, daemon=True)
    self._watcher_thread.start()

    # Start PONG timeout watchdog
    self._pong_watchdog_thread = threading.Thread(target=self._watch_pong_timeout_loop, daemon=True)
    self._pong_watchdog_thread.start()

    # Connect WebSocket
    self._connect_ws()

  def add_tokens(self, new_token_ids: Iterable[str]) -> None:
    """
    Dynamically add new tokens to the subscription.

    This method:
    1. Adds new token IDs to self.token_ids
    2. Seeds orderbooks from REST for new tokens
    3. Subscribes to WebSocket updates for new tokens

    Args:
      new_token_ids: Iterable of token IDs to add
    """
    new_ids = [str(t) for t in new_token_ids if str(t) not in self.token_ids]

    if not new_ids:
      logger.debug("All tokens already subscribed")
      return

    logger.info(f"Adding {len(new_ids)} new tokens to subscription...")

    # Add to token list
    self.token_ids.extend(new_ids)

    # Seed from REST for new tokens
    for token_id in new_ids:
      try:
        full = self.rest._client.get_order_book(token_id)
        store = self._store(token_id)
        store.apply_snapshot(full)
        self._last_resync_ts[token_id] = time.time()
        logger.debug(f"Seeded new token {token_id[:16]}...")
      except Exception as e:
        logger.error(f"Failed to seed new token {token_id}: {e}")

    # Subscribe to new tokens via WebSocket
    if self.ws and self._connected:
      subscribe_msg = {
        "assets_ids": new_ids,
        "operation": "subscribe"
      }
      try:
        self.ws.send(json.dumps(subscribe_msg))
        logger.info(f"Subscribed to {len(new_ids)} new tokens")
      except Exception as e:
        logger.error(f"Failed to subscribe to new tokens: {e}")

  def remove_tokens(self, token_ids_to_remove: Iterable[str]) -> None:
    """
    Dynamically remove tokens from the subscription.

    This method:
    1. Removes token IDs from self.token_ids
    2. Cleans up orderbook stores for removed tokens
    3. Unsubscribes from WebSocket updates for removed tokens

    Args:
      token_ids_to_remove: Iterable of token IDs to remove
    """
    remove_ids = [str(t) for t in token_ids_to_remove if str(t) in self.token_ids]

    if not remove_ids:
      logger.debug("No tokens to remove (already unsubscribed)")
      return

    logger.info(f"Removing {len(remove_ids)} tokens from subscription...")

    # Remove from token list
    self.token_ids = [tid for tid in self.token_ids if tid not in remove_ids]

    # Clean up orderbook stores
    for token_id in remove_ids:
      if token_id in self.stores:
        del self.stores[token_id]
      if token_id in self._last_resync_ts:
        del self._last_resync_ts[token_id]
      logger.debug(f"Cleaned up token {token_id[:16]}...")

    # Unsubscribe from WebSocket
    if self.ws and self._connected:
      unsubscribe_msg = {
        "assets_ids": remove_ids,
        "operation": "unsubscribe"
      }
      try:
        self.ws.send(json.dumps(unsubscribe_msg))
        logger.info(f"Unsubscribed from {len(remove_ids)} tokens")
      except Exception as e:
        logger.error(f"Failed to unsubscribe from tokens: {e}")

  def stop(self) -> None:
    """Stop the WebSocket client."""
    logger.info("Stopping WebSocket client...")
    self._shutdown_requested = True
    self._stop.set()

    # Cancel timers
    if self._ping_timer:
      self._ping_timer.cancel()
      self._ping_timer = None
    if self._reconnect_timer:
      self._reconnect_timer.cancel()
      self._reconnect_timer = None

    # Close WebSocket
    if self.ws:
      try:
        self.ws.close()
      except Exception as e:
        logger.error(f"Error closing WebSocket: {e}")

    # Wait for threads to finish
    if self._watcher_thread and self._watcher_thread.is_alive():
      self._watcher_thread.join(timeout=2.0)
    if self._pong_watchdog_thread and self._pong_watchdog_thread.is_alive():
      self._pong_watchdog_thread.join(timeout=2.0)

    logger.info("WebSocket client stopped")
