"""
WebSocket Trade Client for Polymarket.

Handles authenticated user trade subscriptions via new CLOB WebSocket API.
Uses websocket-client library like WsOrderbookClient for consistency.
"""

import json
import logging
import threading
import time
from typing import Optional, Callable
from dataclasses import dataclass

try:
    import websocket  # websocket-client library
except ImportError:
    raise ImportError(
        "websocket-client required. Install with: pip install websocket-client"
    )

logger = logging.getLogger(__name__)


@dataclass
class WsTradeConfig:
    """Configuration for WebSocket trade client."""
    pong_timeout_sec: float = 30.0  # max time without PONG before connection considered unhealthy


@dataclass
class ClobApiKeyCreds:
    """CLOB API credentials for WebSocket authentication."""
    api_key: str
    secret: str
    passphrase: str


@dataclass
class TradeMessage:
    """Trade message from WebSocket."""
    event_type: str
    payload: dict


class WsTradeClient:
    """
    WebSocket client for authenticated user trade subscriptions.

    Uses new CLOB WebSocket API: wss://ws-subscriptions-clob.polymarket.com/ws/user
    Subscribes to user channel to receive real-time notifications of trade executions.

    Example:
        ```python
        def on_trade(message):
            print(f"Trade: {message.payload}")

        client = WsTradeClient(
            ws_url="wss://ws-subscriptions-clob.polymarket.com/ws/user",
            clob_creds=ClobApiKeyCreds(...),
            on_trade=on_trade
        )
        client.start()
        ```
    """

    DEFAULT_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
    PING_INTERVAL = 10.0  # Polymarket requires PING every 10 seconds

    def __init__(
        self,
        ws_url: str,
        clob_creds: ClobApiKeyCreds,
        on_trade: Callable[[TradeMessage], None],
        cfg: Optional[WsTradeConfig] = None,
        on_status_change: Optional[Callable[[str], None]] = None,
        on_connection_unhealthy: Optional[Callable[[], None]] = None,
        on_connection_healthy: Optional[Callable[[], None]] = None,
        on_order: Optional[Callable[[dict], None]] = None,
    ):
        """
        Initialize WebSocket trade client.

        Args:
            ws_url: WebSocket server URL
            clob_creds: CLOB API credentials for authentication
            on_trade: Callback for trade messages
            cfg: Optional configuration (uses defaults if not provided)
            on_status_change: Optional callback for connection status changes
            on_order: Optional callback for order events (PLACEMENT, UPDATE, CANCELLATION)
        """
        self.ws_url = ws_url
        self.clob_creds = clob_creds
        self.on_trade_callback = on_trade
        self.on_order_callback = on_order
        self.on_status_change_callback = on_status_change
        self._on_connection_unhealthy = on_connection_unhealthy
        self._on_connection_healthy = on_connection_healthy
        self.cfg = cfg or WsTradeConfig()

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
        self._stop = threading.Event()

        logger.info("Initialized WsTradeClient with new CLOB WebSocket API")

    def start(self):
        """Start WebSocket connection and subscribe to trades."""
        logger.info("Starting WebSocket trade client...")

        # Start PONG timeout watchdog
        self._pong_watchdog_thread = threading.Thread(target=self._watch_pong_timeout_loop, daemon=True)
        self._pong_watchdog_thread.start()

        self._connect_ws()
        logger.info("WebSocket trade client started")

    def stop(self):
        """Stop WebSocket connection."""
        logger.info("Stopping WebSocket trade client...")
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

        # Wait for watchdog thread to finish
        if self._pong_watchdog_thread and self._pong_watchdog_thread.is_alive():
            self._pong_watchdog_thread.join(timeout=2.0)

        logger.info("WebSocket trade client stopped")

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

    def _on_open(self, ws_app):
        """
        Callback when WebSocket connection established.
        Authenticates and subscribes to user trade stream.
        """
        logger.info("WebSocket connected to new CLOB user API")
        self._connected = True
        self._reconnect_attempts = 0

        # Initialize PONG timestamp and health status on connect
        with self._pong_lock:
            self._last_pong_ts = time.time()
            self._connection_healthy = True

        # Notify status change
        if self.on_status_change_callback:
            try:
                self.on_status_change_callback("CONNECTED")
            except Exception as e:
                logger.error(f"Error in status change callback: {e}", exc_info=True)

        # Authenticate with CLOB credentials
        auth_msg = {
            "auth": {
                "apiKey": self.clob_creds.api_key,
                "secret": self.clob_creds.secret,
                "passphrase": self.clob_creds.passphrase
            },
            "markets": []  # Subscribe to all markets
        }

        try:
            ws_app.send(json.dumps(auth_msg))
            logger.info("Successfully authenticated and subscribed to user trade stream")
        except Exception as e:
            logger.error(f"Failed to send auth/subscription: {e}", exc_info=True)

        # Start PING heartbeat
        self._schedule_ping()

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

                if event_type == "trade":
                    self._handle_trade_event(msg)
                elif event_type in ("order", "order_update"):
                    logger.info(f"[WS-ORDER] Order event: {event_type} details={msg}")
                    if self.on_order_callback:
                        try:
                            self.on_order_callback(msg)
                        except Exception as e:
                            logger.error(f"Error in order callback: {e}", exc_info=True)
                else:
                    logger.debug(f"Unknown event_type: {event_type}")

        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse message: {e}")
        except Exception as e:
            logger.error(f"Error handling message: {e}", exc_info=True)

    def _handle_trade_event(self, msg: dict):
        """
        Handle 'trade' event - user's trade executions.

        Structure depends on API, but should contain trade details.
        """
        try:
            trade_msg = TradeMessage(
                event_type="trade",
                payload=msg
            )
            self.on_trade_callback(trade_msg)
        except Exception as e:
            logger.error(f"Error in trade callback: {e}", exc_info=True)

    def _on_error(self, _ws, error):
        """Called when WebSocket error occurs."""
        logger.error(f"WebSocket error: {error}")

    def _on_close(self, _ws, close_status_code, close_msg):
        """Called when WebSocket connection is closed."""
        logger.info(f"WebSocket closed: {close_status_code} - {close_msg}")
        self._connected = False

        # Notify status change
        if self.on_status_change_callback:
            try:
                self.on_status_change_callback("DISCONNECTED")
            except Exception as e:
                logger.error(f"Error in status change callback: {e}", exc_info=True)

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

    def _watch_pong_timeout_loop(self):
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

    def stats(self) -> dict:
        """
        Get connection statistics.

        Returns:
            dict: Connection metrics
        """
        return {
            "connected": self._connected,
            "reconnect_attempts": self._reconnect_attempts
        }
