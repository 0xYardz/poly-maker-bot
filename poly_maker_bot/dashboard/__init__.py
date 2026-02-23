"""Dashboard HTTP server with SSE real-time updates."""
from __future__ import annotations

import json
import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from poly_maker_bot.dashboard.data_collector import DashboardDataCollector

logger = logging.getLogger(__name__)

_TEMPLATE_DIR = Path(__file__).parent / "templates"


class _Handler(BaseHTTPRequestHandler):
    """HTTP handler serving the dashboard HTML, JSON API, and SSE stream."""

    collector: Optional[DashboardDataCollector] = None

    def do_GET(self):  # noqa: N802
        path = self.path.split("?")[0]
        if path in ("/", "/index.html"):
            self._serve_html()
        elif path == "/api/state":
            self._serve_json()
        elif path == "/api/stream":
            self._serve_sse()
        else:
            self.send_error(404)

    # suppress per-request logging
    def log_request(self, code="-", size="-"):
        pass

    def _serve_html(self):
        html_path = _TEMPLATE_DIR / "index.html"
        try:
            content = html_path.read_bytes()
        except FileNotFoundError:
            self.send_error(500, "Dashboard template not found")
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def _serve_json(self):
        data = self.collector.collect_full_snapshot()
        body = json.dumps(data).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _serve_sse(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        while True:
            try:
                data = self.collector.collect_realtime_snapshot()
                payload = f"data: {json.dumps(data)}\n\n"
                self.wfile.write(payload.encode())
                self.wfile.flush()
                time.sleep(2.0)
            except (BrokenPipeError, ConnectionResetError, OSError):
                break


class DashboardServer:
    """Manages the dashboard HTTP server in a daemon thread."""

    def __init__(
        self,
        bot,
        db,
        host: str = "0.0.0.0",
        port: int = 8080,
    ):
        from poly_maker_bot.dashboard.data_collector import DashboardDataCollector

        self.host = host
        self.port = port
        self.collector = DashboardDataCollector(bot, db)
        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        _Handler.collector = self.collector
        self._server = ThreadingHTTPServer((self.host, self.port), _Handler)
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="dashboard-http",
            daemon=True,
        )
        self._thread.start()
        logger.info("Dashboard running at http://%s:%d", self.host, self.port)

    def stop(self) -> None:
        if self._server:
            self._server.shutdown()
            logger.info("Dashboard server stopped")
