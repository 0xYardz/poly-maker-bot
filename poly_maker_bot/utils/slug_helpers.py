# polymm/market_data/slug.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal

Underlying = Literal["btc", "eth", "sol"]
Timespan = Literal["5m", "15m", "1h"]

TIMESPAN_TO_SEC: dict[str, int] = {
    "5m": 5 * 60,
    "15m": 15 * 60,
    "1h": 60 * 60,
}

def _parse_iso_dt(s: str) -> datetime:
  # Handles "2025-12-12T20:45:00Z" and isoformat variants.
  s2 = s.replace("Z", "+00:00")
  return datetime.fromisoformat(s2)

def now_utc_unix() -> int:
  return int(datetime.now(timezone.utc).timestamp())


def window_sec_for_timespan(timespan: Timespan) -> int:
  return TIMESPAN_TO_SEC[timespan]


def align_to_window_start(unix_ts: int, window_sec: int) -> int:
  return (unix_ts // window_sec) * window_sec


def slug_for_window(underlying: Underlying, window_start_unix: int, timespan: Timespan) -> str:
  # canonical: btc-updown-15m-<timestamp>
  return f"{underlying}-updown-{timespan}-{int(window_start_unix)}"


def current_window_slug(
    underlying: Underlying,
    timespan: Timespan = "15m",
    *,
    now_unix: int | None = None,
) -> tuple[str, int]:
  t = int(now_unix if now_unix is not None else now_utc_unix())
  ws = window_sec_for_timespan(timespan)
  t0 = align_to_window_start(t, ws)
  return slug_for_window(underlying, t0, timespan), t0


def unix_to_utc_dt(unix_ts: int) -> datetime:
  return datetime.fromtimestamp(int(unix_ts), tz=timezone.utc)
