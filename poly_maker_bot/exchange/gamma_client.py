# poly_maker_bot/exchange/gamma_client.py
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from poly_maker_bot.config import ExchangeConfig

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GammaEvent:
  id: str
  slug: str
  title: str
  start_date: Optional[str]
  end_date: Optional[str]
  raw: dict[str, Any]


class GammaClient:
  def __init__(self, cfg: ExchangeConfig, timeout_sec: float = 10.0):
    self.base = cfg.gamma_api_url.rstrip("/")
    self.timeout_sec = timeout_sec

  def _get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
    url = f"{self.base}{path}"
    if params:
      qs = urlencode({k: v for k, v in params.items() if v is not None}, doseq=True)
      url = f"{url}?{qs}"

    headers = {
      "Accept": "application/json",
      "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
      ),
      "Accept-Language": "en-US,en;q=0.9",
      "Connection": "close",
    }

    req = Request(url, headers=headers)
    with urlopen(req, timeout=self.timeout_sec) as resp:
      body = resp.read().decode("utf-8")
      return json.loads(body)

  def get_event_by_slug(self, slug: str) -> GammaEvent:
    data = self._get_json(f"/events/slug/{slug}")
    return GammaEvent(
      id=str(data.get("id", "")),
      slug=str(data.get("slug", slug)),
      title=str(data.get("title", "")),
      start_date=data.get("startDate") or data.get("start_date"),
      end_date=data.get("endDate") or data.get("end_date"),
      raw=data,
    )

  def get_events(
    self,
    *,
    slug: list[str] | None = None,
    limit: int = 50,
    offset: int = 0,
    start_date_min: str | None = None,
    start_date_max: str | None = None,
    closed: bool | None = None,
  ) -> list[GammaEvent]:
    params: dict[str, Any] = {
      "limit": limit,
      "offset": offset,
      "closed": closed,
      "start_date_min": start_date_min,
      "start_date_max": start_date_max,
    }
    if slug:
      # Gamma supports slug as a query param (array) per docs.
      params["slug"] = slug

    data = self._get_json("/events", params=params)
    if not isinstance(data, list):
      raise RuntimeError(f"Unexpected /events response type: {type(data)}")

    out: list[GammaEvent] = []
    for e in data:
      out.append(GammaEvent(
        id=str(e.get("id", "")),
        slug=str(e.get("slug", "")),
        title=str(e.get("title", "")),
        start_date=e.get("startDate") or e.get("start_date"),
        end_date=e.get("endDate") or e.get("end_date"),
        raw=e,
      ))
    return out

  def search_events_by_slug_prefix_around(
    self,
    *,
    slug_prefix: str,
    around_unix: int,
    window_sec: int = 15 * 60,
    limit: int = 50,
    closed: bool | None = None,
  ) -> list[GammaEvent]:
    # Gamma list endpoint supports time filters. We'll query a small range around `around_unix`.
    # Use ISO 8601 UTC for start_date_min/max (docs show date-time strings).
    min_dt = datetime.fromtimestamp(around_unix - 2 * window_sec, tz=timezone.utc)
    max_dt = datetime.fromtimestamp(around_unix + 2 * window_sec, tz=timezone.utc)

    # Gamma doesn't (reliably) support prefix search directly, so we:
    # 1) Pull events in a small time window
    # 2) Filter by slug prefix locally
    events = self.get_events(
      limit=limit,
      offset=0,
      start_date_min=min_dt.isoformat(),
      start_date_max=max_dt.isoformat(),
      closed=closed,
    )
    return [e for e in events if e.slug.startswith(slug_prefix)]
