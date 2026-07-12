"""Thin Kalshi trade-api/v2 client (public market data only — no API key required).

Mirrors nws_client.py's mechanics where they apply:
- retry with exponential backoff on 429/5xx (Kalshi rate-limits aggressively; the basic
  public tier allows ~10 reads/sec, so a polite collector rarely trips it)
- JSON error bodies ({"error": {"code", "message"}}) parsed into exception messages
- cursor-based pagination handled internally so callers get complete listings

Deliberately NOT mirrored: conditional GET — Kalshi does not serve Last-Modified/ETag
on market listings, so every poll is a full fetch by design.

Authenticated endpoints (orders, portfolio) are out of scope until Phase 10; when they
arrive they belong in a separate authenticated client, keyed from .env, never from git.
"""

from __future__ import annotations

import logging
from typing import Any

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
DEFAULT_TIMEOUT = 30
RETRY_ATTEMPTS = 4
RETRY_INITIAL_SECONDS = 1.0
RETRYABLE_STATUSES = {429, 500, 502, 503}
# markets endpoint hard-caps limit at 1000; 200/page is plenty for 6 series x ~12 markets
PAGE_LIMIT = 200
MAX_PAGES = 10


class KalshiError(Exception):
    """A non-retryable (or retry-exhausted) Kalshi API failure."""

    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


class _RetryableHTTPError(Exception):
    def __init__(self, status: int, detail: str):
        super().__init__(f"HTTP {status}: {detail}")
        self.status = status
        self.detail = detail


def _error_detail(response: requests.Response) -> str:
    """Extract a human-readable message from a Kalshi JSON error body."""
    try:
        body = response.json()
        err = body.get("error") or {}
        return str(err.get("message") or body.get("message") or response.text[:200])
    except ValueError:
        return response.text[:200]


class KalshiClient:
    def __init__(
        self,
        base_url: str = BASE_URL,
        timeout: int = DEFAULT_TIMEOUT,
        retry_initial_seconds: float = RETRY_INITIAL_SECONDS,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        # instance-level retry so tests can zero out the wait
        self._get_with_retry = retry(
            retry=retry_if_exception_type((_RetryableHTTPError, requests.ConnectionError)),
            wait=wait_exponential(multiplier=retry_initial_seconds, min=retry_initial_seconds),
            stop=stop_after_attempt(RETRY_ATTEMPTS),
            before_sleep=lambda rs: logger.warning(
                "retrying after %s (attempt %d)",
                rs.outcome.exception() if rs.outcome else "unknown error",
                rs.attempt_number,
            ),
            reraise=True,
        )(self._get_once)

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.base_url}/{path.lstrip('/')}"
        try:
            return self._get_with_retry(url, params)
        except _RetryableHTTPError as e:
            raise KalshiError(f"GET {url} failed after retries: {e.detail}", e.status) from e
        except requests.RequestException as e:
            raise KalshiError(f"GET {url} failed: {e}") from e

    def _get_once(self, url: str, params: dict[str, Any] | None) -> dict[str, Any]:
        r = self.session.get(url, params=params, timeout=self.timeout)
        if r.status_code in RETRYABLE_STATUSES:
            raise _RetryableHTTPError(r.status_code, _error_detail(r))
        if not r.ok:
            raise KalshiError(
                f"GET {url} -> HTTP {r.status_code}: {_error_detail(r)}", r.status_code
            )
        return r.json()

    # --- endpoint wrappers ------------------------------------------------

    def get_markets(
        self,
        series_ticker: str,
        status: str | None = None,
        min_close_ts: int | None = None,
        max_close_ts: int | None = None,
        page_limit: int = PAGE_LIMIT,
        max_pages: int = MAX_PAGES,
    ) -> list[dict[str, Any]]:
        """All markets for a series (optionally filtered by status), pagination resolved.

        `status` values the API accepts: 'unopened', 'open', 'closed', 'settled'.
        NB listing responses report open markets as status='active' even though the
        query param is 'open' — don't compare the two.
        `min_close_ts`/`max_close_ts` (unix seconds) window on the market close time —
        the backfill's date-range filter.
        """
        markets: list[dict[str, Any]] = []
        cursor: str | None = None
        for _ in range(max_pages):
            params: dict[str, Any] = {"series_ticker": series_ticker, "limit": page_limit}
            if status:
                params["status"] = status
            if min_close_ts is not None:
                params["min_close_ts"] = min_close_ts
            if max_close_ts is not None:
                params["max_close_ts"] = max_close_ts
            if cursor:
                params["cursor"] = cursor
            payload = self.get("/markets", params)
            markets.extend(payload.get("markets", []))
            cursor = payload.get("cursor")
            if not cursor:
                return markets
        logger.warning(
            "%s status=%s: pagination stopped at %d pages with cursor still set",
            series_ticker, status, max_pages,
        )
        return markets

    def get_candlesticks(
        self,
        series_ticker: str,
        market_ticker: str,
        start_ts: int,
        end_ts: int,
        period_interval: int = 60,
    ) -> list[dict[str, Any]]:
        """Historical OHLC bars for one market — the price-history BACKFILL source.

        `period_interval` is bar length in minutes: 1, 60, or 1440. The API caps one
        request at 5000 bars; hourly bars over a ~2-day weather market is ~40, so no
        pagination is needed here. Prices arrive as nested *_dollars string fields
        (verified live 2026-07-11), same convention as market listings.
        """
        payload = self.get(
            f"/series/{series_ticker}/markets/{market_ticker}/candlesticks",
            {"start_ts": start_ts, "end_ts": end_ts, "period_interval": period_interval},
        )
        return payload.get("candlesticks", [])
