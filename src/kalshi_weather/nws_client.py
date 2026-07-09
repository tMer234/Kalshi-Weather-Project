"""Thin api.weather.gov client.

Handles the API-wide mechanics so callers don't have to:
- mandatory User-Agent identifying app + contact email (NWS rejects anonymous clients)
- retry with exponential backoff starting at ~5s on 429/5xx — 5s is NWS's own documented
  retry guidance for 429s, not a guess
- application/problem+json error bodies parsed into exception messages
- conditional GET via If-Modified-Since so unchanged resources short-circuit to a 304
- no cache-busting query params (NWS returns 400s for them)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://api.weather.gov"
DEFAULT_TIMEOUT = 30
RETRY_ATTEMPTS = 4
RETRY_INITIAL_SECONDS = 5.0  # NWS: retry 429s "after ~5 seconds"
RETRYABLE_STATUSES = {429, 500, 502, 503}


class NWSError(Exception):
    """A non-retryable (or retry-exhausted) NWS API failure."""

    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


class _RetryableHTTPError(Exception):
    def __init__(self, status: int, detail: str):
        super().__init__(f"HTTP {status}: {detail}")
        self.status = status
        self.detail = detail


@dataclass(frozen=True)
class NWSResponse:
    status: int
    payload: dict[str, Any] | None  # None on 304 Not Modified
    last_modified: str | None

    @property
    def not_modified(self) -> bool:
        return self.status == 304


def _problem_detail(response: requests.Response) -> str:
    """Extract a human-readable message from an application/problem+json error body."""
    try:
        body = response.json()
        return str(body.get("detail") or body.get("title") or response.text[:200])
    except ValueError:
        return response.text[:200]


class NWSClient:
    def __init__(
        self,
        user_agent: str,
        base_url: str = BASE_URL,
        timeout: int = DEFAULT_TIMEOUT,
        retry_initial_seconds: float = RETRY_INITIAL_SECONDS,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": user_agent, "Accept": "application/geo+json, application/ld+json"}
        )
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

    def get(
        self,
        path_or_url: str,
        params: dict[str, Any] | None = None,
        if_modified_since: str | None = None,
    ) -> NWSResponse:
        url = (
            path_or_url
            if path_or_url.startswith("http")
            else f"{self.base_url}/{path_or_url.lstrip('/')}"
        )
        try:
            return self._get_with_retry(url, params, if_modified_since)
        except _RetryableHTTPError as e:
            raise NWSError(f"GET {url} failed after retries: {e.detail}", e.status) from e
        except requests.RequestException as e:
            raise NWSError(f"GET {url} failed: {e}") from e

    def _get_once(
        self, url: str, params: dict[str, Any] | None, if_modified_since: str | None
    ) -> NWSResponse:
        headers = {"If-Modified-Since": if_modified_since} if if_modified_since else {}
        r = self.session.get(url, params=params, headers=headers, timeout=self.timeout)
        if r.status_code == 304:
            return NWSResponse(304, None, if_modified_since)
        if r.status_code in RETRYABLE_STATUSES:
            raise _RetryableHTTPError(r.status_code, _problem_detail(r))
        if not r.ok:
            raise NWSError(
                f"GET {url} -> HTTP {r.status_code}: {_problem_detail(r)}", r.status_code
            )
        return NWSResponse(r.status_code, r.json(), r.headers.get("Last-Modified"))

    # --- endpoint wrappers ------------------------------------------------

    @staticmethod
    def _coord(value: float) -> str:
        # <=4 decimals required; trailing zeros trigger a redirect, so trim them
        return f"{value:.4f}".rstrip("0").rstrip(".")

    def get_points(self, lat: float, lon: float) -> dict[str, Any]:
        resp = self.get(f"/points/{self._coord(lat)},{self._coord(lon)}")
        assert resp.payload is not None
        return resp.payload

    def get_grid_data(self, grid_data_url: str, if_modified_since: str | None = None) -> NWSResponse:
        """Raw numeric gridpoint layers — the ingestion target for forecasts."""
        return self.get(grid_data_url, if_modified_since=if_modified_since)

    def get_forecast_hourly(self, hourly_url: str, if_modified_since: str | None = None) -> NWSResponse:
        return self.get(hourly_url, if_modified_since=if_modified_since)

    def get_nearby_stations(self, grid_id: str, x: int, y: int) -> dict[str, Any]:
        resp = self.get(f"/gridpoints/{grid_id}/{x},{y}/stations")
        assert resp.payload is not None
        return resp.payload

    def get_latest_observation(self, station_id: str) -> dict[str, Any]:
        resp = self.get(f"/stations/{station_id}/observations/latest")
        assert resp.payload is not None
        return resp.payload

    def get_observations(
        self, station_id: str, start: str | None = None, end: str | None = None
    ) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        resp = self.get(f"/stations/{station_id}/observations", params=params or None)
        assert resp.payload is not None
        return resp.payload

    def get_cli_products(self, wfo_id: str, limit: int | None = None) -> list[dict[str, Any]]:
        """List recent Daily Climate Report products issued by a WFO (newest first).

        NB this endpoint 400s on a ?limit= query param (verified live), so limiting is
        done client-side on the returned listing.
        """
        resp = self.get(f"/products/types/CLI/locations/{wfo_id}")
        assert resp.payload is not None
        products = resp.payload.get("@graph", [])
        return products[:limit] if limit else products

    def get_product(self, product_id: str) -> dict[str, Any]:
        """Fetch one text product, including productText and issuanceTime."""
        resp = self.get(f"/products/{product_id}")
        assert resp.payload is not None
        return resp.payload
