"""Thin Iowa Environmental Mesonet (IEM) AFOS archive client — the NWS backfill source.

IEM archives every NWS text product back decades; CLI (Daily Climate Report) products
are listed per (pil, UTC day) and fetched verbatim, so backfilled bulletins flow through
the exact same cli_parser.py as live ones. Verified live 2026-07-11: IEM's stored text
is byte-compatible with api.weather.gov's productText apart from a leading AFOS sequence
number line, which the parser already ignores.

IEM asks for a descriptive User-Agent and gentle request rates on bulk pulls — the
backfill loop sleeps between requests (see nws_backfill.py); this client only handles
transport.
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

BASE_URL = "https://mesonet.agron.iastate.edu"
DEFAULT_TIMEOUT = 30
RETRY_ATTEMPTS = 4
RETRY_INITIAL_SECONDS = 2.0
RETRYABLE_STATUSES = {429, 500, 502, 503}


class IEMError(Exception):
    """A non-retryable (or retry-exhausted) IEM API failure."""

    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


class _RetryableHTTPError(Exception):
    def __init__(self, status: int, detail: str):
        super().__init__(f"HTTP {status}: {detail}")
        self.status = status
        self.detail = detail


class IEMClient:
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
        self.session.headers.update({"User-Agent": user_agent})
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

    def _get(self, path: str, params: dict[str, Any] | None = None) -> requests.Response:
        url = f"{self.base_url}/{path.lstrip('/')}"
        try:
            return self._get_with_retry(url, params)
        except _RetryableHTTPError as e:
            raise IEMError(f"GET {url} failed after retries: {e.detail}", e.status) from e
        except requests.RequestException as e:
            raise IEMError(f"GET {url} failed: {e}") from e

    def _get_once(self, url: str, params: dict[str, Any] | None) -> requests.Response:
        r = self.session.get(url, params=params, timeout=self.timeout)
        if r.status_code in RETRYABLE_STATUSES:
            raise _RetryableHTTPError(r.status_code, r.text[:200])
        if not r.ok:
            raise IEMError(f"GET {url} -> HTTP {r.status_code}: {r.text[:200]}", r.status_code)
        return r

    # --- endpoint wrappers ------------------------------------------------

    def list_products(self, pil: str, date_utc: str) -> list[dict[str, Any]]:
        """Products issued for `pil` (e.g. 'CLINYC') on a UTC calendar day ('YYYY-MM-DD').

        Returns entries with 'product_id' (e.g. '202607100625-KOKX-CDUS41-CLINYC',
        UTC issuance prefix) and 'entered' (issuance as ISO8601). NB the UTC day of a
        CLI *final* is the morning AFTER its observation day.
        """
        r = self._get("/api/1/nws/afos/list.json", {"pil": pil, "date": date_utc})
        try:
            return r.json().get("data", [])
        except ValueError as e:
            raise IEMError(f"list.json for {pil} {date_utc}: invalid JSON: {e}") from e

    def get_product_text(self, product_id: str) -> str:
        """Raw product text, same bulletin format api.weather.gov serves."""
        return self._get(f"/api/1/nwstext/{product_id}").text
