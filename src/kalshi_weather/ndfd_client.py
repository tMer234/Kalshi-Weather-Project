"""Thin client for NOAA's public NDFD GRIB2 archive (the `noaa-ndfd-pds` S3 bucket).

Historical NDFD forecast grids — the source `backfill nws-grid` reads from. Public,
unauthenticated bucket (`registry.opendata.aws/noaa-ndfd`): no API key, no documented
rate limit, but this client still retries/backs off politely since it's a shared free
resource, same posture as iem_client.py.

Bucket layout (reverse-engineered from a live listing, see docs/runbook.md §2.1):
`wmo/{element}/{yyyy}/{mm}/{dd}/{wmo_prefix}{region}{suffix}_KWBN_{YYYYMMDDHHMM}` — one
WMO-bulletin-wrapped GRIB2 file per (element, issuance). `{region}` is the NDFD sector
(`UZ` = CONUS); `{suffix}` selects resolution/horizon (`98` = full-res day 1-3, the one
this project backfills against — see ingest/ndfd_backfill.py).
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from xml.etree import ElementTree as ET

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

BUCKET_URL = "https://noaa-ndfd-pds.s3.amazonaws.com"
DEFAULT_TIMEOUT = 60
RETRY_ATTEMPTS = 4
RETRY_INITIAL_SECONDS = 2.0
RETRYABLE_STATUSES = {429, 500, 502, 503}

# S3 ListObjectsV2 XML responses are namespaced; every tag lookup needs this prefix.
_S3_NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"


class NDFDError(Exception):
    """A non-retryable (or retry-exhausted) NDFD archive failure."""

    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


class _RetryableHTTPError(Exception):
    def __init__(self, status: int, detail: str):
        super().__init__(f"HTTP {status}: {detail}")
        self.status = status
        self.detail = detail


def issued_time_from_key(key: str) -> datetime:
    """Issuance timestamp from a bucket key's `..._KWBN_YYYYMMDDHHMM` tail (naive UTC)."""
    name = key.rsplit("/", 1)[-1]
    ts = name.rsplit("_", 1)[-1]
    return datetime.strptime(ts, "%Y%m%d%H%M")


class NDFDClient:
    def __init__(
        self,
        user_agent: str,
        base_url: str = BUCKET_URL,
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

    def _get_once(self, url: str, params: dict | None) -> requests.Response:
        r = self.session.get(url, params=params, timeout=self.timeout)
        if r.status_code in RETRYABLE_STATUSES:
            raise _RetryableHTTPError(r.status_code, r.text[:200])
        if not r.ok:
            raise NDFDError(f"GET {url} -> HTTP {r.status_code}: {r.text[:200]}", r.status_code)
        return r

    def _get(self, path: str = "", params: dict | None = None) -> requests.Response:
        url = f"{self.base_url}/{path.lstrip('/')}" if path else f"{self.base_url}/"
        try:
            return self._get_with_retry(url, params)
        except _RetryableHTTPError as e:
            raise NDFDError(f"GET {url} failed after retries: {e.detail}", e.status) from e
        except requests.RequestException as e:
            raise NDFDError(f"GET {url} failed: {e}") from e

    def list_day(self, element_path: str, day: date, region: str, suffix: str) -> list[str]:
        """Bucket keys for one (element, UTC day) matching `*{region}{suffix}_KWBN_*`.

        One directory can hold multiple element sub-products; the filename filter is
        what actually pins down region+resolution (day1-3 full-res `98` vs the others).
        """
        prefix = f"wmo/{element_path}/{day:%Y/%m/%d}/"
        keys: list[str] = []
        token: str | None = None
        while True:
            params = {"list-type": "2", "prefix": prefix}
            if token:
                params["continuation-token"] = token
            resp = self._get(params=params)
            root = ET.fromstring(resp.content)
            for contents in root.findall(f"{_S3_NS}Contents"):
                key = contents.findtext(f"{_S3_NS}Key")
                if key and f"{region}{suffix}_KWBN_" in key.rsplit("/", 1)[-1]:
                    keys.append(key)
            if root.findtext(f"{_S3_NS}IsTruncated") != "true":
                break
            token = root.findtext(f"{_S3_NS}NextContinuationToken")
            if not token:
                break
        return sorted(keys)

    def download(self, key: str) -> bytes:
        return self._get(key).content
