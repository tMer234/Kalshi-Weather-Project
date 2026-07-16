"""Kalshi market-data ingestion (Phase 0b): market definitions, quote snapshots, outcomes.

Single idempotent run, same contract as nws.py: fetch, upsert, exit. Per-series
failures are isolated — one series' error is logged to ingest_runs and never aborts the
others. Each (station, series, endpoint) pass runs in its own transaction.

Idempotency semantics per table:
- markets:          update-in-place (status/close_time evolve; strikes never do)
- market_snapshots: append-only, one row per market per pass; PK (ticker, snapshot_time)
                    makes a same-instant replay a no-op
- market_outcomes:  insert-once (a finalized result must never change; a conflicting
                    re-settlement would be a data incident, not an update)

`run_kalshi_ingest()` covers both collectors by default (the combined `kalshi-weather
ingest kalshi` command); `include_quotes`/`include_resolutions` let the narrower
`ingest kalshi-quotes` / `ingest kalshi-resolutions` subcommands each run only their
half, since quotes want a tight cadence (the cron interval IS the quote-history
resolution) while resolutions only change once per obs_date — see
plans/data_automation_plan.md.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime
from typing import Any

import duckdb

from ..config import SeriesConfig, Settings, StationConfig
from ..kalshi_client import KalshiClient, KalshiError
from .common import RunResult, _record_run, _utcnow

logger = logging.getLogger(__name__)

# statuses collected per pass: 'open' feeds definitions + quote snapshots; 'settled'
# feeds outcomes (and backfills definitions for markets that settled between passes).
# How many settled markets to look back over per series — offices list ~6/day, so 60
# covers well over a week of missed runs.
SETTLED_LOOKBACK = 60

VALID_STRIKE_TYPES = {"greater", "less", "between"}

# event ticker date component, e.g. 'KXHIGHNY-26JUL12' -> ('26', 'JUL', '12')
_EVENT_DATE_RE = re.compile(r"-(\d{2})([A-Z]{3})(\d{2})$")
_MONTHS = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

_MARKET_UPSERT = """
INSERT INTO markets (
    ticker, event_ticker, series_ticker, station_id, variable, obs_date,
    strike_type, floor_strike, cap_strike, title, subtitle,
    open_time, close_time, expected_expiration_time, status,
    first_seen_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (ticker) DO UPDATE SET
    close_time = excluded.close_time,
    expected_expiration_time = excluded.expected_expiration_time,
    status = excluded.status,
    updated_at = excluded.updated_at
"""

_SNAPSHOT_UPSERT = """
INSERT INTO market_snapshots (
    ticker, snapshot_time, yes_bid, yes_ask, no_bid, no_ask, last_price,
    yes_bid_size, yes_ask_size, volume, volume_24h, open_interest, liquidity, status
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (ticker, snapshot_time) DO NOTHING
"""

_OUTCOME_UPSERT = """
INSERT INTO market_outcomes (ticker, result, expiration_value, status, recorded_at)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT (ticker) DO NOTHING
"""


class MarketParseError(Exception):
    """A market payload that doesn't match the expected weather-contract shape."""


def parse_event_date(event_ticker: str) -> date:
    """'KXHIGHNY-26JUL12' -> date(2026, 7, 12) — the contract's observation day."""
    m = _EVENT_DATE_RE.search(event_ticker)
    if not m:
        raise MarketParseError(f"event ticker {event_ticker!r} has no parseable date suffix")
    yy, mon, dd = m.groups()
    try:
        return date(2000 + int(yy), _MONTHS[mon], int(dd))
    except (KeyError, ValueError) as e:
        raise MarketParseError(f"event ticker {event_ticker!r}: bad date component: {e}") from e


def _number(payload: dict[str, Any], key: str) -> float | None:
    """Kalshi serialises prices/sizes as decimal strings ('0.0100', '1642.25')."""
    raw = payload.get(key)
    if raw is None or raw == "":
        return None
    return float(raw)


def parse_market_row(
    market: dict[str, Any],
    station: StationConfig,
    series: SeriesConfig,
    now: datetime,
) -> tuple:
    """Validate one API market object into a `markets` row. Raises MarketParseError."""
    ticker = market.get("ticker")
    if not ticker:
        raise MarketParseError(f"market object has no ticker: {market!r:.120}")
    strike_type = market.get("strike_type")
    floor_strike = market.get("floor_strike")
    cap_strike = market.get("cap_strike")
    if strike_type not in VALID_STRIKE_TYPES:
        raise MarketParseError(f"{ticker}: unsupported strike_type {strike_type!r}")
    if strike_type in ("greater", "between") and floor_strike is None:
        raise MarketParseError(f"{ticker}: strike_type {strike_type} but floor_strike is null")
    if strike_type in ("less", "between") and cap_strike is None:
        raise MarketParseError(f"{ticker}: strike_type {strike_type} but cap_strike is null")
    obs_date = parse_event_date(market["event_ticker"])
    return (
        ticker,
        market["event_ticker"],
        series.ticker,
        station.station_id,
        series.variable,
        obs_date,
        strike_type,
        floor_strike,
        cap_strike,
        market.get("title"),
        market.get("subtitle"),
        _timestamp(market.get("open_time")),
        _timestamp(market.get("close_time")),
        _timestamp(market.get("expected_expiration_time")),
        market.get("status"),
        now,
        now,
    )


def _timestamp(raw: str | None) -> datetime | None:
    if not raw:
        return None
    # API timestamps are UTC ('...Z'); store naive-UTC like every other table
    return datetime.fromisoformat(raw.replace("Z", "+00:00")).replace(tzinfo=None)


def _snapshot_row(market: dict[str, Any], snapshot_time: datetime) -> tuple:
    return (
        market["ticker"],
        snapshot_time,
        _number(market, "yes_bid_dollars"),
        _number(market, "yes_ask_dollars"),
        _number(market, "no_bid_dollars"),
        _number(market, "no_ask_dollars"),
        _number(market, "last_price_dollars"),
        _number(market, "yes_bid_size_fp"),
        _number(market, "yes_ask_size_fp"),
        _number(market, "volume_fp"),
        _number(market, "volume_24h_fp"),
        _number(market, "open_interest_fp"),
        _number(market, "liquidity_dollars"),
        market.get("status"),
    )


# --- per-endpoint collectors -------------------------------------------------


def ingest_open_markets(
    client: KalshiClient,
    conn: duckdb.DuckDBPyConnection,
    station: StationConfig,
    series: SeriesConfig,
) -> RunResult:
    """Upsert definitions and append one quote snapshot for every open market."""
    result = RunResult(station.station_id, f"kalshi_markets:{series.ticker}")
    markets = client.get_markets(series.ticker, status="open")
    result.http_status = 200
    now = _utcnow()

    market_rows, snapshot_rows, parse_failures = [], [], 0
    for market in markets:
        try:
            market_rows.append(parse_market_row(market, station, series, now))
        except MarketParseError as e:
            # fail loudly, never guess: log + skip so one malformed contract can't
            # poison the batch; the next pass retries it
            parse_failures += 1
            logger.error("%s: %s", series.ticker, e)
            continue
        snapshot_rows.append(_snapshot_row(market, now))

    if market_rows:
        conn.executemany(_MARKET_UPSERT, market_rows)
        conn.executemany(_SNAPSHOT_UPSERT, snapshot_rows)
    result.rows_upserted = len(market_rows) + len(snapshot_rows)
    if parse_failures:
        result.error = f"{parse_failures} market(s) failed to parse (see logs)"
    logger.info(
        "%s/%s: %d open markets, %d snapshots at %s",
        station.station_id, series.ticker, len(market_rows), len(snapshot_rows), now,
    )
    return result


def ingest_settled_markets(
    client: KalshiClient,
    conn: duckdb.DuckDBPyConnection,
    station: StationConfig,
    series: SeriesConfig,
) -> RunResult:
    """Record outcomes for recently settled markets (and backfill their definitions)."""
    result = RunResult(station.station_id, f"kalshi_outcomes:{series.ticker}")
    markets = client.get_markets(series.ticker, status="settled")[:SETTLED_LOOKBACK]
    result.http_status = 200
    now = _utcnow()

    market_rows, outcome_rows, parse_failures = [], [], 0
    for market in markets:
        result_str = market.get("result")
        if result_str not in ("yes", "no"):
            parse_failures += 1
            logger.error(
                "%s: settled market %s has result %r — skipped",
                series.ticker, market.get("ticker"), result_str,
            )
            continue
        try:
            market_rows.append(parse_market_row(market, station, series, now))
        except MarketParseError as e:
            parse_failures += 1
            logger.error("%s: %s", series.ticker, e)
            continue
        outcome_rows.append(
            (
                market["ticker"],
                result_str,
                _number(market, "expiration_value"),
                market.get("status"),
                now,
            )
        )

    if market_rows:
        conn.executemany(_MARKET_UPSERT, market_rows)
        conn.executemany(_OUTCOME_UPSERT, outcome_rows)
    result.rows_upserted = len(market_rows) + len(outcome_rows)
    if parse_failures:
        result.error = f"{parse_failures} settled market(s) skipped (see logs)"
    logger.info(
        "%s/%s: %d settled markets recorded",
        station.station_id, series.ticker, len(outcome_rows),
    )
    return result


# --- orchestration -----------------------------------------------------------


def run_kalshi_ingest(
    settings: Settings,
    conn: duckdb.DuckDBPyConnection,
    include_quotes: bool = True,
    include_resolutions: bool = True,
) -> int:
    """Run one Kalshi collection pass. Returns a process exit code:

    0 — at least one (station, series) succeeded end to end
    1 — every series failed, or no station has kalshi_series configured

    `include_quotes`/`include_resolutions` default to True (today's combined `ingest
    kalshi` behavior); the `ingest kalshi-quotes`/`ingest kalshi-resolutions`
    subcommands each pass a single flag to run only their half — see
    plans/data_automation_plan.md. Note both collectors independently upsert market
    *definitions* as a side effect (idempotent/update-in-place), so leaving that
    overlap in place after the split is harmless.
    """
    client = KalshiClient()
    pairs = [(st, s) for st in settings.stations for s in st.kalshi_series]
    if not pairs:
        logger.error("no station has kalshi_series configured — nothing to collect")
        return 1

    collectors = []
    if include_quotes:
        collectors.append(ingest_open_markets)
    if include_resolutions:
        collectors.append(ingest_settled_markets)

    series_ok = 0
    for station, series in pairs:
        results = []
        for collector in collectors:
            started_at = _utcnow()
            conn.execute("BEGIN")
            try:
                result = collector(client, conn, station, series)
                conn.execute("COMMIT")
            except (KalshiError, KeyError, ValueError) as e:
                conn.execute("ROLLBACK")
                result = RunResult(
                    station.station_id,
                    f"kalshi:{series.ticker}",
                    http_status=getattr(e, "status", None),
                    error=str(e)[:500],
                )
                logger.error("%s/%s failed: %s", station.station_id, series.ticker, e)
            results.append(result)
            _record_run(conn, started_at, result)
        if any(r.ok for r in results):
            series_ok += 1

    if series_ok == 0:
        logger.error("all %d series failed", len(pairs))
        return 1
    logger.info("kalshi ingest complete: %d/%d series ok", series_ok, len(pairs))
    return 0
