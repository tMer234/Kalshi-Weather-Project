"""Kalshi historical backfill (Phase 0c): settled-market history + candlestick prices.

Two separate historical datasets, both from the public trade-api/v2, both requiring the
same settled-market listing fetch (so both are built from it in one pass rather than two
separate API round-trips):

1. **Resolutions** — /markets?status=settled paginates arbitrarily far back with a
   close-time window; definitions and outcomes flow through the exact same upserts as
   the live collector, so overlap with already-collected data is a no-op.
2. **Quotes (candlestick price bars)** — the live collector's market_snapshots only
   exist from when it started running; /series/{s}/markets/{m}/candlesticks
   reconstructs hourly (or 1-min/daily) OHLC price history for any market, landing in
   market_candles as the historical stand-in for market_snapshots.

`run_kalshi_backfill()` covers both by default (the combined `kalshi-weather backfill
kalshi` command); `include_quotes`/`include_resolutions` let the narrower `backfill
kalshi-quotes` / `backfill kalshi-resolutions` subcommands each run only their half —
mirrors the live `ingest kalshi-quotes`/`ingest kalshi-resolutions` split in
`ingest/kalshi.py`. Market *definitions* are always upserted regardless of either flag
(cheap side effect of the listing fetch both halves need; same precedent as the live
collector).

Candle fetching is resumable: markets that already have bars at the requested period
are skipped, so an interrupted backfill continues where it left off.
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta, timezone

import duckdb

from ..config import SeriesConfig, Settings, StationConfig
from ..kalshi_client import KalshiClient, KalshiError
from .common import RunResult, _record_run, _utcnow
from .kalshi import (
    _MARKET_UPSERT,
    _OUTCOME_UPSERT,
    MarketParseError,
    _number,
    parse_market_row,
)

logger = logging.getLogger(__name__)

# polite gap between candlestick requests (one request per market)
DEFAULT_SLEEP_SECONDS = 0.2

_CANDLE_UPSERT = """
INSERT INTO market_candles (
    ticker, period_end, period_minutes,
    price_open, price_high, price_low, price_close, price_mean,
    yes_bid_close, yes_ask_close, volume, open_interest, pulled_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (ticker, period_minutes, period_end) DO NOTHING
"""


def _unix(dt: datetime) -> int:
    """Naive-UTC timestamp (our storage convention) -> unix seconds."""
    return int(dt.replace(tzinfo=timezone.utc).timestamp())


def _candle_row(candle: dict, ticker: str, period_minutes: int, pulled_at: datetime) -> tuple:
    price = candle.get("price") or {}
    yes_bid = candle.get("yes_bid") or {}
    yes_ask = candle.get("yes_ask") or {}
    return (
        ticker,
        datetime.fromtimestamp(candle["end_period_ts"], tz=timezone.utc).replace(tzinfo=None),
        period_minutes,
        _number(price, "open_dollars"),
        _number(price, "high_dollars"),
        _number(price, "low_dollars"),
        _number(price, "close_dollars"),
        _number(price, "mean_dollars"),
        _number(yes_bid, "close_dollars"),
        _number(yes_ask, "close_dollars"),
        _number(candle, "volume_fp"),
        _number(candle, "open_interest_fp"),
        pulled_at,
    )


def _has_candles(conn: duckdb.DuckDBPyConnection, ticker: str, period_minutes: int) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM market_candles WHERE ticker = ? AND period_minutes = ? LIMIT 1",
            [ticker, period_minutes],
        ).fetchone()
        is not None
    )


def backfill_series(
    client: KalshiClient,
    conn: duckdb.DuckDBPyConnection,
    station: StationConfig,
    series: SeriesConfig,
    start: date,
    end: date,
    include_quotes: bool = True,
    include_resolutions: bool = True,
    period_minutes: int = 60,
    sleep_seconds: float = DEFAULT_SLEEP_SECONDS,
) -> RunResult:
    """Backfill one series' settled markets for obs_dates in [start, end]: definitions
    always, outcomes iff include_resolutions, candles iff include_quotes."""
    result = RunResult(station.station_id, f"kalshi_backfill:{series.ticker}")

    # markets close ~05:00Z the day after obs_date, so a [start, end+2d] close-time
    # window covers the whole obs_date range; obs_date is then filtered exactly
    min_close = _unix(datetime(start.year, start.month, start.day))
    max_close = _unix(datetime(end.year, end.month, end.day) + timedelta(days=2))
    markets = client.get_markets(
        series.ticker,
        status="settled",
        min_close_ts=min_close,
        max_close_ts=max_close,
        max_pages=100,
    )
    result.http_status = 200
    now = _utcnow()

    market_rows, outcome_rows, kept, parse_failures = [], [], [], 0
    for market in markets:
        if market.get("result") not in ("yes", "no"):
            parse_failures += 1
            logger.error(
                "%s: settled market %s has result %r — skipped",
                series.ticker, market.get("ticker"), market.get("result"),
            )
            continue
        try:
            row = parse_market_row(market, station, series, now)
        except MarketParseError as e:
            parse_failures += 1
            logger.error("%s: %s", series.ticker, e)
            continue
        obs_date = row[5]
        if not (start <= obs_date <= end):
            continue
        market_rows.append(row)
        outcome_rows.append(
            (market["ticker"], market["result"], _number(market, "expiration_value"),
             market.get("status"), now)
        )
        kept.append(market)

    if market_rows:
        conn.executemany(_MARKET_UPSERT, market_rows)
    result.rows_upserted = len(market_rows)
    if include_resolutions and outcome_rows:
        conn.executemany(_OUTCOME_UPSERT, outcome_rows)
        result.rows_upserted += len(outcome_rows)

    candle_count = 0
    if include_quotes:
        for market in kept:
            ticker = market["ticker"]
            if _has_candles(conn, ticker, period_minutes):
                continue
            open_ts = market.get("open_time")
            close_ts = market.get("close_time")
            if not open_ts or not close_ts:
                logger.warning("%s: no open/close time — skipping candles", ticker)
                continue
            candles = client.get_candlesticks(
                series.ticker,
                ticker,
                start_ts=int(datetime.fromisoformat(open_ts.replace("Z", "+00:00")).timestamp()),
                end_ts=int(datetime.fromisoformat(close_ts.replace("Z", "+00:00")).timestamp()),
                period_interval=period_minutes,
            )
            pulled_at = _utcnow()
            rows = [_candle_row(c, ticker, period_minutes, pulled_at) for c in candles]
            if rows:
                conn.executemany(_CANDLE_UPSERT, rows)
            candle_count += len(rows)
            time.sleep(sleep_seconds)
        result.rows_upserted += candle_count

    if parse_failures:
        result.error = f"{parse_failures} settled market(s) skipped (see logs)"
    logger.info(
        "%s/%s: backfilled %d markets, %d candle bars for %s..%s",
        station.station_id, series.ticker, len(market_rows), candle_count, start, end,
    )
    return result


def run_kalshi_backfill(
    settings: Settings,
    conn: duckdb.DuckDBPyConnection,
    start: date,
    end: date,
    series_tickers: list[str] | None = None,
    include_quotes: bool = True,
    include_resolutions: bool = True,
    period_minutes: int = 60,
    sleep_seconds: float = DEFAULT_SLEEP_SECONDS,
) -> int:
    """Backfill all (or selected) configured series. Exit-code contract matches the
    live collectors: 0 = at least one series succeeded, 1 = everything failed.

    `include_quotes`/`include_resolutions` default to True (today's combined `backfill
    kalshi` behavior); the `backfill kalshi-quotes`/`backfill kalshi-resolutions`
    subcommands each pass a single flag to run only their half — see
    plans/data_automation_plan.md."""
    if start > end:
        logger.error("start %s is after end %s", start, end)
        return 1
    client = KalshiClient()
    pairs = [
        (st, s)
        for st in settings.stations
        for s in st.kalshi_series
        if series_tickers is None or s.ticker in series_tickers
    ]
    if not pairs:
        logger.error("no configured series matched %r", series_tickers)
        return 1

    ok = 0
    for station, series in pairs:
        started_at = _utcnow()
        conn.execute("BEGIN")
        try:
            result = backfill_series(
                client, conn, station, series, start, end,
                include_quotes=include_quotes,
                include_resolutions=include_resolutions,
                period_minutes=period_minutes,
                sleep_seconds=sleep_seconds,
            )
            conn.execute("COMMIT")
        except (KalshiError, KeyError, ValueError) as e:
            conn.execute("ROLLBACK")
            result = RunResult(
                station.station_id,
                f"kalshi_backfill:{series.ticker}",
                http_status=getattr(e, "status", None),
                error=str(e)[:500],
            )
            logger.error("%s/%s backfill failed: %s", station.station_id, series.ticker, e)
        _record_run(conn, started_at, result)
        if result.ok:
            ok += 1

    if ok == 0:
        logger.error("all %d series failed", len(pairs))
        return 1
    logger.info("kalshi backfill complete: %d/%d series ok", ok, len(pairs))
    return 0
