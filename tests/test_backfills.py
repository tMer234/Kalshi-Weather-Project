"""Backfill tests: IEM climate-report history and Kalshi settled-market/candle history.

Core guarantees under test: backfilled rows carry their source provenance, can never
regress values the live collector already landed, and re-running a backfill is a no-op.
All HTTP is mocked from recorded real IEM/Kalshi fixtures — no live calls.
"""

import json
import logging
from datetime import date
from pathlib import Path

import duckdb
import pytest
import responses
from responses import matchers

from kalshi_weather import db
from kalshi_weather.config import SeriesConfig, Settings, StationConfig
from kalshi_weather.ingest.kalshi_backfill import run_kalshi_backfill
from kalshi_weather.ingest.nws import CLIMATE_UPSERT
from kalshi_weather.ingest.nws_backfill import run_nws_backfill
from kalshi_weather.resolve import ensure_station_stubs

FIXTURES = Path(__file__).parent / "fixtures"
IEM = "https://mesonet.agron.iastate.edu"
KALSHI = "https://api.elections.kalshi.com/trade-api/v2"
LOGGER = logging.getLogger(__name__)

NYC = StationConfig(
    station_id="nyc",
    display_name="New York City (Central Park)",
    lat=40.7789,
    lon=-73.9692,
    timezone="America/New_York",
    obs_station_id="KNYC",
    cli_site_name="CENTRAL PARK",
    kalshi_series=(SeriesConfig(ticker="KXHIGHNY", variable="max_temp"),),
)


@pytest.fixture
def settings(tmp_path):
    return Settings(
        contact_email="test@example.com",
        app_name="kalshi-weather-test",
        duckdb_path=tmp_path / "test.duckdb",
        stations=[NYC],
    )


@pytest.fixture
def conn(settings):
    connection = db.connect(settings.duckdb_path)
    yield connection
    connection.close()


def _scalar(conn: duckdb.DuckDBPyConnection, sql: str):
    row = conn.execute(sql).fetchone()
    assert row is not None
    return row[0]


# --- NWS / IEM ---------------------------------------------------------------

IEM_LIST = json.loads((FIXTURES / "iem" / "list_clinyc_20260710.json").read_text())
IEM_TEXT = (FIXTURES / "iem" / "text_clinyc_202607100625.txt").read_text()
FINAL_ID = "202607100625-KOKX-CDUS41-CLINYC"


def _register_iem(days_payload: dict[str, list]):
    """days_payload: UTC date string -> listing entries for that day."""
    for day, entries in days_payload.items():
        responses.get(
            f"{IEM}/api/1/nws/afos/list.json",
            json={"data": entries},
            match=[
                matchers.query_param_matcher({"pil": "CLINYC", "date": day})
            ],
        )
    for entries in days_payload.values():
        for e in entries:
            responses.get(f"{IEM}/api/1/nwstext/{e['product_id']}", body=IEM_TEXT)


@responses.activate
def test_nws_backfill_lands_with_iem_source(settings, conn):
    # backfilling obs_date 2026-07-09 lists UTC days Jul 9 and Jul 10; the final for
    # Jul 9 appears on Jul 10 (fixture recorded live)
    final = [e for e in IEM_LIST["data"] if e["product_id"] == FINAL_ID]
    _register_iem({"2026-07-09": [], "2026-07-10": final})

    assert run_nws_backfill(
        settings, conn, date(2026, 7, 9), date(2026, 7, 9), sleep_seconds=0
    ) == 0

    rows = conn.execute(
        "SELECT variable, value, source, product_id FROM climate_reports "
        "WHERE obs_date = DATE '2026-07-09' ORDER BY variable"
    ).fetchall()
    assert [(r[0], r[1]) for r in rows] == [
        ("max_temp", 82.0), ("min_temp", 71.0), ("precip", 0.05), ("snowfall", 0.0)
    ]
    assert all(r[2] == "iem_afos" and r[3] == FINAL_ID for r in rows)


@responses.activate
def test_nws_backfill_rerun_is_noop(settings, conn):
    final = [e for e in IEM_LIST["data"] if e["product_id"] == FINAL_ID]
    _register_iem({"2026-07-09": [], "2026-07-10": final})
    assert run_nws_backfill(
        settings, conn, date(2026, 7, 9), date(2026, 7, 9), sleep_seconds=0
    ) == 0
    first = _scalar(conn, "SELECT count(*) FROM climate_reports")

    _register_iem({"2026-07-09": [], "2026-07-10": final})
    assert run_nws_backfill(
        settings, conn, date(2026, 7, 9), date(2026, 7, 9), sleep_seconds=0
    ) == 0
    assert _scalar(conn, "SELECT count(*) FROM climate_reports") == first
    # the already-seen product must not be re-fetched (http_cache skip)
    text_fetches = [c for c in responses.calls if "/nwstext/" in str(c.request.url)]
    assert len(text_fetches) == 1


@responses.activate
def test_nws_backfill_never_regresses_live_value(settings, conn):
    # the live collector already landed a NEWER final (e.g. a correction) for Jul 9
    ensure_station_stubs(conn, [NYC])
    conn.execute(CLIMATE_UPSERT, [
        "nyc", date(2026, 7, 9), "max_temp", 83.0, None, "degF",
        "live-prod", "2026-07-10 12:00:00", "2026-07-10 12:05:00", "nws_api",
    ])
    final = [e for e in IEM_LIST["data"] if e["product_id"] == FINAL_ID]
    _register_iem({"2026-07-09": [], "2026-07-10": final})

    assert run_nws_backfill(
        settings, conn, date(2026, 7, 9), date(2026, 7, 9), sleep_seconds=0
    ) == 0
    # IEM's 06:25Z issuance is older than the live 12:00Z one -> guard rejects it
    assert conn.execute(
        "SELECT value, source FROM climate_reports "
        "WHERE obs_date = DATE '2026-07-09' AND variable = 'max_temp'"
    ).fetchone() == (83.0, "nws_api")


@responses.activate
def test_live_ingest_stamps_nws_api_source(settings, conn):
    # regression guard on the live path after the source-column change
    from kalshi_weather.ingest.nws import run_ingest

    points = json.loads((FIXTURES / "points_nyc.json").read_text())
    responses.get(f"https://api.weather.gov/points/40.7789,-73.9692", json=points)
    responses.get(
        points["properties"]["forecastGridData"],
        json=json.loads((FIXTURES / "griddata_nyc_trimmed.json").read_text()),
    )
    responses.get(
        "https://api.weather.gov/products/types/CLI/locations/NYC",
        json={"@graph": [{"id": "p1", "issuanceTime": "2026-07-09T06:26:00+00:00"}]},
    )
    responses.get(
        "https://api.weather.gov/products/p1",
        json={
            "id": "p1",
            "issuanceTime": "2026-07-09T06:26:00+00:00",
            "productText": (FIXTURES / "cli" / "nyc_final.txt").read_text(),
        },
    )
    assert run_ingest(settings, conn) == 0
    assert conn.execute("SELECT DISTINCT source FROM climate_reports").fetchall() == [
        ("nws_api",)
    ]
    assert conn.execute("SELECT DISTINCT source FROM grid_forecasts").fetchall() == [
        ("nws_api",)
    ]


# --- Kalshi ------------------------------------------------------------------


def _settled_payload() -> dict:
    return json.loads((FIXTURES / "kalshi" / "kxhighny_settled_trimmed.json").read_text())


def _candles_payload() -> dict:
    return json.loads((FIXTURES / "kalshi" / "candlesticks_trimmed.json").read_text())


def _register_kalshi(settled: dict):
    responses.get(
        f"{KALSHI}/markets",
        json=settled,
        match=[
            matchers.query_param_matcher(
                {"series_ticker": "KXHIGHNY", "status": "settled"}, strict_match=False
            )
        ],
    )
    for m in settled["markets"]:
        responses.get(
            f"{KALSHI}/series/KXHIGHNY/markets/{m['ticker']}/candlesticks",
            json=_candles_payload(),
        )


@responses.activate
def test_kalshi_backfill_lands_markets_outcomes_candles(settings, conn):
    _register_kalshi(_settled_payload())
    assert run_kalshi_backfill(
        settings, conn, date(2026, 7, 1), date(2026, 7, 31), sleep_seconds=0
    ) == 0

    assert _scalar(conn, "SELECT count(*) FROM markets") == 2
    assert _scalar(conn, "SELECT count(*) FROM market_outcomes") == 2
    # 2 markets x 3 candle bars in the trimmed fixture
    assert _scalar(conn, "SELECT count(*) FROM market_candles") == 6

    bar = conn.execute(
        "SELECT period_minutes, price_close, yes_bid_close, yes_ask_close, volume "
        "FROM market_candles WHERE ticker = 'KXHIGHNY-26JUL10-T87' "
        "ORDER BY period_end LIMIT 1"
    ).fetchone()
    assert bar[0] == 60
    assert all(v is not None for v in bar[1:])
    # dollar prices, so every price is a probability in [0, 1]
    assert 0.0 <= bar[1] <= 1.0 and 0.0 <= bar[2] <= 1.0 and 0.0 <= bar[3] <= 1.0


@responses.activate
def test_kalshi_backfill_is_resumable_and_idempotent(settings, conn):
    _register_kalshi(_settled_payload())
    assert run_kalshi_backfill(
        settings, conn, date(2026, 7, 1), date(2026, 7, 31), sleep_seconds=0
    ) == 0
    first = (
        _scalar(conn, "SELECT count(*) FROM markets"),
        _scalar(conn, "SELECT count(*) FROM market_outcomes"),
        _scalar(conn, "SELECT count(*) FROM market_candles"),
    )
    first_candle_calls = len([c for c in responses.calls if "candlesticks" in str(c.request.url)])
    assert first_candle_calls == 2

    _register_kalshi(_settled_payload())
    assert run_kalshi_backfill(
        settings, conn, date(2026, 7, 1), date(2026, 7, 31), sleep_seconds=0
    ) == 0
    assert (
        _scalar(conn, "SELECT count(*) FROM markets"),
        _scalar(conn, "SELECT count(*) FROM market_outcomes"),
        _scalar(conn, "SELECT count(*) FROM market_candles"),
    ) == first
    # markets already holding candles at this period are skipped entirely
    assert (
        len([c for c in responses.calls if "candlesticks" in str(c.request.url)])
        == first_candle_calls
    )


@responses.activate
def test_kalshi_backfill_filters_obs_date_range(settings, conn):
    _register_kalshi(_settled_payload())
    # fixture markets are all obs_date 2026-07-10 — a disjoint range keeps nothing
    assert run_kalshi_backfill(
        settings, conn, date(2026, 6, 1), date(2026, 6, 30), sleep_seconds=0
    ) == 0
    assert _scalar(conn, "SELECT count(*) FROM markets") == 0
    assert _scalar(conn, "SELECT count(*) FROM market_candles") == 0


@responses.activate
def test_kalshi_backfill_no_candles_flag(settings, conn):
    """`backfill kalshi-resolutions` passes include_quotes=False — the candlesticks
    endpoint must never be hit, but definitions + outcomes still land."""
    _register_kalshi(_settled_payload())
    assert run_kalshi_backfill(
        settings, conn, date(2026, 7, 1), date(2026, 7, 31),
        include_quotes=False, sleep_seconds=0,
    ) == 0
    assert _scalar(conn, "SELECT count(*) FROM markets") == 2
    assert _scalar(conn, "SELECT count(*) FROM market_outcomes") == 2
    assert _scalar(conn, "SELECT count(*) FROM market_candles") == 0
    assert not [c for c in responses.calls if "candlesticks" in str(c.request.url)]


@responses.activate
def test_kalshi_backfill_no_resolutions_flag(settings, conn):
    """`backfill kalshi-quotes` passes include_resolutions=False — market_outcomes must
    stay empty, but definitions (needed for the join) and candles still land."""
    _register_kalshi(_settled_payload())
    assert run_kalshi_backfill(
        settings, conn, date(2026, 7, 1), date(2026, 7, 31),
        include_resolutions=False, sleep_seconds=0,
    ) == 0
    assert _scalar(conn, "SELECT count(*) FROM markets") == 2
    assert _scalar(conn, "SELECT count(*) FROM market_outcomes") == 0
    # 2 markets x 3 candle bars in the trimmed fixture — quotes half still runs
    assert _scalar(conn, "SELECT count(*) FROM market_candles") == 6
