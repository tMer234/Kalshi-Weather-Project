"""Kalshi collector tests: parsing of real market payloads, idempotent re-runs, and the
append/update/insert-once semantics of the three market tables.

All HTTP is mocked from recorded real trade-api/v2 fixtures — no live API calls.
"""

import json
import logging
from datetime import date, datetime
from pathlib import Path

import duckdb
import pytest
import responses
from responses import matchers

from kalshi_weather import db
from kalshi_weather.config import SeriesConfig, Settings, StationConfig
from kalshi_weather.ingest import kalshi as kalshi_ingest
from kalshi_weather.ingest.kalshi import parse_event_date, run_kalshi_ingest, MarketParseError

FIXTURES = Path(__file__).parent / "fixtures" / "kalshi"
BASE = "https://api.elections.kalshi.com/trade-api/v2"
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

EMPTY = {"markets": [], "cursor": ""}


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


def _open_payload() -> dict:
    return json.loads((FIXTURES / "kxhighny_open_trimmed.json").read_text())


def _settled_payload() -> dict:
    return json.loads((FIXTURES / "kxhighny_settled_trimmed.json").read_text())


def _register(open_payload: dict, settled_payload: dict):
    responses.get(
        f"{BASE}/markets",
        json=open_payload,
        match=[
            matchers.query_param_matcher(
                {"series_ticker": "KXHIGHNY", "status": "open"}, strict_match=False
            )
        ],
    )
    responses.get(
        f"{BASE}/markets",
        json=settled_payload,
        match=[
            matchers.query_param_matcher(
                {"series_ticker": "KXHIGHNY", "status": "settled"}, strict_match=False
            )
        ],
    )


def _scalar(conn: duckdb.DuckDBPyConnection, sql: str):
    row = conn.execute(sql).fetchone()
    assert row is not None
    return row[0]


def _counts(conn: duckdb.DuckDBPyConnection) -> tuple[int, int, int]:
    return (
        _scalar(conn, "SELECT count(*) FROM markets"),
        _scalar(conn, "SELECT count(*) FROM market_snapshots"),
        _scalar(conn, "SELECT count(*) FROM market_outcomes"),
    )


def test_parse_event_date():
    assert parse_event_date("KXHIGHNY-26JUL12") == date(2026, 7, 12)
    assert parse_event_date("KXHIGHDEN-27JAN01") == date(2027, 1, 1)
    with pytest.raises(MarketParseError):
        parse_event_date("KXHIGHNY")
    with pytest.raises(MarketParseError):
        parse_event_date("KXHIGHNY-26XXX12")


@responses.activate
def test_first_run_populates_all_tables(settings, conn):
    _register(_open_payload(), _settled_payload())
    assert run_kalshi_ingest(settings, conn) == 0

    # 3 open + 2 settled definitions; snapshots only for open; outcomes only for settled
    assert _counts(conn) == (5, 3, 2)

    # strike semantics land verbatim, obs_date comes from the event ticker
    rows = dict(
        (t, (st, fs, cs, od))
        for t, st, fs, cs, od in conn.execute(
            "SELECT ticker, strike_type, floor_strike, cap_strike, obs_date FROM markets"
        ).fetchall()
    )
    assert rows["KXHIGHNY-26JUL12-T89"] == ("greater", 89.0, None, date(2026, 7, 12))
    assert rows["KXHIGHNY-26JUL12-T82"] == ("less", None, 82.0, date(2026, 7, 12))
    assert rows["KXHIGHNY-26JUL12-B88.5"] == ("between", 88.0, 89.0, date(2026, 7, 12))
    assert rows["KXHIGHNY-26JUL10-T94"] == ("greater", 94.0, None, date(2026, 7, 10))

    # station/variable mapping comes from config, enabling the settlement join later
    assert conn.execute(
        "SELECT DISTINCT station_id, variable, series_ticker FROM markets"
    ).fetchall() == [("nyc", "max_temp", "KXHIGHNY")]

    # dollar-string prices parsed to floats
    snap = conn.execute(
        "SELECT yes_bid, yes_ask, last_price, open_interest FROM market_snapshots "
        "WHERE ticker = 'KXHIGHNY-26JUL12-T89'"
    ).fetchone()
    assert snap == (0.0, 0.01, 0.01, 1642.25)

    # outcomes carry Kalshi's own settled value — 85°F on Jul 10
    outcomes = conn.execute(
        "SELECT ticker, result, expiration_value FROM market_outcomes ORDER BY ticker"
    ).fetchall()
    assert outcomes == [
        ("KXHIGHNY-26JUL10-T87", "yes", 85.0),
        ("KXHIGHNY-26JUL10-T94", "no", 85.0),
    ]


@responses.activate
def test_rerun_at_same_instant_is_idempotent(settings, conn, monkeypatch):
    frozen = datetime(2026, 7, 11, 20, 0, 0)
    monkeypatch.setattr(kalshi_ingest, "_utcnow", lambda: frozen)

    _register(_open_payload(), _settled_payload())
    assert run_kalshi_ingest(settings, conn) == 0
    first = _counts(conn)

    _register(_open_payload(), _settled_payload())
    assert run_kalshi_ingest(settings, conn) == 0
    assert _counts(conn) == first, "same-instant replay must not duplicate any row"


@responses.activate
def test_later_pass_appends_snapshots_only(settings, conn):
    _register(_open_payload(), _settled_payload())
    assert run_kalshi_ingest(settings, conn) == 0
    markets1, snaps1, outcomes1 = _counts(conn)

    # next pass, real clock has moved on: quotes append, definitions/outcomes don't
    _register(_open_payload(), _settled_payload())
    assert run_kalshi_ingest(settings, conn) == 0
    markets2, snaps2, outcomes2 = _counts(conn)
    assert (markets2, outcomes2) == (markets1, outcomes1)
    assert snaps2 == 2 * snaps1, "each pass must append one snapshot per open market"
    assert _scalar(conn, "SELECT count(DISTINCT snapshot_time) FROM market_snapshots") == 2


@responses.activate
def test_settlement_updates_market_status_in_place(settings, conn):
    _register(_open_payload(), EMPTY)
    assert run_kalshi_ingest(settings, conn) == 0
    assert _scalar(
        conn, "SELECT status FROM markets WHERE ticker = 'KXHIGHNY-26JUL12-T89'"
    ) == "active"

    # the same market later appears in the settled listing
    settled_now = _open_payload()
    settled_now["markets"] = [
        {**m, "status": "finalized", "result": "no", "expiration_value": "85.00"}
        for m in settled_now["markets"]
    ]
    _register(EMPTY, settled_now)
    assert run_kalshi_ingest(settings, conn) == 0

    assert _scalar(conn, "SELECT count(*) FROM markets") == 3, "no duplicate definitions"
    assert _scalar(
        conn, "SELECT status FROM markets WHERE ticker = 'KXHIGHNY-26JUL12-T89'"
    ) == "finalized"
    assert _scalar(conn, "SELECT count(*) FROM market_outcomes") == 3


@responses.activate
def test_recorded_outcome_is_never_overwritten(settings, conn):
    _register(EMPTY, _settled_payload())
    assert run_kalshi_ingest(settings, conn) == 0

    # a conflicting later listing must not rewrite a recorded result (insert-once)
    flipped = _settled_payload()
    for m in flipped["markets"]:
        m["result"] = "yes" if m["result"] == "no" else "no"
    _register(EMPTY, flipped)
    assert run_kalshi_ingest(settings, conn) == 0

    assert conn.execute(
        "SELECT result FROM market_outcomes WHERE ticker = 'KXHIGHNY-26JUL10-T94'"
    ).fetchone() == ("no",)


@responses.activate
def test_malformed_market_is_skipped_not_fatal(settings, conn):
    bad = _open_payload()
    bad["markets"][0]["event_ticker"] = "KXHIGHNY-GARBAGE"
    _register(bad, EMPTY)

    assert run_kalshi_ingest(settings, conn) == 0
    assert _scalar(conn, "SELECT count(*) FROM markets") == 2, "good markets still land"
    error = conn.execute(
        "SELECT error FROM ingest_runs WHERE endpoint = 'kalshi_markets:KXHIGHNY'"
    ).fetchone()[0]
    assert "failed to parse" in error


@responses.activate
def test_ingest_runs_audit_rows_written(settings, conn):
    _register(_open_payload(), _settled_payload())
    assert run_kalshi_ingest(settings, conn) == 0
    runs = conn.execute(
        "SELECT endpoint, http_status, rows_upserted, error FROM ingest_runs ORDER BY endpoint"
    ).fetchall()
    # open pass: 3 definitions + 3 snapshots; settled pass: 2 definitions + 2 outcomes
    assert runs == [
        ("kalshi_markets:KXHIGHNY", 200, 6, None),
        ("kalshi_outcomes:KXHIGHNY", 200, 4, None),
    ]


@responses.activate
def test_include_resolutions_false_skips_settled_collector(settings, conn):
    """`ingest kalshi-quotes` passes include_resolutions=False — outcomes/settled
    definitions must stay untouched, and the settled endpoint must not even be hit."""
    responses.get(
        f"{BASE}/markets",
        json=_open_payload(),
        match=[
            matchers.query_param_matcher(
                {"series_ticker": "KXHIGHNY", "status": "open"}, strict_match=False
            )
        ],
    )
    assert run_kalshi_ingest(settings, conn, include_resolutions=False) == 0

    markets, snapshots, outcomes = _counts(conn)
    assert markets == 3
    assert snapshots == 3
    assert outcomes == 0
    endpoints = {
        r[0] for r in conn.execute("SELECT DISTINCT endpoint FROM ingest_runs").fetchall()
    }
    assert endpoints == {"kalshi_markets:KXHIGHNY"}


@responses.activate
def test_include_quotes_false_skips_open_collector(settings, conn):
    """`ingest kalshi-resolutions` passes include_quotes=False — no new snapshots, and
    the open-markets endpoint must not even be hit."""
    responses.get(
        f"{BASE}/markets",
        json=_settled_payload(),
        match=[
            matchers.query_param_matcher(
                {"series_ticker": "KXHIGHNY", "status": "settled"}, strict_match=False
            )
        ],
    )
    assert run_kalshi_ingest(settings, conn, include_quotes=False) == 0

    markets, snapshots, outcomes = _counts(conn)
    assert markets == 2
    assert snapshots == 0
    assert outcomes == 2
    endpoints = {
        r[0] for r in conn.execute("SELECT DISTINCT endpoint FROM ingest_runs").fetchall()
    }
    assert endpoints == {"kalshi_outcomes:KXHIGHNY"}


def test_no_configured_series_is_a_config_failure(settings, conn):
    bare = Settings(
        contact_email=settings.contact_email,
        app_name=settings.app_name,
        duckdb_path=settings.duckdb_path,
        stations=[StationConfig(
            station_id="nyc", display_name="x", lat=0.0, lon=0.0,
            timezone="America/New_York",
        )],
    )
    assert run_kalshi_ingest(bare, conn) == 1
