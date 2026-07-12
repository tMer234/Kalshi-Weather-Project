"""Idempotency tests: the pipeline's core guarantee is that re-running against unchanged
NWS data is a pure no-op, while new forecast vintages append and corrected CLI reports
update in place (never regressing to stale values).

All HTTP is mocked from recorded real fixtures — no live API calls.
"""

import json
import logging
from pathlib import Path

import duckdb
import pytest
import responses

from kalshi_weather import db
from kalshi_weather.config import Settings, StationConfig
from kalshi_weather.ingest import run_ingest

FIXTURES = Path(__file__).parent / "fixtures"
BASE = "https://api.weather.gov"
LOGGER = logging.getLogger(__name__)

NYC = StationConfig(
    station_id="nyc",
    display_name="New York City (Central Park)",
    lat=40.7789,
    lon=-73.9692,
    timezone="America/New_York",
    obs_station_id="KNYC",
    cli_site_name="CENTRAL PARK",
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


@pytest.fixture(autouse=True)
def enable_debug_logging(caplog):
    caplog.set_level(logging.DEBUG)
    logging.getLogger("kalshi_weather").setLevel(logging.DEBUG)
    yield


def _points_payload() -> dict:
    return json.loads((FIXTURES / "points_nyc.json").read_text())


def _grid_payload() -> dict:
    return json.loads((FIXTURES / "griddata_nyc_trimmed.json").read_text())


def _cli_text() -> str:
    return (FIXTURES / "cli" / "nyc_final.txt").read_text()


def _register_points_and_grid():
    payload = _points_payload()
    LOGGER.debug("Mocking NWS points endpoint for station lat/lon")
    responses.get(f"{BASE}/points/40.7789,-73.9692", json=payload)
    LOGGER.debug("Mocking forecast grid endpoint: %s", payload["properties"]["forecastGridData"])
    responses.get(
        payload["properties"]["forecastGridData"],
        json=_grid_payload(),
        headers={"Last-Modified": "Thu, 09 Jul 2026 19:03:33 GMT"},
    )


def _register_cli(products: list[tuple[str, str, str]]):
    """products: list of (product_id, issuance_time, product_text), newest first."""
    LOGGER.debug("Mocking CLI product index with %d products", len(products))
    responses.get(
        f"{BASE}/products/types/CLI/locations/NYC",
        json={"@graph": [{"id": pid, "issuanceTime": ts} for pid, ts, _ in products]},
    )
    for pid, ts, text in products:
        LOGGER.debug("Mocking CLI product %s issued at %s", pid, ts)
        responses.get(
            f"{BASE}/products/{pid}",
            json={"id": pid, "issuanceTime": ts, "productText": text},
        )


def _scalar(conn: duckdb.DuckDBPyConnection, sql: str):
    row = conn.execute(sql).fetchone()
    assert row is not None
    return row[0]


def _counts(conn: duckdb.DuckDBPyConnection) -> tuple[int, int]:
    counts = (
        _scalar(conn, "SELECT count(*) FROM grid_forecasts"),
        _scalar(conn, "SELECT count(*) FROM climate_reports"),
    )
    LOGGER.debug("DB counts: grid_forecasts=%d, climate_reports=%d", counts[0], counts[1])
    return counts


@responses.activate
def test_rerun_is_idempotent(settings, conn):
    LOGGER.info("Starting idempotency test")
    _register_points_and_grid()
    _register_cli([("prod-1", "2026-07-09T06:26:00+00:00", _cli_text())])

    result = run_ingest(settings, conn)
    LOGGER.debug("First ingest run exit code: %s", result)
    assert result == 0
    first_grid, first_cli = _counts(conn)
    assert first_grid == 44  # every value row in the trimmed real fixture
    assert first_cli == 4  # max_temp, min_temp, precip, snowfall

    # second run: same (unchanged) NWS responses
    _register_points_and_grid()
    _register_cli([("prod-1", "2026-07-09T06:26:00+00:00", _cli_text())])
    second_result = run_ingest(settings, conn)
    LOGGER.debug("Second ingest run exit code: %s", second_result)
    assert second_result == 0

    assert _counts(conn) == (first_grid, first_cli), "re-run must not duplicate rows"
    # the already-seen CLI product must not have been re-fetched
    product_fetches = [c for c in responses.calls if "/products/prod-1" in str(c.request.url)]
    assert len(product_fetches) == 1


@responses.activate
def test_304_short_circuits_grid_refetch(settings, conn):
    LOGGER.info("Starting 304 short-circuit test")
    _register_points_and_grid()
    _register_cli([("prod-1", "2026-07-09T06:26:00+00:00", _cli_text())])
    first_result = run_ingest(settings, conn)
    LOGGER.debug("Initial ingest run exit code: %s", first_result)
    assert first_result == 0
    first = _counts(conn)

    grid_url = _points_payload()["properties"]["forecastGridData"]
    responses.get(grid_url, status=304)
    _register_cli([("prod-1", "2026-07-09T06:26:00+00:00", _cli_text())])
    second_result = run_ingest(settings, conn)
    LOGGER.debug("Retry ingest run exit code: %s", second_result)
    assert second_result == 0
    assert _counts(conn) == first

    # second grid request must carry If-Modified-Since from the stored Last-Modified
    grid_calls = [c for c in responses.calls if c.request.url == grid_url]
    assert grid_calls[-1].request.headers["If-Modified-Since"] == "Thu, 09 Jul 2026 19:03:33 GMT"


@responses.activate
def test_new_forecast_vintage_appends_new_rows(settings, conn):
    LOGGER.info("Starting forecast vintage append test")
    _register_points_and_grid()
    _register_cli([("prod-1", "2026-07-09T06:26:00+00:00", _cli_text())])
    first_result = run_ingest(settings, conn)
    LOGGER.debug("Initial ingest run exit code: %s", first_result)
    assert first_result == 0
    first_grid, _ = _counts(conn)

    reissued = _grid_payload()
    reissued["properties"]["updateTime"] = "2026-07-09T22:00:00+00:00"
    payload = _points_payload()
    LOGGER.debug("Reissuing forecast grid payload with updateTime=%s", reissued["properties"]["updateTime"])
    responses.get(payload["properties"]["forecastGridData"], json=reissued)
    _register_cli([("prod-1", "2026-07-09T06:26:00+00:00", _cli_text())])
    second_result = run_ingest(settings, conn)
    LOGGER.debug("Second ingest run exit code: %s", second_result)
    assert second_result == 0

    grid, _ = _counts(conn)
    assert grid == 2 * first_grid, "a new issued_time must land as new rows (vintages kept)"
    vintages = conn.execute(
        "SELECT count(DISTINCT issued_time) FROM grid_forecasts"
    ).fetchone()[0]
    assert vintages == 2


@responses.activate
def test_corrected_cli_report_updates_in_place(settings, conn):
    LOGGER.info("Starting CLI correction update test")
    _register_points_and_grid()
    _register_cli([("prod-1", "2026-07-09T06:26:00+00:00", _cli_text())])
    first_result = run_ingest(settings, conn)
    LOGGER.debug("Initial ingest run exit code: %s", first_result)
    assert first_result == 0
    row = conn.execute(
        "SELECT value, product_id FROM climate_reports WHERE variable = 'max_temp'"
    ).fetchone()
    assert row == (85.0, "prod-1")

    # a corrected re-issue for the same date: max revised 85 -> 84, newer issuanceTime
    corrected = _cli_text().replace("  MAXIMUM         85", "  MAXIMUM         84")
    LOGGER.debug("Injecting corrected CLI payload for max_temp")
    _register_points_and_grid()
    _register_cli(
        [
            ("prod-2", "2026-07-09T15:00:00+00:00", corrected),
            ("prod-1", "2026-07-09T06:26:00+00:00", _cli_text()),
        ]
    )
    assert run_ingest(settings, conn) == 0

    rows = conn.execute(
        "SELECT value, product_id FROM climate_reports WHERE variable = 'max_temp'"
    ).fetchall()
    assert rows == [(84.0, "prod-2")], "corrected report must UPDATE, not duplicate"

    # a stale product with an OLDER issuanceTime must never regress the corrected value
    stale = _cli_text().replace("  MAXIMUM         85", "  MAXIMUM         99")
    LOGGER.debug("Injecting stale CLI payload that should be ignored")
    _register_points_and_grid()
    _register_cli([("prod-0", "2026-07-09T01:00:00+00:00", stale)])
    third_result = run_ingest(settings, conn)
    LOGGER.debug("Stale ingest run exit code: %s", third_result)
    assert third_result == 0
    rows = conn.execute(
        "SELECT value, product_id FROM climate_reports WHERE variable = 'max_temp'"
    ).fetchall()
    assert rows == [(84.0, "prod-2")], "stale re-fetch must not overwrite newer correction"


@responses.activate
def test_unparseable_cli_product_is_skipped_not_fatal(settings, conn):
    LOGGER.info("Starting malformed CLI test")
    _register_points_and_grid()
    _register_cli([("prod-bad", "2026-07-09T06:26:00+00:00", "GARBAGE NOT A REPORT")])
    # grid succeeded for the only station, so the run still exits 0; the parse failure
    # is recorded on the climate_reports ingest_runs row
    assert run_ingest(settings, conn) == 0
    assert _counts(conn)[1] == 0
    error = conn.execute(
        "SELECT error FROM ingest_runs WHERE endpoint = 'climate_reports'"
    ).fetchone()[0]
    assert "failed to parse" in error


@responses.activate
def test_ingest_runs_audit_rows_written(settings, conn):
    LOGGER.info("Starting audit-row test")
    _register_points_and_grid()
    _register_cli([("prod-1", "2026-07-09T06:26:00+00:00", _cli_text())])
    result = run_ingest(settings, conn)
    LOGGER.debug("Audit ingest exit code: %s", result)
    assert result == 0
    runs = conn.execute(
        "SELECT endpoint, http_status, rows_upserted, error FROM ingest_runs ORDER BY endpoint"
    ).fetchall()
    assert runs == [("climate_reports", 200, 4, None), ("grid_forecasts", 200, 44, None)]
