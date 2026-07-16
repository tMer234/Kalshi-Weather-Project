"""NDFD GRIB2 archive backfill tests (`backfill nws-grid`).

Layered to avoid needing real GRIB2 binary fixtures:
- `NDFDClient` (S3 listing/download) is tested against recorded-shape XML via `responses`
  — same approach as the IEM/Kalshi backfills' HTTP mocking.
- `_decode_element_file` (the pygrib-dependent layer) is tested against a *fake* pygrib
  module (a tiny stand-in exposing the same `.open()`/message interface pygrib does) —
  this is real pygrib IS installed in this project's venv (see pyproject.toml's `ndfd`
  extra), but tests shouldn't depend on that, and a fake keeps horizon-filtering /
  window-math logic (`_message_window`) under test without shipping a binary fixture.
- End-to-end orchestration tests monkeypatch `_decode_element_file` itself, so they
  exercise the S3 listing/download/upsert/idempotency machinery without touching pygrib
  at all.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import duckdb
import numpy as np
import pytest
import responses

from kalshi_weather import db
from kalshi_weather.config import SeriesConfig, Settings, StationConfig
from kalshi_weather.ingest import ndfd_backfill
from kalshi_weather.ingest.ndfd_backfill import (
    GridMessage,
    _decode_element_file,
    _message_window,
    _nearest_value,
    run_ndfd_backfill,
)
from kalshi_weather.ndfd_client import issued_time_from_key

BUCKET = "https://noaa-ndfd-pds.s3.amazonaws.com"

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
CHI = StationConfig(
    station_id="chi",
    display_name="Chicago (Midway)",
    lat=41.7861,
    lon=-87.7522,
    timezone="America/Chicago",
    obs_station_id="KMDW",
    cli_site_name="CHICAGO-MIDWAY",
    kalshi_series=(SeriesConfig(ticker="KXHIGHCHI", variable="max_temp"),),
)


@pytest.fixture
def settings(tmp_path):
    return Settings(
        contact_email="test@example.com",
        app_name="kalshi-weather-test",
        duckdb_path=tmp_path / "test.duckdb",
        stations=[NYC, CHI],
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


# --- pure functions: key parsing, window math, nearest-cell lookup -------------------


def test_issued_time_from_key():
    key = "wmo/maxt/2026/07/10/YGUZ98_KWBN_202607100600"
    assert issued_time_from_key(key) == datetime(2026, 7, 10, 6, 0)


def test_message_window_period_uses_start_end_step():
    grb = type("Grb", (), {"startStep": 12, "endStep": 24})()
    issued = datetime(2026, 7, 10, 0, 0)
    start, end = _message_window(grb, issued, period=True)
    assert (start, end) == (issued + timedelta(hours=12), issued + timedelta(hours=24))


def test_message_window_instantaneous_synthesizes_1h_bucket():
    grb = type("Grb", (), {"startStep": 6, "endStep": 6})()
    issued = datetime(2026, 7, 10, 0, 0)
    start, end = _message_window(grb, issued, period=False)
    assert start == issued + timedelta(hours=6)
    assert end == start + timedelta(hours=1)


def test_nearest_value_picks_closest_gridcell():
    lats = np.array([[40.7789, 5.0], [5.0, 5.0]])
    lons = np.array([[-73.9692, 5.0], [5.0, 5.0]])
    values = np.array([[300.0, 999.0], [999.0, 999.0]])
    assert _nearest_value(values, lats, lons, 40.7789, -73.9692) == 300.0


def test_nearest_value_returns_none_for_masked_nan():
    lats = np.array([[40.7789]])
    lons = np.array([[-73.9692]])
    values = np.array([[np.nan]])
    assert _nearest_value(values, lats, lons, 40.7789, -73.9692) is None


# --- _decode_element_file against a fake pygrib (no binary fixture needed) -----------


class _FakeGrb:
    def __init__(self, startStep, endStep, values, lats, lons):
        self.startStep = startStep
        self.endStep = endStep
        self.values = values
        self._lats = lats
        self._lons = lons

    def latlons(self):
        return self._lats, self._lons


class _FakeGribFile:
    def __init__(self, messages):
        self._messages = messages

    def __enter__(self):
        return iter(self._messages)

    def __exit__(self, *exc):
        return False


class _FakePygribModule:
    def __init__(self, messages):
        self._messages = messages

    def open(self, path):
        return _FakeGribFile(self._messages)


@pytest.fixture
def fake_pygrib(monkeypatch):
    """Install a fake pygrib module + real numpy into ndfd_backfill's lazy-import slot."""

    def _install(messages):
        monkeypatch.setattr(ndfd_backfill, "_pygrib", _FakePygribModule(messages))
        monkeypatch.setattr(ndfd_backfill, "_np", np)

    return _install


def test_decode_element_file_filters_beyond_max_horizon(fake_pygrib):
    lats, lons = np.array([[40.0]]), np.array([[-74.0]])
    in_range = _FakeGrb(6, 6, np.array([[300.0]]), lats, lons)
    out_of_range = _FakeGrb(100, 100, np.array([[300.0]]), lats, lons)
    fake_pygrib([in_range, out_of_range])

    issued = datetime(2026, 7, 10, 0, 0)
    messages = _decode_element_file(b"unused", issued, period=True)

    assert len(messages) == 1
    assert messages[0].valid_start == issued + timedelta(hours=6)


def test_decode_element_file_unmasks_nan(fake_pygrib):
    lats, lons = np.array([[40.0]]), np.array([[-74.0]])
    masked = np.ma.masked_array([[300.0]], mask=[[True]])
    fake_pygrib([_FakeGrb(1, 1, masked, lats, lons)])

    messages = _decode_element_file(b"unused", datetime(2026, 7, 10), period=True)

    assert np.isnan(messages[0].values[0, 0])


# --- NDFDClient S3 listing -------------------------------------------------------------


LISTING_XML = """<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <IsTruncated>false</IsTruncated>
  <Contents><Key>wmo/maxt/2026/07/10/YGUZ98_KWBN_202607100600</Key></Contents>
  <Contents><Key>wmo/maxt/2026/07/10/YGUZ97_KWBN_202607100600</Key></Contents>
  <Contents><Key>wmo/maxt/2026/07/10/YGAZ98_KWBN_202607100600</Key></Contents>
</ListBucketResult>"""


@responses.activate
def test_list_day_filters_region_and_suffix():
    from kalshi_weather.ndfd_client import NDFDClient

    responses.get(f"{BUCKET}/", body=LISTING_XML)
    client = NDFDClient(user_agent="test")
    keys = client.list_day("maxt", date(2026, 7, 10), "UZ", "98")
    assert keys == ["wmo/maxt/2026/07/10/YGUZ98_KWBN_202607100600"]


# --- end-to-end orchestration (decode layer monkeypatched) ---------------------------


def _fake_decode_returning(values_by_variable):
    """Fake _decode_element_file: one message at +6h, value keyed by variable name via
    a closure over the raw bytes payload (the tests below encode the variable name into
    `raw` so one fake serves every element)."""

    def _decode(raw: bytes, issued, period: bool):
        lats = np.array([[40.7789, 41.7861], [5.0, 5.0]])
        lons = np.array([[-73.9692, -87.7522], [5.0, 5.0]])
        raw_value = values_by_variable[raw.decode()]
        values = np.array([[raw_value, raw_value], [999.0, 999.0]])
        start = issued + timedelta(hours=6)
        return [GridMessage(start, start, values, lats, lons)]

    return _decode


@responses.activate
def test_ndfd_backfill_lands_with_archive_source(settings, conn, monkeypatch):
    key = "wmo/maxt/2026/07/10/YGUZ98_KWBN_202607100600"
    responses.get(f"{BUCKET}/", body=LISTING_XML.replace("YGAZ98", "ZZAZ98").replace("YGUZ97", "ZZUZ97"))
    responses.get(f"{BUCKET}/{key}", body="maxTemperature")
    monkeypatch.setattr(
        ndfd_backfill, "_decode_element_file", _fake_decode_returning({"maxTemperature": 300.0})
    )
    monkeypatch.setattr(ndfd_backfill, "_ensure_pygrib", lambda: (object(), np))

    assert run_ndfd_backfill(
        settings, conn, date(2026, 7, 10), date(2026, 7, 10),
        variables=["maxTemperature"], sleep_seconds=0,
    ) == 0

    rows = conn.execute(
        "SELECT station_id, variable, round(value, 2), unit, source, horizon_hours "
        "FROM grid_forecasts ORDER BY station_id"
    ).fetchall()
    assert rows == [
        ("chi", "maxTemperature", 26.85, "degC", "ndfd_archive", 6.0),
        ("nyc", "maxTemperature", 26.85, "degC", "ndfd_archive", 6.0),
    ]


@responses.activate
def test_ndfd_backfill_rerun_is_noop(settings, conn, monkeypatch):
    key = "wmo/maxt/2026/07/10/YGUZ98_KWBN_202607100600"
    responses.get(f"{BUCKET}/", body=LISTING_XML.replace("YGAZ98", "ZZAZ98").replace("YGUZ97", "ZZUZ97"))
    responses.get(f"{BUCKET}/{key}", body="maxTemperature")
    monkeypatch.setattr(
        ndfd_backfill, "_decode_element_file", _fake_decode_returning({"maxTemperature": 300.0})
    )
    monkeypatch.setattr(ndfd_backfill, "_ensure_pygrib", lambda: (object(), np))

    assert run_ndfd_backfill(
        settings, conn, date(2026, 7, 10), date(2026, 7, 10),
        variables=["maxTemperature"], sleep_seconds=0,
    ) == 0
    first = _scalar(conn, "SELECT count(*) FROM grid_forecasts")

    responses.get(f"{BUCKET}/", body=LISTING_XML.replace("YGAZ98", "ZZAZ98").replace("YGUZ97", "ZZUZ97"))
    assert run_ndfd_backfill(
        settings, conn, date(2026, 7, 10), date(2026, 7, 10),
        variables=["maxTemperature"], sleep_seconds=0,
    ) == 0
    assert _scalar(conn, "SELECT count(*) FROM grid_forecasts") == first
    # the already-seen key must not be re-downloaded (http_cache skip)
    downloads = [c for c in responses.calls if (c.request.url or "").endswith(key)]
    assert len(downloads) == 1


@responses.activate
def test_ndfd_backfill_variable_filter(settings, conn, monkeypatch):
    """Requesting only windSpeed must never touch the maxt element at all."""
    key = "wmo/wspd/2026/07/10/YCUZ98_KWBN_202607100600"
    listing = LISTING_XML.replace("maxt", "wspd").replace("YGUZ98", "YCUZ98").replace(
        "YGAZ98", "ZZAZ98"
    ).replace("YGUZ97", "ZZUZ97")
    responses.get(f"{BUCKET}/", body=listing)
    responses.get(f"{BUCKET}/{key}", body="windSpeed")
    monkeypatch.setattr(
        ndfd_backfill, "_decode_element_file", _fake_decode_returning({"windSpeed": 10.0})
    )
    monkeypatch.setattr(ndfd_backfill, "_ensure_pygrib", lambda: (object(), np))

    assert run_ndfd_backfill(
        settings, conn, date(2026, 7, 10), date(2026, 7, 10),
        station_ids=["nyc"], variables=["windSpeed"], sleep_seconds=0,
    ) == 0

    rows = conn.execute(
        "SELECT station_id, variable, round(value, 2), unit FROM grid_forecasts"
    ).fetchall()
    assert rows == [("nyc", "windSpeed", 36.0, "km_h-1")]  # 10 m/s * 3.6


def test_ndfd_backfill_missing_pygrib_raises_clear_error(settings, conn, monkeypatch):
    def _raise():
        raise RuntimeError(
            "backfill nws-grid needs the optional NDFD dependencies "
            "(pygrib + numpy): pip install -e '.[ndfd]'"
        )

    monkeypatch.setattr(ndfd_backfill, "_ensure_pygrib", _raise)
    with pytest.raises(RuntimeError, match="pip install"):
        run_ndfd_backfill(settings, conn, date(2026, 7, 10), date(2026, 7, 10))
