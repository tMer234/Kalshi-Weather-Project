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


def _key(hhmm: str) -> str:
    return f"wmo/temp/2026/07/10/YEUZ98_KWBN_20260710{hhmm}"


def test_thin_to_cadence_keeps_one_per_hour():
    from kalshi_weather.ingest.ndfd_backfill import _thin_to_cadence

    keys = [_key("0016"), _key("0046"), _key("0116"), _key("0146")]
    assert _thin_to_cadence(keys, 60) == [_key("0016"), _key("0116")]


def test_thin_to_cadence_keeps_earliest_in_bucket_regardless_of_input_order():
    from kalshi_weather.ingest.ndfd_backfill import _thin_to_cadence

    keys = [_key("0046"), _key("0016")]  # deliberately out of chronological order
    assert _thin_to_cadence(keys, 60) == [_key("0016")]


def test_thin_to_cadence_none_or_zero_disables_thinning():
    from kalshi_weather.ingest.ndfd_backfill import _thin_to_cadence

    keys = [_key("0016"), _key("0046"), _key("0116")]
    assert _thin_to_cadence(keys, None) == keys
    assert _thin_to_cadence(keys, 0) == keys


def test_message_window_period_uses_valid_datetimes():
    # Real NDFD maxt: minute step units -> pygrib gives validDate (period start) and
    # validityDate/validityTime (period end); startStep/endStep are unit-suffixed strings.
    start = datetime(2026, 7, 13, 12, 0)
    end = datetime(2026, 7, 14, 0, 0)
    grb = type("Grb", (), {"validDate": start, "validityDate": 20260714, "validityTime": 0})()
    assert _message_window(grb, period=True) == (start, end)


def test_message_window_instantaneous_synthesizes_1h_bucket():
    start = datetime(2026, 7, 13, 1, 0)
    grb = type("Grb", (), {"validDate": start})()
    s, e = _message_window(grb, period=False)
    assert s == start
    assert e == start + timedelta(hours=1)


def test_validity_end_zero_pads_time():
    # validityTime is HHMM as an int: 0 -> 00:00, 100 -> 01:00.
    from kalshi_weather.ingest.ndfd_backfill import _validity_end

    assert _validity_end(type("G", (), {"validityDate": 20260714, "validityTime": 0})()) == datetime(2026, 7, 14, 0, 0)
    assert _validity_end(type("G", (), {"validityDate": 20260713, "validityTime": 100})()) == datetime(2026, 7, 13, 1, 0)


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
    def __init__(self, valid_start, valid_end, values, lats, lons):
        # Mirrors the real pygrib attributes _message_window reads: validDate (a datetime,
        # the period/instant start) and validityDate/validityTime (HHMM int, the end).
        self.validDate = valid_start
        self.validityDate = int(valid_end.strftime("%Y%m%d"))
        self.validityTime = int(valid_end.strftime("%H%M"))
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
    issued = datetime(2026, 7, 10, 0, 0)
    in_range = _FakeGrb(
        issued + timedelta(hours=6), issued + timedelta(hours=18), np.array([[300.0]]), lats, lons
    )
    out_of_range = _FakeGrb(
        issued + timedelta(hours=100), issued + timedelta(hours=112), np.array([[300.0]]), lats, lons
    )
    fake_pygrib([in_range, out_of_range])

    messages = list(_decode_element_file(b"unused", issued, period=True))

    assert len(messages) == 1
    assert messages[0].valid_start == issued + timedelta(hours=6)


def test_decode_element_file_unmasks_nan(fake_pygrib):
    lats, lons = np.array([[40.0]]), np.array([[-74.0]])
    masked = np.ma.masked_array([[300.0]], mask=[[True]])
    issued = datetime(2026, 7, 10)
    fake_pygrib([_FakeGrb(issued + timedelta(hours=1), issued + timedelta(hours=13), masked, lats, lons)])

    messages = list(_decode_element_file(b"unused", issued, period=True))

    assert np.isnan(messages[0].values[0, 0])


def test_decode_element_file_shares_latlons_across_messages(fake_pygrib):
    """The lat/lon grid is decoded once and shared across a file's messages (they're the
    same fixed NDFD grid) — every yielded message must reference the identical arrays."""
    lats, lons = np.array([[40.0]]), np.array([[-74.0]])
    issued = datetime(2026, 7, 10)
    m1 = _FakeGrb(issued + timedelta(hours=1), issued + timedelta(hours=2), np.array([[300.0]]), lats, lons)
    # a distinct-object lat/lon grid on the 2nd message must be ignored in favor of the 1st's
    m2 = _FakeGrb(
        issued + timedelta(hours=2), issued + timedelta(hours=3), np.array([[301.0]]),
        np.array([[40.0]]), np.array([[-74.0]]),
    )
    fake_pygrib([m1, m2])

    messages = list(_decode_element_file(b"unused", issued, period=False))

    assert len(messages) == 2
    assert messages[0].lats is messages[1].lats
    assert messages[0].lons is messages[1].lons


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


# --- hard wall-clock timeout ------------------------------------------------------------
# Regression coverage for a real hang seen against NOAA's S3 archive: `requests`' own
# `timeout=` only bounds the gap *between* socket reads, not a request's total wall-clock
# time, so a connection the server silently killed (picked back up from the pool) or one
# trickling bytes just under that gap can block forever. `_run_with_hard_timeout` wraps
# the call in a real ceiling so it fails fast (and is retryable) instead of hanging.


def test_run_with_hard_timeout_raises_on_hang():
    import time

    from kalshi_weather.ndfd_client import _HardTimeoutError, _run_with_hard_timeout

    with pytest.raises(_HardTimeoutError):
        _run_with_hard_timeout(time.sleep, 0.05, 5)


def test_run_with_hard_timeout_passes_through_result_and_errors():
    from kalshi_weather.ndfd_client import _run_with_hard_timeout

    assert _run_with_hard_timeout(lambda x: x + 1, 1.0, 41) == 42

    def raises():
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        _run_with_hard_timeout(raises, 1.0)


def test_hung_connection_surfaces_as_ndfd_error_not_a_hang(monkeypatch):
    """A GET that never returns must fail fast via the hard-timeout ceiling and come out
    as NDFDError, not hang forever or raise an uncaught exception."""
    import time

    from kalshi_weather import ndfd_client as ndfd_client_module
    from kalshi_weather.ndfd_client import NDFDClient, NDFDError

    monkeypatch.setattr(ndfd_client_module, "HARD_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr(ndfd_client_module, "RETRY_ATTEMPTS", 2)
    client = NDFDClient(user_agent="test", retry_initial_seconds=0.01)
    monkeypatch.setattr(client.session, "get", lambda *a, **k: time.sleep(5))

    with pytest.raises(NDFDError):
        client._get()


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


def _listing_for(key: str) -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        "<IsTruncated>false</IsTruncated>"
        f"<Contents><Key>{key}</Key></Contents>"
        "</ListBucketResult>"
    )


@responses.activate
def test_ndfd_backfill_commits_days_before_a_mid_run_failure(settings, conn, monkeypatch):
    """A download failure partway through a variable must leave every already-committed
    issuance day durably upserted (per-day commit granularity), not roll the whole run back.

    Listing window for start=end=07-10 is 07-05..07-11 (lookback + 1). Days 05/06/07 are
    served normally; 07-08's download 404s (a non-retryable NDFDError). Days 05-07 must be
    committed (2 stations x 1 message each = 2 rows/day), and 07-08 onward must be absent.
    """
    days = [date(2026, 7, d) for d in range(5, 12)]  # 07-05 .. 07-11
    fail_day = date(2026, 7, 8)
    for day in days:
        key = f"wmo/maxt/{day:%Y/%m/%d}/YGUZ98_KWBN_{day:%Y%m%d}0600"
        responses.get(f"{BUCKET}/", body=_listing_for(key))
        if day == fail_day:
            responses.get(f"{BUCKET}/{key}", status=404)
        else:
            responses.get(f"{BUCKET}/{key}", body="maxTemperature")
    monkeypatch.setattr(
        ndfd_backfill, "_decode_element_file", _fake_decode_returning({"maxTemperature": 300.0})
    )
    monkeypatch.setattr(ndfd_backfill, "_ensure_pygrib", lambda: (object(), np))

    # every variable failed (the one requested variable errored) -> exit code 1 ...
    assert run_ndfd_backfill(
        settings, conn, date(2026, 7, 10), date(2026, 7, 10),
        variables=["maxTemperature"], sleep_seconds=0,
    ) == 1

    # ... but the days that committed before the failure are durable, and no later day landed.
    committed_days = conn.execute(
        "SELECT DISTINCT CAST(issued_time AS DATE) FROM grid_forecasts ORDER BY 1"
    ).fetchall()
    assert [r[0] for r in committed_days] == [date(2026, 7, d) for d in (5, 6, 7)]
    assert _scalar(conn, "SELECT count(*) FROM grid_forecasts") == 6  # 3 days x 2 stations

    # http_cache marks committed alongside their rows -> a resume skips exactly those keys,
    # and the failed day's key is NOT marked (so it will be retried).
    cached = {r[0] for r in conn.execute("SELECT url FROM http_cache").fetchall()}
    assert f"wmo/maxt/2026/07/07/YGUZ98_KWBN_202607070600" in cached
    assert f"wmo/maxt/2026/07/08/YGUZ98_KWBN_202607080600" not in cached


SAME_HOUR_LISTING_XML = """<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <IsTruncated>false</IsTruncated>
  <Contents><Key>wmo/maxt/2026/07/10/YGUZ98_KWBN_202607100016</Key></Contents>
  <Contents><Key>wmo/maxt/2026/07/10/YGUZ98_KWBN_202607100046</Key></Contents>
</ListBucketResult>"""


@responses.activate
def test_ndfd_backfill_default_cadence_thins_same_hour_issuances(settings, conn, monkeypatch):
    """Two issuances 30 min apart, same UTC hour: default hourly cadence keeps only the
    earlier one, matching the live collector's own cadence (docs/runbook.md §1.1)."""
    key1 = "wmo/maxt/2026/07/10/YGUZ98_KWBN_202607100016"
    key2 = "wmo/maxt/2026/07/10/YGUZ98_KWBN_202607100046"
    responses.get(f"{BUCKET}/", body=SAME_HOUR_LISTING_XML)
    responses.get(f"{BUCKET}/{key1}", body="maxTemperature")
    monkeypatch.setattr(
        ndfd_backfill, "_decode_element_file", _fake_decode_returning({"maxTemperature": 300.0})
    )
    monkeypatch.setattr(ndfd_backfill, "_ensure_pygrib", lambda: (object(), np))

    assert run_ndfd_backfill(
        settings, conn, date(2026, 7, 10), date(2026, 7, 10),
        variables=["maxTemperature"], sleep_seconds=0,
    ) == 0

    urls = [c.request.url or "" for c in responses.calls]
    assert any(u.endswith(key1) for u in urls)
    assert not any(u.endswith(key2) for u in urls)


@responses.activate
def test_ndfd_backfill_full_cadence_keeps_every_issuance(settings, conn, monkeypatch):
    """`issuance_cadence_minutes=None` opts back into every archived issuance."""
    key1 = "wmo/maxt/2026/07/10/YGUZ98_KWBN_202607100016"
    key2 = "wmo/maxt/2026/07/10/YGUZ98_KWBN_202607100046"
    responses.get(f"{BUCKET}/", body=SAME_HOUR_LISTING_XML)
    responses.get(f"{BUCKET}/{key1}", body="maxTemperature")
    responses.get(f"{BUCKET}/{key2}", body="maxTemperature")
    monkeypatch.setattr(
        ndfd_backfill, "_decode_element_file", _fake_decode_returning({"maxTemperature": 300.0})
    )
    monkeypatch.setattr(ndfd_backfill, "_ensure_pygrib", lambda: (object(), np))

    assert run_ndfd_backfill(
        settings, conn, date(2026, 7, 10), date(2026, 7, 10),
        variables=["maxTemperature"], sleep_seconds=0, issuance_cadence_minutes=None,
    ) == 0

    urls = [c.request.url or "" for c in responses.calls]
    assert any(u.endswith(key1) for u in urls)
    assert any(u.endswith(key2) for u in urls)


def test_ndfd_backfill_missing_pygrib_raises_clear_error(settings, conn, monkeypatch):
    def _raise():
        raise RuntimeError(
            "backfill nws-grid needs the optional NDFD dependencies "
            "(pygrib + numpy): pip install -e '.[ndfd]'"
        )

    monkeypatch.setattr(ndfd_backfill, "_ensure_pygrib", _raise)
    with pytest.raises(RuntimeError, match="pip install"):
        run_ndfd_backfill(settings, conn, date(2026, 7, 10), date(2026, 7, 10))
