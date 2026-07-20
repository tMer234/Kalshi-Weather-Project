"""Historical forecast-vintage backfill from the NDFD GRIB2 archive (Phase 0c).

Fills `grid_forecasts` for forecast vintages that predate the live collector, using
NOAA's public `noaa-ndfd-pds` S3 archive — the same NDFD grids api.weather.gov serves
live, just packaged as WMO-bulletin-wrapped GRIB2 instead of JSON. Rows land with
`source='ndfd_archive'`.

See docs/runbook.md §2.1 for the investigation behind every decision encoded here:
bucket layout, region/suffix selection, the nearest-gridcell extraction approach, and
the per-variable empirical validation against real live-collected data (9 of 11
variables match closely; `probabilityOfPrecipitation` backfills at a coarser 12h
resolution — never directly comparable row-for-row to live 3h/6h PoP rows;
`snowfallAmount`'s unit conversion is well-reasoned but not yet confirmed against real
winter data).

Needs the optional `pygrib`/`numpy` dependencies (`pip install -e '.[ndfd]'`) — not
required just to import this module, only to actually run a backfill, so the rest of
the pipeline (and the test suite) never needs a GRIB2 decoder installed.

Safety properties, matching the other backfills:
- The `grid_forecasts` upsert is `ON CONFLICT ... DO NOTHING` on (station, variable,
  issued_time, valid_start) — re-running a backfill, or overlapping it with dates the
  live collector already covered, can only add rows, never regress or duplicate one.
- Downloaded bucket keys are marked in `http_cache`, so an interrupted run resumes
  without re-fetching (delete those rows to force a re-decode after a parsing fix).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Callable

import duckdb

from ..config import Settings
from ..ndfd_client import NDFDClient, NDFDError, issued_time_from_key
from ..resolve import ensure_station_stubs
from ..time_utils import horizon_hours
from .common import RunResult, _record_run, _utcnow
from .nws import GRID_VARIABLES, MAX_HORIZON_HOURS, _GRID_UPSERT, _mark_fetched, _seen

logger = logging.getLogger(__name__)

DEFAULT_SLEEP_SECONDS = 0.2

# NDFD sector + resolution/horizon suffix — see docs/runbook.md §2.1. UZ = CONUS;
# 98 = full-res day1-3, the target for this 72h-scoped backfill (97/88/87 are the
# day4-7 and half-res variants, deliberately not fetched).
REGION = "UZ"
SUFFIX = "98"

# how many UTC days before `start` to also list issuances for, so vintages whose horizon
# reaches into `start` from up to MAX_HORIZON_HOURS earlier (any station's timezone,
# UTC-5..UTC-8) aren't missed. Deliberately generous: extra listed days only cost cheap,
# deduped S3 listing calls, never wrong data.
_LOOKBACK_DAYS = MAX_HORIZON_HOURS // 24 + 2

# The archive reissues every element roughly every 30 min (~48 issuances/UTC day); the
# live collector (`ingest nws-grid`) only runs hourly (~24/day) and can never do better.
# Backfilling at full archive cadence stores forecast vintages ~2x denser than live
# history ever will be — near-duplicate pseudo-replicates of the same NWS model cycle
# that add no independent statistical information (nothing downstream needs finer than
# hourly resolution) but do inflate pooled-sample-size statistics and create a density
# discontinuity right at the backfill/live boundary. Default to hourly so backfilled and
# live-collected history are density-consistent; `--issuance-cadence full` opts back into
# every archived vintage for a specific research question. See docs/runbook.md §2.1.
DEFAULT_ISSUANCE_CADENCE_MINUTES = 60


@dataclass(frozen=True)
class NDFDElement:
    variable: str  # grid_forecasts.variable — must be one of GRID_VARIABLES
    path: str  # S3 `wmo/{path}/...` directory segment
    wmo_prefix: str  # 2-letter WMO T1T2 bulletin prefix (informational; region+suffix pin the file)
    unit: str  # grid_forecasts.unit string to store — matches the live collector's convention
    convert: Callable[[float], float]  # raw GRIB (SI) value -> stored unit
    period: bool  # True: use the message's real start/end step (accumulation/extreme
    # fields). False: instantaneous field, sampled once per hour in the day1-3 archive —
    # synthesize a 1h [valid_start, valid_start+1h) window to match the live collector's
    # hourly-state shape. Not yet confirmed bit-exact against live window boundaries;
    # see docs/runbook.md §2.1.


NDFD_ELEMENTS = [
    NDFDElement("maxTemperature", "maxt", "YG", "degC", lambda k: k - 273.15, True),
    NDFDElement("minTemperature", "mint", "YH", "degC", lambda k: k - 273.15, True),
    NDFDElement("temperature", "temp", "YE", "degC", lambda k: k - 273.15, False),
    NDFDElement("dewpoint", "td", "YF", "degC", lambda k: k - 273.15, False),
    NDFDElement("relativeHumidity", "rhm", "YR", "percent", lambda v: v, False),
    NDFDElement("windSpeed", "wspd", "YC", "km_h-1", lambda v: v * 3.6, False),
    NDFDElement("windGust", "wgust", "YW", "km_h-1", lambda v: v * 3.6, False),
    NDFDElement("skyCover", "sky", "YA", "percent", lambda v: v, False),
    NDFDElement("quantitativePrecipitation", "qpf", "YI", "mm", lambda v: v, True),
    NDFDElement("probabilityOfPrecipitation", "pop12", "YD", "percent", lambda v: v, True),
    NDFDElement("snowfallAmount", "snow", "YS", "mm", lambda v: v * 1000.0, True),
]
assert {e.variable for e in NDFD_ELEMENTS} == set(GRID_VARIABLES), (
    "NDFD_ELEMENTS must cover exactly the live collector's GRID_VARIABLES"
)


# --- pygrib/numpy: lazy, so importing this module never requires them ---------------

_pygrib: Any = None
_np: Any = None


def _ensure_pygrib() -> tuple[Any, Any]:
    global _pygrib, _np
    if _pygrib is None:
        try:
            import numpy as np_mod
            import pygrib as pygrib_mod
        except ImportError as e:
            raise RuntimeError(
                "backfill nws-grid needs the optional NDFD dependencies "
                "(pygrib + numpy): pip install -e '.[ndfd]'"
            ) from e
        _pygrib, _np = pygrib_mod, np_mod
    return _pygrib, _np


@dataclass(frozen=True)
class GridMessage:
    valid_start: datetime
    valid_end: datetime
    values: Any  # 2D array, decoder-native (numpy.ndarray once pygrib is loaded)
    lats: Any
    lons: Any


def _validity_end(grb) -> datetime:
    """End of a range/period message, from its validityDate+validityTime (HHMM), naive UTC."""
    return datetime.strptime(f"{int(grb.validityDate):08d}{int(grb.validityTime):04d}", "%Y%m%d%H%M")


def _message_window(grb, period: bool) -> tuple[datetime, datetime]:
    """[valid_start, valid_end) for one GRIB message, from pygrib's decoded valid datetimes.

    Deliberately does NOT parse startStep/endStep: real NDFD day1-3 messages carry
    stepUnits=0 (minutes), so pygrib returns steps as unit-suffixed strings like '690m'
    that int() can't parse, and those steps are offset from the GRIB reference time
    (grb.analDate), not the file's transmission timestamp. grb.validDate (= analDate +
    forecastTime) is the start of a statistically-processed period (max/min/accum) or the
    instant of a point field, and lands on whole hours matching the live collector."""
    valid_start = grb.validDate
    if period:
        return valid_start, _validity_end(grb)
    return valid_start, valid_start + timedelta(hours=1)


def _decode_element_file(raw: bytes, issued: datetime, period: bool):
    """Yield every in-horizon message's grid from one GRIB2 file's bytes, lazily.

    A generator, not a list, on purpose: an instantaneous day1-3 element (temperature,
    dewpoint, ...) carries one message per forecast hour, so ~72 full-CONUS-grid messages
    land in a single file. Each message's `values` array alone is ~24 MB (a ~2145x1377
    float64 grid), so materializing all of them at once would pin ~5 GB of numpy memory
    per file. Yielding one at a time lets the caller extract its station values and drop
    the array before the next is decoded, bounding resident grid memory to O(1) message.

    The lat/lon grids are identical for every message in a file (same fixed NDFD grid), so
    they're decoded once and shared across all yielded messages rather than re-materialized
    per message — `_nearest_value` only ever reads them, so sharing is safe and saves a
    further ~2x on grid memory.

    pygrib only opens from a filepath, so the bytes are staged to a temp file and removed
    once the generator is exhausted (or closed early on an exception in the caller).
    """
    pygrib, np = _ensure_pygrib()
    import os
    import tempfile

    fd, path = tempfile.mkstemp(suffix=".grib2")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(raw)
        lats = lons = None
        with pygrib.open(path) as gribs:
            for grb in gribs:
                start, end = _message_window(grb, period)
                if horizon_hours(issued, start) > MAX_HORIZON_HOURS:
                    continue
                values = grb.values
                if np.ma.is_masked(values):
                    values = values.filled(np.nan)
                if lats is None:  # same grid for every message in the file — decode once
                    lats, lons = grb.latlons()
                yield GridMessage(start, end, values, lats, lons)
    finally:
        os.unlink(path)


def _nearest_value(values, lats, lons, station_lat: float, station_lon: float) -> float | None:
    """Value at the gridcell nearest (station_lat, station_lon); None if masked/NaN."""
    _, np = _ensure_pygrib()
    dist2 = (lats - station_lat) ** 2 + (lons - station_lon) ** 2
    idx = np.unravel_index(np.argmin(dist2), dist2.shape)
    val = float(values[idx])
    return None if np.isnan(val) else val


def _thin_to_cadence(keys: list[str], cadence_minutes: int | None) -> list[str]:
    """Keep one key per `cadence_minutes` bucket of issuance time (the earliest in each).

    `cadence_minutes=None` (or <= 0) disables thinning — every listed key is kept. Keys
    are grouped by `issued_time // cadence_minutes`, e.g. cadence_minutes=60 keeps the
    first archive issuance in each UTC hour, matching the live collector's own hourly
    cadence so backfilled and live-collected history are density-consistent. Purely a
    pre-fetch filter: dropped keys are never downloaded, decoded, or `_mark_fetched`'d,
    and re-running derives the identical kept set, so resumability is unaffected.
    """
    if not cadence_minutes or cadence_minutes <= 0:
        return keys
    kept: list[str] = []
    seen_buckets: set[datetime] = set()
    for key in sorted(keys, key=issued_time_from_key):
        issued = issued_time_from_key(key)
        bucket_index = (issued.hour * 60 + issued.minute) // cadence_minutes
        bucket = issued.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(
            minutes=bucket_index * cadence_minutes
        )
        if bucket not in seen_buckets:
            seen_buckets.add(bucket)
            kept.append(key)
    return kept


# --- orchestration -------------------------------------------------------------------


def backfill_element(
    client: NDFDClient,
    conn: duckdb.DuckDBPyConnection,
    element: NDFDElement,
    stations: list,
    list_start: date,
    list_end: date,
    sleep_seconds: float,
    issuance_cadence_minutes: int | None = DEFAULT_ISSUANCE_CADENCE_MINUTES,
) -> RunResult:
    """Backfill one variable across every station for UTC issuance days [list_start, list_end].

    Commits one UTC issuance day at a time, each in its own transaction: an instantaneous
    hourly element over the multi-day listing window is ~1M candidate rows, and buffering
    the whole variable x date-range x station cross product before a single bulk insert (as
    an earlier version did) pinned that entire list in Python *and* held one hour-long
    uncommitted DuckDB transaction — which is what OOM'd a real run. Per-day commits bound
    both to a single day's worth (~thousands of rows) and make resumability finer-grained:
    a mid-run failure leaves every already-committed day durably upserted, and the http_cache
    marks committed alongside them mean a re-run skips exactly those days' keys. The caller
    (`run_ndfd_backfill`) owns no transaction — all transaction control lives here.

    `issuance_cadence_minutes` thins the archive's ~30-min-cadence listing down to one
    issuance per bucket (see `_thin_to_cadence`) before anything is downloaded — pass None
    for full archive fidelity.
    """
    result = RunResult("ALL", f"ndfd_backfill:{element.variable}")
    total = 0
    day = list_start
    while day <= list_end:
        listed = client.list_day(element.path, day, REGION, SUFFIX)  # network I/O, pre-transaction
        result.http_status = 200
        thinned = _thin_to_cadence(listed, issuance_cadence_minutes)
        to_fetch = [k for k in thinned if not _seen(conn, k)]
        logger.info(
            "%s %s: %d keys listed, %d kept after cadence thinning, %d already cached, %d to download",
            element.variable, day, len(listed), len(thinned), len(thinned) - len(to_fetch), len(to_fetch),
        )
        rows: list = []
        conn.execute("BEGIN")
        try:
            for i, key in enumerate(to_fetch, 1):
                issued = issued_time_from_key(key)
                t_dl = time.monotonic()
                logger.info(
                    "%s %s: downloading %s (%d/%d)", element.variable, day, key, i, len(to_fetch)
                )
                raw = client.download(key)
                logger.info(
                    "%s %s: downloaded %s in %.1fs (%.1f MB)",
                    element.variable, day, key, time.monotonic() - t_dl, len(raw) / 1e6,
                )
                t_decode = time.monotonic()
                n_messages = 0
                for msg in _decode_element_file(raw, issued, element.period):
                    n_messages += 1
                    h = horizon_hours(issued, msg.valid_start)
                    for station in stations:
                        val = _nearest_value(
                            msg.values, msg.lats, msg.lons, station.lat, station.lon
                        )
                        if val is None:
                            continue
                        rows.append(
                            (
                                station.station_id,
                                element.variable,
                                issued,
                                msg.valid_start,
                                msg.valid_end,
                                h,
                                element.convert(val),
                                element.unit,
                                _utcnow(),
                                "ndfd_archive",
                            )
                        )
                logger.info(
                    "%s %s: decoded %s in %.1fs (%d in-horizon messages, %d rows so far)",
                    element.variable, day, key, time.monotonic() - t_decode, n_messages, len(rows),
                )
                _mark_fetched(conn, key, None)
                time.sleep(sleep_seconds)
            if rows:
                logger.info("%s %s: committing %d rows", element.variable, day, len(rows))
                conn.executemany(_GRID_UPSERT, rows)
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")  # drop this day's partial work; earlier days stay committed
            raise
        total += len(rows)
        logger.info("%s %s: committed %d rows", element.variable, day, len(rows))
        day += timedelta(days=1)

    result.rows_upserted = total
    logger.info(
        "%s: backfilled %d grid_forecasts rows for issuance days %s..%s",
        element.variable, total, list_start, list_end,
    )
    return result


def run_ndfd_backfill(
    settings: Settings,
    conn: duckdb.DuckDBPyConnection,
    start: date,
    end: date,
    station_ids: list[str] | None = None,
    variables: list[str] | None = None,
    sleep_seconds: float = DEFAULT_SLEEP_SECONDS,
    issuance_cadence_minutes: int | None = DEFAULT_ISSUANCE_CADENCE_MINUTES,
) -> int:
    """Backfill grid_forecasts for obs_dates in [start, end] from the NDFD archive.

    Exit-code contract matches the other backfills: 0 = at least one variable's backfill
    succeeded, 1 = every variable failed (or a config error prevented starting).

    `issuance_cadence_minutes` (default 60, i.e. hourly) thins the archive's ~30-min
    native cadence down to one issuance per bucket, matching the live collector's own
    cadence so backfilled and live-collected history stay density-consistent — pass None
    for full archive fidelity.
    """
    if start > end:
        logger.error("start %s is after end %s", start, end)
        return 1
    _ensure_pygrib()  # fail fast with a clear message before any downloads

    stations = [
        s for s in settings.stations if station_ids is None or s.station_id in station_ids
    ]
    if not stations:
        logger.error("no stations matched %r", station_ids)
        return 1
    elements = [
        e for e in NDFD_ELEMENTS if variables is None or e.variable in variables
    ]
    if not elements:
        logger.error("no variables matched %r", variables)
        return 1
    ensure_station_stubs(conn, stations)

    client = NDFDClient(user_agent=settings.user_agent)
    list_start = start - timedelta(days=_LOOKBACK_DAYS)
    list_end = end + timedelta(days=1)

    ok = 0
    for element in elements:
        logger.info(
            "%s: starting (issuance days %s..%s, %d stations)",
            element.variable, list_start, list_end, len(stations),
        )
        started_at = _utcnow()
        try:
            # backfill_element owns per-day transactions; on failure it has already rolled
            # back the in-flight day and left every earlier day committed and durable, so
            # there is no transaction to unwind here.
            result = backfill_element(
                client, conn, element, stations, list_start, list_end, sleep_seconds,
                issuance_cadence_minutes,
            )
        except (NDFDError, ValueError, OSError) as e:
            result = RunResult(
                "ALL", f"ndfd_backfill:{element.variable}", error=str(e)[:500]
            )
            logger.error("%s backfill failed: %s", element.variable, e)
        _record_run(conn, started_at, result)
        if result.ok:
            ok += 1

    if ok == 0:
        logger.error("all %d variables failed", len(elements))
        return 1
    return 0
