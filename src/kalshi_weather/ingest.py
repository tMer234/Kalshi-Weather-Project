"""Ingestion orchestration: grid forecasts + CLI climate reports (+ optional METAR).

Single idempotent run: fetch, upsert, exit. Per-station failures are isolated — one
station's error is logged to ingest_runs and must never abort the others. Stations are
processed sequentially (polite API citizenship), each inside its own transaction.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import partial

import duckdb

from .cli_parser import CLIParseError, parse_cli_product
from .config import Settings
from .nws_client import NWSClient, NWSError
from .resolve import ensure_stations_resolved
from .time_utils import horizon_hours, parse_interval, to_utc_naive

logger = logging.getLogger(__name__)

# Layers weather-threshold contracts key off — a fixed allowlist rather than "only max/min
# temp", so adding markets later doesn't require re-collecting history.
GRID_VARIABLES = [
    "maxTemperature",
    "minTemperature",
    "temperature",
    "dewpoint",
    "relativeHumidity",
    "windSpeed",
    "windGust",
    "skyCover",
    "quantitativePrecipitation",
    "probabilityOfPrecipitation",
    "snowfallAmount",
]

# how many recent CLI products to consider per station per run (finals + intermediates;
# offices issue 1-3/day, so this covers roughly the past week)
CLI_PRODUCTS_PER_RUN = 15

_GRID_UPSERT = """
INSERT INTO grid_forecasts (
    station_id, variable, issued_time, valid_start, valid_end,
    horizon_hours, value, unit, pulled_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (station_id, variable, issued_time, valid_start) DO NOTHING
"""

# update-on-newer-issuance: a corrected/re-issued CLI product overwrites the stored value,
# but a stale re-fetch can never regress a newer correction
_CLIMATE_UPSERT = """
INSERT INTO climate_reports (
    station_id, obs_date, variable, value, value_time, unit,
    product_id, issued_time, pulled_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (station_id, obs_date, variable) DO UPDATE SET
    value = excluded.value,
    value_time = excluded.value_time,
    unit = excluded.unit,
    product_id = excluded.product_id,
    issued_time = excluded.issued_time,
    pulled_at = excluded.pulled_at
WHERE excluded.issued_time > climate_reports.issued_time
"""

_OBS_UPSERT = """
INSERT INTO observations (
    obs_station_id, variable, timestamp, value, unit, quality_control, pulled_at
) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (obs_station_id, variable, timestamp) DO NOTHING
"""

# METAR properties worth keeping as supplementary signals
OBS_VARIABLES = ["temperature", "dewpoint", "windSpeed", "windGust", "precipitationLastHour"]


@dataclass
class RunResult:
    station_id: str
    endpoint: str
    http_status: int | None = None
    rows_upserted: int = 0
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _normalize_unit(uom: str | None) -> str | None:
    return uom.removeprefix("wmoUnit:") if uom else None


def _station_rows(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    cur = conn.execute("SELECT * FROM stations ORDER BY station_id")
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


# --- http_cache helpers (conditional GET + product-seen dedupe) -------------


def _cached_last_modified(conn: duckdb.DuckDBPyConnection, url: str) -> str | None:
    row = conn.execute("SELECT last_modified FROM http_cache WHERE url = ?", [url]).fetchone()
    return row[0] if row else None


def _mark_fetched(
    conn: duckdb.DuckDBPyConnection, url: str, last_modified: str | None
) -> None:
    conn.execute(
        "INSERT INTO http_cache (url, last_modified, fetched_at) VALUES (?, ?, ?) "
        "ON CONFLICT (url) DO UPDATE SET last_modified = excluded.last_modified, "
        "fetched_at = excluded.fetched_at",
        [url, last_modified, _utcnow()],
    )


def _seen(conn: duckdb.DuckDBPyConnection, url: str) -> bool:
    return conn.execute("SELECT 1 FROM http_cache WHERE url = ?", [url]).fetchone() is not None


# --- per-endpoint collectors -------------------------------------------------


def _count_rows(conn: duckdb.DuckDBPyConnection, station_id: str) -> int:
    row = conn.execute(
        "SELECT count(*) FROM grid_forecasts WHERE station_id = ?", [station_id]
    ).fetchone()
    return row[0] if row else 0


def ingest_grid_forecasts(
    client: NWSClient, conn: duckdb.DuckDBPyConnection, station: dict
) -> RunResult:
    """Pull the raw numeric gridpoint layers and append any new forecast vintage."""
    result = RunResult(station["station_id"], "grid_forecasts")
    url = station["forecast_grid_data_url"]
    resp = client.get_grid_data(url, if_modified_since=_cached_last_modified(conn, url))
    result.http_status = resp.status
    if resp.not_modified:
        logger.info("%s: grid data unchanged (304)", station["station_id"])
        return result

    assert resp.payload is not None
    props = resp.payload["properties"]
    issued = to_utc_naive(datetime.fromisoformat(props["updateTime"]))
    pulled_at = _utcnow()

    rows = []
    for variable in GRID_VARIABLES:
        layer = props.get(variable)
        if not layer or "values" not in layer:
            continue
        unit = _normalize_unit(layer.get("uom"))
        for entry in layer["values"]:
            if entry.get("value") is None:  # NWS uses null for unavailable periods
                continue
            valid_start, valid_end = parse_interval(entry["validTime"])
            rows.append(
                (
                    station["station_id"],
                    variable,
                    issued,
                    to_utc_naive(valid_start),
                    to_utc_naive(valid_end),
                    horizon_hours(issued, to_utc_naive(valid_start)),
                    float(entry["value"]),
                    unit,
                    pulled_at,
                )
            )

    before = _count_rows(conn, station["station_id"])
    if rows:
        conn.executemany(_GRID_UPSERT, rows)
    result.rows_upserted = _count_rows(conn, station["station_id"]) - before
    _mark_fetched(conn, url, resp.last_modified)
    logger.info(
        "%s: grid forecast issued %s — %d/%d rows new",
        station["station_id"], issued, result.rows_upserted, len(rows),
    )
    return result


def ingest_climate_reports(
    client: NWSClient, conn: duckdb.DuckDBPyConnection, station: dict
) -> RunResult:
    """Pull new CLI (Daily Climate Report) products and upsert parsed values.

    Products are listed under the station's 3-letter site code (cli_location_id), NOT the
    WFO id — verified live against the products API. Each product already fetched once is
    skipped via http_cache, since a text product is immutable per product_id.
    """
    result = RunResult(station["station_id"], "climate_reports")
    if not station["cli_location_id"] or not station["cli_site_name"]:
        result.error = "station has no cli_location_id/cli_site_name configured"
        logger.warning("%s: %s — skipping climate reports", station["station_id"], result.error)
        return result

    products = client.get_cli_products(station["cli_location_id"], limit=CLI_PRODUCTS_PER_RUN)
    result.http_status = 200
    pulled_at = _utcnow()
    rows = []
    parse_failures = 0
    # oldest first so newer issuances land last (the WHERE guard makes this safe either way)
    for listing in reversed(products):
        product_url = f"/products/{listing['id']}"
        if _seen(conn, client.base_url + product_url):
            continue
        product = client.get_product(listing["id"])
        issued = to_utc_naive(datetime.fromisoformat(product["issuanceTime"]))
        try:
            report = parse_cli_product(product["productText"], station["cli_site_name"])
        except CLIParseError as e:
            # fail loudly per plan: log + skip, never guess; do NOT mark seen so a parser
            # fix can pick the product up on a later run
            parse_failures += 1
            logger.error("%s: CLI product %s unparseable: %s", station["station_id"], listing["id"], e)
            continue
        for value in report.values:
            if value.value is None:
                logger.warning(
                    "%s: %s is MM (missing) in product %s — skipped",
                    station["station_id"], value.variable, listing["id"],
                )
                continue
            rows.append(
                (
                    station["station_id"],
                    report.obs_date,
                    value.variable,
                    value.value,
                    value.value_time,
                    value.unit,
                    listing["id"],
                    issued,
                    pulled_at,
                )
            )
        _mark_fetched(conn, client.base_url + product_url, None)

    if rows:
        conn.executemany(_CLIMATE_UPSERT, rows)
    result.rows_upserted = len(rows)
    if parse_failures:
        result.error = f"{parse_failures} CLI product(s) failed to parse (see logs)"
    logger.info(
        "%s: %d climate-report values upserted from %d product(s) listed",
        station["station_id"], len(rows), len(products),
    )
    return result


def ingest_observations(
    client: NWSClient, conn: duckdb.DuckDBPyConnection, station: dict, days: int = 7
) -> RunResult:
    """SECONDARY: raw METAR observations — supplementary signal, not settlement truth."""
    result = RunResult(station["station_id"], "observations")
    start = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat(timespec="seconds")
    payload = client.get_observations(station["obs_station_id"], start=start)
    result.http_status = 200
    pulled_at = _utcnow()
    rows = []
    for feature in payload.get("features", []):
        props = feature["properties"]
        ts = to_utc_naive(datetime.fromisoformat(props["timestamp"]))
        for variable in OBS_VARIABLES:
            reading = props.get(variable)
            if not reading or reading.get("value") is None:
                continue
            rows.append(
                (
                    station["obs_station_id"],
                    variable,
                    ts,
                    float(reading["value"]),
                    _normalize_unit(reading.get("unitCode")),
                    reading.get("qualityControl"),
                    pulled_at,
                )
            )
    if rows:
        conn.executemany(_OBS_UPSERT, rows)
    result.rows_upserted = len(rows)
    logger.info("%s: %d METAR readings staged", station["station_id"], len(rows))
    return result


# --- orchestration -----------------------------------------------------------


def _record_run(
    conn: duckdb.DuckDBPyConnection, started_at: datetime, result: RunResult
) -> None:
    conn.execute(
        "INSERT INTO ingest_runs (started_at, finished_at, station_id, endpoint, "
        "http_status, rows_upserted, error) VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            started_at,
            _utcnow(),
            result.station_id,
            result.endpoint,
            result.http_status,
            result.rows_upserted,
            result.error,
        ],
    )


def run_ingest(
    settings: Settings,
    conn: duckdb.DuckDBPyConnection,
    include_metar: bool = False,
    metar_days: int = 7,
) -> int:
    """Run one full collection pass. Returns a process exit code:

    0 — at least one station succeeded end to end
    1 — every station failed (config/auth errors raise before we get here)
    """
    client = NWSClient(user_agent=settings.user_agent)
    ensure_stations_resolved(client, conn, settings.stations)

    collectors: list[tuple[str, Callable[..., RunResult]]] = [
        ("grid_forecasts", ingest_grid_forecasts),
        ("climate_reports", ingest_climate_reports),
    ]
    if include_metar:
        collectors.append(
            ("observations", partial(ingest_observations, days=metar_days))
        )

    stations_ok = 0
    for station in _station_rows(conn):
        endpoint_results = []
        for endpoint, collector in collectors:
            started_at = _utcnow()
            conn.execute("BEGIN")
            try:
                result = collector(client, conn, station)
                conn.execute("COMMIT")
            except (NWSError, KeyError, ValueError) as e:
                conn.execute("ROLLBACK")
                result = RunResult(
                    station["station_id"],
                    endpoint,
                    http_status=getattr(e, "status", None),
                    error=str(e)[:500],
                )
                logger.error("%s/%s failed: %s", station["station_id"], endpoint, e)
            endpoint_results.append(result)
            _record_run(conn, started_at, result)
        # a station counts as failed only when NO endpoint succeeded — partial failures
        # (e.g. one unparseable CLI product) are recorded in ingest_runs, not the exit
        # code, so cron alerting stays quiet on transient hiccups
        if any(r.ok for r in endpoint_results):
            stations_ok += 1

    if stations_ok == 0:
        logger.error("all %d stations failed", len(settings.stations))
        return 1
    logger.info("ingest complete: %d/%d stations ok", stations_ok, len(settings.stations))
    return 0
