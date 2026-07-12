"""Historical climate-report backfill from the IEM AFOS archive (Phase 0c).

Fills climate_reports for dates before the live collector existed. Same parser, same
upsert, different transport: IEM archives the identical CLI bulletins api.weather.gov
only serves for a few days. Rows land with source='iem_afos'.

Safety properties:
- The newer-issuance upsert guard means a backfill can NEVER regress a value the live
  collector already landed (finals outrank intermediates purely by issued_time), so
  overlapping date ranges are harmless.
- Fetched IEM product URLs are marked in http_cache, so re-running a backfill skips
  already-processed products (delete those rows to force a re-parse after a parser fix).
- Products that fail to parse are logged and skipped, never guessed at, and NOT marked
  seen — a parser fix picks them up on the next run.

NB: this backfills settlement truth only. Historical FORECAST vintages (grid_forecasts)
would need the NDFD archive (GRIB2 via NCEI) — deliberately not built yet; see the
master plan Phase 0c.
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta

import duckdb

from .cli_parser import CLIParseError, parse_cli_product
from .config import Settings
from .iem_client import IEMClient, IEMError
from .ingest import CLIMATE_UPSERT, RunResult, _mark_fetched, _record_run, _seen, _utcnow
from .resolve import ensure_station_stubs

logger = logging.getLogger(__name__)

# polite gap between IEM requests on bulk pulls (their archive is a free public service)
DEFAULT_SLEEP_SECONDS = 0.5


def _iem_issued_time(entry: dict) -> datetime:
    """Issuance from the listing's 'entered' field ('2026-07-10T06:25:00Z') as naive UTC."""
    return datetime.fromisoformat(entry["entered"].replace("Z", "+00:00")).replace(tzinfo=None)


def backfill_station_climate(
    client: IEMClient,
    conn: duckdb.DuckDBPyConnection,
    station,
    start: date,
    end: date,
    sleep_seconds: float = DEFAULT_SLEEP_SECONDS,
) -> RunResult:
    """Backfill one station's climate reports for obs_dates in [start, end]."""
    result = RunResult(station.station_id, "nws_backfill")
    pil_location = station.effective_cli_location_id
    if not pil_location or not station.cli_site_name:
        result.error = "station has no cli_location_id/cli_site_name configured"
        logger.warning("%s: %s — skipping backfill", station.station_id, result.error)
        return result
    pil = f"CLI{pil_location}"

    # a day's FINAL report is published the following morning, so listing UTC days
    # [start, end+1] covers every product whose obs_date falls in [start, end]
    rows, parse_failures = [], 0
    day = start
    while day <= end + timedelta(days=1):
        listings = client.list_products(pil, day.isoformat())
        result.http_status = 200
        for entry in listings:
            text_url = entry.get("text_link") or f"{client.base_url}/api/1/nwstext/{entry['product_id']}"
            if _seen(conn, text_url):
                continue
            text = client.get_product_text(entry["product_id"])
            time.sleep(sleep_seconds)
            try:
                report = parse_cli_product(text, station.cli_site_name)
            except CLIParseError as e:
                parse_failures += 1
                logger.error(
                    "%s: IEM product %s unparseable: %s",
                    station.station_id, entry["product_id"], e,
                )
                continue
            if not (start <= report.obs_date <= end):
                _mark_fetched(conn, text_url, None)
                continue
            issued = _iem_issued_time(entry)
            pulled_at = _utcnow()
            for value in report.values:
                if value.value is None:
                    continue
                rows.append(
                    (
                        station.station_id,
                        report.obs_date,
                        value.variable,
                        value.value,
                        value.value_time,
                        value.unit,
                        entry["product_id"],
                        issued,
                        pulled_at,
                        "iem_afos",
                    )
                )
            _mark_fetched(conn, text_url, None)
        time.sleep(sleep_seconds)
        day += timedelta(days=1)

    if rows:
        conn.executemany(CLIMATE_UPSERT, rows)
    result.rows_upserted = len(rows)
    if parse_failures:
        result.error = f"{parse_failures} IEM product(s) failed to parse (see logs)"
    logger.info(
        "%s: backfilled %d climate values for %s..%s",
        station.station_id, len(rows), start, end,
    )
    return result


def run_nws_backfill(
    settings: Settings,
    conn: duckdb.DuckDBPyConnection,
    start: date,
    end: date,
    station_ids: list[str] | None = None,
    sleep_seconds: float = DEFAULT_SLEEP_SECONDS,
) -> int:
    """Backfill climate reports for all (or selected) stations. Exit-code contract
    matches run_ingest: 0 = at least one station succeeded, 1 = every station failed."""
    if start > end:
        logger.error("start %s is after end %s", start, end)
        return 1
    client = IEMClient(user_agent=settings.user_agent)
    stations = [
        s for s in settings.stations if station_ids is None or s.station_id in station_ids
    ]
    if not stations:
        logger.error("no stations matched %r", station_ids)
        return 1
    # satisfy the climate_reports FK on a fresh DB without touching the NWS API
    ensure_station_stubs(conn, stations)

    ok = 0
    for station in stations:
        started_at = _utcnow()
        conn.execute("BEGIN")
        try:
            result = backfill_station_climate(
                client, conn, station, start, end, sleep_seconds
            )
            conn.execute("COMMIT")
        except (IEMError, KeyError, ValueError) as e:
            conn.execute("ROLLBACK")
            result = RunResult(
                station.station_id,
                "nws_backfill",
                http_status=getattr(e, "status", None),
                error=str(e)[:500],
            )
            logger.error("%s backfill failed: %s", station.station_id, e)
        _record_run(conn, started_at, result)
        if result.ok:
            ok += 1

    if ok == 0:
        logger.error("all %d stations failed", len(stations))
        return 1
    logger.info("nws backfill complete: %d/%d stations ok", ok, len(stations))
    return 0
