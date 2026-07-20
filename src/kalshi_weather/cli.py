"""The `kalshi-weather` command — single entry point for every collector and backfill.

    kalshi-weather ingest nws                # live NWS pass: forecasts + climate reports (manual/combined)
    kalshi-weather ingest kalshi             # live Kalshi pass: markets + quotes + outcomes (manual/combined)
    kalshi-weather ingest nws-grid           # narrow: gridpoint forecast vintages only
    kalshi-weather ingest nws-cli            # narrow: CLI climate reports only
    kalshi-weather ingest kalshi-quotes      # narrow: market defs + quote snapshots only
    kalshi-weather ingest kalshi-resolutions # narrow: settled outcomes only
    kalshi-weather backfill nws-cli             --start 2026-01-01 --end 2026-07-01
    kalshi-weather backfill nws-grid            --start 2026-01-01 --end 2026-07-01  # NDFD archive
    kalshi-weather backfill kalshi              --start 2026-01-01 --end 2026-07-01  # both halves
    kalshi-weather backfill kalshi-quotes       --start 2026-01-01 --end 2026-07-01  # candles only
    kalshi-weather backfill kalshi-resolutions  --start 2026-01-01 --end 2026-07-01  # outcomes only

The four narrow `ingest` subcommands exist for the scheduled GitHub Actions workflows in
plans/data_automation_plan.md, each with its own cadence; `ingest nws`/`ingest kalshi`
remain as manual convenience commands running both halves in one pass. Same split for
`backfill kalshi`; `backfill nws-cli` was already single-stream. `backfill nws-grid`
(historical forecast vintages, from the NDFD GRIB2 archive) needs the optional
`pygrib`/`numpy` dependencies: `pip install -e '.[ndfd]'` — see docs/runbook.md §2.1.

Installed via [project.scripts] in pyproject.toml (`pip install -e .` registers it).
The scripts/*.py files are thin cron-compatible shims over the `ingest nws`/`ingest
kalshi`/`backfill *` commands. Full operating guide: docs/runbook.md.

Exit codes everywhere: 0 = at least one station/series succeeded, 1 = everything
failed, 2 = configuration error.
"""

from __future__ import annotations

import logging
import sys
from datetime import date
from logging.handlers import RotatingFileHandler
from pathlib import Path

import typer

from . import db
from .config import REPO_ROOT, ConfigError, Settings, load_settings

app = typer.Typer(
    add_completion=False,
    help="Kalshi weather-market data pipelines: live ingests (NWS forecasts + climate "
    "reports, Kalshi markets + quotes + outcomes) and historical backfills (IEM CLI "
    "archive, Kalshi settled markets + candlesticks). Operating guide: docs/runbook.md",
)
ingest_app = typer.Typer(help="One live collection pass (idempotent; safe to re-run).")
backfill_app = typer.Typer(help="Historical backfills from archive sources.")
app.add_typer(ingest_app, name="ingest")
app.add_typer(backfill_app, name="backfill")

EXIT_CONFIG_ERROR = 2

DB_OPTION = typer.Option(None, "--db", help="Override DUCKDB_PATH from the environment.")


def _setup_logging(logfile: str) -> None:
    log_dir = REPO_ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            RotatingFileHandler(log_dir / logfile, maxBytes=5_000_000, backupCount=5),
        ],
    )


def _load_settings_or_exit() -> Settings:
    try:
        return load_settings()
    except ConfigError as e:
        logging.getLogger(__name__).error("config error: %s", e)
        raise typer.Exit(EXIT_CONFIG_ERROR)


def _parse_date(value: str, name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        logging.getLogger(__name__).error("%s must be YYYY-MM-DD, got %r", name, value)
        raise typer.Exit(EXIT_CONFIG_ERROR)


def _run(runner, settings: Settings, duckdb_path: Path | None) -> None:
    conn = db.connect(duckdb_path or settings.duckdb_path)
    try:
        exit_code = runner(conn)
    finally:
        conn.close()
    if exit_code:
        raise typer.Exit(exit_code)


# --- ingest ------------------------------------------------------------------


@ingest_app.command("nws")
def ingest_nws(
    metar: bool = typer.Option(
        False, "--metar", help="Also pull raw METAR observations (secondary signal)."
    ),
    metar_days: int = typer.Option(
        7, "--metar-days", help="How many days of METAR history to request."
    ),
    duckdb_path: Path | None = DB_OPTION,
) -> None:
    """Live NWS pass: gridpoint forecast vintages + CLI climate reports (both halves;
    manual convenience command — see `nws-grid`/`nws-cli` for scheduled narrow runs)."""
    from .ingest.nws import run_ingest

    _setup_logging("ingest.log")
    settings = _load_settings_or_exit()
    _run(
        lambda conn: run_ingest(settings, conn, include_metar=metar, metar_days=metar_days),
        settings,
        duckdb_path,
    )


@ingest_app.command("nws-grid")
def ingest_nws_grid(duckdb_path: Path | None = DB_OPTION) -> None:
    """Live NWS pass: gridpoint forecast vintages only. Scheduled hourly — NWS re-issues
    roughly hourly, and a missed vintage can never be recovered (docs/runbook.md §1.1)."""
    from .ingest.nws import run_ingest

    _setup_logging("ingest.log")
    settings = _load_settings_or_exit()
    _run(
        lambda conn: run_ingest(settings, conn, include_climate=False),
        settings,
        duckdb_path,
    )


@ingest_app.command("nws-cli")
def ingest_nws_cli(duckdb_path: Path | None = DB_OPTION) -> None:
    """Live NWS pass: CLI climate reports only. Not latency-critical — the products API
    serves about a week of history, so a missed run self-heals on the next one."""
    from .ingest.nws import run_ingest

    _setup_logging("ingest.log")
    settings = _load_settings_or_exit()
    _run(
        lambda conn: run_ingest(settings, conn, include_grid=False),
        settings,
        duckdb_path,
    )


@ingest_app.command("kalshi")
def ingest_kalshi(duckdb_path: Path | None = DB_OPTION) -> None:
    """Live Kalshi pass: market definitions + one quote snapshot per open market +
    recently settled outcomes (both halves; manual convenience command — see
    `kalshi-quotes`/`kalshi-resolutions` for scheduled narrow runs)."""
    from .ingest.kalshi import run_kalshi_ingest

    _setup_logging("kalshi_ingest.log")
    settings = _load_settings_or_exit()
    _run(lambda conn: run_kalshi_ingest(settings, conn), settings, duckdb_path)


@ingest_app.command("kalshi-quotes")
def ingest_kalshi_quotes(duckdb_path: Path | None = DB_OPTION) -> None:
    """Live Kalshi pass: market definitions + one quote snapshot per open market only.
    Cron cadence IS the quote-history resolution — append-only, never recoverable."""
    from .ingest.kalshi import run_kalshi_ingest

    _setup_logging("kalshi_ingest.log")
    settings = _load_settings_or_exit()
    _run(
        lambda conn: run_kalshi_ingest(settings, conn, include_resolutions=False),
        settings,
        duckdb_path,
    )


@ingest_app.command("kalshi-resolutions")
def ingest_kalshi_resolutions(duckdb_path: Path | None = DB_OPTION) -> None:
    """Live Kalshi pass: recently settled outcomes only. Not latency-critical — each
    pass re-checks the last ~60 settled markets per series regardless of prior runs."""
    from .ingest.kalshi import run_kalshi_ingest

    _setup_logging("kalshi_ingest.log")
    settings = _load_settings_or_exit()
    _run(
        lambda conn: run_kalshi_ingest(settings, conn, include_quotes=False),
        settings,
        duckdb_path,
    )


# --- backfill ----------------------------------------------------------------


@backfill_app.command("nws-cli")
def backfill_nws_cli(
    start: str = typer.Option(..., "--start", help="First obs_date to backfill (YYYY-MM-DD)."),
    end: str = typer.Option(..., "--end", help="Last obs_date to backfill (YYYY-MM-DD)."),
    station: list[str] = typer.Option(
        None, "--station", help="Station slug(s) to backfill (default: all). Repeatable."
    ),
    sleep: float = typer.Option(
        0.5, "--sleep", help="Seconds between IEM requests (politeness)."
    ),
    duckdb_path: Path | None = DB_OPTION,
) -> None:
    """Backfill historical climate reports (settlement truth) from the IEM AFOS archive.

    Source: mesonet.agron.iastate.edu — the same CLI bulletins NWS only serves for a few
    days, archived back decades. Safe to overlap with live data: the newer-issuance
    guard never regresses a value. Does NOT backfill forecasts (see runbook)."""
    from .ingest.nws_backfill import run_nws_backfill

    _setup_logging("nws_backfill.log")
    settings = _load_settings_or_exit()
    start_d, end_d = _parse_date(start, "--start"), _parse_date(end, "--end")
    _run(
        lambda conn: run_nws_backfill(
            settings, conn, start_d, end_d,
            station_ids=list(station) if station else None,
            sleep_seconds=sleep,
        ),
        settings,
        duckdb_path,
    )


@backfill_app.command("nws-grid")
def backfill_nws_grid(
    start: str = typer.Option(..., "--start", help="First obs_date to backfill (YYYY-MM-DD)."),
    end: str = typer.Option(..., "--end", help="Last obs_date to backfill (YYYY-MM-DD)."),
    station: list[str] = typer.Option(
        None, "--station", help="Station slug(s) to backfill (default: all). Repeatable."
    ),
    variable: list[str] = typer.Option(
        None, "--variable",
        help="grid_forecasts variable(s) to backfill (default: all 11). Repeatable.",
    ),
    sleep: float = typer.Option(
        0.2, "--sleep", help="Seconds between NDFD archive requests (politeness)."
    ),
    issuance_cadence: str = typer.Option(
        "hourly", "--issuance-cadence",
        help="'hourly' (default) thins the archive's ~30-min native cadence to match the "
        "live collector's own hourly cadence, so backfilled and live-collected history "
        "stay density-consistent; 'full' keeps every archived issuance (~2x the rows).",
    ),
    duckdb_path: Path | None = DB_OPTION,
) -> None:
    """Backfill historical forecast vintages (grid_forecasts) from the NDFD GRIB2 archive.

    Source: the public `noaa-ndfd-pds` S3 bucket — the same NDFD grids api.weather.gov
    serves live, packaged as GRIB2. Needs the optional NDFD dependencies:
    `pip install -e '.\\[ndfd]'`. Rows land with source='ndfd_archive'; the upsert can
    never regress or duplicate a live-collected row. See docs/runbook.md §2.1 for what
    was validated (9/11 variables match closely; probabilityOfPrecipitation backfills
    at a coarser 12h resolution; snowfallAmount's unit conversion is unconfirmed against
    real winter data)."""
    from .ingest.ndfd_backfill import DEFAULT_ISSUANCE_CADENCE_MINUTES, run_ndfd_backfill

    if issuance_cadence not in ("hourly", "full"):
        logging.getLogger(__name__).error("--issuance-cadence must be 'hourly' or 'full'")
        raise typer.Exit(EXIT_CONFIG_ERROR)
    cadence_minutes = DEFAULT_ISSUANCE_CADENCE_MINUTES if issuance_cadence == "hourly" else None

    _setup_logging("ndfd_backfill.log")
    settings = _load_settings_or_exit()
    start_d, end_d = _parse_date(start, "--start"), _parse_date(end, "--end")
    try:
        _run(
            lambda conn: run_ndfd_backfill(
                settings, conn, start_d, end_d,
                station_ids=list(station) if station else None,
                variables=list(variable) if variable else None,
                sleep_seconds=sleep,
                issuance_cadence_minutes=cadence_minutes,
            ),
            settings,
            duckdb_path,
        )
    except RuntimeError as e:
        logging.getLogger(__name__).error(str(e))
        raise typer.Exit(EXIT_CONFIG_ERROR)


PERIOD_OPTION = typer.Option(
    60, "--period", help="Candlestick bar length in minutes: 1, 60, or 1440."
)


def _check_period(period: int) -> None:
    if period not in (1, 60, 1440):
        logging.getLogger(__name__).error("--period must be 1, 60, or 1440")
        raise typer.Exit(EXIT_CONFIG_ERROR)


@backfill_app.command("kalshi")
def backfill_kalshi(
    start: str = typer.Option(..., "--start", help="First obs_date to backfill (YYYY-MM-DD)."),
    end: str = typer.Option(..., "--end", help="Last obs_date to backfill (YYYY-MM-DD)."),
    series: list[str] = typer.Option(
        None, "--series", help="Series ticker(s) to backfill (default: all configured). Repeatable."
    ),
    candles: bool = typer.Option(
        True, "--candles/--no-candles",
        help="Also fetch candlestick price history (one request per market).",
    ),
    period: int = PERIOD_OPTION,
    sleep: float = typer.Option(
        0.2, "--sleep", help="Seconds between candlestick requests (politeness)."
    ),
    duckdb_path: Path | None = DB_OPTION,
) -> None:
    """Backfill settled Kalshi markets, outcomes, and (optionally) candlestick price
    history for a historical obs_date range (both halves; manual convenience command —
    see `kalshi-quotes`/`kalshi-resolutions` for single-stream backfills). Resumable:
    markets that already have candles at the requested period are skipped."""
    from .ingest.kalshi_backfill import run_kalshi_backfill

    _check_period(period)
    _setup_logging("kalshi_backfill.log")
    settings = _load_settings_or_exit()
    start_d, end_d = _parse_date(start, "--start"), _parse_date(end, "--end")
    _run(
        lambda conn: run_kalshi_backfill(
            settings, conn, start_d, end_d,
            series_tickers=list(series) if series else None,
            include_quotes=candles,
            period_minutes=period,
            sleep_seconds=sleep,
        ),
        settings,
        duckdb_path,
    )


@backfill_app.command("kalshi-quotes")
def backfill_kalshi_quotes(
    start: str = typer.Option(..., "--start", help="First obs_date to backfill (YYYY-MM-DD)."),
    end: str = typer.Option(..., "--end", help="Last obs_date to backfill (YYYY-MM-DD)."),
    series: list[str] = typer.Option(
        None, "--series", help="Series ticker(s) to backfill (default: all configured). Repeatable."
    ),
    period: int = PERIOD_OPTION,
    sleep: float = typer.Option(
        0.2, "--sleep", help="Seconds between candlestick requests (politeness)."
    ),
    duckdb_path: Path | None = DB_OPTION,
) -> None:
    """Backfill candlestick price history only — the historical stand-in for
    market_snapshots. Still upserts market definitions (needed for the join) but never
    touches market_outcomes. One request per market; resumable."""
    from .ingest.kalshi_backfill import run_kalshi_backfill

    _check_period(period)
    _setup_logging("kalshi_backfill.log")
    settings = _load_settings_or_exit()
    start_d, end_d = _parse_date(start, "--start"), _parse_date(end, "--end")
    _run(
        lambda conn: run_kalshi_backfill(
            settings, conn, start_d, end_d,
            series_tickers=list(series) if series else None,
            include_resolutions=False,
            period_minutes=period,
            sleep_seconds=sleep,
        ),
        settings,
        duckdb_path,
    )


@backfill_app.command("kalshi-resolutions")
def backfill_kalshi_resolutions(
    start: str = typer.Option(..., "--start", help="First obs_date to backfill (YYYY-MM-DD)."),
    end: str = typer.Option(..., "--end", help="Last obs_date to backfill (YYYY-MM-DD)."),
    series: list[str] = typer.Option(
        None, "--series", help="Series ticker(s) to backfill (default: all configured). Repeatable."
    ),
    sleep: float = typer.Option(
        0.2, "--sleep", help="Seconds between requests (politeness)."
    ),
    duckdb_path: Path | None = DB_OPTION,
) -> None:
    """Backfill settled-market outcomes only (+ their definitions). No candlestick
    requests at all — much faster than `backfill kalshi`/`backfill kalshi-quotes` when
    you only need settlement history."""
    from .ingest.kalshi_backfill import run_kalshi_backfill

    _setup_logging("kalshi_backfill.log")
    settings = _load_settings_or_exit()
    start_d, end_d = _parse_date(start, "--start"), _parse_date(end, "--end")
    _run(
        lambda conn: run_kalshi_backfill(
            settings, conn, start_d, end_d,
            series_tickers=list(series) if series else None,
            include_quotes=False,
            sleep_seconds=sleep,
        ),
        settings,
        duckdb_path,
    )


if __name__ == "__main__":
    sys.exit(app())
