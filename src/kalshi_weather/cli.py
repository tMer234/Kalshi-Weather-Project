"""The `kalshi-weather` command — single entry point for every collector and backfill.

    kalshi-weather ingest nws        # live NWS pass: forecasts + climate reports
    kalshi-weather ingest kalshi     # live Kalshi pass: markets + quotes + outcomes
    kalshi-weather backfill nws-cli  --start 2026-01-01 --end 2026-07-01
    kalshi-weather backfill kalshi   --start 2026-01-01 --end 2026-07-01

Installed via [project.scripts] in pyproject.toml (`pip install -e .` registers it).
The scripts/*.py files are thin cron-compatible shims over these same commands.
Full operating guide: docs/runbook.md.

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
    """Live NWS pass: gridpoint forecast vintages + CLI climate reports."""
    from .ingest import run_ingest

    _setup_logging("ingest.log")
    settings = _load_settings_or_exit()
    _run(
        lambda conn: run_ingest(settings, conn, include_metar=metar, metar_days=metar_days),
        settings,
        duckdb_path,
    )


@ingest_app.command("kalshi")
def ingest_kalshi(duckdb_path: Path | None = DB_OPTION) -> None:
    """Live Kalshi pass: market definitions + one quote snapshot per open market +
    recently settled outcomes. Cron cadence = quote-history resolution."""
    from .kalshi_ingest import run_kalshi_ingest

    _setup_logging("kalshi_ingest.log")
    settings = _load_settings_or_exit()
    _run(lambda conn: run_kalshi_ingest(settings, conn), settings, duckdb_path)


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
    from .nws_backfill import run_nws_backfill

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
    period: int = typer.Option(
        60, "--period", help="Candlestick bar length in minutes: 1, 60, or 1440."
    ),
    sleep: float = typer.Option(
        0.2, "--sleep", help="Seconds between candlestick requests (politeness)."
    ),
    duckdb_path: Path | None = DB_OPTION,
) -> None:
    """Backfill settled Kalshi markets, outcomes, and (optionally) candlestick price
    history for a historical obs_date range. Resumable: markets that already have
    candles at the requested period are skipped."""
    from .kalshi_backfill import run_kalshi_backfill

    if period not in (1, 60, 1440):
        logging.getLogger(__name__).error("--period must be 1, 60, or 1440")
        raise typer.Exit(EXIT_CONFIG_ERROR)
    _setup_logging("kalshi_backfill.log")
    settings = _load_settings_or_exit()
    start_d, end_d = _parse_date(start, "--start"), _parse_date(end, "--end")
    _run(
        lambda conn: run_kalshi_backfill(
            settings, conn, start_d, end_d,
            series_tickers=list(series) if series else None,
            include_candles=candles,
            period_minutes=period,
            sleep_seconds=sleep,
        ),
        settings,
        duckdb_path,
    )


if __name__ == "__main__":
    sys.exit(app())
