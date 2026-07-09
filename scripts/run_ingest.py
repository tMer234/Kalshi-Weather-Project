#!/usr/bin/env python
"""Run one full NWS collection pass (grid forecasts + CLI climate reports).

Idempotent: safe to re-run any time; re-running against unchanged NWS data is a no-op.
Designed to be invoked from cron/launchd/a hosted scheduler — the exit code is non-zero
only when every station failed or configuration prevented startup, so transient
single-station hiccups don't page anyone.

    .venv/bin/python scripts/run_ingest.py [--metar] [--metar-days 7]
"""

from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

import typer

from kalshi_weather import db
from kalshi_weather.config import REPO_ROOT, ConfigError, load_settings
from kalshi_weather.ingest import run_ingest

app = typer.Typer(add_completion=False)

EXIT_CONFIG_ERROR = 2


def _setup_logging() -> None:
    log_dir = REPO_ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            RotatingFileHandler(
                log_dir / "ingest.log", maxBytes=5_000_000, backupCount=5
            ),
        ],
    )


@app.command()
def main(
    metar: bool = typer.Option(
        False, "--metar", help="Also pull raw METAR observations (secondary signal)."
    ),
    metar_days: int = typer.Option(
        7, "--metar-days", help="How many days of METAR history to request."
    ),
    duckdb_path: Path | None = typer.Option(
        None, "--db", help="Override DUCKDB_PATH from the environment."
    ),
) -> None:
    _setup_logging()
    try:
        settings = load_settings()
    except ConfigError as e:
        logging.getLogger(__name__).error("config error: %s", e)
        raise typer.Exit(EXIT_CONFIG_ERROR)

    conn = db.connect(duckdb_path or settings.duckdb_path)
    try:
        exit_code = run_ingest(settings, conn, include_metar=metar, metar_days=metar_days)
    finally:
        conn.close()
    if exit_code:
        raise typer.Exit(exit_code)


if __name__ == "__main__":
    sys.exit(app())
