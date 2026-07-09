#!/usr/bin/env python
"""Resolve all configured stations' NWS grid metadata into the stations table.

Run once at setup and re-run occasionally — NWS grid mappings can drift.

    .venv/bin/python scripts/resolve_stations.py [--force]
"""

from __future__ import annotations

import logging

import typer

from kalshi_weather import db
from kalshi_weather.config import load_settings
from kalshi_weather.nws_client import NWSClient
from kalshi_weather.resolve import ensure_stations_resolved

app = typer.Typer(add_completion=False)


@app.command()
def main(
    force: bool = typer.Option(
        False, "--force", help="Re-resolve stations already present in the table."
    ),
) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    settings = load_settings()
    client = NWSClient(user_agent=settings.user_agent)
    conn = db.connect(settings.duckdb_path)
    try:
        resolved = ensure_stations_resolved(client, conn, settings.stations, force=force)
        typer.echo(f"resolved {len(resolved)} station(s)")
        for row in conn.execute(
            "SELECT station_id, grid_id, grid_x, grid_y, wfo_id, obs_station_id,"
            " station_verified FROM stations ORDER BY station_id"
        ).fetchall():
            typer.echo(f"  {row}")
    finally:
        conn.close()


if __name__ == "__main__":
    app()
