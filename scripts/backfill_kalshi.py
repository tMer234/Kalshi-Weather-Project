#!/usr/bin/env python
"""Shim for `kalshi-weather backfill kalshi` — settled market history + candlestick
price bars.

    .venv/bin/python scripts/backfill_kalshi.py --start 2026-01-01 --end 2026-07-01 [--no-candles]

Canonical implementation: kalshi_weather/cli.py. Full operating guide: docs/runbook.md.
"""

from __future__ import annotations

import sys

import typer

from kalshi_weather.cli import backfill_kalshi

app = typer.Typer(add_completion=False)
app.command()(backfill_kalshi)

if __name__ == "__main__":
    sys.exit(app())
