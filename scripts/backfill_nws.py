#!/usr/bin/env python
"""Shim for `kalshi-weather backfill nws-cli` — historical climate reports from the
IEM AFOS archive.

    .venv/bin/python scripts/backfill_nws.py --start 2026-01-01 --end 2026-07-01 [--station nyc]

Canonical implementation: kalshi_weather/cli.py. Full operating guide: docs/runbook.md.
"""

from __future__ import annotations

import sys

import typer

from kalshi_weather.cli import backfill_nws_cli

app = typer.Typer(add_completion=False)
app.command()(backfill_nws_cli)

if __name__ == "__main__":
    sys.exit(app())
