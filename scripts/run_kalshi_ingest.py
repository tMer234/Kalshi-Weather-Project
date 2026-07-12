#!/usr/bin/env python
"""Cron-compatible shim for `kalshi-weather ingest kalshi` — same options, same behavior.

    .venv/bin/python scripts/run_kalshi_ingest.py [--db PATH]

Canonical implementation: kalshi_weather/cli.py. Full operating guide: docs/runbook.md.
"""

from __future__ import annotations

import sys

import typer

from kalshi_weather.cli import ingest_kalshi

app = typer.Typer(add_completion=False)
app.command()(ingest_kalshi)

if __name__ == "__main__":
    sys.exit(app())
