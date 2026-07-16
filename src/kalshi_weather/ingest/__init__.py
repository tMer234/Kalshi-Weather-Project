"""Ingestion runners: NWS (grid forecasts + CLI climate reports) and Kalshi (market
quotes + settlement outcomes), each split into narrow-cadence pieces plus their
historical backfills.

    kalshi_weather.ingest.nws              grid + climate collectors, run_ingest()
    kalshi_weather.ingest.kalshi           quote + outcome collectors, run_kalshi_ingest()
    kalshi_weather.ingest.nws_backfill     historical climate reports (IEM archive)
    kalshi_weather.ingest.kalshi_backfill  historical settled markets + candles

See docs/runbook.md for the operating guide and plans/data_automation_plan.md for why
each collector is split into narrow subcommands.
"""

from __future__ import annotations
