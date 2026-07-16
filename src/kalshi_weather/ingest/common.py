"""Shared by every collector and backfill in this package: the per-endpoint result
type, the ingest_runs audit-row writer, and the naive-UTC clock helper."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import duckdb


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
