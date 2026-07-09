"""DuckDB connection handling and idempotent schema creation."""

from __future__ import annotations

from pathlib import Path

import duckdb

SCHEMA_SQL = Path(__file__).with_name("schema.sql")


def connect(db_path: Path | str) -> duckdb.DuckDBPyConnection:
    """Open (creating if needed) the DuckDB file and ensure the schema exists."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    conn.execute(SCHEMA_SQL.read_text())
    return conn
