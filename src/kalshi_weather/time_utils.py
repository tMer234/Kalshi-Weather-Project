"""Time helpers: NWS ISO8601 interval parsing and forecast-horizon arithmetic.

NWS gridpoint `validTime` values are start/duration intervals like
"2019-07-04T18:00:00+00:00/PT3H". Durations use ISO8601 (PT3H, P1D, P1DT6H, ...), which
`isodate` parses; hand-rolled regex would miss compound forms.
"""

from __future__ import annotations

from datetime import datetime, timezone

import isodate

HOUR_SECONDS = 3600.0


def parse_interval(interval: str) -> tuple[datetime, datetime]:
    """Parse an NWS validTime interval into UTC-aware (valid_start, valid_end)."""
    try:
        start_raw, duration_raw = interval.split("/", 1)
    except ValueError as e:
        raise ValueError(f"validTime {interval!r} is not a start/duration interval") from e
    try:
        start = datetime.fromisoformat(start_raw)
        duration = isodate.parse_duration(duration_raw)
    except (ValueError, isodate.ISO8601Error) as e:
        raise ValueError(f"could not parse validTime {interval!r}: {e}") from e
    if start.tzinfo is None:
        raise ValueError(f"validTime {interval!r} has no UTC offset")
    start = start.astimezone(timezone.utc)
    return start, start + duration


def horizon_hours(issued: datetime, valid_start: datetime) -> float:
    """Forecast lead time in hours. Negative when the valid period is already underway."""
    return (valid_start - issued).total_seconds() / HOUR_SECONDS


def to_utc_naive(dt: datetime) -> datetime:
    """Convert an aware datetime to naive UTC for storage in DuckDB TIMESTAMP columns."""
    if dt.tzinfo is None:
        raise ValueError(f"refusing to treat naive datetime {dt!r} as UTC")
    return dt.astimezone(timezone.utc).replace(tzinfo=None)
