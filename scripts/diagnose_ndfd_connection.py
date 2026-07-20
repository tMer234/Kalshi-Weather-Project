#!/usr/bin/env python
"""Live diagnostic probe against the REAL noaa-ndfd-pds S3 archive.

Not a pytest mock — this hits the actual public bucket the backfill uses, so you can
characterize the RemoteDisconnected / ConnectionResetError pattern seen during
`backfill nws-grid` in a couple of minutes instead of waiting through a multi-hour run.

    .venv/bin/python scripts/diagnose_ndfd_connection.py
    .venv/bin/python scripts/diagnose_ndfd_connection.py --element temp --day 2026-07-06 --count 30

Runs the same sequence of real downloads under three connection strategies and reports,
per strategy: success rate, how many requests needed a retry, average/median/p95 latency,
and whether failures cluster right after an idle gap (the "stale pooled keep-alive
socket" pattern) or seem to hit uniformly at random (an "endpoint is just flaky under
load" pattern). That distinction is the actual question — the two point at different
fixes (connection recycling vs nothing-to-fix-just-retry).

Strategies compared:
  shared      - one requests.Session reused for every call (today's NDFDClient behavior).
  close       - same shared session, but with `Connection: close` on every request, so
                the server can't hand back a keep-alive socket to be reused later.
  fresh       - a brand-new NDFDClient (and thus new Session/connection pool) per request.

Read-only; only ever GETs from the public unauthenticated bucket. No credentials, no
writes, doesn't touch the project's DuckDB file.
"""

from __future__ import annotations

import argparse
import logging
import statistics
import time
from dataclasses import dataclass, field

from kalshi_weather.config import load_settings
from kalshi_weather.ingest.ndfd_backfill import NDFD_ELEMENTS, REGION, SUFFIX
from kalshi_weather.ndfd_client import NDFDClient

logger = logging.getLogger("ndfd_client")


class _RetryCounter(logging.Handler):
    """Counts WARNING-level 'retrying after ...' emissions from ndfd_client during a call."""

    def __init__(self):
        super().__init__(level=logging.WARNING)
        self.count = 0

    def emit(self, record):
        self.count += 1


@dataclass
class Attempt:
    key: str
    ok: bool
    seconds: float
    retries: int
    idle_gap_before: float  # seconds since the previous attempt finished
    error: str = ""


@dataclass
class StrategyResult:
    name: str
    attempts: list[Attempt] = field(default_factory=list)

    @property
    def successes(self) -> int:
        return sum(1 for a in self.attempts if a.ok)

    def summarize(self) -> str:
        n = len(self.attempts)
        if n == 0:
            return f"{self.name}: no attempts"
        ok = self.successes
        retried = sum(1 for a in self.attempts if a.retries > 0)
        times = [a.seconds for a in self.attempts if a.ok]
        failed_after_idle = [
            a for a in self.attempts if not a.ok and a.idle_gap_before > 5.0
        ]
        failed_no_idle = [a for a in self.attempts if not a.ok and a.idle_gap_before <= 5.0]
        lines = [
            f"\n=== {self.name} ===",
            f"  success:        {ok}/{n} ({100 * ok / n:.0f}%)",
            f"  needed a retry: {retried}/{n} ({100 * retried / n:.0f}%)",
        ]
        if times:
            times_sorted = sorted(times)
            p95 = times_sorted[min(len(times_sorted) - 1, int(0.95 * len(times_sorted)))]
            lines.append(
                f"  latency (s):    avg={statistics.mean(times):.2f} "
                f"median={statistics.median(times):.2f} p95={p95:.2f}"
            )
        if n - ok:
            lines.append(
                f"  failures after >5s idle gap: {len(failed_after_idle)}   "
                f"failures with no idle gap:   {len(failed_no_idle)}"
            )
            for a in self.attempts:
                if not a.ok:
                    lines.append(f"    - {a.key}: idle_before={a.idle_gap_before:.1f}s  {a.error}")
        return "\n".join(lines)


def _run_strategy(name: str, make_client, keys: list[str], sleep_between: float) -> StrategyResult:
    result = StrategyResult(name)
    counter = _RetryCounter()
    logger.addHandler(counter)
    last_finish: float | None = None
    try:
        client = make_client()
        for key in keys:
            counter.count = 0
            now = time.monotonic()
            idle_gap = 0.0 if last_finish is None else now - last_finish
            if name == "fresh":
                client = make_client()  # brand-new Session -> new connection pool per request
            start = time.monotonic()
            try:
                client.download(key)
                elapsed = time.monotonic() - start
                result.attempts.append(Attempt(key, True, elapsed, counter.count, idle_gap))
            except Exception as e:  # noqa: BLE001 - want every failure mode, not just NDFDError
                elapsed = time.monotonic() - start
                result.attempts.append(
                    Attempt(key, False, elapsed, counter.count, idle_gap, error=str(e)[:150])
                )
            last_finish = time.monotonic()
            time.sleep(sleep_between)
    finally:
        logger.removeHandler(counter)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--element", default="temp", help="NDFD element path segment, e.g. temp, maxt, dewpoint's 'td'")
    parser.add_argument("--day", default=None, help="UTC day to list, YYYY-MM-DD (default: today)")
    parser.add_argument("--count", type=int, default=20, help="number of files to download per strategy")
    parser.add_argument("--sleep", type=float, default=0.2, help="seconds between downloads within a strategy")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    from datetime import date, timedelta

    day = date.fromisoformat(args.day) if args.day else date.today() - timedelta(days=1)

    settings = load_settings()
    element = next((e for e in NDFD_ELEMENTS if e.path == args.element or e.variable == args.element), None)
    if element is None:
        valid = ", ".join(f"{e.variable}({e.path})" for e in NDFD_ELEMENTS)
        raise SystemExit(f"unknown --element {args.element!r}; choose one of: {valid}")

    probe_client = NDFDClient(user_agent=settings.user_agent)
    print(f"Listing {element.path} for {day} ...")
    keys = probe_client.list_day(element.path, day, REGION, SUFFIX)
    if not keys:
        raise SystemExit(f"no keys found for {element.path} on {day} — try a different --day")
    keys = keys[: args.count]
    print(f"Got {len(keys)} keys. Running each connection strategy over the same {len(keys)} files.\n")

    results = [
        _run_strategy("shared", lambda: NDFDClient(user_agent=settings.user_agent), keys, args.sleep),
        _run_strategy(
            "close",
            lambda: _client_with_connection_close(settings.user_agent),
            keys,
            args.sleep,
        ),
        _run_strategy("fresh", lambda: NDFDClient(user_agent=settings.user_agent), keys, args.sleep),
    ]

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for r in results:
        print(r.summarize())

    print(
        "\nHow to read this: if 'shared' fails mostly right after idle gaps and 'close' "
        "has a much lower retry rate at the cost of latency, the resets are stale "
        "keep-alive sockets and setting Connection:close (or recycling the session "
        "periodically) is worth doing. If all three strategies fail at similar rates "
        "regardless of idle gap, the endpoint is just flaky under sequential load and "
        "the existing retry logic is already the correct (and only) fix."
    )


def _client_with_connection_close(user_agent: str) -> NDFDClient:
    client = NDFDClient(user_agent=user_agent)
    client.session.headers["Connection"] = "close"
    return client


if __name__ == "__main__":
    main()
