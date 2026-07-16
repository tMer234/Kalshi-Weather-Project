# Tests

```bash
.venv/bin/python -m pytest          # everything, ~1-2s, no network
.venv/bin/python -m pytest -q tests/test_backfills.py   # one file
.venv/bin/python -m pytest -k idempotent                # by keyword
```

83 tests, no live network calls, no `.env`/credentials needed. Every HTTP-backed test
uses [`responses`](https://github.com/getsentry/responses) to mock `requests` calls —
`@responses.activate` on the test function, `responses.get(url, json=..., match=[...])`
to register expected calls. If a test makes a real network call, that's a bug (or a
missing `responses.get` registration causing the library to fall through).

## Philosophy

- **Real recorded fixtures over synthetic ones, wherever the format is fragile.** The
  CLI (Daily Climate Report) text parser is the most brittle part of the pipeline —
  every station's WFO formats it slightly differently — so `tests/fixtures/cli/*.txt`
  are byte-for-byte real bulletins fetched live from `api.weather.gov`, not hand-written
  approximations. Same idea for the NWS gridpoint JSON, IEM archive responses, and
  Kalshi market payloads (all under `tests/fixtures/`, trimmed to keep them small but
  otherwise untouched). Only the low-level pure-math tests (`test_time_utils.py`, the
  NDFD nearest-gridcell lookup) use synthetic inputs — there's no fragile parsing to
  protect there, just arithmetic.
- **Every collector/backfill test asserts the pipeline's core invariants**, not just
  "it doesn't crash": idempotency (re-running against unchanged upstream data is a
  no-op), the upsert semantics per table (`grid_forecasts` append-only,
  `climate_reports` update-on-newer-issuance, `market_outcomes` insert-once), and
  `source`-column provenance (`nws_api` vs `iem_afos` vs `ndfd_archive` vs backfill
  tables). These are the guarantees the rest of the system (residual dataset, models)
  depends on being true — see `docs/data_dictionary.md` for why they matter.
- **DuckDB is never mocked.** Every ingest/backfill test opens a real (temporary,
  per-test) DuckDB file via the `conn`/`settings` fixtures (`tmp_path`-scoped, torn down
  automatically) and runs real SQL through `db.py`'s schema. Only the network layer is
  faked.

## Fixture provenance

| Path | Source | Notes |
|---|---|---|
| `fixtures/cli/*.txt` + `meta.json` | `api.weather.gov` CLI products, fetched live 2026-07-09 | One final + one intermediate report per station; `meta.json` records each fixture's real `issuanceTime`/product id so tests can assert on them |
| `fixtures/points_nyc.json`, `griddata_nyc_trimmed.json` | `api.weather.gov` `/points` + gridpoint data for NYC, trimmed | Trimmed = extra forecast periods removed, structure otherwise real |
| `fixtures/iem/*` | `mesonet.agron.iastate.edu` AFOS archive, fetched live 2026-07-10 | Confirms IEM's stored text is byte-compatible with the live API's `productText` (see `docs/runbook.md` §2.1) |
| `fixtures/kalshi/*` | `api.elections.kalshi.com/trade-api/v2`, fetched live, trimmed | Real market/candlestick shapes; dollar-string price fields, `_fp` size fields |

No fixture for the NDFD GRIB2 archive (`backfill nws-grid`'s source) — see below.

## Per-file map

| File | Covers | Core invariant under test |
|---|---|---|
| `test_time_utils.py` | ISO8601 interval parsing, horizon math | Pure functions, no I/O — the arithmetic every other test's assertions rely on being correct |
| `test_nws_client.py` | `NWSClient` transport | Retry/backoff, conditional GET (304 short-circuit), problem+json error parsing |
| `test_cli_parser.py` | CLI bulletin text → structured values | Every station's real text format quirks (see `README.md`'s "Notes & deviations") parse correctly; malformed input fails loudly, never silently guesses |
| `test_ingest_upsert.py` | Live `ingest nws` pass | Idempotent re-runs; `grid_forecasts` appends new vintages without duplicating; `climate_reports` updates on newer issuance but never regresses to a stale value |
| `test_kalshi_ingest.py` | Live `ingest kalshi` pass | Market/snapshot/outcome parsing from real payloads; append/update/insert-once semantics per table; dollar-price and `_fp` size-field handling |
| `test_backfills.py` | `backfill nws-cli`, `backfill kalshi[-quotes\|-resolutions]` | Backfilled rows carry correct `source` provenance; a backfill can never regress a value the live collector already landed; resumability via `http_cache` (interrupted runs pick up where they stopped, already-fetched items aren't re-fetched) |
| `test_ndfd_backfill.py` | `backfill nws-grid` | See below — same invariants as `test_backfills.py`, plus unit conversion and horizon-window correctness for the GRIB2 layer |

## Why `test_ndfd_backfill.py` doesn't use a binary fixture

Every other backfill's source format (CLI text, IEM JSON, Kalshi JSON) is cheap to
record and commit as a fixture. NDFD's source is a binary GRIB2 file (decoded via
`pygrib`, an optional dependency — see `pyproject.toml`'s `ndfd` extra) — not something
to hand-craft or want committed to the repo. Instead the tests are layered so no real
GRIB2 bytes are ever needed:

1. **`NDFDClient`** (S3 listing/download) is tested like every other client — mocked
   HTTP via `responses`, real XML-shaped listing responses.
2. **`_decode_element_file`** (the pygrib-dependent decode step, including the
   horizon-filtering and accumulation-vs-instantaneous window math in
   `_message_window`) is tested against a **fake pygrib module** — a small stand-in
   class exposing the same `.open()` / message-iteration / `.values` / `.latlons()`
   interface real pygrib does (see the `_FakeGrb`/`_FakePygribModule` classes in the
   test file). This keeps the actual decode *logic* under test without a binary file.
3. **End-to-end orchestration tests** (does a backfill land the right rows, is it
   idempotent, do `--variable`/`--station` filters work) monkeypatch
   `_decode_element_file` itself, so they exercise everything around it (S3
   listing/download mocking, unit conversion, the upsert, `http_cache` resumability)
   without touching pygrib at all.

pygrib *is* installed in this project's `.venv` (part of the `ndfd` extra), so nothing
stops a real end-to-end trial run — see `docs/runbook.md` §2.1's note that
`backfill nws-grid` hasn't been run against the live archive yet; a small
`--variable maxTemperature --station nyc` range is the recommended first real trial.

## Adding a new test

- New collector/backfill logic: follow `test_backfills.py`'s pattern — a `settings`/
  `conn` fixture pair (real temp DuckDB), `@responses.activate` with recorded-shape
  mocked HTTP, then assert both the landed rows *and* the invariant (idempotency,
  provenance, non-regression).
- New CLI text format quirk: add the real fetched bulletin to `fixtures/cli/`, a
  `meta.json` entry recording its real issuance metadata, and a case in
  `test_cli_parser.py` — never hand-write a synthetic CLI bulletin, the whole point of
  that fixture set is that it's real.
- Pure logic (math, parsing helpers): no fixture needed — synthetic inputs are fine
  where there's no fragile external format to protect against.
