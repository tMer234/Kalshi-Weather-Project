# Kalshi Weather — NWS + Kalshi Ingestion Pipelines

Pulls National Weather Service data and Kalshi market data for the 6 standard Kalshi
weather-market cities and lands both in one DuckDB with the schema needed for
forecast-horizon and market analysis later:

- **`grid_forecasts`** — raw numeric gridpoint forecast layers (`/gridpoints/{office}/{x},{y}`),
  append-only: every forecast vintage is kept, keyed by (station, variable, issued_time,
  valid_start). Every row carries both the forecast-issued timestamp and the valid window.
- **`climate_reports`** — parsed NWS Daily Climate Report (CLI) values: **the same numbers
  Kalshi settles against**. Update-in-place on newer product issuance (corrections
  overwrite; stale re-fetches can never regress a corrected value).
- **`observations`** — optional raw METAR feed (`--metar`), supplementary signal only.
- **`markets`** / **`market_snapshots`** / **`market_outcomes`** — Kalshi contract
  definitions (parsed strikes + observation date), append-only quote history (prices in
  dollars ≈ probabilities), and Kalshi's own settlement results — collected from the
  public trade-api/v2 by a separate collector, no API key needed.
- **`stations`**, **`ingest_runs`**, **`http_cache`** — resolved grid metadata, per-run audit
  trail, and conditional-GET state.

**Before analyzing any of this data, read [`docs/data_dictionary.md`](docs/data_dictionary.md)** —
it explains how to interpret every table and column: forecast interval semantics
(run-length encoding vs accumulations vs period extremes), the metric-vs-imperial unit
split, timezone conventions, the CLI report lifecycle, and the forecast↔settlement join
with worked, tested SQL.

## Setup

```bash
python3.14 -m venv .venv
.venv/bin/pip install -e ".[dev]"
cp .env.example .env        # fill in NWS_CONTACT_EMAIL (NWS requires it in User-Agent)
.venv/bin/python scripts/resolve_stations.py   # one-time; re-run occasionally (grids drift)
```

## Running

Everything runs through the installed **`kalshi-weather`** command (registered by
`pip install -e .`). **Full operating guide — scheduling, backfills, monitoring,
troubleshooting: [`docs/runbook.md`](docs/runbook.md).**

```bash
.venv/bin/kalshi-weather ingest nws          # live NWS pass: forecasts + climate reports
.venv/bin/kalshi-weather ingest kalshi       # live Kalshi pass: markets + quotes + outcomes
.venv/bin/kalshi-weather backfill nws-cli --start 2026-01-01 --end 2026-07-01
                                             # historical climate reports (IEM archive)
.venv/bin/kalshi-weather backfill kalshi  --start 2026-01-01 --end 2026-07-01
                                             # historical settled markets + candle prices
.venv/bin/python -m pytest                   # fixture-based, no network
```

(`scripts/run_ingest.py`, `run_kalshi_ingest.py`, `backfill_nws.py`, `backfill_kalshi.py`
are equivalent shims kept for cron use.)

Every command is one idempotent pass: fetch, upsert, exit — safe to re-run any time, and
backfills can never regress live-collected values. Exit codes: `0` at least one
station/series succeeded, `1` everything failed, `2` configuration error — so cron
alerting only fires when something is systemically wrong (per-endpoint errors are
recorded in `ingest_runs`).

### Scheduling (cron)

See the [runbook](docs/runbook.md) for cadence reasoning. Short version — NWS hourly;
Kalshi every 10 minutes (its cadence IS the quote-history resolution):

```cron
17 * * * *   cd /path/to/Kalshi_Weather_Project && .venv/bin/kalshi-weather ingest nws    >> logs/cron.log 2>&1
*/10 * * * * cd /path/to/Kalshi_Weather_Project && .venv/bin/kalshi-weather ingest kalshi >> logs/kalshi_cron.log 2>&1
```

The commands keep no local state beyond the DuckDB file (`DUCKDB_PATH`), so they lift
unchanged into GitHub Actions / Fly.io / Render cron.

## Settlement stations (hardcoded, deliberately)

Kalshi settles exclusively against the NWS Daily Climate Report for specific stations —
several non-obvious (Chicago = Midway **not** O'Hare, Austin = KAUS **not** Camp Mabry):
KNYC, KMDW, KAUS, KDEN, KMIA, KPHL. These are hardcoded in `config/stations.yaml`;
nearest-station auto-discovery runs only for cities missing from the config and is flagged
`station_verified = FALSE`.

## Notes & deviations from the plan discovered during implementation

- **CLI products are listed under the station's 3-letter site code, not the WFO id**
  (verified live): `/products/types/CLI/locations/MDW`, not `.../LOT`. Stored as
  `stations.cli_location_id`, derived from the ICAO code (strip leading `K`), overridable
  via `cli_location_id` in `stations.yaml`.
- That products endpoint **400s on a `?limit=` query param**; limiting is client-side.
- CLI text format drift handled (all with real recorded fixtures in `tests/fixtures/cli/`):
  occurrence times appear as both `347 PM` (OKX/BOU) and `2:56 PM` (LOT/EWX); record
  values carry an `R` suffix (`100R`); `T` = trace (stored 0.0), `MM` = missing (skipped,
  logged); intermediate same-day reports say `VALID [TODAY] AS OF ...` and are superseded
  by the next morning's final via the newer-issuance upsert guard.
- `climate_reports.obs_date` comes from the report header text, never from re-bucketing
  timestamps — during DST the report's climatological day is 1:00 AM–12:59 AM local, not
  midnight-to-midnight. `value_time` is stored as printed (naive local), no tz conversion.
- All stored timestamps are naive UTC except `value_time` (above).
- **Kalshi's series listing contains retired duplicates** (`KXDENHIGH`, `KXHIGHTEMPDEN`,
  `HIGHMIA` all list with 0 open markets) — the active series in `stations.yaml` were
  verified live by checking open-market counts, never taken from the listing.
- Kalshi strike semantics (verified live + against the NHIGH CFTC filing): `greater` and
  `less` are **strict**, `between` is **inclusive both ends**, and the ticker's `-T`/`-B`
  suffix does not identify the side — only `strike_type` does.
- Kalshi prices come as decimal-dollar strings (`"0.0100"`), sizes/volumes as fractional
  `_fp` strings — the old integer-cent fields are no longer in listing responses.

## Layout

```
config/stations.yaml        station config incl. confirmed settlement stations + Kalshi series
docs/data_dictionary.md     what every table/column MEANS (read before analysis)
docs/runbook.md             how to RUN everything (ingests, backfills, cron, monitoring)
src/kalshi_weather/
  cli.py                    the `kalshi-weather` command (ingest/backfill subcommands)
  config.py                 .env + stations.yaml -> typed settings
  nws_client.py             requests wrapper: User-Agent, retry (~5s backoff per NWS
                            guidance), problem+json errors, conditional GET
  kalshi_client.py          public trade-api/v2 wrapper: retry/backoff, pagination,
                            candlesticks
  iem_client.py             Iowa Environmental Mesonet AFOS archive (backfill source)
  time_utils.py             ISO8601 interval parsing, horizon math
  cli_parser.py             CLI text bulletins -> per-station values (fails loudly)
  resolve.py                /points -> grid metadata into stations table
  ingest.py                 NWS orchestration, per-station error isolation
  kalshi_ingest.py          Kalshi orchestration: definitions, snapshots, outcomes
  nws_backfill.py           historical climate reports via IEM
  kalshi_backfill.py        historical settled markets + candle price bars
  db.py / schema.sql        DuckDB connection + idempotent DDL (incl. migrations)
scripts/                    cron-compatible shims over the same CLI commands
tests/                      65 tests; fixtures are real recorded NWS/IEM/Kalshi responses
```
