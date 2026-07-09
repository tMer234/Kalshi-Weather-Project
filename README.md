# Kalshi Weather — NWS Ingestion Pipeline

Pulls National Weather Service data for the 6 standard Kalshi weather-market cities and
lands it in DuckDB with the schema needed for forecast-horizon analysis later:

- **`grid_forecasts`** — raw numeric gridpoint forecast layers (`/gridpoints/{office}/{x},{y}`),
  append-only: every forecast vintage is kept, keyed by (station, variable, issued_time,
  valid_start). Every row carries both the forecast-issued timestamp and the valid window.
- **`climate_reports`** — parsed NWS Daily Climate Report (CLI) values: **the same numbers
  Kalshi settles against**. Update-in-place on newer product issuance (corrections
  overwrite; stale re-fetches can never regress a corrected value).
- **`observations`** — optional raw METAR feed (`--metar`), supplementary signal only.
- **`stations`**, **`ingest_runs`**, **`http_cache`** — resolved grid metadata, per-run audit
  trail, and conditional-GET state.

Kalshi market/settlement data is explicitly out of scope — it will be a separate pipeline.

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

```bash
.venv/bin/python scripts/run_ingest.py            # forecasts + climate reports
.venv/bin/python scripts/run_ingest.py --metar    # also pull raw METAR (secondary)
.venv/bin/python -m pytest                        # fixture-based, no network
```

One idempotent pass: fetch, upsert, exit. Safe to re-run any time — unchanged forecasts
short-circuit to HTTP 304s and already-seen CLI products are skipped, so a repeat run
writes zero rows. Exit codes: `0` at least one station succeeded, `1` all stations failed,
`2` configuration error — so cron alerting only fires when something is systemically wrong
(per-endpoint errors are recorded in `ingest_runs`).

### Scheduling (cron)

NWS re-issues gridpoint forecasts roughly hourly; final climate reports arrive early
morning local time, with intermediate updates during the day. A single hourly entry covers
both (the run is cheap when nothing changed):

```cron
17 * * * * cd /path/to/Kalshi_Weather_Project && .venv/bin/python scripts/run_ingest.py >> logs/cron.log 2>&1
```

The script keeps no local state beyond the DuckDB file (`DUCKDB_PATH`), so the same
command lifts unchanged into GitHub Actions / Fly.io / Render cron.

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

## Layout

```
config/stations.yaml        station config incl. confirmed settlement stations
src/kalshi_weather/
  config.py                 .env + stations.yaml -> typed settings
  nws_client.py             requests wrapper: User-Agent, retry (~5s backoff per NWS
                            guidance), problem+json errors, conditional GET
  time_utils.py             ISO8601 interval parsing, horizon math
  cli_parser.py             CLI text bulletins -> per-station values (fails loudly)
  resolve.py                /points -> grid metadata into stations table
  ingest.py                 orchestration, per-station error isolation
  db.py / schema.sql        DuckDB connection + idempotent DDL
scripts/run_ingest.py       cron entrypoint
scripts/resolve_stations.py station resolution entrypoint
tests/                      46+ tests; fixtures are real recorded NWS responses
```
