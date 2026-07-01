# NWS Data Ingestion Pipeline — Implementation Plan

## Context

This project (Kalshi weather fair-value modeling) is greenfield — the directory currently
contains only planning PDFs, no code. Three docs define the target system, but this plan is
scoped **only** to the piece they call "Stage 0 — Infrastructure & Data Collection" /
"Week 1-2: Market universe + weather feature pipeline" as it applies to **NWS data** — i.e.
everything needed to reliably pull National Weather Service forecasts and observations and
land them in durable storage with a schema that supports horizon arithmetic later. Kalshi
market collection and all statistical modeling (Gaussian model, calibration, evaluation) are
explicitly out of scope and will be separate future work.

The docs agree on the non-negotiable design constraint: **every stored row must carry both the
forecast-issued timestamp and the valid/resolution timestamp**, because horizon buckets and the
later chronological train/test split depend on it. They disagree on storage format (parquet vs.
DuckDB) — user has already decided **DuckDB** as source of truth (upsert semantics avoid manual
read-modify-write merge logic on every poll; can still export to parquet later if needed).

Confirmed decisions from user:
- **Storage**: DuckDB (single local file), not raw parquet.
- **Locations**: standard Kalshi weather-market cities — NYC, Chicago, Austin, Denver, Miami,
  Philadelphia. Exact NWS gridpoints/station IDs resolved programmatically per city (see below);
  exact ASOS/settlement station IDs should be flagged for manual verification against Kalshi's
  actual contract specs since I can't confirm those with certainty from training data alone.
- **Execution model**: single idempotent script run (fetch + upsert, then exit) — not a
  long-running daemon. Must be safe to re-run repeatedly (no duplicate rows if NWS hasn't
  refreshed). Designed so a cron/launchd entry can be added later without code changes, and so
  it can be lifted into a hosted scheduler (GitHub Actions, Fly.io, Render cron, etc.) later —
  meaning no reliance on local-machine-only state beyond the DuckDB file path (env-configurable).

Confirmed API mechanics (fetched live from weather-gov.github.io/api/general-faqs +
/api/gridpoints, since training data may be stale):
- `User-Agent` header is **mandatory** on every request, identifying app + contact email.
- No hard documented rate limit, but NWS asks for identifiable, well-behaved traffic; must
  handle 429/503 with backoff. Should respect `Cache-Control`/`Last-Modified` and avoid
  cache-busting query params (causes 400s).
- Lookup chain: `GET /points/{lat},{lon}` → response `properties.forecast`,
  `properties.forecastHourly`, `properties.forecastGridData`, `properties.observationStations`.
  `/points` results are stable and should be cached, not re-fetched every run.
- Coordinates must be given to ≤4 decimal places.
- The **raw gridpoint data** endpoint (`/gridpoints/{gridId}/{x},{y}`) is the best ingestion
  target, not the human-readable `/forecast` endpoint: it returns numeric time-series "layers"
  (e.g. `maxTemperature`, `minTemperature`, `temperature`, `dewpoint`, `quantitativePrecipitation`,
  `probabilityOfPrecipitation`, `windSpeed`, `skyCover`, ...), each a list of
  `{validTime: "2019-07-04T18:00:00+00:00/PT3H", value, uom}` entries, plus a payload-level
  `updateTime` marking when NWS generated that forecast. This maps directly onto Kalshi's daily
  high/low temperature contracts and gives us `issued_time` (`updateTime`) and `valid_time`
  (parsed from the ISO8601 interval) for free, satisfying the schema requirement above.
- Observations come from `/stations/{stationId}/observations` (historical, API-side retention is
  limited — days/weeks, not months) and `/stations/{stationId}/observations/latest`.
  `/gridpoints/{gridId}/{x},{y}/stations` lists nearby stations ordered by distance, for
  auto-discovery when no explicit override is configured.

## Repository Structure

```
kalshi_weather/
  pyproject.toml
  .env.example                     # NWS_CONTACT_EMAIL, DUCKDB_PATH, APP_NAME
  config/
    stations.yaml                  # human-edited: name, lat, lon, optional station_id override
  src/kalshi_weather/
    __init__.py
    config.py                      # loads .env + stations.yaml into typed settings
    nws_client.py                  # thin requests wrapper: get_points, get_grid_data,
                                    #   get_forecast_hourly, get_nearby_stations,
                                    #   get_latest_observation, get_observations
                                    #   -- handles User-Agent, retry/backoff, conditional GET
    time_utils.py                  # parse ISO8601 interval -> (valid_start, valid_end),
                                    #   horizon_hours(issued, valid_start)
    db.py                          # DuckDB connection + idempotent schema creation
    schema.sql                     # table DDL (stations, grid_forecasts, observations, ingest_runs)
    ingest.py                      # orchestration: resolve stations -> pull grid data ->
                                    #   pull observations -> upsert -> log run
  scripts/
    run_ingest.py                  # CLI entrypoint: single run, exit code reflects success
    resolve_stations.py            # one-off: hits /points + /stations for each configured
                                    #   lat/lon, writes resolved metadata into `stations` table
  data/
    weather.duckdb                 # gitignored
  tests/
    fixtures/                      # recorded sample JSON responses (points, griddata, obs)
    test_time_utils.py             # ISO8601 interval parsing, horizon math edge cases
    test_nws_client.py             # mocked HTTP (responses lib), retry/backoff behavior
    test_ingest_upsert.py          # idempotency: run ingest twice against temp DuckDB, assert
                                    #   no duplicate rows; assert new issued_time creates new rows
  logs/                            # gitignored, plain rotating file logs
```

## Database Schema (DuckDB)

```sql
-- resolved once via resolve_stations.py, re-resolved only if stale/missing
CREATE TABLE IF NOT EXISTS stations (
    station_id      VARCHAR PRIMARY KEY,   -- our slug, e.g. 'nyc', 'chi'
    display_name    VARCHAR,
    lat             DOUBLE,
    lon             DOUBLE,
    grid_id         VARCHAR,               -- e.g. 'OKX'
    grid_x          INTEGER,
    grid_y          INTEGER,
    forecast_grid_data_url VARCHAR,
    forecast_hourly_url    VARCHAR,
    obs_station_id  VARCHAR,               -- e.g. 'KNYC' -- explicit override or nearest-discovered
    timezone        VARCHAR,
    resolved_at     TIMESTAMP
);

-- one row per (station, variable, issued_time, valid_start)
CREATE TABLE IF NOT EXISTS grid_forecasts (
    station_id      VARCHAR REFERENCES stations(station_id),
    variable        VARCHAR,               -- 'maxTemperature', 'minTemperature', 'dewpoint', ...
    issued_time     TIMESTAMP,             -- from payload updateTime
    valid_start     TIMESTAMP,             -- parsed from validTime interval
    valid_end       TIMESTAMP,             -- valid_start + parsed duration
    horizon_hours   DOUBLE,                -- (valid_start - issued_time) in hours
    value           DOUBLE,
    unit            VARCHAR,               -- uom, normalized (e.g. 'degC', 'wmoUnit:degC' stripped)
    pulled_at       TIMESTAMP,             -- when our collector fetched this
    PRIMARY KEY (station_id, variable, issued_time, valid_start)
);

-- one row per (station, variable, timestamp)
CREATE TABLE IF NOT EXISTS observations (
    obs_station_id  VARCHAR,
    variable        VARCHAR,
    timestamp       TIMESTAMP,
    value           DOUBLE,
    unit            VARCHAR,
    quality_control VARCHAR,               -- NWS qualityControl flag, if present
    pulled_at       TIMESTAMP,
    PRIMARY KEY (obs_station_id, variable, timestamp)
);

-- audit trail for each collector invocation, used for monitoring/debugging & cron health
CREATE TABLE IF NOT EXISTS ingest_runs (
    run_id          UUID DEFAULT uuid(),
    started_at      TIMESTAMP,
    finished_at     TIMESTAMP,
    station_id      VARCHAR,
    endpoint        VARCHAR,
    http_status     INTEGER,
    rows_upserted   INTEGER,
    error           VARCHAR,
    PRIMARY KEY (run_id)
);
```

Upsert semantics: `INSERT INTO ... ON CONFLICT (...) DO NOTHING`. This is deliberate — if NWS
hasn't re-issued a forecast since the last poll, re-running the script is a pure no-op on that
row. If NWS *has* re-issued (new `updateTime`), the new `issued_time` makes the primary key
different, so it lands as a **new row** rather than overwriting history — we want every forecast
vintage retained, not just the latest, so later Stage-1 horizon analysis has real
issued-vs-valid pairs to work with.

## Ingestion Flow (`ingest.py`)

1. Load settings (`config.py`): contact email for `User-Agent`, DuckDB path, station list.
2. For each configured station, ensure it's resolved in the `stations` table (if not, run the
   resolve step inline — `resolve_stations.py`'s logic is a callable, not just a CLI script).
3. For each resolved station, sequentially (not parallel — be a polite API citizen):
   a. `GET forecast_grid_data_url` (raw gridpoints endpoint). Parse `properties.updateTime` as
      `issued_time`. For each relevant layer key (a fixed allowlist: `maxTemperature`,
      `minTemperature`, `temperature`, `dewpoint`, `relativeHumidity`, `windSpeed`, `windGust`,
      `skyCover`, `quantitativePrecipitation`, `probabilityOfPrecipitation`, `snowfallAmount` —
      chosen because they're the variables weather-threshold contracts key off; not a hardcoded
      "only 2 fields" approach so it's robust to which markets get added later), iterate its
      `values` list, parse each `validTime` interval via `time_utils.parse_interval`, compute
      `horizon_hours`, and stage rows for upsert.
   b. `GET /stations/{obs_station_id}/observations` (bounded to, say, last 7 days by default —
      configurable) for verified observed values, staged the same way.
   c. Upsert staged rows into DuckDB inside a transaction per station.
   d. Write one `ingest_runs` row per (station, endpoint) with status/row-count/error.
4. Non-fatal per-station error handling: a failure fetching/parsing one station must not abort
   the others — catch, log to `ingest_runs.error`, continue. Script exit code is non-zero only if
   *all* stations failed or a config/auth error prevented startup, so cron alerting has a
   meaningful signal without being noisy on transient single-station hiccups.
5. HTTP layer (`nws_client.py`): mandatory `User-Agent`, `tenacity`-based retry with exponential
   backoff on 429/500/502/503 (a handful of attempts, then give up and let step 4's error
   handling record it), and pass through `If-Modified-Since` using the last-seen `Last-Modified`
   per URL (stored alongside `stations` or a small cache table) so unchanged forecasts short
   -circuit to a cheap 304 instead of a full payload re-parse.

## Key Implementation Details Worth Calling Out

- **ISO8601 interval parsing** (`time_utils.parse_interval`): NWS `validTime` values look like
  `"2019-07-04T18:00:00+00:00/PT3H"` (start/duration) — use the `isodate` package for the
  `PT3H`/`P1D` duration parsing rather than hand-rolling regex; write tests for multi-day
  durations (`maxTemperature`/`minTemperature` layers often span ~24h) and for the timezone
  offset always being present.
- **Unit normalization**: grid data `uom` values come back as `wmoUnit:degC` style strings; strip
  the `wmoUnit:` prefix and store the bare unit so later stages don't have to special-case this.
- **Station ID accuracy is a known open risk**: I do not have verified certainty on which exact
  ASOS/observation station Kalshi settles each city's contract against (e.g. NYC could be
  Central Park vs. LaGuardia vs. JFK). `resolve_stations.py` will auto-discover the nearest
  station via `/gridpoints/{gridId}/{x},{y}/stations` as a default, but `config/stations.yaml`
  supports an explicit `obs_station_id` override per city — flagging this in the README as
  "verify against actual Kalshi contract settlement rules before relying on this for real
  markets" rather than silently guessing.
- **Idempotent-by-design for future hosting**: no in-process state beyond the DuckDB file and the
  `Last-Modified` cache; `DUCKDB_PATH` and `NWS_CONTACT_EMAIL` come from env vars so the same
  script runs unchanged under local cron, GitHub Actions, or a hosted cron service later.

## Dependencies

`duckdb`, `requests`, `tenacity` (retry/backoff), `isodate` (ISO8601 duration parsing), `pyyaml`
(station config), `python-dotenv` (env loading), `typer` (CLI entrypoint). Dev: `pytest`,
`responses` (HTTP mocking so tests never hit the live API).

## Build Order

1. `config.py` + `.env.example` + `config/stations.yaml` (6 cities, lat/lon only initially).
2. `db.py` + `schema.sql` — create tables, verify idempotent `CREATE TABLE IF NOT EXISTS`.
3. `nws_client.py` with retry/backoff + mandatory User-Agent — unit-test against recorded
   fixtures, no live calls in tests.
4. `resolve_stations.py` — resolves and persists gridpoint + nearest station metadata; run once
   live to populate `stations` table and manually sanity-check the resolved grid IDs/station IDs
   against weather.gov before trusting them.
5. `time_utils.py` — interval parsing + horizon math, test-first given how easy this is to get
   subtly wrong.
6. `ingest.py` + `scripts/run_ingest.py` — full orchestration, per-station error isolation.
7. `tests/test_ingest_upsert.py` — the idempotency test is the most important one: run twice
   against a temp DuckDB, assert row counts don't double.
8. Manual end-to-end run against the live API for all 6 cities, eyeball the resulting DuckDB
   tables (`duckdb data/weather.duckdb` → `SELECT * FROM grid_forecasts LIMIT 20;`), then document
   a cron entry (e.g. hourly) in the README as the "how to schedule this" follow-up.

## Verification

- `pytest` for the full suite (fixture-based, no network).
- One live manual run of `scripts/run_ingest.py`, followed by ad hoc DuckDB queries to confirm:
  row counts per station/variable are sane, `issued_time`/`valid_start`/`horizon_hours` line up
  correctly for a couple of spot-checked rows against the raw JSON, and re-running the script
  immediately after produces zero new rows (proving the idempotent upsert works).
