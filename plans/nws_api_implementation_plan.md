# NWS Data Ingestion Pipeline — Implementation Plan

> **HISTORICAL DOCUMENT — this plan is fully implemented** (commit `efba24a` and later).
> The living plan for everything after it is `plans/master_plan.md`; operations live in
> `docs/runbook.md`. Kept for the design reasoning; where it disagrees with the code,
> the code and data dictionary win.

## Context

This project (Kalshi weather fair-value modeling) is greenfield — the directory currently
contains only planning PDFs, no code. Three docs define the target system, but this plan is
scoped **only** to the piece they call "Stage 0 — Infrastructure & Data Collection" /
"Week 1-2: Market universe + weather feature pipeline" as it applies to **NWS data** — i.e.
everything needed to reliably pull National Weather Service forecasts and observations and
land them in durable storage with a schema that supports horizon arithmetic later. Kalshi
market collection and all statistical modeling (Gaussian model, calibration, evaluation) are
explicitly out of scope and will be separate future work.

**Data sourcing split (clarified by user)**: the end goal is a model that exploits correlations
between weather data and Kalshi weather-market resolutions to find mispricings and make
risk-optimized bets. This NWS pipeline supplies **only the predictor/feature side** — forecasts
and observations. Market resolution/settlement outcomes as Kalshi reports them (which contract
resolved YES/NO, at what settlement price) will be ingested from a **separate source/pipeline**,
not this one — this pipeline should not grow a Kalshi-outcome collector.

**Correction from user, refining the split above**: "observations" in this pipeline specifically
means **the same NWS-sourced daily high/low value Kalshi uses to resolve the contract** (the
Daily Climate Report / CLI product — see "Kalshi Settlement Mechanics" below), not a generic
proxy for it. This is still squarely *NWS* data (a different NWS product than the gridpoint
forecast, not a Kalshi product), so it belongs in this pipeline rather than the separate
market-resolution pipeline. Put simply: `forecasts` = NWS gridpoint forecast data (unchanged from
the original plan); `observations` = the NWS Daily Climate Report values for the same
station/date, i.e. the actual ground truth Kalshi resolves against — not the raw METAR/ASOS feed
originally planned as a proxy for it.

The docs agree on the non-negotiable design constraint: **every stored row must carry both the
forecast-issued timestamp and the valid/resolution timestamp**, because horizon buckets and the
later chronological train/test split depend on it. They disagree on storage format (parquet vs.
DuckDB) — user has already decided **DuckDB** as source of truth (upsert semantics avoid manual
read-modify-write merge logic on every poll; can still export to parquet later if needed).

Confirmed decisions from user:
- **Storage**: DuckDB (single local file), not raw parquet.
- **Locations**: standard Kalshi weather-market cities — NYC, Chicago, Austin, Denver, Miami,
  Philadelphia. Settlement station IDs are now **confirmed** (see "Kalshi Settlement Mechanics"
  below) rather than left to auto-discovery — hardcode them in `config/stations.yaml`.
- **Execution model**: single idempotent script run (fetch + upsert, then exit) — not a
  long-running daemon. Must be safe to re-run repeatedly (no duplicate rows if NWS hasn't
  refreshed). Designed so a cron/launchd entry can be added later without code changes, and so
  it can be lifted into a hosted scheduler (GitHub Actions, Fly.io, Render cron, etc.) later —
  meaning no reliance on local-machine-only state beyond the DuckDB file path (env-configurable).

Confirmed API mechanics (fetched live from weather-gov.github.io/api/general-faqs,
/api/gridpoints, and weather.gov/documentation/services-web-api, since training data may be
stale):
- `User-Agent` header is **mandatory** on every request, identifying app + contact email — NWS's
  own recommended format is `(myweatherapp.com, contact@myweatherapp.com)`; use
  `NWS_CONTACT_EMAIL` for the contact part per `.env.example`.
- Rate limit is intentionally undocumented ("not public, but generous for typical use"), but NWS
  explicitly states 429s should be retried **after ~5 seconds** — bake that as the initial
  backoff delay in `nws_client.py`'s `tenacity` retry policy rather than guessing a value.
  Errors follow `application/problem+json`; parse that shape for error messages/logging rather
  than assuming plain text. Should also respect `Cache-Control`/`Last-Modified` and avoid
  cache-busting query params (causes 400s).
- Lookup chain: `GET /points/{lat},{lon}` → response `properties.forecast`,
  `properties.forecastHourly`, `properties.forecastGridData`, `properties.observationStations`.
  **Correction**: `/points` results are *not* permanently stable — NWS states the office/gridX/
  gridY mapping for a location "may occasionally change" (grid redefinitions). `resolve_stations.py`
  should be safe to re-run periodically (not just once at setup) to catch drift, even though the
  common case is that it never changes.
- Coordinates must be given to ≤4 decimal places.
- Observations lag reality by **up to ~20 minutes** due to upstream MADIS QC processing before
  they're available via the API — worth knowing so "why isn't the newest reading here yet" doesn't
  read as an ingestion bug when it's normal latency.
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
  `/gridpoints/{gridId}/{x},{y}/stations` lists nearby stations ordered by distance, but this is
  now a **fallback/cross-check only** — see below for why it shouldn't be the primary source of
  truth for settlement station IDs.

## Kalshi Settlement Mechanics (resolves the station-ID risk flagged earlier)

Researched from Kalshi's own help article (help.kalshi.com weather-markets) and a third-party
Apify scraper's documented station mapping (apify.com/bigdavidson/kalshi-weather-markets) — the
Reddit "unofficial guide" thread referenced alongside these could not be fetched (Reddit is
blocked for WebFetch, no accessible mirror found), so it's not reflected here. Two findings
materially change the plan:

1. **Kalshi does not settle off live METAR/ASOS observations or NWS forecast data at all.** Per
   Kalshi's own docs, contracts settle exclusively against **the final NWS Daily Climate Report
   (the "CLI" text product)**, typically issued the *next morning*. That report can revise a
   preliminary high downward, and settlement is explicitly delayed if the reported high isn't
   consistent with the 6-hr/24-hr highs from METAR. **This pipeline's `observations` data
   therefore needs to come from the CLI product itself, not raw METAR** — see the new "NWS Daily
   Climate Report (CLI) Ingestion" section below for how. Raw METAR (`/stations/{id}/observations`)
   is no longer the primary observation source; it's retained only as an optional secondary/
   supplementary table (e.g. for future features about conditions leading up to the report), not
   as the ground truth for residuals.
2. **Confirmed settlement stations per city** (station chosen matters — several cities settle
   against a non-obvious airport):

   | City | Settlement station | Note |
   |---|---|---|
   | NYC | `KNYC` (Central Park) | — |
   | Chicago | `KMDW` (Midway) | **not** O'Hare (`KORD`) |
   | Austin | `KAUS` | **not** Camp Mabry (`KATT`) |
   | Denver | `KDEN` | — |
   | Miami | `KMIA` | — |
   | Philadelphia | `KPHL` | — |

   This means `resolve_stations.py`'s nearest-station auto-discovery (`/gridpoints/{gridId}/{x},{y}/stations`)
   must **not** be the default path for these 6 cities — a naive "closest station to the city's
   lat/lon" lookup is exactly how Chicago or Austin would get resolved to the wrong airport. The
   architecture flips: `config/stations.yaml` ships with these `obs_station_id` values
   **hardcoded**, and auto-discovery becomes an optional cross-check/fallback used only when
   expanding to a city not in this table (with an explicit "unverified, confirm before trusting"
   flag on anything it resolves).
3. **DST local-day boundary quirk**: during Daylight Saving Time, Kalshi's "daily high" window
   for a given market date is 1:00 AM–12:59 AM local time the *following* day, not standard
   midnight-to-midnight — because the NWS climate report itself uses local standard time
   underneath. Matters directly now: when assigning `obs_date` to a parsed CLI value, don't
   naively bucket by UTC or local-midnight-to-midnight; it's worth a one-line comment in
   `schema.md` regardless so it's not a rediscovered bug later.

## NWS Daily Climate Report (CLI) Ingestion

This is new scope added by the settlement-mechanics research and the user's follow-up
clarification: `observations` must come from the actual NWS Daily Climate Report, not METAR.
Mechanically this is a different corner of the NWS API than everything else in this plan — text
bulletins, not clean JSON — so it deserves its own section.

- **Endpoints**: `/products/types/CLI/locations/{wfoId}` lists recent CLI products for a Weather
  Forecast Office (WFO); `/products/{productId}` returns the specific product, including its raw
  text body and `issuanceTime`. The `locationId` here is a **WFO id** (e.g. `OKX` for the NYC
  area), not the station's ICAO code — and conveniently, `/points/{lat},{lon}` already returns
  the WFO under `properties.cwa`, so no extra lookup is needed; just store it as `wfo_id` on the
  `stations` table alongside `grid_id`.
- **One bulletin can cover multiple sites.** A WFO's CLI product often bundles several stations
  in its area into one text bulletin (e.g. OKX's CLI report may include Central Park, LaGuardia,
  and JFK all in the same product, in separate text blocks). The parser must locate the correct
  station's block by its **spelled-out site name** (CLI text uses names like "CENTRAL PARK", not
  ICAO codes) — so `config/stations.yaml` needs a `cli_site_name` alias per city, distinct from
  `obs_station_id`, and this alias should be verified against a real fetched sample before trusting
  it (station-name headers can vary in exact wording between WFOs).
- **Text format is semi-structured, not JSON.** CLI products follow NOAA's traditional
  fixed-width climate report template (roughly: a `TEMPERATURE (F)` section with `MAXIMUM`/
  `MINIMUM` lines including the time of occurrence, plus precipitation/snowfall sections). This
  is the single most fragile part of the whole pipeline — minor formatting drift between WFOs or
  over time is expected. Treat `cli_parser.py` as its own well-tested module with **real recorded
  sample text per city** as fixtures (not synthetic examples), and fail loudly (log + skip, don't
  guess) if a station's block or expected fields aren't found in the expected shape.
- **Revisions happen.** Kalshi's own docs note settlement can be delayed if a preliminary CLI
  value is later corrected. A re-issued CLI product for a date already ingested should **update**
  the stored value (not sit alongside it as a duplicate) — but only if the new product's
  `issuanceTime` is more recent than what's already stored, so a stale re-fetch can never
  regress a newer corrected value. This is different from `grid_forecasts`'s "keep every
  vintage" philosophy: here we want the current best-known settlement value, not full history.

## Repository Structure

```
kalshi_weather/
  pyproject.toml
  .env.example                     # NWS_CONTACT_EMAIL, DUCKDB_PATH, APP_NAME
  config/
    stations.yaml                  # human-edited: name, lat, lon, hardcoded obs_station_id +
                                    #   cli_site_name (confirmed Kalshi settlement stations,
                                    #   not auto-discovered)
  src/kalshi_weather/
    __init__.py
    config.py                      # loads .env + stations.yaml into typed settings
    nws_client.py                  # thin requests wrapper: get_points, get_grid_data,
                                    #   get_forecast_hourly, get_nearby_stations,
                                    #   get_latest_observation, get_observations,
                                    #   get_cli_products(wfo_id), get_product(product_id)
                                    #   -- handles User-Agent, retry/backoff, conditional GET
    cli_parser.py                  # parses raw CLI text bulletins -> per-station
                                    #   max/min temp + occurrence time + obs_date
    time_utils.py                  # parse ISO8601 interval -> (valid_start, valid_end),
                                    #   horizon_hours(issued, valid_start)
    db.py                          # DuckDB connection + idempotent schema creation
    schema.sql                     # table DDL (stations, grid_forecasts, climate_reports,
                                    #   observations, ingest_runs)
    ingest.py                      # orchestration: resolve stations -> pull grid data ->
                                    #   pull CLI climate reports -> pull METAR (secondary) ->
                                    #   upsert -> log run
  scripts/
    run_ingest.py                  # CLI entrypoint: single run, exit code reflects success
    resolve_stations.py            # one-off: hits /points + /stations for each configured
                                    #   lat/lon, writes resolved metadata into `stations` table
  data/
    weather.duckdb                 # gitignored
  tests/
    fixtures/                      # recorded sample JSON responses (points, griddata, obs)
                                    #   + real recorded CLI product text per city
    test_time_utils.py             # ISO8601 interval parsing, horizon math edge cases
    test_nws_client.py             # mocked HTTP (responses lib), retry/backoff behavior
    test_cli_parser.py             # CLI text parsing against real recorded per-city fixtures
    test_ingest_upsert.py          # idempotency: run ingest twice against temp DuckDB, assert
                                    #   no duplicate rows; assert new issued_time creates new rows
  logs/                            # gitignored, plain rotating file logs
```

## Database Schema (DuckDB)

```sql
-- resolved once via resolve_stations.py, safe to re-run periodically (grid mappings can drift)
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
    obs_station_id  VARCHAR,               -- confirmed Kalshi settlement station, e.g. 'KNYC'
                                            -- (hardcoded per city, see Kalshi Settlement Mechanics)
    wfo_id          VARCHAR,               -- e.g. 'OKX' -- from /points properties.cwa, used to
                                            -- look up CLI products for this station's area
    cli_site_name   VARCHAR,               -- e.g. 'CENTRAL PARK' -- spelled-out name used to find
                                            -- this station's block within a multi-site CLI bulletin
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

-- PRIMARY observation ground truth: one row per (station, calendar date, variable), sourced from
-- the NWS Daily Climate Report (CLI product) -- the same data Kalshi settles against.
CREATE TABLE IF NOT EXISTS climate_reports (
    station_id      VARCHAR REFERENCES stations(station_id),
    obs_date        DATE,                  -- calendar date the report covers (local standard
                                            -- time day -- see DST local-day boundary note above)
    variable        VARCHAR,               -- 'max_temp', 'min_temp', 'precip', 'snowfall', ...
    value           DOUBLE,
    value_time      TIMESTAMP,             -- time-of-day the max/min occurred, if the report gives it
    unit            VARCHAR,
    product_id      VARCHAR,               -- NWS product ID this row was parsed from (traceability)
    issued_time     TIMESTAMP,             -- product's issuanceTime
    pulled_at       TIMESTAMP,
    PRIMARY KEY (station_id, obs_date, variable)
);

-- SECONDARY/optional: raw METAR feed, one row per (station, variable, timestamp). Kept only as a
-- supplementary signal (e.g. conditions leading up to the report) -- NOT the residual ground
-- truth; that's climate_reports above. Safe to defer/skip in an initial build if time-constrained.
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

Upsert semantics differ by table, deliberately:
- `grid_forecasts`/`observations`: `INSERT INTO ... ON CONFLICT (...) DO NOTHING`. If NWS hasn't
  re-issued a forecast since the last poll, re-running the script is a pure no-op on that row. If
  NWS *has* re-issued (new `updateTime`), the new `issued_time` makes the primary key different,
  so it lands as a **new row** rather than overwriting history — we want every forecast vintage
  retained, not just the latest, so later Stage-1 horizon analysis has real issued-vs-valid pairs
  to work with.
- `climate_reports`: `INSERT INTO ... ON CONFLICT (station_id, obs_date, variable) DO UPDATE SET
  value = excluded.value, value_time = excluded.value_time, product_id = excluded.product_id,
  issued_time = excluded.issued_time, pulled_at = excluded.pulled_at WHERE excluded.issued_time >
  climate_reports.issued_time`. This is intentionally different — a corrected/re-issued CLI
  product for a date we already have should **overwrite** the stored value (we want the current
  best-known settlement-equivalent value, not every historical vintage), but the `WHERE` guard
  ensures a stale re-fetch can never regress a value that's already been corrected.

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
   b. `GET /products/types/CLI/locations/{wfo_id}` to list recent CLI products for the station's
      WFO, then `GET /products/{productId}` for each new-to-us product (dedupe against
      `product_id`s already in `climate_reports`/an ingest-runs-based "seen products" check).
      Parse the raw text via `cli_parser.py`, locating this station's block by `cli_site_name`,
      extracting max/min temp + occurrence time (and precip/snowfall if present) plus the
      `obs_date` the report covers. Stage rows for `climate_reports` upsert (update-on-newer-
      issuance semantics, not append-only — see Upsert semantics above).
   c. (secondary, can be deferred) `GET /stations/{obs_station_id}/observations` (bounded to,
      say, last 7 days by default — configurable) for supplementary raw METAR values, staged into
      `observations`.
   d. Upsert staged rows into DuckDB inside a transaction per station.
   e. Write one `ingest_runs` row per (station, endpoint) with status/row-count/error.
4. Non-fatal per-station error handling: a failure fetching/parsing one station must not abort
   the others — catch, log to `ingest_runs.error`, continue. Script exit code is non-zero only if
   *all* stations failed or a config/auth error prevented startup, so cron alerting has a
   meaningful signal without being noisy on transient single-station hiccups.
5. HTTP layer (`nws_client.py`): mandatory `User-Agent`, `tenacity`-based retry with exponential
   backoff on 429/500/502/503 starting at **~5s** (NWS's own documented retry guidance for 429s;
   a handful of attempts, then give up and let step 4's error handling record it), parsing
   `application/problem+json` error bodies for the logged message, and passing through
   `If-Modified-Since` using the last-seen `Last-Modified` per URL (stored alongside `stations` or
   a small cache table) so unchanged forecasts short-circuit to a cheap 304 instead of a full
   payload re-parse.

## Key Implementation Details Worth Calling Out

- **ISO8601 interval parsing** (`time_utils.parse_interval`): NWS `validTime` values look like
  `"2019-07-04T18:00:00+00:00/PT3H"` (start/duration) — use the `isodate` package for the
  `PT3H`/`P1D` duration parsing rather than hand-rolling regex; write tests for multi-day
  durations (`maxTemperature`/`minTemperature` layers often span ~24h) and for the timezone
  offset always being present.
- **Unit normalization**: grid data `uom` values come back as `wmoUnit:degC` style strings; strip
  the `wmoUnit:` prefix and store the bare unit so later stages don't have to special-case this.
- **Station IDs are now hardcoded, not auto-discovered**: the settlement stations for all 6
  cities are confirmed (`KNYC`, `KMDW`, `KAUS`, `KDEN`, `KMIA`, `KPHL` — see Kalshi Settlement
  Mechanics above), sourced from a third-party scraper's documented mapping rather than Kalshi's
  own literal rulebook, so still worth one manual spot-check against a live Kalshi contract page
  before treating it as gospel — but solid enough to hardcode as the default rather than derive
  from nearest-station distance, which would silently pick the wrong airport for Chicago/Austin.
  `resolve_stations.py`'s auto-discovery path stays in the codebase only as a fallback for adding
  a city not in this table, explicitly flagged "unverified" in its output.
- **Idempotent-by-design for future hosting**: no in-process state beyond the DuckDB file and the
  `Last-Modified` cache; `DUCKDB_PATH` and `NWS_CONTACT_EMAIL` come from env vars so the same
  script runs unchanged under local cron, GitHub Actions, or a hosted cron service later.
- **CLI text parsing is the highest-risk part of this pipeline**: unlike every other endpoint used
  here, CLI products are free-text bulletins, not JSON — treat `cli_parser.py` as needing
  disproportionately more test coverage (real per-city recorded fixtures, not synthetic ones) than
  the rest of the codebase, and design it to fail loudly (skip + log) rather than silently
  misparse when a station's block or expected fields don't match the anticipated shape.

## Dependencies

`duckdb`, `requests`, `tenacity` (retry/backoff), `isodate` (ISO8601 duration parsing), `pyyaml`
(station config), `python-dotenv` (env loading), `typer` (CLI entrypoint). Dev: `pytest`,
`responses` (HTTP mocking so tests never hit the live API).

## Build Order

1. `config.py` + `.env.example` + `config/stations.yaml` (6 cities, lat/lon, and the confirmed
   `obs_station_id` hardcoded per the settlement table above).
2. `db.py` + `schema.sql` — create tables (including `climate_reports`), verify idempotent
   `CREATE TABLE IF NOT EXISTS`.
3. `nws_client.py` with retry/backoff + mandatory User-Agent — unit-test against recorded
   fixtures, no live calls in tests.
4. `resolve_stations.py` — resolves and persists gridpoint metadata (`grid_id`/`grid_x`/`grid_y`/
   forecast URLs, `wfo_id`) via `/points`; takes `obs_station_id` from config rather than
   discovering it, only falling back to nearest-station lookup (flagged unverified) for a city
   missing from the config table. Run once live and manually sanity-check the resolved grid IDs
   against weather.gov before trusting them.
5. `time_utils.py` — interval parsing + horizon math, test-first given how easy this is to get
   subtly wrong.
6. `cli_parser.py` — fetch one real CLI product per city first (manually, via `nws_client.py`),
   save as a fixture, then write the parser test-first against those real fixtures. Confirm
   `cli_site_name` per city actually matches what appears in the fetched text before trusting
   `config/stations.yaml`'s aliases. This step should be expected to take longer than the
   JSON-endpoint work given the text-parsing risk noted above.
7. `ingest.py` + `scripts/run_ingest.py` — full orchestration (grid forecasts + CLI climate
   reports; METAR `observations` can be stubbed/deferred if time-constrained), per-station error
   isolation.
8. `tests/test_ingest_upsert.py` — the idempotency test is the most important one: run twice
   against a temp DuckDB, assert row counts don't double for `grid_forecasts`, and separately
   assert `climate_reports` correctly *updates* (not duplicates) when a re-ingested product has a
   newer `issuanceTime`.
9. Manual end-to-end run against the live API for all 6 cities, eyeball the resulting DuckDB
   tables (`duckdb data/weather.duckdb` → `SELECT * FROM grid_forecasts LIMIT 20;` and
   `SELECT * FROM climate_reports;`), spot-check a couple of `climate_reports` rows against the
   raw CLI text by eye, then document a cron entry (e.g. hourly for forecasts, once-daily-morning
   for CLI) in the README as the "how to schedule this" follow-up.

## Verification

- `pytest` for the full suite (fixture-based, no network).
- One live manual run of `scripts/run_ingest.py`, followed by ad hoc DuckDB queries to confirm:
  row counts per station/variable are sane, `issued_time`/`valid_start`/`horizon_hours` line up
  correctly for a couple of spot-checked rows against the raw JSON, and re-running the script
  immediately after produces zero new rows for `grid_forecasts` (proving the idempotent upsert
  works).
- Specifically for `climate_reports`: manually compare a handful of parsed rows against the raw
  CLI product text (by eye) to confirm `cli_site_name` matching and max/min extraction are
  correct for each of the 6 cities, and confirm re-running against an unchanged CLI product
  doesn't spuriously bump `pulled_at`/overwrite with identical values (the `WHERE issued_time >`
  guard should make this a no-op).
