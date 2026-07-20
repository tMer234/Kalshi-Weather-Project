# Runbook — Operating the Ingestion Pipelines

How to run, schedule, monitor, and backfill every collector in this repo. For what the
*data means*, read [`data_dictionary.md`](data_dictionary.md); this document is about
*operating* the pipelines.

**Scheduling is moving off this laptop.** The manual/local instructions below (§1.3)
remain accurate for running things by hand, but the planned always-on setup — GitHub
Actions workflows + a private GCS bucket holding the canonical database — is designed
in [`plans/data_automation_plan.md`](../plans/data_automation_plan.md). The code side of
that plan is done: `ingest nws`/`ingest kalshi` are now split into narrower
cadence-specific subcommands (§1.1/§1.2) for those future workflows to call. Still
pending: the GCP resources, the workflow YAML files themselves, and this section's
eventual rewrite to point at them instead of local cron.

Everything is driven by one installed command, **`kalshi-weather`** (registered by
`pip install -e .`; re-run that after pulling a version that changes `pyproject.toml`):

```
kalshi-weather ingest nws                 live NWS pass (forecasts + climate reports)
kalshi-weather ingest kalshi              live Kalshi pass (markets + quotes + outcomes)
kalshi-weather ingest nws-grid            narrow: gridpoint forecast vintages only
kalshi-weather ingest nws-cli             narrow: CLI climate reports only
kalshi-weather ingest kalshi-quotes       narrow: market defs + quote snapshots only
kalshi-weather ingest kalshi-resolutions  narrow: settled outcomes only
kalshi-weather backfill nws-cli           historical climate reports (IEM archive)
kalshi-weather backfill kalshi            historical settled markets + candle prices
```

The four narrow subcommands exist for scheduled automation (one workflow per stream,
each on its own cadence — see `plans/data_automation_plan.md`); `ingest nws`/`ingest
kalshi` run both halves in one pass and remain the convenient choice for manual/ad hoc
runs.

Every command accepts `--db PATH` to override `DUCKDB_PATH`, and `--help` for full
options. The `scripts/*.py` files are identical shims kept for cron compatibility
(`.venv/bin/python scripts/run_ingest.py` ≡ `.venv/bin/kalshi-weather ingest nws`).

**Shared exit-code contract** (every ingest/backfill command): `0` = at least one station/series
succeeded end-to-end, `1` = everything failed, `2` = configuration error. Partial
failures (one bad product, one 500) do NOT change the exit code — they are recorded in
`ingest_runs` (see §Monitoring).

**Concurrency warning:** DuckDB takes a single-writer lock. An open `duckdb` CLI session
on `data/weather.duckdb` blocks every collector (they fail with "Could not set lock").
Close interactive sessions before ingests fire, or open them read-only:
`duckdb -readonly data/weather.duckdb`.

---

## 1. Live ingests

### 1.1 `kalshi-weather ingest nws` (+ narrow `nws-grid` / `nws-cli`)

One idempotent pass over all configured stations: gridpoint forecast vintages
(append-only) + CLI climate reports (update-on-newer-issuance). Unchanged forecasts
short-circuit to HTTP 304; already-seen CLI products are skipped. Options: `--metar`
(also pull raw METAR, secondary signal), `--metar-days N`.

Implemented as `run_ingest()` in `src/kalshi_weather/ingest/nws.py`, with
`include_grid`/`include_climate` flags (both default `True`). `ingest nws` runs both;
`ingest nws-grid` and `ingest nws-cli` each pass a single flag to run only their half —
the narrow subcommands scheduled automation is meant to call, per the differing
cadences below (see `plans/data_automation_plan.md`).

- **Horizon cap:** NWS returns ~7 days of periods per pull (same request regardless), but
  only periods within 72h of `issued_time` are stored (`MAX_HORIZON_HOURS` in
  `ingest/nws.py`) — matches the residual dataset's scoped horizon (data dictionary
  §6.1); nothing further out is ever live-tradeable or backfill-relevant. Added
  2026-07-13; rows collected before that may still exceed 72h and aren't retroactively
  pruned.

- **Cadence**: `nws-grid` hourly (NWS re-issues gridpoint forecasts roughly hourly);
  `nws-cli` twice daily (CLI finals land early morning local time ~1–3 AM, intermediates
  during the day — polling more often just re-checks an unchanged product list).
- **Missing a run is recoverable** for climate reports (the products API serves about a
  week of history) but **not for forecast vintages** — a vintage never fetched is gone
  (until an NDFD-archive backfill exists; see §2.3). This is the run you most don't want
  gaps in — hence `nws-grid`'s tighter hourly cadence.
- Requires `.env` with `NWS_CONTACT_EMAIL` (NWS rejects anonymous clients) and a one-time
  `scripts/resolve_stations.py` (re-run occasionally; grid mappings drift). Station
  resolution runs unconditionally on every pass regardless of which flags are set.

### 1.2 `kalshi-weather ingest kalshi` (+ narrow `kalshi-quotes` / `kalshi-resolutions`)

One idempotent pass over all series configured in `config/stations.yaml`
(`kalshi_series`): market definitions (update-in-place), **one quote snapshot per open
market** (append-only), and the ~60 most recently settled markets' outcomes
(insert-once). Public API, no key needed.

Implemented as `run_kalshi_ingest()` in `src/kalshi_weather/ingest/kalshi.py`, with
`include_quotes`/`include_resolutions` flags (both default `True`). `ingest kalshi` runs
both; `ingest kalshi-quotes` and `ingest kalshi-resolutions` each pass a single flag —
note both halves independently upsert market *definitions* as a side effect regardless
of which flag is set (idempotent, harmless overlap).

- **Cadence: `kalshi-quotes`'s interval IS the quote-history resolution** — every 10
  minutes gives ~144 quote points per market per day. Phase 0b target is 5–15 min.
  `kalshi-resolutions` runs twice daily instead — contracts settle once per obs_date, so
  checking at quote frequency would be pure waste.
- Missing runs: definitions and outcomes self-heal (the settled lookback covers ~10
  days); **snapshot gaps are permanent** for the missed window, though candlestick
  backfill (§2.2) reconstructs hourly bars after the fact — another reason
  `kalshi-quotes` needs the tight cadence and `kalshi-resolutions` doesn't.
- ~13 requests per pass across 6 series; occasional 429s are expected and retried
  automatically.

### 1.3 Suggested crontab

```cron
17 * * * *   cd /path/to/Kalshi_Weather_Project && .venv/bin/kalshi-weather ingest nws    >> logs/cron.log 2>&1
*/10 * * * * cd /path/to/Kalshi_Weather_Project && .venv/bin/kalshi-weather ingest kalshi >> logs/kalshi_cron.log 2>&1
```

As of 2026-07-11 **no cron is installed** — both collectors only run when invoked by
hand. Data since the last manual run is missing until the next invocation (recoverable
per the rules above). Scheduling these is the single most important operational TODO.

---

## 2. Backfills

Backfills use **different data sources** than the live collectors (NWS/Kalshi only serve
recent data on their live endpoints) but land through the **same upsert paths**, so they
are safe to overlap with live data and with themselves (re-running is a no-op) — true
today, while the DB is local. **Once the canonical DB moves to GCS**
(`plans/data_automation_plan.md`), that safety property no longer covers backfills:
a local pull→run→push cycle racing a scheduled live workflow's own write can silently
clobber the whole file. The decided procedure is to pause the four live workflows
first — see that plan's "Running backfills safely against the GCS-hosted DB" section
for the full `gh workflow disable`/`enable` steps. Not yet relevant: no workflows exist
yet, so backfills today still just run directly against the local file below.

The grid-forecast backfill (NWS grid forecasts / `grid_forecasts`, via the NDFD GRIB2
archive) was **built 2026-07-12** but, as of 2026-07-16, has **not yet been run** against
the real archive — see §2.1's last bullet. (The CLI settlement backfill below **has** been
run: `climate_reports` holds 4,339 `source='iem_afos'` rows, 2026-01-01 → 2026-07-08.)

### 2.1 `kalshi-weather backfill nws-cli` — historical settlement truth

```bash
kalshi-weather backfill nws-cli --start 2026-01-01 --end 2026-07-08
kalshi-weather backfill nws-cli --start 2026-06-01 --end 2026-06-30 --station nyc --station den
```

- **Source**: Iowa Environmental Mesonet's AFOS archive
  (`mesonet.agron.iastate.edu/api/1/nws/afos/...`) — a free public archive of every NWS
  text product going back decades, byte-compatible with the live bulletins, so the
  **exact same parser** processes both. Rows land with `source='iem_afos'`.
- `--start/--end` are **observation dates** (the day the temperature happened), not
  publication dates; the command handles the publication-lands-next-morning offset.
- The newer-issuance guard means a backfill can never regress a value the live collector
  already landed. Where both exist, live-API data wins if its product is newer.
- Politeness: one request per (station, UTC day) to list + one per product to fetch,
  `--sleep 0.5` between them by default. A 6-station, 6-month backfill ≈ 1,100 listing
  requests + ~2,300 product fetches ≈ 30–40 min. IEM is a university-run free service —
  do not lower `--sleep` aggressively.
- Resumable: fetched products are marked in `http_cache`, so an interrupted run picks up
  where it left off. Products that fail to parse are logged, skipped, and NOT marked, so
  a parser fix re-processes them on the next run.
- **What this does NOT backfill**: forecast vintages (`grid_forecasts`). Historical
  gridpoint forecasts only exist in the NDFD GRIB2 archive — a separate command
  (`backfill nws-grid`, **built 2026-07-12, not yet run** as of 2026-07-16; see the
  "Built" bullet at the end of this section). Until it is run, residual-model history is
  bounded by when live forecast collection started (2026-07-09).
  - **Scope if/when built** (decided 2026-07-12, see data dictionary §6.1): only the
    72h-max, same-day-excluded predictor window is needed, not the full ~168h NDFD
    archive horizon — cuts the download volume roughly 55–60% versus a naive full-week
    pull.
  - **Source verified 2026-07-12** (corrects an earlier wrong assumption in this doc):
    the public S3 bucket `noaa-ndfd-pds` (`registry.opendata.aws/noaa-ndfd`) archives
    **native per-element NDFD GRIB2 products** — `wmo/maxt/`, `wmo/mint/`, `wmo/temp/`,
    `wmo/td/`, `wmo/rhm/`, `wmo/wspd/`, `wmo/wgust/`, `wmo/sky/`, `wmo/qpf/`,
    `wmo/pop12/`, `wmo/snow/` — a 1:1 match to all 11 `GRID_VARIABLES`. This is
    NOT the "hourly instantaneous temp only, derive max/min yourself" archive
    previously assumed here; `maxt`/`mint` are native forecast-office products, same as
    what the live API's `maxTemperature`/`minTemperature` layers serve, just packaged
    as WMO-bulletin-wrapped GRIB2 (confirmed live: `GRIB` magic bytes present) instead
    of JSON. Files are timestamped per issuance (multiple per day, e.g.
    `wmo/maxt/2020/04/16/YGAZ98_KWBN_202004161548`), so real forecast **vintages** are
    preserved, not just a daily snapshot — exactly the shape `grid_forecasts` needs.
    Archive starts 2020-04-16 (confirmed via bucket listing) — five-plus years of
    history, far more than the σ-fitting bootstrap needs.
  - **Sector/gridpoint extraction resolved (2026-07-12)**: each WMO file covers a NDFD
    *sector*; CONUS is region code `UZ` (others seen: `AZ`=Puerto Rico/VI,
    `RZ`=Alaska, `SZ`=Hawaii, `TZ`=Guam). Suffix digit pattern: `98`=full-res day-1–3,
    `97`=full-res day-4–7, `88`/`87` the half-res equivalents — `*UZ98` is the target
    for the 72h-scoped backfill. Per-element WMO prefix varies (`YG`=maxt, `YH`=mint,
    `YE`=temp, `YF`=td, `YR`=rhm, `YC`=wspd, `YW`=wgust, `YA`=sky, `YI`=qpf,
    `YD`=pop12, `YS`=snow) but the region+suffix convention is consistent across all of
    them. Station gridpoint extraction is a plain nearest-cell lookup on `pygrib`'s
    decoded `.latlons()` — no reverse map-projection math needed.
  - **Empirical validation against live data — done (2026-07-12)**, diffing real
    archive files against already-collected live `grid_forecasts` rows, all 6
    stations, same-day and multi-day-out horizons:
    - **Essentially exact match** (rounding-noise only): `maxTemperature`/
      `minTemperature` (mean abs diff 0.05°F, max 0.09°F), `temperature`/`dewpoint`
      (mean ~0.13°F, max ~1.7°F), `quantitativePrecipitation` (mean 0.001mm).
    - **Close, small real noise**: `windSpeed`/`windGust` (mean 0.3–0.9 km/h) —
      explained by the live API rounding wind to whole knots before storage while the
      archive gives raw unrounded values, not a data problem; `relativeHumidity`/
      `skyCover` (mean <1%, max up to 12%) — spatially sharp fields where a simple
      nearest-gridcell pick can land on a slightly different cell than the live
      gridpoint service used. Safe to backfill; not bit-exact.
    - **`probabilityOfPrecipitation`: coarser-resolution, decided acceptable
      (2026-07-12)**. Root cause found: live PoP's real native cadence is 3-hourly for
      the first ~36–48h, widening to 6-hourly further out (confirmed across all 6
      stations — this is NDFD's genuine near-term-vs-later-horizon resolution
      pattern, the same reason day1-3/day4-7 archive files are split at different
      grid resolutions). The archived `pop12` product is genuinely a coarser
      **12-hour** bucket — checked the full NDFD element list for a `pop6`/`pop3`
      variant; none exists in this archive. **Decision: backfill PoP at the coarser
      12h resolution anyway** — acceptable because PoP isn't used as a model feature
      until a much later phase (see `master_plan.md`), so exact-resolution parity
      isn't load-bearing yet. Any backfilled `grid_forecasts` row for PoP must be
      understood as **12h-resolution, not directly comparable row-for-row to
      live-collected 3h/6h PoP rows** — flag this if PoP ever becomes a feature.
    - **`snowfallAmount`: very likely correct, still empirically unconfirmed**. NWS's
      own gridpoint API docs (`weather-gov/api` gridpoints.md +
      discussions) define `quantitativePrecipitation` as liquid precipitation
      "including the liquid equivalent amount for snow and ice" — by elimination,
      `snowfallAmount` is the separate actual-new-snow-accumulation field, not a
      second liquid-equivalent measure. This matches the archive's `snow` element,
      confirmed via its raw GRIB2 keys to be the **standard WMO parameter
      "Total Snowfall"** (discipline 0, category 1, number 29 — not an NDFD-local
      table collision like `pop12`/`sky` had), units meters. Both sides are most
      likely the same physical quantity, differing only by a units conversion
      (m → mm, ×1000) — this **corrects** the data dictionary's earlier "liquid-
      equivalent-style" guess (never actually verified, now understood to likely be
      wrong; see `docs/data_dictionary.md` §4.4). Still genuinely untested — no snow
      fell in the July validation window (0≈0 both sides proves nothing) — needs a
      real winter overlap day to confirm the magnitude empirically before fully
      trusting it.
  - **Net assessment**: all 11 variables are cleared to backfill. 9 match
    exactly-or-closely, confirmed. PoP backfills at accepted-lower (12h) resolution.
    Snow backfills on a well-reasoned but not yet empirically confirmed unit
    conversion — re-verify against real winter data before relying on it for
    anything beyond IS-it-nonzero sanity checks.
  - **Built (2026-07-12): `kalshi-weather backfill nws-grid --start ... --end ...`.**
    Needs the optional `pygrib`/`numpy` dependencies (`pip install -e '.\[ndfd]'`, not
    part of `dev` — heavy binary geo deps the rest of the pipeline never needs).
    Downloads one GRIB2 file per (element, issuance) from `noaa-ndfd-pds`, decodes every
    in-file forecast period via `pygrib`, and does a plain nearest-gridcell lookup per
    station — one file covers all 6 stations at once (CONUS-wide grid), so this is far
    cheaper than one request per station. Rows land with `source='ndfd_archive'`,
    through the same `ON CONFLICT DO NOTHING` upsert the live collector uses, so
    overlapping a backfill with live-collected dates is harmless (can only add rows,
    never regress or duplicate one) — same resumability contract as `backfill nws-cli`
    (`http_cache` marks downloaded keys, safe to re-run/interrupt). `--variable`/
    `--station` narrow a run to specific layers/cities; `--sleep` (default 0.2s) paces
    requests against the S3 bucket. Implementation: `src/kalshi_weather/ndfd_client.py`
    (S3 listing/download) + `src/kalshi_weather/ingest/ndfd_backfill.py`
    (orchestration, unit conversion, the `NDFD_ELEMENTS` table this whole section's
    findings are encoded into). **Not yet run against the real archive** — validated via
    fixture/fake-pygrib tests (`tests/test_ndfd_backfill.py`) only; run a small
    `--variable maxTemperature --station nyc` range first and spot-check against real
    `grid_forecasts` rows before trusting a full run.

### 2.2 `kalshi-weather backfill kalshi` (+ narrow `kalshi-quotes` / `kalshi-resolutions`) — historical markets + prices

```bash
kalshi-weather backfill kalshi --start 2026-01-01 --end 2026-07-08                       # both halves
kalshi-weather backfill kalshi --start 2026-01-01 --end 2026-07-08 --no-candles          # outcomes only, much faster
kalshi-weather backfill kalshi-resolutions --start 2026-01-01 --end 2026-07-08           # same as above, dedicated subcommand
kalshi-weather backfill kalshi-quotes --start 2026-07-01 --end 2026-07-08 --series KXHIGHNY --period 1440
```

- **Sources**: the same public trade-api/v2, but two endpoints the live collector
  doesn't use for history: `/markets?status=settled` paginated arbitrarily far back
  (definitions + outcomes), and `/series/{s}/markets/{m}/candlesticks` (OHLC price bars
  → `market_candles`, the historical stand-in for `market_snapshots`). Both endpoints
  are fetched from the same settled-market listing, so market *definitions* land
  regardless of which subcommand you run — only outcomes vs. candles are gated.
- Implemented as `run_kalshi_backfill()` in `src/kalshi_weather/ingest/kalshi_backfill.py`,
  with `include_quotes`/`include_resolutions` flags (both default `True`). `backfill
  kalshi` runs both; `backfill kalshi-quotes` and `backfill kalshi-resolutions` each
  pass a single flag — mirrors the `ingest kalshi-quotes`/`ingest kalshi-resolutions`
  split in §1.2. `backfill kalshi --no-candles` and `backfill kalshi-resolutions` are
  equivalent; the dedicated subcommand exists for scriptability (e.g. a future
  workflow_dispatch backfill job, see `plans/data_automation_plan.md`).
- `--start/--end` are observation dates, matched against each market's parsed
  `obs_date`.
- `--period` sets bar length in minutes (1, 60, 1440), only meaningful for the quotes
  half. Default 60 (hourly): ~40 bars per market. 1-minute bars are ~2,400/market — use
  only for targeted studies.
- Cost model: definitions/outcomes are a handful of paginated requests per series;
  candles are **one request per market** (~6 markets per city-day → a 6-city, 6-month
  candle backfill ≈ 6,500 requests ≈ 25 min at the default `--sleep 0.2`) — this is why
  `backfill kalshi-resolutions` (no candle requests at all) is the much faster option
  when only settlement history is needed.
- Resumable: markets that already have candles at the requested period are skipped
  entirely; re-running an interrupted backfill continues where it stopped.
- Outcomes are **insert-once**: a recorded settlement is never rewritten by a later
  listing (a re-settlement would be a data incident to investigate, not silently apply).

### 2.3 Backfill order for Phase 0c (residual dataset)

1. `backfill nws-cli` for as much history as you want σ estimates from (IEM has years).
2. `backfill kalshi-resolutions` for the same range (fast, no candle requests), then
   `backfill kalshi-quotes` for the price history you care about (or just `backfill
   kalshi` for both in one pass).
3. `backfill nws-grid` for forecast-vintage history (needs `pip install -e '.\[ndfd]'`
   first) — previously the binding constraint (organic accumulation from live
   `ingest nws-grid` runs only), now backfillable from the NDFD archive. See §2.1's
   last bullet for what's validated vs. still-to-confirm (PoP resolution, snow units).

---

## 3. Monitoring

`ingest_runs` is the fine-grained health signal for **all** commands (live + backfill):
endpoints are `grid_forecasts`, `climate_reports`, `observations`, `nws_backfill`,
`ndfd_backfill:{variable}`, `kalshi_markets:{SERIES}`, `kalshi_outcomes:{SERIES}`,
`kalshi_backfill:{SERIES}`.

```sql
-- anything unhealthy in the last day?
SELECT * FROM ingest_runs
WHERE error IS NOT NULL AND started_at > now() - INTERVAL 1 DAY;

-- is data actually flowing? (per endpoint, last 24h)
SELECT endpoint, count(*) runs, sum(rows_upserted) new_rows, max(finished_at) last_run
FROM ingest_runs WHERE started_at > now() - INTERVAL 1 DAY GROUP BY endpoint;

-- staleness check: how old is the newest data in each table?
SELECT 'grid_forecasts' t, max(pulled_at) newest FROM grid_forecasts
UNION ALL SELECT 'climate_reports', max(pulled_at) FROM climate_reports
UNION ALL SELECT 'market_snapshots', max(snapshot_time) FROM market_snapshots;

-- settlement cross-check: Kalshi's settled value vs our independently parsed CLI value
-- (mismatch on a *settled* day = investigate; today mismatching = normal, see below)
SELECT m.station_id, m.obs_date,
       any_value(o.expiration_value) kalshi, any_value(cr.value) ours
FROM market_outcomes o
JOIN markets m USING (ticker)
JOIN climate_reports cr ON cr.station_id = m.station_id
 AND cr.obs_date = m.obs_date AND cr.variable = m.variable
GROUP BY 1, 2
HAVING any_value(o.expiration_value) != any_value(cr.value);
```

Known benign "mismatch": if `ingest nws` hasn't run since a day's final CLI report, that
day's `climate_reports` row is still an intermediate (e.g. Denver showing 65 when Kalshi
settled 91, observed 2026-07-11) — run `ingest nws` before suspecting a bug. Genuine
divergence on settled days is expected only from NWS corrections issued *after* market
expiration (contract ignores them; `climate_reports` doesn't).

## 4. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `Could not set lock on file` | Another process (often an interactive `duckdb` shell) holds the DB. Close it or use `-readonly`. |
| `config error: NWS_CONTACT_EMAIL is not set` | Copy `.env.example` → `.env`, set a real email. |
| Exit code 1 from `ingest kalshi` with 429 logs | Kalshi rate-limit exhaustion after retries — rare; re-run. If persistent, raise the sleep between series (it's a constant in `kalshi_client.py`). |
| CLI product "failed to parse" in `ingest_runs.error` | New WFO text format quirk. The product is deliberately NOT marked seen; fix `cli_parser.py`, add a fixture, and the next run picks it up. |
| Backfill needs a redo after a parser fix | `DELETE FROM http_cache WHERE url LIKE '%nwstext%'` (IEM) plus delete the affected `climate_reports` rows (the newer-issuance guard otherwise ignores re-parses of the same products). |
| `kalshi-weather: command not found` | `pip install -e .` hasn't run in this venv, or the venv isn't activated — use `.venv/bin/kalshi-weather`. |
