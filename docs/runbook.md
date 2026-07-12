# Runbook — Operating the Ingestion Pipelines

How to run, schedule, monitor, and backfill every collector in this repo. For what the
*data means*, read [`data_dictionary.md`](data_dictionary.md); this document is about
*operating* the pipelines.

**Scheduling is moving off this laptop.** The manual/local instructions below (§1.3)
remain accurate for running things by hand, but the planned always-on setup — GitHub
Actions workflows + a private GCS bucket holding the canonical database, with the
`ingest nws`/`ingest kalshi` commands split into narrower cadence-specific subcommands
— is designed in [`plans/data_automation_plan.md`](../plans/data_automation_plan.md).
Not yet implemented; this section will be rewritten to point at it once it is.

Everything is driven by one installed command, **`kalshi-weather`** (registered by
`pip install -e .`; re-run that after pulling a version that changes `pyproject.toml`):

```
kalshi-weather ingest nws            live NWS pass (forecasts + climate reports)
kalshi-weather ingest kalshi         live Kalshi pass (markets + quotes + outcomes)
kalshi-weather backfill nws-cli      historical climate reports (IEM archive)
kalshi-weather backfill kalshi       historical settled markets + candle prices
```

Every command accepts `--db PATH` to override `DUCKDB_PATH`, and `--help` for full
options. The `scripts/*.py` files are identical shims kept for cron compatibility
(`.venv/bin/python scripts/run_ingest.py` ≡ `.venv/bin/kalshi-weather ingest nws`).

**Shared exit-code contract** (all four commands): `0` = at least one station/series
succeeded end-to-end, `1` = everything failed, `2` = configuration error. Partial
failures (one bad product, one 500) do NOT change the exit code — they are recorded in
`ingest_runs` (see §Monitoring).

**Concurrency warning:** DuckDB takes a single-writer lock. An open `duckdb` CLI session
on `data/weather.duckdb` blocks every collector (they fail with "Could not set lock").
Close interactive sessions before ingests fire, or open them read-only:
`duckdb -readonly data/weather.duckdb`.

---

## 1. Live ingests

### 1.1 `kalshi-weather ingest nws`

One idempotent pass over all configured stations: gridpoint forecast vintages
(append-only) + CLI climate reports (update-on-newer-issuance). Unchanged forecasts
short-circuit to HTTP 304; already-seen CLI products are skipped. Options: `--metar`
(also pull raw METAR, secondary signal), `--metar-days N`.

- **Horizon cap:** NWS returns ~7 days of periods per pull (same request regardless), but
  only periods within 72h of `issued_time` are stored (`MAX_HORIZON_HOURS` in
  `ingest.py`) — matches the residual dataset's scoped horizon (data dictionary §6.1);
  nothing further out is ever live-tradeable or backfill-relevant. Added 2026-07-13; rows
  collected before that may still exceed 72h and aren't retroactively pruned.

- **Cadence**: hourly. NWS re-issues gridpoint forecasts roughly hourly; CLI finals land
  early morning local time (~1–3 AM), intermediates during the day.
- **Missing a run is recoverable** for climate reports (the products API serves about a
  week of history) but **not for forecast vintages** — a vintage never fetched is gone
  (until an NDFD-archive backfill exists; see §2.3). This is the run you most don't want
  gaps in.
- Requires `.env` with `NWS_CONTACT_EMAIL` (NWS rejects anonymous clients) and a one-time
  `scripts/resolve_stations.py` (re-run occasionally; grid mappings drift).

### 1.2 `kalshi-weather ingest kalshi`

One idempotent pass over all series configured in `config/stations.yaml`
(`kalshi_series`): market definitions (update-in-place), **one quote snapshot per open
market** (append-only), and the ~60 most recently settled markets' outcomes
(insert-once). Public API, no key needed.

- **Cadence: the cron interval IS the quote-history resolution** — every 10 minutes
  gives ~144 quote points per market per day. Phase 0b target is 5–15 min.
- Missing runs: definitions and outcomes self-heal (the settled lookback covers ~10
  days); **snapshot gaps are permanent** for the missed window, though candlestick
  backfill (§2.2) reconstructs hourly bars after the fact.
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
are safe to overlap with live data and with themselves (re-running is a no-op).

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
  gridpoint forecasts only exist in the NDFD GRIB2 archive at NCEI — a separate, much
  heavier build documented as open work in the master plan (Phase 0c). Until then,
  residual-model history is bounded by when live forecast collection started
  (2026-07-09).
  - **Scope if/when built** (decided 2026-07-12, see data dictionary §6.1): only the
    72h-max, same-day-excluded predictor window is needed, not the full ~168h NDFD
    archive horizon — cuts the download volume roughly 55–60% versus a naive full-week
    pull. NCEI's archive stores hourly instantaneous 2m temperature grids (not a
    precomputed daily max/min layer), so max/min would need deriving from those and
    validating against live-API values on overlap days before trusting it — this
    validation step, not GRIB tooling (verified: `pygrib` installs cleanly, no system
    deps), is the real risk in that build.

### 2.2 `kalshi-weather backfill kalshi` — historical markets + prices

```bash
kalshi-weather backfill kalshi --start 2026-01-01 --end 2026-07-08
kalshi-weather backfill kalshi --start 2026-01-01 --end 2026-07-08 --no-candles   # outcomes only, much faster
kalshi-weather backfill kalshi --start 2026-07-01 --end 2026-07-08 --series KXHIGHNY --period 1440
```

- **Sources**: the same public trade-api/v2, but two endpoints the live collector
  doesn't use for history: `/markets?status=settled` paginated arbitrarily far back
  (definitions + outcomes), and `/series/{s}/markets/{m}/candlesticks` (OHLC price bars
  → `market_candles`, the historical stand-in for `market_snapshots`).
- `--start/--end` are observation dates, matched against each market's parsed
  `obs_date`.
- `--period` sets bar length in minutes (1, 60, 1440). Default 60 (hourly): ~40 bars per
  market. 1-minute bars are ~2,400/market — use only for targeted studies.
- Cost model: definitions/outcomes are a handful of paginated requests per series;
  candles are **one request per market** (~6 markets per city-day → a 6-city, 6-month
  candle backfill ≈ 6,500 requests ≈ 25 min at the default `--sleep 0.2`).
- Resumable: markets that already have candles at the requested period are skipped
  entirely; re-running an interrupted backfill continues where it stopped.
- Outcomes are **insert-once**: a recorded settlement is never rewritten by a later
  listing (a re-settlement would be a data incident to investigate, not silently apply).

### 2.3 Backfill order for Phase 0c (residual dataset)

1. `backfill nws-cli` for as much history as you want σ estimates from (IEM has years).
2. `backfill kalshi` for the same range (`--no-candles` first for outcomes fast, then
   candles for the price history you care about).
3. Forecast history remains the binding constraint (§2.1 last bullet) — organic
   accumulation from live `ingest nws` runs, or the future NDFD backfill.

---

## 3. Monitoring

`ingest_runs` is the fine-grained health signal for **all** commands (live + backfill):
endpoints are `grid_forecasts`, `climate_reports`, `observations`, `nws_backfill`,
`kalshi_markets:{SERIES}`, `kalshi_outcomes:{SERIES}`, `kalshi_backfill:{SERIES}`.

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
