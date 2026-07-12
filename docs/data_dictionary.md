# Data Dictionary — NWS Ingestion Pipeline

This is the authoritative reference for interpreting every table and column the pipeline
writes to DuckDB (`data/weather.duckdb`). Read this before doing any analysis on the data.
If code and this document ever disagree, treat it as a bug and fix one of them.

Facts marked **(observed)** were verified against real ingested data on 2026-07-09, not
just taken from NWS documentation.

---

## 1. Mental model: two data planes

The database holds two fundamentally different kinds of weather data, and almost every
analysis mistake comes from blurring them:

1. **Predictions** (`grid_forecasts`) — what NWS *forecast* would happen, captured as
   every forecast vintage. Append-only. A given quantity (say, tomorrow's high at Central
   Park) appears many times, once per forecast issuance, each with a different
   `issued_time` and therefore a different lead time (`horizon_hours`).
2. **Settlement truth** (`climate_reports`) — what NWS *says actually happened*, parsed
   from the Daily Climate Report (CLI) text product, which is **the exact product Kalshi
   uses to resolve its weather contracts**. Update-in-place: we keep only the current
   best-known value per (station, date, variable), not a history of revisions.

`observations` (raw METAR, optional) is a third, supplementary plane: instrument readings
leading up to the report. It is **never** settlement truth — the CLI report can and does
differ from what you'd compute off raw METAR.

Since Phase 0b there is a third plane: **Kalshi market data** (`markets`,
`market_snapshots`, `market_outcomes`) — what the market *believed* (quote history) and
how Kalshi *actually settled* each contract (§11). Its collector
(`scripts/run_kalshi_ingest.py`) is independent of the NWS one; either can run without
the other.

---

## 2. Global conventions

- **All timestamps are naive UTC** — timezone-stripped after conversion — with **one
  deliberate exception**: `climate_reports.value_time` is naive **station-local** time as
  printed in the report (see §5.4).
- **Units are stored per row** in a `unit` column, never assumed. Forecast units are
  metric (WMO codes, `wmoUnit:` prefix stripped); climate-report units are imperial as
  printed in the text. **You will need degF↔degC conversion to compare the two planes**
  (§7).
- **Provenance**: every row carries `pulled_at` (when our collector fetched it).
  `climate_reports` rows also carry `product_id` (the exact NWS text product they were
  parsed from) so any value can be traced back to its source bulletin and re-verified by
  eye at `https://api.weather.gov/products/{product_id}`.
- `station_id` everywhere is our own slug (`nyc`, `chi`, `aus`, `den`, `mia`, `phl`) —
  join to `stations` for everything else. Exception: `observations.obs_station_id` is the
  ICAO code (`KNYC`, ...) because METAR data is keyed to the physical instrument site.

---

## 3. `stations` — resolved station metadata

One row per configured city. Written by `scripts/resolve_stations.py` (or inline on first
ingest); safe to re-resolve — NWS grid mappings occasionally change.

| column | meaning |
|---|---|
| `station_id` | our slug, primary key (`nyc`, `chi`, ...) |
| `display_name` | human label, e.g. `New York City (Central Park)` |
| `lat`, `lon` | coordinates the grid was resolved from — the **settlement station's own** coordinates (≤4 decimals), so forecasts cover the spot contracts resolve at |
| `grid_id`, `grid_x`, `grid_y` | NWS forecast grid cell, e.g. `OKX/34,45`. From `/points/{lat},{lon}` |
| `forecast_grid_data_url` | the exact `/gridpoints/...` URL the forecast collector fetches |
| `forecast_hourly_url` | human-readable hourly forecast URL (kept for reference, not ingested) |
| `obs_station_id` | **the confirmed Kalshi settlement station** (ICAO): KNYC, KMDW, KAUS, KDEN, KMIA, KPHL. Hardcoded in `config/stations.yaml`, never auto-discovered for these 6 — nearest-station lookup would pick the wrong airport for Chicago (O'Hare ≠ Midway) and Austin (Camp Mabry ≠ KAUS) |
| `wfo_id` | issuing Weather Forecast Office (`properties.cwa` from `/points`) — metadata only |
| `cli_location_id` | 3-letter site code that keys CLI product listing (`NYC`, `MDW`, ...). **Not** the WFO id — verified live. Derived as ICAO minus leading `K`, overridable in yaml |
| `cli_site_name` | the spelled-out site name used to find this station's block inside CLI text (`CENTRAL PARK`, `CHICAGO-MIDWAY` — note the hyphen). Verified against real fetched bulletins |
| `timezone` | IANA tz for the station (`America/New_York`, ...) — needed to convert UTC `valid_start` to the local calendar date |
| `station_verified` | `TRUE` = settlement station came from the confirmed config table. `FALSE` = auto-discovered nearest station for a city not in the table — **do not trust for settlement until manually confirmed** |
| `resolved_at` | when `/points` was last resolved (UTC) |

---

## 4. `grid_forecasts` — forecast vintages (append-only)

Source: the **raw numeric gridpoint endpoint** (`/gridpoints/{office}/{x},{y}`), not the
human-readable forecast. One row per `(station_id, variable, issued_time, valid_start)`.

### 4.1 Columns

| column | meaning |
|---|---|
| `variable` | NWS layer name, camelCase, e.g. `maxTemperature` (full list §4.4) |
| `issued_time` | when NWS generated this forecast (payload `updateTime`, UTC). **This is the vintage key** |
| `valid_start`, `valid_end` | the time window the value applies to (UTC), parsed from NWS's `validTime` interval, e.g. `2026-07-10T06:00:00+00:00/PT6H` → start 06:00, end 12:00 |
| `horizon_hours` | `(valid_start − issued_time)` in hours: the forecast lead time. **Can be negative** — NWS payloads include periods already underway when the forecast was issued (observed: −6h) |
| `value` | numeric forecast value; interpretation depends on the variable class (§4.3) |
| `unit` | WMO unit with `wmoUnit:` stripped: `degC`, `percent`, `mm`, `km_h-1` (= km/h) **(observed)** |
| `pulled_at` | when our collector fetched it (UTC) |
| `source` | provenance: always `'nws_api'` today; reserved for a future NDFD-archive forecast backfill (see runbook §2.1) |

### 4.2 Why intervals span multiple hours: run-length encoding

The underlying forecast grid is (for most variables) hourly, but NWS compresses payloads:
**consecutive hours with identical values are collapsed into a single interval**. A
dewpoint row spanning 6 hours does **not** mean "6-hour average" — it means the forecast
dewpoint is that exact value at *each hour* of the window. Interval lengths therefore vary
row to row within the same variable: stable overnight air compresses into one long row; a
frontal passage produces many 1-hour rows. Observed run lengths: `temperature` 1–5h,
`dewpoint` 1–16h, `skyCover` 1–18h, `probabilityOfPrecipitation` 1–36h.

**To reconstruct an hourly series** for hourly-class variables, expand each row across
each hour `h` with `valid_start ≤ h < valid_end` (half-open on the right — `valid_end` is
the start of the *next* period, not part of this one):

```sql
-- hourly dewpoint series for one vintage
SELECT station_id, h AS hour_utc, value, unit
FROM grid_forecasts,
     LATERAL unnest(generate_series(valid_start, valid_end - INTERVAL 1 HOUR,
                                    INTERVAL 1 HOUR)) AS t(h)
WHERE variable = 'dewpoint' AND station_id = 'nyc'
  AND issued_time = (SELECT max(issued_time) FROM grid_forecasts WHERE station_id='nyc');
```

### 4.3 The three variable classes — how to read `value` against the window

This is the single most important thing in this document.

**A. Hourly-state variables** — `temperature`, `dewpoint`, `relativeHumidity`,
`windSpeed`, `windGust`, `skyCover`, `probabilityOfPrecipitation`:
the value holds at **every hour inside the window** (run-length encoding, §4.2). Expanding
by repetition is correct. Averaging across rows without expanding first will silently
weight long runs wrong. (PoP is defined per-hour by NWS; treat identically.)

**B. Accumulation variables** — `quantitativePrecipitation`, `snowfallAmount`:
the value is a **total over the whole window**, not a repeated hourly value. Observed:
these arrive in blocks of up to 6 hours (NWS QPF is bucketed 6-hourly). **Never expand by
repetition** — that multiple-counts the precipitation. To get a daily total, sum the rows
whose windows fall in the day (windows don't straddle day boundaries in practice, but
guard anyway by splitting on overlap if you need exactness).

**C. Period-extreme variables** — `maxTemperature`, `minTemperature`:
the value is the forecast **max (or min) over that specific window**, and the window
itself carries meaning:

- `maxTemperature` windows are the NWS "daytime" period: **8 AM–9 PM local** (observed:
  NYC 12:00→01:00 UTC = 8AM–9PM EDT; Denver 14:00→03:00 UTC = 8AM–9PM MDT). One row per
  forecast day, ~13h span.
- `minTemperature` windows are the overnight period: **8 PM–10 AM local** (observed: NYC
  00:00→14:00 UTC). Note an overnight window **straddles two calendar dates** — a min
  window "starting July 9, 8 PM" is the low associated with the morning of July 10.
- **The first window of each vintage is usually truncated**: it starts at (roughly) the
  issuance hour, not 8 AM — observed spans as short as 10h for max and 1h for min. A
  truncated window is a "rest of the period" forecast; treat it as a shorter-horizon,
  partially-informed version of the same day's number, or filter `horizon_hours < 0`
  rows out if you only want clean pre-period forecasts.

These map directly onto Kalshi daily high/low contracts, with one subtlety in §6.

### 4.4 Variables collected (the allowlist) with observed units

| variable | class | unit (observed) | notes |
|---|---|---|---|
| `maxTemperature` | period-extreme | `degC` | daytime window, ~1/day |
| `minTemperature` | period-extreme | `degC` | overnight window, ~1/day |
| `temperature` | hourly-state | `degC` | 2m air temperature |
| `dewpoint` | hourly-state | `degC` | |
| `relativeHumidity` | hourly-state | `percent` | |
| `windSpeed` | hourly-state | `km_h-1` | sustained; km/h, **not** mph or knots |
| `windGust` | hourly-state | `km_h-1` | |
| `skyCover` | hourly-state | `percent` | |
| `probabilityOfPrecipitation` | hourly-state | `percent` | |
| `quantitativePrecipitation` | accumulation | `mm` | ≤6h buckets |
| `snowfallAmount` | accumulation | `mm` | liquid-equivalent-style depth in mm; all 0.0 in July data so far — re-verify semantics against winter data before first use |

Null values in the NWS payload (periods where a layer has no data) are **skipped at
ingest**, not stored as NULL rows. Absence of a row ≠ zero — especially for accumulation
variables, check window coverage before assuming "no rain forecast".

### 4.5 Vintages: how re-issued forecasts land

Each NWS re-issue gets a new `updateTime` → new `issued_time` → entirely **new rows**
(the primary key includes `issued_time`). Old vintages are never modified or deleted.
Re-running the collector when NWS hasn't re-issued is a no-op (conditional GET returns
304; even on a full re-fetch, `ON CONFLICT DO NOTHING` keeps rows unique).

Typical queries:

```sql
-- latest vintage only, per station/variable
SELECT * FROM grid_forecasts gf
WHERE issued_time = (
  SELECT max(issued_time) FROM grid_forecasts
  WHERE station_id = gf.station_id AND variable = gf.variable);

-- every vintage of "NYC's forecast high for a given local date" (horizon study)
SELECT issued_time, horizon_hours, value
FROM grid_forecasts
WHERE station_id = 'nyc' AND variable = 'maxTemperature'
  AND CAST(valid_start AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS DATE)
      = DATE '2026-07-12'
ORDER BY issued_time;
```

---

## 5. `climate_reports` — settlement ground truth (update-in-place)

Source: the NWS **Daily Climate Report** (CLI) text product — the exact product Kalshi
settles against. One row per `(station_id, obs_date, variable)`; only the **current
best-known** value is kept.

### 5.1 Columns

| column | meaning |
|---|---|
| `obs_date` | the climatological day the report covers, **taken verbatim from the report's header text** ("...CLIMATE SUMMARY FOR JULY 8 2026...") — never re-derived from timestamps (§5.3) |
| `variable` | `max_temp`, `min_temp`, `precip`, `snowfall` |
| `value` | numeric value as printed (see §5.5 for `T`/`MM` handling) |
| `value_time` | time-of-day the max/min occurred, **naive station-local**, only for temps (§5.4) |
| `unit` | as printed in the report: `degF` for temps, `in` for precip/snow **(observed)** — imperial, unlike forecasts |
| `product_id` | product id this row was parsed from. For `source='nws_api'`: fetch `https://api.weather.gov/products/{product_id}` to see the raw bulletin. For `source='iem_afos'`: an IEM id like `202607100625-KOKX-CDUS41-CLINYC` — fetch `https://mesonet.agron.iastate.edu/api/1/nwstext/{product_id}` |
| `issued_time` | the product's issuance time (UTC) — drives the revision logic (§5.2) |
| `pulled_at` | fetch time (UTC) |
| `source` | provenance: `'nws_api'` (live collector) or `'iem_afos'` (historical backfill, runbook §2.1). Both go through the same parser and upsert guard, so a backfill can never regress a live value |

### 5.2 Report lifecycle: intermediate → final → corrections

For a given `obs_date`, a station's WFO typically issues:

1. **Intermediate report(s)** during the day: header carries `VALID [TODAY] AS OF 0400 PM
   LOCAL TIME` — a *partial-day* summary. The "daily max" in it can still be exceeded
   later that day.
2. **The final report** the next morning (~1–3 AM local): full-day summary, no "AS OF"
   line. **This is what Kalshi settles on.**
3. Occasionally, a **corrected re-issue** — same date, newer `issuanceTime`.

All of these funnel into the same `(station, obs_date, variable)` row via
newer-issuance-wins upsert: an incoming value overwrites the stored one **only if its
`issuanceTime` is strictly newer** (`WHERE excluded.issued_time > climate_reports.issued_time`).
Consequences:

- Intermediate values appear during the day, then get overwritten by the final that night.
  **Today's row is provisional until the next morning's final lands.** There is no flag
  column; the practical test for "is this final?" is `issued_time`'s local time falling in
  the early-morning hours *after* `obs_date`, or re-fetching `product_id` and checking for
  the `AS OF` line.
- A stale re-fetch can never regress a corrected value.
- Unlike `grid_forecasts`, revision history is **not** kept. If you ever need
  intermediate-vs-final revision analysis, that requires a schema change (or replaying
  `product_id`s from the NWS archive) — know that before deleting anything.

### 5.3 `obs_date` and the DST boundary quirk

The CLI report's "day" is the **local-standard-time day**. During Daylight Saving Time
that means the window is effectively **1:00 AM → 12:59 AM local the following day**, not
midnight-to-midnight — and Kalshi's "daily high" window inherits this. This is why
`obs_date` is parsed from the report header text and never re-derived by bucketing
timestamps: re-bucketing by UTC or local-midnight day boundaries will occasionally assign
values to the wrong day (most likely for events near midnight).

### 5.4 `value_time` — the one non-UTC timestamp in the database

`value_time` is `obs_date` + the printed clock time (`347 PM`, `2:56 PM` — both formats
occur, varying by WFO). It is stored **naive, station-local, exactly as printed** — no
timezone conversion. Rationale: the report column header claims LST, but observed behavior
of "AS OF" stamps matches local *daylight* time; rather than encode a possibly-wrong
conversion, we preserve the source. If you need UTC, decide the LST-vs-LDT question
deliberately at analysis time. Do **not** compare `value_time` directly against
`grid_forecasts` timestamps without converting.

### 5.5 Sentinel values in the source text and how they land

| in report | meaning | stored as |
|---|---|---|
| `85` | plain value | `85.0` |
| `100R` | value; record set or tied | `100.0` (R suffix stripped; record-ness not stored) |
| `T` | trace precipitation (< 0.005 in) | `0.0` — correct for Kalshi purposes since rain contracts key on *measurable* precip (≥ 0.01 in), but note "did any water fall" is lost |
| `MM` | missing | **row not written** (logged at ingest). Absence of a row can therefore mean "missing in source", not just "not yet ingested" |
| `-` prefix | negative value (winter temps) | negative float |

`snowfall` rows are only written when the report has a SNOWFALL section (Denver/Miami
omit or MM it in summer — **(observed)**), so per-station row counts differ legitimately.

### 5.6 Parser trust boundary

The CLI text format is the most fragile input in the pipeline (it's a human-readable
bulletin, not an API contract). The parser **fails loudly and skips** rather than guessing:
a product that doesn't match the expected shape is logged in `ingest_runs.error` and *not*
marked seen, so a parser fix picks it up on the next run. Real per-city bulletins are
recorded as test fixtures in `tests/fixtures/cli/` — when a new WFO quirk appears, the
product text should be added there and a regression test written (that's how the
`2:56 PM` and `100R` variants were caught).

---

## 6. Joining forecasts to settlement (the core analysis join)

The join key is the **station-local calendar date**:

- `grid_forecasts` side: convert `valid_start` from UTC to the station's timezone, take
  the date. For `maxTemperature` this is the date of the 8 AM–9 PM window. For
  `minTemperature`, note the overnight window straddles dates — decide explicitly whether
  "the low for July 10" means the window *starting* July 9 evening (NWS's association is
  with the morning date, i.e. use the date of `valid_end`).
- `climate_reports` side: `obs_date`, directly.

```sql
-- forecast error by horizon: NYC daily high
SELECT gf.issued_time, gf.horizon_hours,
       gf.value * 9/5 + 32           AS forecast_high_f,   -- degC -> degF!
       cr.value                      AS settled_high_f,
       gf.value * 9/5 + 32 - cr.value AS error_f
FROM grid_forecasts gf
JOIN climate_reports cr
  ON cr.station_id = gf.station_id
 AND cr.variable = 'max_temp'
 AND cr.obs_date = CAST(gf.valid_start AT TIME ZONE 'UTC'
                        AT TIME ZONE 'America/New_York' AS DATE)
WHERE gf.station_id = 'nyc' AND gf.variable = 'maxTemperature'
ORDER BY cr.obs_date, gf.horizon_hours DESC;
```

Two caveats, both deliberate and worth re-reading:

1. **Unit mismatch**: forecasts are `degC`, settlement is `degF`. Always convert
   (`F = C × 9/5 + 32`). A forgotten conversion produces plausible-looking garbage.
2. **Window mismatch**: the forecast max window (8 AM–9 PM local) is *not* the report's
   climatological day (1:00 AM–12:59 AM during DST). A daily high occurring outside
   8 AM–9 PM (e.g. a midnight warm front) can make the settled value exceed anything the
   forecast window "saw". Rare, but it is real model error you'll observe in residuals —
   not a data bug.

---

## 7. Unit reference

| unit string | meaning | convert |
|---|---|---|
| `degC` | Celsius | °F = °C × 9/5 + 32 |
| `degF` | Fahrenheit | °C = (°F − 32) × 5/9 |
| `mm` | millimetres | in = mm / 25.4 |
| `in` | inches | mm = in × 25.4 |
| `km_h-1` | km per hour (WMO notation) | mph = km/h ÷ 1.609344 |
| `percent` | 0–100 | — |

Unit strings come from NWS's WMO codes with the `wmoUnit:` prefix stripped at ingest
(`wmoUnit:degC` → `degC`). Never hardcode a unit assumption in analysis code — read the
`unit` column; NWS reserves the right to change layer units, and the per-row column is
what makes that survivable.

---

## 8. `observations` — raw METAR (secondary, optional, off by default)

Only populated when ingest runs with `--metar`. One row per
`(obs_station_id, variable, timestamp)` from `/stations/{icao}/observations`.

- **Not settlement truth.** The CLI report is produced from quality-controlled data and
  can differ from raw METAR-derived numbers; Kalshi even delays settlement when they
  disagree. Use this table only as a supplementary signal (e.g. intraday temperature
  trajectory features).
- Units are SI as delivered by the API (`degC`, `km_h-1`, ...), stored per row in `unit`.
- `quality_control` is NWS's QC flag (e.g. `V` validated, `Z` preliminary) — passed
  through verbatim.
- Readings surface **up to ~20 minutes late** (upstream MADIS QC) — a missing newest
  reading is latency, not an ingestion bug.
- API-side retention is **days-to-weeks**; this table only accumulates history from the
  time you start collecting.
- Variables kept: `temperature`, `dewpoint`, `windSpeed`, `windGust`,
  `precipitationLastHour`.

---

## 9. `ingest_runs` — audit trail / monitoring

One row per (station, endpoint) per collector invocation. `endpoint` ∈ `grid_forecasts`,
`climate_reports`, `observations`, plus the Kalshi collector's per-series endpoints
`kalshi_markets:{SERIES}` and `kalshi_outcomes:{SERIES}` (e.g. `kalshi_markets:KXHIGHNY`).

- `http_status`: 200 normal, **304 = "nothing changed" (healthy no-op, not an error)**,
  NULL when the failure happened before/without an HTTP status.
- `rows_upserted`: for `grid_forecasts` this counts rows *actually new* (post-conflict);
  for `climate_reports` it counts staged values (an upsert that lost to the
  newer-issuance guard still counts — treat as "values processed").
- `error`: NULL = clean. Populated for exceptions **and** for partial problems like
  unparseable CLI products (which don't fail the run).

The process exit code is deliberately coarse (0 = at least one station fully ok, 1 = all
stations failed, 2 = config error) so cron alerting is quiet; **this table is the
fine-grained health signal**:

```sql
-- anything unhealthy in the last day?
SELECT * FROM ingest_runs
WHERE error IS NOT NULL AND started_at > now() - INTERVAL 1 DAY;

-- is data actually flowing? (per endpoint, last 24h)
SELECT endpoint, count(*) runs, sum(rows_upserted) new_rows, max(finished_at) last_run
FROM ingest_runs WHERE started_at > now() - INTERVAL 1 DAY GROUP BY endpoint;
```

## 10. `http_cache` — conditional-GET state (internal)

Keyed by URL. Two uses: stores the last `Last-Modified` per gridpoint URL (sent back as
`If-Modified-Since` → 304 short-circuit), and marks CLI product URLs as fetched (products
are immutable per `product_id`, so "seen" = skip). `last_modified` is NULL for product
URLs. Deleting rows from this table is always safe — it only causes re-fetching, and the
upsert guards make re-fetching harmless. (That is exactly how to force a CLI re-parse
after a parser fix: `DELETE FROM http_cache WHERE url LIKE '%/products/%'`, plus delete
the affected `climate_reports` rows since the newer-issuance guard would otherwise ignore
re-parsed identical products.)

---

## 11. Kalshi market data — `markets`, `market_snapshots`, `market_outcomes`

Collected by `scripts/run_kalshi_ingest.py` from Kalshi's **public** trade-api/v2 (no API
key; market data is unauthenticated). Which series are collected is configured per station
in `config/stations.yaml` under `kalshi_series`; the six active daily-high series
(`KXHIGHNY`, `KXHIGHCHI`, `KXHIGHAUS`, `KXHIGHDEN`, `KXHIGHMIA`, `KXHIGHPHIL`) were
verified live on 2026-07-11 — Kalshi's series listing also contains **retired duplicates**
(`KXDENHIGH`, `KXHIGHTEMPDEN`, `HIGHMIA`, ...) with zero open markets, so never pick
series tickers from the listing alone; check for open markets.

### 11.1 `markets` — contract definitions (update-in-place)

One row per contract ticker (e.g. `KXHIGHNY-26JUL12-T89`). Strikes/title never change
after listing; `status`, `close_time`, `expected_expiration_time`, `updated_at` are
refreshed on every pass.

| column | meaning |
|---|---|
| `ticker` | Kalshi market ticker — PK |
| `event_ticker` | groups the strike ladder for one (station, day), e.g. `KXHIGHNY-26JUL12` |
| `series_ticker` | e.g. `KXHIGHNY` |
| `station_id`, `variable` | our config mapping to the settlement join: `climate_reports(station_id, obs_date, variable)` |
| `obs_date` | the contract's observation day, parsed from the event-ticker date suffix (`26JUL12` → 2026-07-12) |
| `strike_type`, `floor_strike`, `cap_strike` | payout criterion — see §11.4, the most important table in this section |
| `subtitle` | Kalshi's human-readable band (`"88° to 89°"`) — display only, never parse it |
| `open_time`, `close_time` | UTC. Close is ~11:59 PM ET on `obs_date` (04:59Z next day during EDT) |
| `expected_expiration_time` | UTC. Per current market rules: "the first 7:00 or 8:00 AM ET following the release of the data" — i.e. the morning after the CLI final lands |
| `status` | latest seen: `active` → `closed` → `settled`/`finalized` **(observed: listings show open markets as `active`, though the query param is `open`)** |

### 11.2 `market_snapshots` — quote history (append-only)

One row per open market per collector pass; `snapshot_time` is shared across the whole
pass so a pass's snapshots join to each other and to `grid_forecasts` vintages cleanly.
The cron cadence IS the quote-history resolution (target: every 5–15 min while open).

- **All prices are dollars per $1 contract** (parsed from the API's `*_dollars` string
  fields), so `yes_bid`/`yes_ask` ∈ [0, 1] read directly as probabilities: mid =
  `(yes_bid + yes_ask)/2` is the market-implied P(YES) before any spread/bias adjustment.
- `yes_bid_size`/`yes_ask_size` are **contracts displayed at the best level only** (from
  `*_size_fp`) — not book depth. Full order-book depth collection is a Phase 8 add-on.
- `volume` is lifetime contracts traded; `volume_24h` trailing 24h; `open_interest`
  outstanding contracts; `liquidity` is Kalshi's dollar-value-resting-on-book metric.
- Contracts can trade fractionally (the `_fp` fields carry decimals like `1642.25`).

### 11.2b `market_candles` — historical price bars (backfill counterpart to snapshots)

`market_snapshots` only exists from when the live collector started running; for any
earlier market, `kalshi-weather backfill kalshi` reconstructs price history from Kalshi's
candlesticks endpoint into this table. One row per `(ticker, period_minutes, period_end)`.

| column | meaning |
|---|---|
| `period_end` | UTC end of the bar (the API's `end_period_ts`); the bar covers the `period_minutes` before it |
| `period_minutes` | bar length: 1, 60 (default), or 1440 |
| `price_open/high/low/close/mean` | **trade** prices within the bar, dollars ∈ [0,1] ≈ probability. NULL when nothing traded in the bar |
| `yes_bid_close`, `yes_ask_close` | best quotes at bar end — present even in bars with no trades; `(bid+ask)/2` is the market-implied probability at `period_end` |
| `volume` | contracts traded within the bar (fractional allowed) |
| `open_interest` | outstanding contracts at bar end |

Snapshot-vs-candle choice for analysis: prefer `market_snapshots` where it exists
(higher resolution, includes displayed size); fall back to candles for history. The two
agree on convention (dollars, UTC) by construction.

### 11.3 `market_outcomes` — Kalshi's own settlement (insert-once)

One row per settled contract. **Insert-once by design**: a finalized result must never
change, so a conflicting later listing is silently ignored rather than applied — if you
ever suspect a re-settlement, that's a manual investigation, not an upsert.

- `result` ∈ `yes` | `no`.
- `expiration_value` — the underlying (°F) Kalshi settled against, **as of expiration**.
  This is the market-P&L ground truth. It can legitimately differ from
  `climate_reports.value` because the contract ignores NWS revisions issued *after*
  expiration, while `climate_reports` keeps the current-best value forever. Verified
  2026-07-11: 12/12 station-days agreed exactly; expect rare post-expiration-correction
  divergences, not systematic ones.

### 11.4 Payout criterion semantics (verified against live markets + the NHIGH CFTC filing)

| `strike_type` | YES iff | example (live, 2026-07-12 NYC) |
|---|---|---|
| `greater` | `value > floor_strike` — **strict** | floor 89, subtitle "90° or above" |
| `less` | `value < cap_strike` — **strict** | cap 82, subtitle "81° or below" |
| `between` | `floor_strike <= value <= cap_strike` — **inclusive both ends** | floor 88, cap 89, "88° to 89°" |

Settlement values are integer °F, so `greater` with floor 89 ⇔ "≥ 90". **The ticker's
`-T`/`-B` suffix does NOT encode the side** — a `-T87` ticker was observed as the *less*
tail on one day and tickers encode the tail threshold differently across days; only
`strike_type` + strikes are authoritative.

Settlement source, per the NHIGH terms and every market's `rules_primary`: the NWS
Climatological Report (Daily) — exactly what `climate_reports` parses. Revisions between
close and expiration count; revisions after expiration don't.

### 11.5 The market⇄settlement join

```sql
-- did the market's settled value match our independently parsed CLI value?
SELECT m.station_id, m.obs_date,
       any_value(o.expiration_value) AS kalshi_settled,
       any_value(cr.value)           AS cli_value
FROM market_outcomes o
JOIN markets m USING (ticker)
LEFT JOIN climate_reports cr
       ON cr.station_id = m.station_id
      AND cr.obs_date   = m.obs_date
      AND cr.variable   = m.variable
GROUP BY 1, 2;
```

Two gotchas inherited from §5: (a) `climate_reports` values for *today* are intermediate
until the final lands, so never cross-check an unsettled day; (b) if the NWS collector
has not run since a day's final report, its `climate_reports` row is stale-intermediate —
a wildly implausible mismatch here (e.g. 65 vs 91) means "run `scripts/run_ingest.py`",
not "parser bug" (observed exactly this on 2026-07-11).

---

## 12. Gotchas checklist (the short version)

1. Multi-hour `grid_forecasts` windows are run-length encoding for hourly-state variables,
   totals for accumulation variables, and meaningful periods for max/min temperature.
2. Forecasts are metric; climate reports are imperial. Convert.
3. `maxTemperature`'s 8 AM–9 PM window ≠ the settlement day's 1 AM–12:59 AM window.
4. `minTemperature` windows straddle calendar dates; associate with the morning date.
5. The first forecast window per vintage is truncated; `horizon_hours` can be negative.
6. Today's `climate_reports` row is an intermediate value until tomorrow's final report.
7. `value_time` is naive local; everything else is naive UTC.
8. Missing row ≠ zero (MM values and null forecast periods are skipped, not stored).
9. Trace precip is stored as 0.0.
10. Only current-best climate values are kept — no revision history.
11. Kalshi `greater`/`less` are strict; `between` is inclusive both ends. The ticker
    suffix does not encode the side — use `strike_type`.
12. Kalshi prices are stored in dollars (0–1 ≈ probability), not cents.
13. `market_outcomes.expiration_value` is settlement-as-of-expiration; it can diverge
    from `climate_reports` after a post-expiration NWS correction.
14. Kalshi's series listing contains retired duplicate series — verify a series has open
    markets before adding it to `stations.yaml`.
15. `climate_reports.source` tells you whether a row came from the live API or the IEM
    backfill; both are the same bulletins, but `product_id` resolves at different URLs.
16. `market_candles.price_*` is NULL in bars with no trades — use `yes_bid_close`/
    `yes_ask_close` (always present) for a continuous implied-probability series.
