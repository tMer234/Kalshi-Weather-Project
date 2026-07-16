-- DuckDB schema for the NWS ingestion pipeline. All DDL is idempotent (IF NOT EXISTS) so
-- db.connect() can run it on every invocation.
--
-- Column comments here are summaries. The authoritative guide to interpreting the DATA
-- (interval semantics per variable class, unit conventions, timezone rules, CLI report
-- lifecycle, join recipes) is docs/data_dictionary.md — read it before doing analysis.

-- Resolved via resolve_stations.py; safe to re-run periodically (NWS grid mappings can drift).
CREATE TABLE IF NOT EXISTS stations (
    station_id      VARCHAR PRIMARY KEY,   -- our slug, e.g. 'nyc', 'chi'
    display_name    VARCHAR,
    lat             DOUBLE,
    lon             DOUBLE,
    grid_id         VARCHAR,               -- NWS forecast office grid id, e.g. 'OKX'
    grid_x          INTEGER,
    grid_y          INTEGER,
    forecast_grid_data_url VARCHAR,
    forecast_hourly_url    VARCHAR,
    obs_station_id  VARCHAR,               -- confirmed Kalshi settlement station, e.g. 'KNYC'
                                           -- (hardcoded per city in config/stations.yaml)
    wfo_id          VARCHAR,               -- from /points properties.cwa (issuing office metadata)
    cli_location_id VARCHAR,               -- 3-letter site code keying /products/types/CLI/
                                           -- locations/{id}, e.g. 'NYC', 'MDW' — verified live:
                                           -- the products API lists CLI products under the SITE
                                           -- code, NOT the WFO id
    cli_site_name   VARCHAR,               -- spelled-out name matching this station's block in a
                                           -- CLI bulletin, e.g. 'CENTRAL PARK'
    timezone        VARCHAR,
    station_verified BOOLEAN DEFAULT TRUE, -- FALSE when obs_station_id came from nearest-station
                                           -- auto-discovery instead of the confirmed config table
    resolved_at     TIMESTAMP
);

-- Forecast vintages: one row per (station, variable, issued_time, valid_start). Append-only —
-- a re-issued forecast gets a new issued_time and therefore a new row; history is retained.
CREATE TABLE IF NOT EXISTS grid_forecasts (
    station_id      VARCHAR REFERENCES stations(station_id),
    variable        VARCHAR,               -- 'maxTemperature', 'minTemperature', 'dewpoint', ...
    issued_time     TIMESTAMP,             -- payload updateTime (UTC)
    valid_start     TIMESTAMP,             -- parsed from validTime interval (UTC)
    valid_end       TIMESTAMP,             -- valid_start + parsed ISO8601 duration (UTC)
    horizon_hours   DOUBLE,                -- (valid_start - issued_time) in hours
    value           DOUBLE,
    unit            VARCHAR,               -- uom with 'wmoUnit:' prefix stripped, e.g. 'degC'
    pulled_at       TIMESTAMP,
    source          VARCHAR DEFAULT 'nws_api',  -- 'nws_api' (live) | 'ndfd_archive'
                                               -- (backfill nws-grid, the NDFD archive)
    PRIMARY KEY (station_id, variable, issued_time, valid_start)
);
-- migration for databases created before the source column existed
ALTER TABLE grid_forecasts ADD COLUMN IF NOT EXISTS source VARCHAR DEFAULT 'nws_api';

-- PRIMARY observation ground truth: parsed from the NWS Daily Climate Report (CLI product),
-- the same data Kalshi settles against. Update-on-newer-issuance, not append-only.
-- NB obs_date is the report's local-standard-time day: during DST Kalshi's "daily high" window
-- is 1:00 AM - 12:59 AM local the following day, not midnight-to-midnight — take obs_date from
-- the CLI text itself, never by re-bucketing timestamps.
CREATE TABLE IF NOT EXISTS climate_reports (
    station_id      VARCHAR REFERENCES stations(station_id),
    obs_date        DATE,
    variable        VARCHAR,               -- 'max_temp', 'min_temp', 'precip', 'snowfall', ...
    value           DOUBLE,
    value_time      TIMESTAMP,             -- time-of-day the max/min occurred, if reported
    unit            VARCHAR,
    product_id      VARCHAR,               -- NWS/IEM product id this row was parsed from
    issued_time     TIMESTAMP,             -- product issuanceTime
    pulled_at       TIMESTAMP,
    source          VARCHAR DEFAULT 'nws_api',  -- 'nws_api' (live collector) | 'iem_afos' (backfill)
    PRIMARY KEY (station_id, obs_date, variable)
);
-- migration for databases created before the source column existed
ALTER TABLE climate_reports ADD COLUMN IF NOT EXISTS source VARCHAR DEFAULT 'nws_api';

-- SECONDARY/optional raw METAR feed — supplementary signal only, NOT settlement ground truth.
CREATE TABLE IF NOT EXISTS observations (
    obs_station_id  VARCHAR,
    variable        VARCHAR,
    timestamp       TIMESTAMP,
    value           DOUBLE,
    unit            VARCHAR,
    quality_control VARCHAR,
    pulled_at       TIMESTAMP,
    PRIMARY KEY (obs_station_id, variable, timestamp)
);

-- Audit trail: one row per (station, endpoint) per collector invocation.
CREATE TABLE IF NOT EXISTS ingest_runs (
    run_id          UUID DEFAULT uuid() PRIMARY KEY,
    started_at      TIMESTAMP,
    finished_at     TIMESTAMP,
    station_id      VARCHAR,
    endpoint        VARCHAR,
    http_status     INTEGER,
    rows_upserted   INTEGER,
    error           VARCHAR
);

-- Conditional-GET cache: last-seen Last-Modified per URL, so unchanged forecasts short-circuit
-- to a 304 instead of a full payload re-parse.
CREATE TABLE IF NOT EXISTS http_cache (
    url             VARCHAR PRIMARY KEY,
    last_modified   VARCHAR,               -- verbatim Last-Modified header value
    fetched_at      TIMESTAMP
);

-- ============================================================================
-- Kalshi market data (Phase 0b). Collected from the public trade-api/v2 —
-- no API key needed for market data. station_id deliberately has NO foreign key
-- so the Kalshi collector can run independently of NWS station resolution
-- (same reasoning as `observations`).
-- ============================================================================

-- One row per contract, updated in place as status/close_time evolve.
-- Strike semantics verified against live markets AND the NHIGH CFTC filing:
--   'greater' : YES iff settled value >  floor_strike (STRICT — "T89" = "90° or above")
--   'less'    : YES iff settled value <  cap_strike   (STRICT — cap 82 = "81° or below")
--   'between' : YES iff floor_strike <= value <= cap_strike (INCLUSIVE both ends)
-- NB the ticker's T/B prefix does NOT identify the side — a "-T87" can be either
-- tail; only strike_type is authoritative.
CREATE TABLE IF NOT EXISTS markets (
    ticker          VARCHAR PRIMARY KEY,   -- e.g. 'KXHIGHNY-26JUL12-T89'
    event_ticker    VARCHAR,               -- e.g. 'KXHIGHNY-26JUL12'
    series_ticker   VARCHAR,               -- e.g. 'KXHIGHNY'
    station_id      VARCHAR,               -- our slug ('nyc', ...) from config mapping
    variable        VARCHAR,               -- climate_reports variable it settles on ('max_temp')
    obs_date        DATE,                  -- parsed from the event ticker date component
    strike_type     VARCHAR,               -- 'greater' | 'less' | 'between'
    floor_strike    DOUBLE,                -- NULL for 'less'
    cap_strike      DOUBLE,                -- NULL for 'greater'
    title           VARCHAR,
    subtitle        VARCHAR,               -- human-readable band, e.g. '88° to 89°'
    open_time       TIMESTAMP,             -- UTC
    close_time      TIMESTAMP,             -- UTC (last trading ~11:59 PM ET on obs_date)
    expected_expiration_time TIMESTAMP,    -- UTC (first 7/8 AM ET after the CLI final)
    status          VARCHAR,               -- latest seen: 'active'/'closed'/'settled'/'finalized'
    first_seen_at   TIMESTAMP,
    updated_at      TIMESTAMP
);

-- Quote history: append-only, one row per market per collector pass. All prices in
-- DOLLARS per $1 contract (the API's *_dollars fields), i.e. 0.01–0.99 ≈ probability.
CREATE TABLE IF NOT EXISTS market_snapshots (
    ticker          VARCHAR,
    snapshot_time   TIMESTAMP,             -- one shared timestamp per collector pass
    yes_bid         DOUBLE,
    yes_ask         DOUBLE,
    no_bid          DOUBLE,
    no_ask          DOUBLE,
    last_price      DOUBLE,
    yes_bid_size    DOUBLE,                -- contracts displayed at best bid (fp fields)
    yes_ask_size    DOUBLE,
    volume          DOUBLE,                -- lifetime contracts traded
    volume_24h      DOUBLE,
    open_interest   DOUBLE,
    liquidity       DOUBLE,                -- dollar value resting on the book
    status          VARCHAR,
    PRIMARY KEY (ticker, snapshot_time)
);

-- Historical price bars from Kalshi's candlesticks endpoint — the BACKFILL counterpart
-- to market_snapshots (which only exists from when our collector started running).
-- All price fields in dollars per $1 contract, like market_snapshots. A candle row
-- summarises the period ENDING at period_end; periods with no trades still carry
-- bid/ask closes. price_* fields are trade prices (NULL when the period had no trades).
CREATE TABLE IF NOT EXISTS market_candles (
    ticker          VARCHAR,
    period_end      TIMESTAMP,             -- UTC end of the bar (API end_period_ts)
    period_minutes  INTEGER,               -- bar length: 1, 60, or 1440
    price_open      DOUBLE,
    price_high      DOUBLE,
    price_low       DOUBLE,
    price_close     DOUBLE,
    price_mean      DOUBLE,
    yes_bid_close   DOUBLE,
    yes_ask_close   DOUBLE,
    volume          DOUBLE,                -- contracts traded within the bar
    open_interest   DOUBLE,                -- outstanding at bar end
    pulled_at       TIMESTAMP,
    PRIMARY KEY (ticker, period_minutes, period_end)
);

-- Settlement outcomes: Kalshi's own resolution, the ground truth for market P&L.
-- expiration_value is what Kalshi settled on AS OF expiration — this can differ from
-- climate_reports if NWS corrects a report after expiration (contract terms: revisions
-- past expiration are ignored). Never assume the two always agree.
CREATE TABLE IF NOT EXISTS market_outcomes (
    ticker          VARCHAR PRIMARY KEY,
    result          VARCHAR,               -- 'yes' | 'no'
    expiration_value DOUBLE,               -- settled underlying (°F), NULL if not reported
    status          VARCHAR,               -- 'settled' | 'finalized'
    recorded_at     TIMESTAMP
);
