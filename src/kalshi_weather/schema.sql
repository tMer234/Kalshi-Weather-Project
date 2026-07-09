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
    PRIMARY KEY (station_id, variable, issued_time, valid_start)
);

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
    product_id      VARCHAR,               -- NWS product id this row was parsed from
    issued_time     TIMESTAMP,             -- product issuanceTime
    pulled_at       TIMESTAMP,
    PRIMARY KEY (station_id, obs_date, variable)
);

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
