"""Resolve configured lat/lon into NWS grid metadata and persist to the stations table.

Safe to re-run periodically: NWS states the office/gridX/gridY mapping for a location "may
occasionally change" (grid redefinitions), so this is not strictly one-time setup.

Settlement station IDs are NOT discovered here for the 6 confirmed cities — they come
hardcoded from config/stations.yaml (nearest-station lookup would silently pick the wrong
airport for Chicago/Austin). Auto-discovery runs only for a station whose config omits
obs_station_id, and the result is flagged station_verified = FALSE.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import duckdb

from .config import StationConfig
from .nws_client import NWSClient

logger = logging.getLogger(__name__)

_UPSERT_SQL = """
INSERT INTO stations (
    station_id, display_name, lat, lon, grid_id, grid_x, grid_y,
    forecast_grid_data_url, forecast_hourly_url, obs_station_id, wfo_id,
    cli_location_id, cli_site_name, timezone, station_verified, resolved_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (station_id) DO UPDATE SET
    display_name = excluded.display_name,
    lat = excluded.lat,
    lon = excluded.lon,
    grid_id = excluded.grid_id,
    grid_x = excluded.grid_x,
    grid_y = excluded.grid_y,
    forecast_grid_data_url = excluded.forecast_grid_data_url,
    forecast_hourly_url = excluded.forecast_hourly_url,
    obs_station_id = excluded.obs_station_id,
    wfo_id = excluded.wfo_id,
    cli_location_id = excluded.cli_location_id,
    cli_site_name = excluded.cli_site_name,
    timezone = excluded.timezone,
    station_verified = excluded.station_verified,
    resolved_at = excluded.resolved_at
"""


def resolve_station(
    client: NWSClient, conn: duckdb.DuckDBPyConnection, cfg: StationConfig
) -> dict:
    """Resolve one station via /points and upsert it. Returns the resolved metadata."""
    points = client.get_points(cfg.lat, cfg.lon)
    props = points["properties"]

    obs_station_id = cfg.obs_station_id
    verified = True
    if not obs_station_id:
        nearby = client.get_nearby_stations(
            props["gridId"], props["gridX"], props["gridY"]
        )
        features = nearby.get("features", [])
        if not features:
            raise ValueError(f"{cfg.station_id}: no nearby stations returned by NWS")
        obs_station_id = features[0]["properties"]["stationIdentifier"]
        verified = False
        logger.warning(
            "%s: obs_station_id %s was AUTO-DISCOVERED (nearest station) and is UNVERIFIED — "
            "confirm it against the Kalshi contract rules before trusting it",
            cfg.station_id,
            obs_station_id,
        )

    resolved = {
        "station_id": cfg.station_id,
        "display_name": cfg.display_name,
        "lat": cfg.lat,
        "lon": cfg.lon,
        "grid_id": props["gridId"],
        "grid_x": props["gridX"],
        "grid_y": props["gridY"],
        "forecast_grid_data_url": props["forecastGridData"],
        "forecast_hourly_url": props["forecastHourly"],
        "obs_station_id": obs_station_id,
        "wfo_id": props["cwa"],
        "cli_location_id": cfg.effective_cli_location_id,
        "cli_site_name": cfg.cli_site_name,
        "timezone": cfg.timezone,
        "station_verified": verified,
        "resolved_at": datetime.now(timezone.utc).replace(tzinfo=None),
    }
    conn.execute(_UPSERT_SQL, list(resolved.values()))
    logger.info(
        "%s: grid %s/%s,%s wfo=%s obs=%s%s",
        cfg.station_id,
        resolved["grid_id"],
        resolved["grid_x"],
        resolved["grid_y"],
        resolved["wfo_id"],
        obs_station_id,
        "" if verified else " (UNVERIFIED)",
    )
    return resolved


def ensure_stations_resolved(
    client: NWSClient,
    conn: duckdb.DuckDBPyConnection,
    stations: list[StationConfig],
    force: bool = False,
) -> list[dict]:
    """Resolve any configured station missing from the stations table (all, if force)."""
    existing = {
        row[0] for row in conn.execute("SELECT station_id FROM stations").fetchall()
    }
    resolved = []
    for cfg in stations:
        if force or cfg.station_id not in existing:
            resolved.append(resolve_station(client, conn, cfg))
    return resolved
