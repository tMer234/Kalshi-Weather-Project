"""Typed settings: environment variables (.env) + station config (config/stations.yaml)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_STATIONS_YAML = REPO_ROOT / "config" / "stations.yaml"


@dataclass(frozen=True)
class SeriesConfig:
    """One Kalshi market series this station's contracts trade under.

    `variable` names the climate_reports variable the series settles on ('max_temp',
    'min_temp'), which is what lets market rows join to settlement truth later.
    """

    ticker: str
    variable: str


@dataclass(frozen=True)
class StationConfig:
    station_id: str
    display_name: str
    lat: float
    lon: float
    timezone: str
    # None => not a confirmed Kalshi settlement station; resolve_stations falls back to
    # nearest-station auto-discovery and flags the result unverified.
    obs_station_id: str | None = None
    cli_site_name: str | None = None
    # 3-letter location id for CLI product listing; defaults to obs_station_id minus the
    # leading 'K' (e.g. KMDW -> MDW), which holds for CONUS ICAO codes.
    cli_location_id: str | None = None
    # Kalshi series to collect for this station; empty tuple => Kalshi collector skips it.
    kalshi_series: tuple[SeriesConfig, ...] = ()

    @property
    def effective_cli_location_id(self) -> str | None:
        if self.cli_location_id:
            return self.cli_location_id
        if self.obs_station_id and len(self.obs_station_id) == 4 and self.obs_station_id[0] == "K":
            return self.obs_station_id[1:]
        return None


@dataclass(frozen=True)
class Settings:
    contact_email: str
    app_name: str
    duckdb_path: Path
    stations: list[StationConfig] = field(default_factory=list)

    @property
    def user_agent(self) -> str:
        return f"{self.app_name} ({self.contact_email})"


class ConfigError(Exception):
    """Raised when required configuration is missing or malformed."""


def load_stations(path: Path = DEFAULT_STATIONS_YAML) -> list[StationConfig]:
    with open(path) as f:
        raw = yaml.safe_load(f)
    if not raw or "stations" not in raw:
        raise ConfigError(f"{path} is missing the top-level 'stations' key")
    stations = []
    for entry in raw["stations"]:
        try:
            stations.append(
                StationConfig(
                    station_id=entry["station_id"],
                    display_name=entry["display_name"],
                    # NWS API requires <=4 decimal places on coordinates
                    lat=round(float(entry["lat"]), 4),
                    lon=round(float(entry["lon"]), 4),
                    timezone=entry["timezone"],
                    obs_station_id=entry.get("obs_station_id"),
                    cli_site_name=entry.get("cli_site_name"),
                    cli_location_id=entry.get("cli_location_id"),
                    kalshi_series=tuple(
                        SeriesConfig(ticker=s["ticker"], variable=s["variable"])
                        for s in entry.get("kalshi_series", [])
                    ),
                )
            )
        except KeyError as e:
            raise ConfigError(f"station entry {entry!r} is missing required key {e}") from e
    ids = [s.station_id for s in stations]
    if len(ids) != len(set(ids)):
        raise ConfigError("duplicate station_id in stations.yaml")
    return stations


def load_settings(
    env_file: Path | None = None,
    stations_yaml: Path = DEFAULT_STATIONS_YAML,
) -> Settings:
    load_dotenv(env_file or REPO_ROOT / ".env")

    contact_email = os.environ.get("NWS_CONTACT_EMAIL", "").strip()
    if not contact_email or contact_email == "you@example.com":
        raise ConfigError(
            "NWS_CONTACT_EMAIL is not set. Copy .env.example to .env and fill in a real "
            "contact email — NWS requires it in the User-Agent header."
        )

    duckdb_path = Path(os.environ.get("DUCKDB_PATH", "data/weather.duckdb"))
    if not duckdb_path.is_absolute():
        duckdb_path = REPO_ROOT / duckdb_path

    return Settings(
        contact_email=contact_email,
        app_name=os.environ.get("APP_NAME", "kalshi-weather").strip(),
        duckdb_path=duckdb_path,
        stations=load_stations(stations_yaml),
    )
