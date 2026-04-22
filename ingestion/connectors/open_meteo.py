"""
open_meteo.py
-------------
Connector for the Open-Meteo historical weather API.

Endpoint : https://archive-api.open-meteo.com/v1/archive
Docs     : https://open-meteo.com/en/docs/historical-weather-api
Auth     : None (public API, no API key required)

What it fetches
---------------
Daily aggregated weather variables for a configurable set of locations.
Each record represents one (date, location) pair and is structured to land
directly into the BigQuery Bronze table `raw.weather_daily`.

BigQuery target schema
----------------------
    date            DATE       REQUIRED   -- partition column
    location_id     STRING     REQUIRED   -- clustering column
    location_name   STRING     REQUIRED
    latitude        FLOAT64    NULLABLE
    longitude       FLOAT64    NULLABLE
    temp_max_c      FLOAT64    NULLABLE
    temp_min_c      FLOAT64    NULLABLE
    temp_mean_c     FLOAT64    NULLABLE
    precipitation_mm FLOAT64   NULLABLE
    wind_speed_max_kmh FLOAT64 NULLABLE
    wind_direction_deg FLOAT64 NULLABLE
    fetched_at      TIMESTAMP  REQUIRED
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from ingestion.connectors.base_connector import BaseConnector, ConnectorError, ConnectorResult

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class Location:
    """A geographic point of interest to fetch weather data for.

    Attributes:
        id        : URL-safe, stable identifier used as the BQ clustering key.
        name      : Human-readable label (e.g. "São Paulo").
        latitude  : Decimal degrees, WGS-84.
        longitude : Decimal degrees, WGS-84.
        timezone  : IANA timezone string accepted by Open-Meteo.
    """

    id: str
    name: str
    latitude: float
    longitude: float
    timezone: str = "America/Sao_Paulo"

DEFAULT_LOCATIONS: list[Location] = [
    Location(id="sao_paulo",      name="São Paulo",      latitude=-23.5505, longitude=-46.6333),
    Location(id="rio_de_janeiro", name="Rio de Janeiro", latitude=-22.9068, longitude=-43.1729),
    Location(id="recife",         name="Recife",          latitude=-8.0539,  longitude=-34.8811),
    Location(id="brasilia",       name="Brasília",        latitude=-15.7801, longitude=-47.9292),
]

_BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

_DAILY_VARIABLES: list[str] = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "wind_speed_10m_max",
    "wind_direction_10m_dominant",
]

# Maps Open-Meteo field names → our schema column names
_FIELD_MAP: dict[str, str] = {
    "temperature_2m_max":            "temp_max_c",
    "temperature_2m_min":            "temp_min_c",
    "temperature_2m_mean":           "temp_mean_c",
    "precipitation_sum":             "precipitation_mm",
    "wind_speed_10m_max":            "wind_speed_max_kmh",
    "wind_direction_10m_dominant":   "wind_direction_deg",
}


class OpenMeteoConnector(BaseConnector):
    """Fetches daily weather data from the Open-Meteo archive API.

    Usage
    -----
    >>> from datetime import date
    >>> connector = OpenMeteoConnector()
    >>> result = connector.run(date(2024, 6, 1))
    >>> print(result.summary())
    [open_meteo] 4 records fetched at 2024-06-02T...
    """

    def __init__(
        self,
        locations: list[Location] | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Args:
            locations : Locations to fetch. Defaults to DEFAULT_LOCATIONS.
            **kwargs  : Forwarded to BaseConnector (retry_config, timeout, …).
        """
        super().__init__(**kwargs)
        self._locations = locations or DEFAULT_LOCATIONS

    @property
    def source_name(self) -> str:
        return "open_meteo"

    def fetch(self, target_date: date) -> ConnectorResult:
        """Fetch daily weather for all configured locations on *target_date*.

        Args:
            target_date : The date for which weather data is requested.

        Returns:
            ConnectorResult whose `.records` is a list of flat dicts, one per
            location, ready for BigQuery streaming insert.

        Raises:
            ConnectorError : If the API returns unexpected data or all retries
                             are exhausted.
        """
        date_str = target_date.isoformat()  # "YYYY-MM-DD"
        records: list[dict[str, Any]] = []
        failures: list[str] = []

        for location in self._locations:
            try:
                record = self._fetch_location(date_str, location)
                records.append(record)
            except ConnectorError as exc:
                logger.warning(
                    "[%s] Failed to fetch location '%s': %s — skipping.",
                    self.source_name,
                    location.id,
                    exc.message,
                )
                failures.append(location.id)

        # Raise if every location failed (total extraction failure)
        if failures and len(failures) == len(self._locations):
            raise ConnectorError(
                self.source_name,
                f"All {len(self._locations)} locations failed: {failures}",
            )

        return ConnectorResult(
            source=self.source_name,
            records=records,
            fetched_at=datetime.now(timezone.utc),
            metadata={
                "target_date": date_str,
                "locations_requested": len(self._locations),
                "locations_succeeded": len(records),
                "locations_failed": failures,
                "api_diagnostics": self.diagnostics(),
            },
        )

    def _fetch_location(self, date_str: str, location: Location) -> dict[str, Any]:
        """Call the API for a single location and return a flat record dict.

        Args:
            date_str : ISO date string "YYYY-MM-DD".
            location : Location dataclass instance.

        Returns:
            Flat dict matching the BigQuery Bronze schema.

        Raises:
            ConnectorError : On HTTP error or unexpected response shape.
        """
        params = {
            "latitude":   location.latitude,
            "longitude":  location.longitude,
            "start_date": date_str,
            "end_date":   date_str,
            "daily":      ",".join(_DAILY_VARIABLES),
            "timezone":   location.timezone,
        }

        response = self.get(_BASE_URL, params=params)

        try:
            payload = response.json()
        except ValueError as exc:
            raise ConnectorError(
                self.source_name,
                f"Could not decode JSON for location '{location.id}': {exc}",
            ) from exc

        return self._parse_record(payload, location, date_str)

    def _parse_record(
        self,
        payload: dict[str, Any],
        location: Location,
        date_str: str,
    ) -> dict[str, Any]:
        """Extract and flatten one day's weather values from the API response.

        Open-Meteo returns arrays even when a single date is requested, so we
        always read index 0.

        Args:
            payload  : Parsed JSON dict from the API.
            location : Corresponding Location instance.
            date_str : The requested date ("YYYY-MM-DD").

        Returns:
            Flat dict ready for BigQuery insertion.

        Raises:
            ConnectorError : If required keys are absent or the date array is empty.
        """
        daily = payload.get("daily")
        if not daily:
            raise ConnectorError(
                self.source_name,
                f"Missing 'daily' key in response for location '{location.id}'. "
                f"Keys received: {list(payload.keys())}",
            )

        dates = daily.get("time", [])
        if not dates:
            raise ConnectorError(
                self.source_name,
                f"Empty 'daily.time' array for location '{location.id}' on {date_str}.",
            )

        idx = 0  # single-date request → always index 0

        record: dict[str, Any] = {
            "date":          dates[idx],
            "location_id":   location.id,
            "location_name": location.name,
            "latitude":      location.latitude,
            "longitude":     location.longitude,
        }

        for api_key, bq_col in _FIELD_MAP.items():
            values = daily.get(api_key, [None])
            raw_val = values[idx] if idx < len(values) else None
            record[bq_col] = self._safe_float(raw_val, api_key, location.id)

        record["fetched_at"] = datetime.now(timezone.utc).isoformat()

        return record

    @staticmethod
    def _safe_float(value: Any, field: str, location_id: str) -> float | None:
        """Coerce *value* to float; log a warning and return None on failure."""
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            logger.warning(
                "[open_meteo] Could not cast field '%s' value %r to float "
                "for location '%s' — storing NULL.",
                field,
                value,
                location_id,
            )
            return None
