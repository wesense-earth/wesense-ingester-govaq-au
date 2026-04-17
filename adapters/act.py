"""
ACT Government air quality adapter.

Fetches air quality readings from the ACT Open Data Portal (Socrata API).
Single endpoint returns recent records for all stations in one JSON response.

Endpoint: https://www.data.act.gov.au/api/id/94a5-zqnn.json
No authentication required. JSON format.

API notes:
- Returns JSON array of records ordered by datetime DESC
- Each record has: name (station), gps (lat/lon), datetime, pollutant fields
- Pollutant values are numeric strings (e.g. "13.56")
- ~3 stations: Monash, Florey, Civic
- Datetimes are Australian Eastern Time (AEST/AEDT) without explicit timezone
- Gas concentrations (O3, NO2) are in ppm
- CO is in ppm
- Particulates (PM10, PM2.5) are in ug/m3
"""

import logging
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

from adapters.base import GovAQAdapter

logger = logging.getLogger(__name__)

# ACT is in the Australian Eastern timezone (observes daylight saving)
ACT_TZ = ZoneInfo("Australia/Sydney")

# Map ACT API field names to WeSense standard (reading_type, unit).
# Units verified from live API responses:
# - O3 values ~0.018-0.030 → ppm
# - CO values ~0-0.17 → ppm
# - PM values ~7-20 → ug/m3
# - NO2 values when present → ppm (same source as O3/CO)
FIELD_MAP = {
    "pm10":         ("pm10", "ug/m3"),
    "pm2_5":        ("pm2_5", "ug/m3"),
    "pm10_1_hr":    ("pm10_1hr", "ug/m3"),
    "pm2_5_1_hr":   ("pm2_5_1hr", "ug/m3"),
    "o3_1hr":       ("ozone_1hr", "ppm"),
    "o3_4hr":       ("ozone_4hr", "ppm"),
    "o3_8hr":       ("ozone_8hr", "ppm"),
    "no2":          ("no2", "ppm"),
    "co":           ("co", "ppm"),
}


def _parse_act_timestamp(dt_str: str) -> int | None:
    """Parse ACT datetime string to Unix epoch (UTC).

    The API returns datetimes like '2026-04-17T16:00:00.000' without
    timezone info. These are Australian Eastern Time (AEST/AEDT).

    Returns None if parsing fails.
    """
    if not dt_str:
        return None
    try:
        # Handle both with and without milliseconds
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
            try:
                dt = datetime.strptime(dt_str, fmt)
                dt = dt.replace(tzinfo=ACT_TZ)
                return int(dt.timestamp())
            except ValueError:
                continue
        logger.debug("Failed to parse ACT timestamp: %s", dt_str)
        return None
    except (TypeError, OverflowError):
        logger.debug("Failed to parse ACT timestamp: %s", dt_str)
        return None


class ACTAdapter(GovAQAdapter):
    """Adapter for ACT Government Socrata air quality API."""

    def __init__(self, source_id: str, config: dict):
        super().__init__(source_id, config)
        self._api_url = config["api_url"]
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "WeSense-Ingester-GovAQ/1.0 (https://wesense.earth)",
            "Accept": "application/json",
        })
        # Cache: station_id -> list of reading dicts, populated by fetch_stations()
        self._cached_readings: dict[str, list[dict]] = {}
        # Per-station last-seen timestamps for dedup
        self._last_timestamps: dict[str, int] = {}

    def fetch_stations(self) -> list[dict]:
        """Fetch and parse all records from the ACT Socrata API.

        Parses all records in one pass, grouping readings by station.
        Readings are cached internally so fetch_readings() needs no
        additional HTTP call.

        Returns list of station dicts.
        """
        self._cached_readings = {}

        params = {
            "$limit": "1000",
            "$order": "datetime DESC",
        }

        try:
            resp = self._session.get(self._api_url, params=params, timeout=30)
            resp.raise_for_status()
            records = resp.json()
        except Exception as e:
            logger.error("Failed to fetch ACT data: %s", e)
            return []

        if not isinstance(records, list):
            logger.error("ACT API: expected JSON array, got %s", type(records).__name__)
            return []

        # Group records by station, collecting readings and coordinates
        # station_id -> {name, lat, lon, readings}
        station_data: dict[str, dict] = {}
        station_readings: dict[str, list[dict]] = defaultdict(list)

        for record in records:
            try:
                name = record.get("name")
                if not name:
                    continue

                station_id = name.lower().replace(" ", "_")

                # Parse timestamp
                dt_str = record.get("datetime")
                timestamp = _parse_act_timestamp(dt_str)
                if timestamp is None:
                    continue

                # Skip readings at or before last-seen timestamp for this station
                last_ts = self._last_timestamps.get(station_id, 0)
                if timestamp <= last_ts:
                    continue

                # Extract coordinates (only need to do this once per station)
                if station_id not in station_data:
                    gps = record.get("gps")
                    if not isinstance(gps, dict):
                        logger.debug("ACT: skipping record with missing gps for '%s'", name)
                        continue
                    try:
                        lat = float(gps["latitude"])
                        lon = float(gps["longitude"])
                    except (KeyError, ValueError, TypeError):
                        logger.debug("ACT: bad coordinates for station '%s'", name)
                        continue
                    station_data[station_id] = {
                        "station_id": station_id,
                        "name": name,
                        "latitude": lat,
                        "longitude": lon,
                    }

                # Parse pollutant fields
                for api_field, (reading_type, unit) in FIELD_MAP.items():
                    value_str = record.get(api_field)
                    if value_str is None or value_str == "":
                        continue
                    try:
                        value = float(value_str)
                    except (ValueError, TypeError):
                        continue

                    station_readings[station_id].append({
                        "timestamp": timestamp,
                        "reading_type": reading_type,
                        "value": value,
                        "unit": unit,
                    })

            except Exception as e:
                logger.debug("ACT: skipping record: %s", e)
                continue

        # Build station list and cache readings
        stations = []
        for station_id, info in station_data.items():
            stations.append(info)
            readings = station_readings[station_id]
            self._cached_readings[station_id] = readings

            # Update last-seen timestamp
            if readings:
                max_ts = max(r["timestamp"] for r in readings)
                self._last_timestamps[station_id] = max_ts

        logger.info("ACT: %d stations parsed, %d total readings",
                     len(stations),
                     sum(len(r) for r in self._cached_readings.values()))
        return stations

    def fetch_readings(self, station: dict) -> list[dict]:
        """Return cached readings for a station.

        All readings are parsed during fetch_stations(). This method
        simply returns the cached result — no additional HTTP request.
        """
        station_id = station["station_id"]
        readings = self._cached_readings.get(station_id, [])
        if readings:
            logger.debug(
                "ACT station %s (%s): %d readings",
                station_id, station["name"], len(readings),
            )
        return readings

    def get_last_timestamps(self) -> dict[str, int]:
        """Return last-seen timestamps for cache persistence."""
        return dict(self._last_timestamps)

    def set_last_timestamps(self, timestamps: dict[str, int]) -> None:
        """Restore last-seen timestamps from cache."""
        self._last_timestamps = dict(timestamps)
