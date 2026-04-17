"""
NSW Department of Planning and Environment air quality adapter.

Fetches hourly air quality readings from the NSW Air Quality API.
Covers ~120 monitoring stations across New South Wales.

API docs: https://data.airquality.nsw.gov.au
No authentication required. JSON format.

API notes:
- Sites: GET /api/Data/get_SiteDetails returns JSON array
- Observations: POST /api/Data/get_Observations with JSON body
  - Category "Averages" returns actual parameter values
  - Category "Site AQC" returns only AQI category summaries
  - StartDate/EndDate are required (yyyy-MM-dd format)
  - Hour is 1-24 where Hour N = the average for (N-1):00 to N:00 AEST
  - Parameters:[] returns all available parameters for the site
"""

import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests

from adapters.base import GovAQAdapter

logger = logging.getLogger(__name__)

# Australia/Sydney handles both AEST (UTC+10) and AEDT (UTC+11) automatically
AEST = ZoneInfo("Australia/Sydney")

# Map NSW ParameterCode (as returned by the live API) to WeSense standard
# (reading_type, unit). Units match what the API actually returns.
PARAMETER_MAP = {
    "PM10":     ("pm10", "ug/m3"),
    "PM2.5":    ("pm2_5", "ug/m3"),
    "OZONE":    ("ozone", "pphm"),
    "NO2":      ("no2", "pphm"),
    "NO":       ("no", "pphm"),
    "CO":       ("co", "ppm"),
    "SO2":      ("so2", "pphm"),
    "NEPH":     ("visibility_bsp", "10^-4 m^-1"),
    "HUMID":    ("humidity", "%"),
    "TEMP":     ("temperature", "C"),
    "WSP":      ("wind_speed", "m/s"),
    "WDR":      ("wind_direction", "degrees"),
}


def _parse_nsw_timestamp(date_str: str | None, hour: int | None) -> int | None:
    """Parse NSW date string and hour integer to Unix epoch (UTC).

    NSW API returns Date as 'yyyy-MM-dd' and Hour as 1-24 integer,
    where Hour N represents the average for the period (N-1):00 to N:00.
    We use the start of the averaging period as the timestamp.
    Timestamps are in Australian Eastern time (AEST/AEDT).
    """
    if date_str is None or hour is None:
        return None
    try:
        hour = int(hour)
        if hour < 1 or hour > 24:
            return None
        # Hour N means average for (N-1):00 to N:00
        # Use start of period as timestamp
        actual_hour = hour - 1
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        dt = dt.replace(hour=actual_hour, tzinfo=AEST)
        return int(dt.timestamp())
    except (ValueError, TypeError):
        logger.debug("Failed to parse NSW timestamp: date=%s hour=%s", date_str, hour)
        return None


class NSWAdapter(GovAQAdapter):
    """Adapter for NSW DPIE air quality data."""

    def __init__(self, source_id: str, config: dict):
        super().__init__(source_id, config)
        self._sites_url = config["sites_url"]
        self._observations_url = config["observations_url"]
        self._last_timestamps: dict[str, int] = {}
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "WeSense-Ingester-GovAQ/1.0 (https://wesense.earth)",
            "Accept": "application/json",
        })

    def fetch_stations(self) -> list[dict]:
        """Fetch monitoring stations from NSW API."""
        try:
            resp = self._session.get(self._sites_url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error("Failed to fetch NSW station list: %s", e)
            return []

        stations = []
        for site in data:
            try:
                site_id = site.get("Site_Id")
                name = site.get("SiteName", "")
                lat = site.get("Latitude")
                lon = site.get("Longitude")

                if site_id is None or lat is None or lon is None:
                    logger.debug("Skipping NSW site with missing fields: %s", site)
                    continue

                stations.append({
                    "station_id": str(site_id),
                    "name": name,
                    "latitude": float(lat),
                    "longitude": float(lon),
                })
            except (ValueError, TypeError) as e:
                logger.debug("Skipping NSW site with bad data: %s (%s)", site, e)
                continue

        logger.info("NSW: %d stations fetched", len(stations))
        return stations

    def fetch_readings(self, station: dict) -> list[dict]:
        """Fetch hourly readings for a single station.

        Requests the last 2 days of hourly averages from the NSW API.
        Only returns readings newer than the last seen timestamp for
        this station (dedup via _last_timestamps).
        """
        station_id = station["station_id"]
        last_ts = self._last_timestamps.get(station_id, 0)

        # Fetch last 2 days to avoid missing data around midnight
        aest_now = datetime.now(AEST)
        end_date = aest_now.strftime("%Y-%m-%d")
        start_date = (aest_now - timedelta(days=1)).strftime("%Y-%m-%d")

        body = {
            "Parameters": [],
            "Sites": [int(station_id)],
            "Categories": ["Averages"],
            "SubCategories": ["Hourly"],
            "Frequency": ["Hourly average"],
            "StartDate": start_date,
            "EndDate": end_date,
        }

        try:
            resp = self._session.post(
                self._observations_url,
                json=body,
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.warning("Failed to fetch NSW data for station %s: %s", station_id, e)
            return []

        readings = []

        for obs in data:
            try:
                # Extract parameter code from nested Parameter object
                param = obs.get("Parameter")
                if isinstance(param, dict):
                    param_code = param.get("ParameterCode")
                else:
                    param_code = obs.get("ParameterCode")

                if param_code is None:
                    continue

                mapping = PARAMETER_MAP.get(param_code)
                if mapping is None:
                    continue

                reading_type, unit = mapping

                value = obs.get("Value")
                if value is None:
                    continue

                try:
                    value = float(value)
                except (ValueError, TypeError):
                    continue

                # Parse timestamp from Date + Hour
                date_str = obs.get("Date")
                hour = obs.get("Hour")
                timestamp = _parse_nsw_timestamp(date_str, hour)
                if timestamp is None:
                    continue

                if timestamp <= last_ts:
                    continue

                readings.append({
                    "timestamp": timestamp,
                    "reading_type": reading_type,
                    "value": value,
                    "unit": unit,
                })
            except Exception as e:
                logger.debug("Skipping NSW observation: %s (%s)", obs, e)
                continue

        # Update last-seen timestamp for this station
        if readings:
            max_ts = max(r["timestamp"] for r in readings)
            self._last_timestamps[station_id] = max_ts
            logger.debug(
                "NSW station %s (%s): %d new readings",
                station_id, station["name"], len(readings),
            )

        return readings

    def get_last_timestamps(self) -> dict[str, int]:
        """Return last-seen timestamps for cache persistence."""
        return dict(self._last_timestamps)

    def set_last_timestamps(self, timestamps: dict[str, int]) -> None:
        """Restore last-seen timestamps from cache."""
        self._last_timestamps = dict(timestamps)
