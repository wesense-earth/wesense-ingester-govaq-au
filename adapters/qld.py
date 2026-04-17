"""
Queensland Department of Environment and Science air quality adapter.

Fetches hourly air quality readings from the QLD DES XML feed.
Single endpoint returns all regions, stations, and measurements in one response.

Feed URL: https://apps.des.qld.gov.au/air-quality/xml/feed.php?category=1&region=ALL
No authentication required. XML format.

Feed notes:
- One timestamp (measurementdate + measurementhour) applies to the entire feed
- station elements carry @name, @latitude, @longitude
- measurement elements carry @name (pollutant), @index, @rating, text = numeric value
- index="-2222" is the sentinel for unavailable data
- Times are AEST (UTC+10), Queensland does not observe daylight saving
- Gas concentrations (O3, NO2, SO2, CO) are in ppm
- Particulates (PM10, PM2.5, TSP) are in ug/m3
- Visibility (bsp) is in Mm-1 (inverse megametres)
"""

import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

from adapters.base import GovAQAdapter

logger = logging.getLogger(__name__)

# Queensland does not observe daylight saving — always UTC+10
AEST = ZoneInfo("Australia/Queensland")

# Map QLD measurement @name (as returned by the live XML feed) to WeSense standard
# (reading_type, unit). Units match what the API actually returns.
MEASUREMENT_MAP = {
    "Particle PM10":    ("pm10", "ug/m3"),
    "Particle PM2.5":   ("pm2_5", "ug/m3"),
    "Particles TSP":    ("tsp", "ug/m3"),
    "Ozone":            ("ozone", "ppm"),
    "Nitrogen dioxide": ("no2", "ppm"),
    "Sulfur dioxide":   ("so2", "ppm"),
    "Carbon monoxide":  ("co", "ppm"),
    "Visibility":       ("visibility_bsp", "Mm-1"),
}

# Sentinel value in the @index attribute meaning data is unavailable
INVALID_INDEX = "-2222"


def _parse_qld_timestamp(date_str: str, hour_str: str) -> int | None:
    """Parse QLD feed date and hour strings to Unix epoch (UTC).

    The feed provides measurementdate as 'yyyy-MM-dd' and measurementhour
    as an integer string (0-23) representing the hour of the measurement.
    All times are AEST (UTC+10) — Queensland has no daylight saving.

    Returns None if parsing fails.
    """
    try:
        hour = int(hour_str)
        if hour < 0 or hour > 23:
            return None
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        dt = dt.replace(hour=hour, tzinfo=AEST)
        return int(dt.timestamp())
    except (ValueError, TypeError):
        logger.debug("Failed to parse QLD timestamp: date=%s hour=%s", date_str, hour_str)
        return None


class QLDAdapter(GovAQAdapter):
    """Adapter for Queensland DES air quality XML feed."""

    def __init__(self, source_id: str, config: dict):
        super().__init__(source_id, config)
        self._feed_url = config["feed_url"]
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "WeSense-Ingester-GovAQ/1.0 (https://wesense.earth)",
            "Accept": "application/xml",
        })
        # Single feed timestamp — if unchanged, skip the entire feed
        self._last_feed_timestamp: int | None = None
        # Cache: station_id -> list of reading dicts, populated by fetch_stations()
        self._cached_readings: dict[str, list[dict]] = {}
        # For get_last_timestamps / set_last_timestamps compatibility
        self._last_timestamps: dict[str, int] = {}

    def fetch_stations(self) -> list[dict]:
        """Fetch and parse the entire QLD XML feed.

        Parses all stations and their readings in one pass. Readings are
        cached internally so fetch_readings() needs no additional HTTP call.

        Returns list of station dicts.
        """
        self._cached_readings = {}

        try:
            resp = self._session.get(self._feed_url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.error("Failed to fetch QLD feed: %s", e)
            return []

        try:
            root = ET.fromstring(resp.text)
        except ET.ParseError as e:
            logger.error("Failed to parse QLD XML: %s", e)
            return []

        # Extract the single feed timestamp from the category element
        category = root.find("category")
        if category is None:
            logger.error("QLD feed: no <category> element found")
            return []

        date_str = category.get("measurementdate")
        hour_str = category.get("measurementhour")
        if date_str is None or hour_str is None:
            logger.error("QLD feed: missing timestamp attributes on <category>")
            return []

        feed_timestamp = _parse_qld_timestamp(date_str, hour_str)
        if feed_timestamp is None:
            logger.error("QLD feed: failed to parse timestamp: date=%s hour=%s", date_str, hour_str)
            return []

        # Skip entire feed if timestamp hasn't advanced
        if self._last_feed_timestamp is not None and feed_timestamp <= self._last_feed_timestamp:
            logger.info("QLD: feed timestamp unchanged (%s %s:00), skipping", date_str, hour_str)
            return []

        stations = []

        for region in category.findall("region"):
            for station_el in region.findall("station"):
                try:
                    name = station_el.get("name")
                    lat_str = station_el.get("latitude")
                    lon_str = station_el.get("longitude")

                    if not name or lat_str is None or lon_str is None:
                        logger.debug("Skipping QLD station with missing fields: %s", ET.tostring(station_el, encoding="unicode")[:200])
                        continue

                    lat = float(lat_str)
                    lon = float(lon_str)
                    station_id = name.lower().replace(" ", "_")

                    # Parse measurements for this station
                    readings = []
                    for meas in station_el.findall("measurement"):
                        try:
                            meas_name = meas.get("name")
                            index = meas.get("index")

                            # Skip invalid/unavailable readings
                            if index == INVALID_INDEX:
                                continue

                            mapping = MEASUREMENT_MAP.get(meas_name)
                            if mapping is None:
                                logger.debug("QLD: unmapped measurement '%s' at station '%s'", meas_name, name)
                                continue

                            reading_type, unit = mapping

                            value_text = meas.text
                            if value_text is None:
                                continue

                            value = float(value_text.strip())

                            readings.append({
                                "timestamp": feed_timestamp,
                                "reading_type": reading_type,
                                "value": value,
                                "unit": unit,
                            })
                        except (ValueError, TypeError) as e:
                            logger.debug("Skipping QLD measurement at station '%s': %s", name, e)
                            continue

                    station = {
                        "station_id": station_id,
                        "name": name,
                        "latitude": lat,
                        "longitude": lon,
                    }
                    stations.append(station)
                    self._cached_readings[station_id] = readings

                except (ValueError, TypeError) as e:
                    logger.debug("Skipping QLD station with bad data: %s", e)
                    continue

        # Update the feed timestamp after successful parse
        self._last_feed_timestamp = feed_timestamp
        # Set per-station timestamps for compatibility
        for station_id in self._cached_readings:
            self._last_timestamps[station_id] = feed_timestamp

        logger.info("QLD: %d stations parsed, feed timestamp %s %s:00 AEST", len(stations), date_str, hour_str)
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
                "QLD station %s (%s): %d readings",
                station_id, station["name"], len(readings),
            )
        return readings

    def get_last_timestamps(self) -> dict[str, int]:
        """Return last-seen timestamps for cache persistence."""
        return dict(self._last_timestamps)

    def set_last_timestamps(self, timestamps: dict[str, int]) -> None:
        """Restore last-seen timestamps from cache.

        Also restores the feed-level timestamp as the max of all station
        timestamps, so the skip-if-unchanged logic works across restarts.
        """
        self._last_timestamps = dict(timestamps)
        if timestamps:
            self._last_feed_timestamp = max(timestamps.values())
