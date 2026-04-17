"""
Victoria EPA air quality adapter.

Fetches air quality readings from the EPA Victoria API.
Requires a free API key from portal.api.epa.vic.gov.au.

API endpoints (based on OpenAQ adapter analysis — may need adjustment):
- Sites: GET https://gateway.api.epa.vic.gov.au/environmentMonitoring/v1/sites?environmentalSegment=air
- Readings: GET .../sites/{siteID}/parameters
- Auth: X-API-Key header

NOTE: The response schema below is inferred from the OpenAQ Victoria adapter
source code analysis. Field names and nesting may differ from the actual API.
If you have a VIC_EPA_API_KEY, enable this adapter and check the logs for
any parsing errors — adjustments to field names may be needed.
"""

import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

from adapters.base import GovAQAdapter

logger = logging.getLogger(__name__)

# Melbourne timezone handles both AEST (UTC+10) and AEDT (UTC+11)
MELB_TZ = ZoneInfo("Australia/Melbourne")

# Map VIC EPA parameter names to WeSense standard (reading_type, unit).
# Parameter names are based on OpenAQ adapter analysis — may need adjustment.
PARAMETER_MAP = {
    "PM2.5":    ("pm2_5", "ug/m3"),
    "PM10":     ("pm10", "ug/m3"),
    "O3":       ("ozone", "ppm"),
    "NO2":      ("no2", "ppm"),
    "SO2":      ("so2", "ppm"),
    "CO":       ("co", "ppm"),
    # Alternative casing seen in some EPA APIs
    "pm2.5":    ("pm2_5", "ug/m3"),
    "pm10":     ("pm10", "ug/m3"),
    "o3":       ("ozone", "ppm"),
    "no2":      ("no2", "ppm"),
    "so2":      ("so2", "ppm"),
    "co":       ("co", "ppm"),
}


def _parse_vic_timestamp(dt_str: str | None) -> int | None:
    """Parse VIC EPA timestamp string to Unix epoch (UTC).

    Expected format is ISO 8601, e.g. '2026-04-13T14:00:00+10:00' or
    '2026-04-13T14:00:00' (assumed Melbourne local time if no offset).

    Returns None if parsing fails.
    """
    if not dt_str:
        return None
    try:
        # Try ISO 8601 with timezone offset first
        try:
            dt = datetime.fromisoformat(dt_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=MELB_TZ)
            return int(dt.timestamp())
        except ValueError:
            pass

        # Fallback: common datetime formats
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(dt_str, fmt)
                dt = dt.replace(tzinfo=MELB_TZ)
                return int(dt.timestamp())
            except ValueError:
                continue

        logger.debug("Failed to parse VIC timestamp: %s", dt_str)
        return None
    except (TypeError, OverflowError):
        logger.debug("Failed to parse VIC timestamp: %s", dt_str)
        return None


class VICAdapter(GovAQAdapter):
    """Adapter for EPA Victoria air quality API.

    Requires VIC_EPA_API_KEY environment variable. Disabled by default
    in sources.json.
    """

    def __init__(self, source_id: str, config: dict):
        super().__init__(source_id, config)
        self._sites_url = config["sites_url"]
        self._api_key = os.environ.get("VIC_EPA_API_KEY", "")
        self._last_timestamps: dict[str, int] = {}
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "WeSense-Ingester-GovAQ/1.0 (https://wesense.earth)",
            "Accept": "application/json",
        })

        if not self._api_key:
            logger.warning(
                "VIC: VIC_EPA_API_KEY not set — adapter will not be able to fetch data. "
                "Register for a free key at portal.api.epa.vic.gov.au"
            )
        else:
            self._session.headers["X-API-Key"] = self._api_key

    def fetch_stations(self) -> list[dict]:
        """Fetch monitoring sites from the VIC EPA API.

        Expected response structure (based on OpenAQ analysis, may need adjustment):
        JSON array or object containing site records, each with fields like:
            siteID, siteName, latitude, longitude
        """
        if not self._api_key:
            logger.warning("VIC: skipping fetch — no API key configured")
            return []

        try:
            resp = self._session.get(self._sites_url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error("Failed to fetch VIC station list: %s", e)
            return []

        # The API may return a JSON array directly, or wrap it in an object.
        # Try to extract a list of sites from common wrapper patterns.
        sites = data if isinstance(data, list) else data.get("records", data.get("sites", []))
        if not isinstance(sites, list):
            logger.error(
                "VIC: unexpected response structure — expected list of sites, got %s. "
                "Response keys: %s. First 500 chars: %.500s",
                type(sites).__name__,
                list(data.keys()) if isinstance(data, dict) else "N/A",
                str(data)[:500],
            )
            return []

        stations = []
        for site in sites:
            try:
                # Try common field name patterns for site ID
                site_id = (
                    site.get("siteID")
                    or site.get("siteId")
                    or site.get("site_id")
                    or site.get("id")
                )
                name = (
                    site.get("siteName")
                    or site.get("site_name")
                    or site.get("name")
                    or ""
                )
                lat = site.get("latitude") or site.get("lat")
                lon = site.get("longitude") or site.get("lon") or site.get("lng")

                if site_id is None or lat is None or lon is None:
                    logger.debug("Skipping VIC site with missing fields: %s", site)
                    continue

                stations.append({
                    "station_id": str(site_id),
                    "name": str(name),
                    "latitude": float(lat),
                    "longitude": float(lon),
                })
            except (ValueError, TypeError) as e:
                logger.debug("Skipping VIC site with bad data: %s (%s)", site, e)
                continue

        logger.info("VIC: %d stations fetched", len(stations))
        return stations

    def fetch_readings(self, station: dict) -> list[dict]:
        """Fetch readings for a single VIC station.

        Calls the per-site parameters endpoint to get latest readings.
        Expected response structure (based on OpenAQ analysis, may need adjustment):
        JSON containing parameter records with time series data.
        """
        if not self._api_key:
            return []

        station_id = station["station_id"]
        last_ts = self._last_timestamps.get(station_id, 0)

        # Build the per-site readings URL
        # Expected pattern: .../sites/{siteID}/parameters
        readings_url = (
            self._sites_url.split("?")[0].rstrip("/")
            + f"/{station_id}/parameters"
        )

        try:
            resp = self._session.get(readings_url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.warning("Failed to fetch VIC data for station %s: %s", station_id, e)
            return []

        # Extract parameter records from response
        params = data if isinstance(data, list) else data.get("records", data.get("parameters", []))
        if not isinstance(params, list):
            logger.debug(
                "VIC station %s: unexpected response structure — got %s. First 500 chars: %.500s",
                station_id, type(params).__name__, str(data)[:500],
            )
            return []

        readings = []
        seen_timestamps: set[tuple[str, int]] = set()

        for param in params:
            try:
                param_name = (
                    param.get("name")
                    or param.get("parameterName")
                    or param.get("parameter")
                    or ""
                )

                mapping = PARAMETER_MAP.get(param_name)
                if mapping is None:
                    logger.debug("VIC: unmapped parameter '%s' at station %s", param_name, station_id)
                    continue

                reading_type, unit = mapping

                # The API may return time series as a nested list or flat values.
                # Try nested time series first, then flat single-value.
                time_series = param.get("timeSeries", param.get("readings", []))
                if isinstance(time_series, list):
                    for point in time_series:
                        ts_str = (
                            point.get("dateTime")
                            or point.get("datetime")
                            or point.get("timestamp")
                            or point.get("date")
                        )
                        value = point.get("value") or point.get("averageValue")
                        if value is None or ts_str is None:
                            continue
                        try:
                            value = float(value)
                        except (ValueError, TypeError):
                            continue

                        timestamp = _parse_vic_timestamp(ts_str)
                        if timestamp is None:
                            continue
                        if timestamp <= last_ts:
                            continue

                        # Dedup within this fetch
                        dedup_key = (reading_type, timestamp)
                        if dedup_key in seen_timestamps:
                            continue
                        seen_timestamps.add(dedup_key)

                        readings.append({
                            "timestamp": timestamp,
                            "reading_type": reading_type,
                            "value": value,
                            "unit": unit,
                        })
                else:
                    # Single latest value
                    value = param.get("value") or param.get("averageValue")
                    ts_str = (
                        param.get("dateTime")
                        or param.get("datetime")
                        or param.get("timestamp")
                    )
                    if value is not None and ts_str is not None:
                        try:
                            value = float(value)
                        except (ValueError, TypeError):
                            continue

                        timestamp = _parse_vic_timestamp(ts_str)
                        if timestamp is not None and timestamp > last_ts:
                            dedup_key = (reading_type, timestamp)
                            if dedup_key not in seen_timestamps:
                                seen_timestamps.add(dedup_key)
                                readings.append({
                                    "timestamp": timestamp,
                                    "reading_type": reading_type,
                                    "value": value,
                                    "unit": unit,
                                })
            except Exception as e:
                logger.debug("Skipping VIC parameter at station %s: %s", station_id, e)
                continue

        # Update last-seen timestamp
        if readings:
            max_ts = max(r["timestamp"] for r in readings)
            self._last_timestamps[station_id] = max_ts
            logger.debug(
                "VIC station %s (%s): %d new readings",
                station_id, station["name"], len(readings),
            )

        return readings

    def get_last_timestamps(self) -> dict[str, int]:
        """Return last-seen timestamps for cache persistence."""
        return dict(self._last_timestamps)

    def set_last_timestamps(self, timestamps: dict[str, int]) -> None:
        """Restore last-seen timestamps from cache."""
        self._last_timestamps = dict(timestamps)
