"""
Tasmania EPA air quality adapter (STUB).

Endpoint: https://epa.tas.gov.au/Air/Live/EPA_tas_latest_particle_data.txt
Format: Plain text
Timezone: Australia/Hobart (UTC+10/+11, observes DST)

This adapter is a stub. The text endpoint was marked inactive by OpenAQ.
When enabled, it attempts to fetch the endpoint and logs the response
structure for future development, but returns no stations or readings.
"""

import logging

import requests

from adapters.base import GovAQAdapter

logger = logging.getLogger(__name__)


class TASAdapter(GovAQAdapter):
    """Stub adapter for Tasmania EPA particle data text feed.

    Not fully implemented — the endpoint needs verification before
    a real parser can be written. Enable this adapter to probe the
    endpoint and check logs for the response structure.
    """

    def __init__(self, source_id: str, config: dict):
        super().__init__(source_id, config)
        self._data_url = config["data_url"]
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "WeSense-Ingester-GovAQ/1.0 (https://wesense.earth)",
            "Accept": "text/plain, text/csv, */*",
        })

    def fetch_stations(self) -> list[dict]:
        """Probe the TAS EPA text endpoint and log the response.

        Returns empty list — this adapter is not yet implemented.
        """
        logger.warning(
            "TAS adapter is a stub — endpoint needs verification before full implementation. "
            "Probing %s to check availability...", self._data_url,
        )

        try:
            resp = self._session.get(self._data_url, timeout=30)
            resp.raise_for_status()
            content_type = resp.headers.get("Content-Type", "unknown")
            body = resp.text

            logger.info(
                "TAS: endpoint responded (status %d, content-type: %s, length: %d bytes)",
                resp.status_code, content_type, len(body),
            )
            # Log first few lines for development reference
            preview_lines = body.splitlines()[:10]
            for i, line in enumerate(preview_lines):
                logger.info("TAS response line %d: %.200s", i + 1, line)

        except requests.exceptions.HTTPError as e:
            logger.warning("TAS: endpoint returned HTTP error: %s", e)
        except requests.exceptions.ConnectionError as e:
            logger.warning("TAS: endpoint unreachable: %s", e)
        except requests.exceptions.Timeout:
            logger.warning("TAS: endpoint timed out")
        except Exception as e:
            logger.warning("TAS: failed to probe endpoint: %s", e)

        return []

    def fetch_readings(self, station: dict) -> list[dict]:
        """Not implemented — returns empty list."""
        return []
