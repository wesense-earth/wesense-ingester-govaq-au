"""
South Australia EPA air quality adapter (STUB).

Endpoint: https://www.epa.sa.gov.au/air_quality/rss
Format: RSS/XML feed
Licence: CC-BY-4.0
Timezone: Australia/Adelaide (UTC+9:30, observes DST)

This adapter is a stub. The RSS endpoint was marked inactive by OpenAQ.
When enabled, it attempts to fetch the endpoint and logs the response
structure for future development, but returns no stations or readings.
"""

import logging

import requests

from adapters.base import GovAQAdapter

logger = logging.getLogger(__name__)


class SAAdapter(GovAQAdapter):
    """Stub adapter for South Australia EPA air quality RSS feed.

    Not fully implemented — the endpoint needs verification before
    a real parser can be written. Enable this adapter to probe the
    endpoint and check logs for the response structure.
    """

    def __init__(self, source_id: str, config: dict):
        super().__init__(source_id, config)
        self._rss_url = config["rss_url"]
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "WeSense-Ingester-GovAQ/1.0 (https://wesense.earth)",
            "Accept": "application/rss+xml, application/xml, text/xml",
        })

    def fetch_stations(self) -> list[dict]:
        """Probe the SA EPA RSS endpoint and log the response.

        Returns empty list — this adapter is not yet implemented.
        """
        logger.warning(
            "SA adapter is a stub — endpoint needs verification before full implementation. "
            "Probing %s to check availability...", self._rss_url,
        )

        try:
            resp = self._session.get(self._rss_url, timeout=30)
            resp.raise_for_status()
            content_type = resp.headers.get("Content-Type", "unknown")
            body = resp.text

            logger.info(
                "SA: endpoint responded (status %d, content-type: %s, length: %d bytes)",
                resp.status_code, content_type, len(body),
            )
            # Log first few lines for development reference
            preview_lines = body.splitlines()[:10]
            for i, line in enumerate(preview_lines):
                logger.info("SA response line %d: %.200s", i + 1, line)

        except requests.exceptions.HTTPError as e:
            logger.warning("SA: endpoint returned HTTP error: %s", e)
        except requests.exceptions.ConnectionError as e:
            logger.warning("SA: endpoint unreachable: %s", e)
        except requests.exceptions.Timeout:
            logger.warning("SA: endpoint timed out")
        except Exception as e:
            logger.warning("SA: failed to probe endpoint: %s", e)

        return []

    def fetch_readings(self, station: dict) -> list[dict]:
        """Not implemented — returns empty list."""
        return []
