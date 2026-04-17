"""Abstract base class for Australian government air quality adapters."""

from abc import ABC, abstractmethod


class GovAQAdapter(ABC):
    """Base adapter for a single Australian government AQ data source."""

    def __init__(self, source_id: str, config: dict):
        self.source_id = source_id
        self.config = config

    @abstractmethod
    def fetch_stations(self) -> list[dict]:
        """Return list of station dicts with keys:
            station_id: str
            name: str
            latitude: float
            longitude: float
        """

    @abstractmethod
    def fetch_readings(self, station: dict) -> list[dict]:
        """Fetch latest readings for a single station.

        Return list of reading dicts with keys:
            timestamp: int      (Unix epoch, UTC)
            reading_type: str   (WeSense standard: "pm10", "pm2_5", etc.)
            value: float
            unit: str           (e.g. "ug/m3", "ppb")
        """

    def get_last_timestamps(self) -> dict[str, int]:
        """Return dict of station_id -> last Unix timestamp. Override if adapter tracks state."""
        return {}

    def set_last_timestamps(self, timestamps: dict[str, int]) -> None:
        """Restore state from persisted timestamps. Override if adapter tracks state."""
        pass
