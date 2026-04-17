"""Tests for Queensland DES air quality adapter."""

from datetime import datetime
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

from adapters.qld import (
    AEST,
    INVALID_INDEX,
    MEASUREMENT_MAP,
    QLDAdapter,
    _parse_qld_timestamp,
)

# --- Sample XML for testing ---

SAMPLE_XML = """\
<?xml version="1.0"?>
<airdata provider="Department of Environment and Science" state="Queensland" country="Australia">
  <category name="Air Quality" measurementhour="14" measurementdate="2024-06-15">
    <region name="South East Queensland">
      <station name="Rocklea" longitude="152.9934" latitude="-27.5358">
        <measurement name="Ozone" rating_name="Good" rating="2" index="53.8">0.035</measurement>
        <measurement name="Particle PM10" rating_name="Good" rating="2" index="60">30</measurement>
        <measurement name="Particle PM2.5" rating_name="Good" rating="2" index="49.2">12.3</measurement>
      </station>
      <station name="Brisbane CBD" longitude="153.0281" latitude="-27.4774">
        <measurement name="Visibility" rating_name="Very Good" rating="1" index="14">33</measurement>
        <measurement name="Particle PM10" rating_name="Good" rating="2" index="45.6">22.8</measurement>
      </station>
    </region>
    <region name="Central Queensland">
      <station name="Bluff" longitude="149.074" latitude="-23.582">
        <measurement name="Particle PM10" rating_name="Unavailable" rating="0" index="-2222">-1111</measurement>
      </station>
    </region>
  </category>
</airdata>
"""

SAMPLE_XML_NO_TIMESTAMP = """\
<?xml version="1.0"?>
<airdata>
  <category name="Air Quality">
    <region name="Test">
      <station name="Test" longitude="150" latitude="-25">
        <measurement name="Particle PM10" rating="2" index="30">15</measurement>
      </station>
    </region>
  </category>
</airdata>
"""


def _make_adapter(**overrides):
    """Create a QLDAdapter with test defaults."""
    config = {"feed_url": "https://example.com/feed.xml"}
    config.update(overrides)
    return QLDAdapter("qld", config)


def _mock_response(text, status_code=200):
    """Create a mock requests.Response."""
    resp = MagicMock()
    resp.text = text
    resp.status_code = status_code
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = Exception(f"HTTP {status_code}")
    return resp


class TestParseQLDTimestamp:
    """Tests for _parse_qld_timestamp.

    QLD feed returns measurementdate as 'yyyy-MM-dd' and measurementhour
    as 0-23 integer string. All times are AEST (UTC+10, no daylight saving).
    """

    def test_midnight(self):
        """Hour 0 = midnight AEST."""
        ts = _parse_qld_timestamp("2024-06-15", "0")
        assert ts is not None
        dt = datetime.fromtimestamp(ts, tz=ZoneInfo("UTC"))
        # 00:00 AEST = 14:00 UTC previous day
        assert dt.day == 14
        assert dt.hour == 14

    def test_noon(self):
        """Hour 12 = noon AEST."""
        ts = _parse_qld_timestamp("2024-06-15", "12")
        assert ts is not None
        dt = datetime.fromtimestamp(ts, tz=ZoneInfo("UTC"))
        # 12:00 AEST = 02:00 UTC same day
        assert dt.day == 15
        assert dt.hour == 2

    def test_hour_23(self):
        """Hour 23 = 11pm AEST."""
        ts = _parse_qld_timestamp("2024-06-15", "23")
        assert ts is not None
        dt = datetime.fromtimestamp(ts, tz=ZoneInfo("UTC"))
        # 23:00 AEST = 13:00 UTC same day
        assert dt.day == 15
        assert dt.hour == 13

    def test_hour_18(self):
        """Hour 18 = 6pm AEST."""
        ts = _parse_qld_timestamp("2024-06-15", "18")
        assert ts is not None
        dt = datetime.fromtimestamp(ts, tz=ZoneInfo("UTC"))
        # 18:00 AEST = 08:00 UTC same day
        assert dt.hour == 8

    def test_no_daylight_saving(self):
        """Queensland does not observe DST — summer should still be UTC+10."""
        winter = _parse_qld_timestamp("2024-06-15", "12")
        summer = _parse_qld_timestamp("2024-01-15", "12")
        assert winter is not None
        assert summer is not None
        winter_dt = datetime.fromtimestamp(winter, tz=ZoneInfo("UTC"))
        summer_dt = datetime.fromtimestamp(summer, tz=ZoneInfo("UTC"))
        # Both should be 02:00 UTC (12:00 - 10h offset)
        assert winter_dt.hour == 2
        assert summer_dt.hour == 2

    def test_bad_date_format(self):
        ts = _parse_qld_timestamp("15/06/2024", "12")
        assert ts is None

    def test_invalid_date_string(self):
        ts = _parse_qld_timestamp("not-a-date", "5")
        assert ts is None

    def test_invalid_hour_string(self):
        ts = _parse_qld_timestamp("2024-06-15", "abc")
        assert ts is None

    def test_hour_negative(self):
        ts = _parse_qld_timestamp("2024-06-15", "-1")
        assert ts is None

    def test_hour_24_invalid(self):
        """Hour 24 is out of range (QLD feed uses 0-23)."""
        ts = _parse_qld_timestamp("2024-06-15", "24")
        assert ts is None


class TestMeasurementMap:
    """Tests for MEASUREMENT_MAP — verified against live XML feed."""

    def test_all_live_measurements_mapped(self):
        """All measurement names observed in the live feed should be mapped."""
        live_names = [
            "Particle PM10",
            "Particle PM2.5",
            "Particles TSP",
            "Ozone",
            "Nitrogen dioxide",
            "Sulfur dioxide",
            "Carbon monoxide",
            "Visibility",
        ]
        for name in live_names:
            assert name in MEASUREMENT_MAP, f"'{name}' not in MEASUREMENT_MAP"

    def test_pm25_mapping(self):
        assert MEASUREMENT_MAP["Particle PM2.5"] == ("pm2_5", "ug/m3")

    def test_pm10_mapping(self):
        assert MEASUREMENT_MAP["Particle PM10"] == ("pm10", "ug/m3")

    def test_tsp_mapping(self):
        assert MEASUREMENT_MAP["Particles TSP"] == ("tsp", "ug/m3")

    def test_ozone_mapping(self):
        assert MEASUREMENT_MAP["Ozone"] == ("ozone", "ppm")

    def test_no2_mapping(self):
        assert MEASUREMENT_MAP["Nitrogen dioxide"] == ("no2", "ppm")

    def test_so2_mapping(self):
        assert MEASUREMENT_MAP["Sulfur dioxide"] == ("so2", "ppm")

    def test_co_mapping(self):
        assert MEASUREMENT_MAP["Carbon monoxide"] == ("co", "ppm")

    def test_visibility_mapping(self):
        assert MEASUREMENT_MAP["Visibility"] == ("visibility_bsp", "Mm-1")

    def test_all_values_are_tuples(self):
        for key, value in MEASUREMENT_MAP.items():
            assert isinstance(value, tuple), f"{key} value is not a tuple"
            assert len(value) == 2
            assert isinstance(value[0], str)
            assert isinstance(value[1], str)


class TestQLDAdapterParsing:
    """Tests for QLDAdapter XML parsing via fetch_stations()."""

    @patch("adapters.qld.requests.Session.get")
    def test_parses_stations(self, mock_get):
        mock_get.return_value = _mock_response(SAMPLE_XML)
        adapter = _make_adapter()
        stations = adapter.fetch_stations()
        assert len(stations) == 3
        names = {s["name"] for s in stations}
        assert names == {"Rocklea", "Brisbane CBD", "Bluff"}

    @patch("adapters.qld.requests.Session.get")
    def test_station_coordinates(self, mock_get):
        mock_get.return_value = _mock_response(SAMPLE_XML)
        adapter = _make_adapter()
        stations = adapter.fetch_stations()
        rocklea = next(s for s in stations if s["name"] == "Rocklea")
        assert rocklea["latitude"] == -27.5358
        assert rocklea["longitude"] == 152.9934

    @patch("adapters.qld.requests.Session.get")
    def test_station_id_format(self, mock_get):
        mock_get.return_value = _mock_response(SAMPLE_XML)
        adapter = _make_adapter()
        stations = adapter.fetch_stations()
        ids = {s["station_id"] for s in stations}
        assert "rocklea" in ids
        assert "brisbane_cbd" in ids
        assert "bluff" in ids

    @patch("adapters.qld.requests.Session.get")
    def test_readings_cached(self, mock_get):
        mock_get.return_value = _mock_response(SAMPLE_XML)
        adapter = _make_adapter()
        stations = adapter.fetch_stations()
        rocklea = next(s for s in stations if s["name"] == "Rocklea")
        readings = adapter.fetch_readings(rocklea)
        assert len(readings) == 3
        types = {r["reading_type"] for r in readings}
        assert types == {"ozone", "pm10", "pm2_5"}

    @patch("adapters.qld.requests.Session.get")
    def test_reading_values(self, mock_get):
        mock_get.return_value = _mock_response(SAMPLE_XML)
        adapter = _make_adapter()
        stations = adapter.fetch_stations()
        rocklea = next(s for s in stations if s["name"] == "Rocklea")
        readings = adapter.fetch_readings(rocklea)
        pm10 = next(r for r in readings if r["reading_type"] == "pm10")
        assert pm10["value"] == 30.0
        assert pm10["unit"] == "ug/m3"

    @patch("adapters.qld.requests.Session.get")
    def test_reading_timestamp(self, mock_get):
        mock_get.return_value = _mock_response(SAMPLE_XML)
        adapter = _make_adapter()
        stations = adapter.fetch_stations()
        rocklea = next(s for s in stations if s["name"] == "Rocklea")
        readings = adapter.fetch_readings(rocklea)
        # All readings should have the same feed timestamp
        # 2024-06-15 14:00 AEST = 2024-06-15 04:00 UTC
        expected = _parse_qld_timestamp("2024-06-15", "14")
        for r in readings:
            assert r["timestamp"] == expected

    @patch("adapters.qld.requests.Session.get")
    def test_invalid_index_skipped(self, mock_get):
        """Measurements with index='-2222' should be excluded."""
        mock_get.return_value = _mock_response(SAMPLE_XML)
        adapter = _make_adapter()
        stations = adapter.fetch_stations()
        bluff = next(s for s in stations if s["name"] == "Bluff")
        readings = adapter.fetch_readings(bluff)
        # Bluff has only one measurement with index="-2222" — all filtered out
        assert len(readings) == 0

    @patch("adapters.qld.requests.Session.get")
    def test_skip_unchanged_timestamp(self, mock_get):
        """Second call with same timestamp should return empty stations."""
        mock_get.return_value = _mock_response(SAMPLE_XML)
        adapter = _make_adapter()

        # First call — parses normally
        stations1 = adapter.fetch_stations()
        assert len(stations1) == 3

        # Second call with same XML — skips
        mock_get.return_value = _mock_response(SAMPLE_XML)
        stations2 = adapter.fetch_stations()
        assert len(stations2) == 0

    @patch("adapters.qld.requests.Session.get")
    def test_no_readings_for_unknown_station(self, mock_get):
        mock_get.return_value = _mock_response(SAMPLE_XML)
        adapter = _make_adapter()
        adapter.fetch_stations()
        readings = adapter.fetch_readings({"station_id": "nonexistent", "name": "Nowhere"})
        assert readings == []

    @patch("adapters.qld.requests.Session.get")
    def test_http_error_returns_empty(self, mock_get):
        mock_get.return_value = _mock_response("", status_code=500)
        adapter = _make_adapter()
        stations = adapter.fetch_stations()
        assert stations == []

    @patch("adapters.qld.requests.Session.get")
    def test_invalid_xml_returns_empty(self, mock_get):
        mock_get.return_value = _mock_response("this is not xml")
        adapter = _make_adapter()
        stations = adapter.fetch_stations()
        assert stations == []

    @patch("adapters.qld.requests.Session.get")
    def test_missing_timestamp_returns_empty(self, mock_get):
        mock_get.return_value = _mock_response(SAMPLE_XML_NO_TIMESTAMP)
        adapter = _make_adapter()
        stations = adapter.fetch_stations()
        assert stations == []


class TestQLDAdapterTimestampPersistence:
    """Tests for get_last_timestamps / set_last_timestamps."""

    @patch("adapters.qld.requests.Session.get")
    def test_get_last_timestamps_after_fetch(self, mock_get):
        mock_get.return_value = _mock_response(SAMPLE_XML)
        adapter = _make_adapter()
        adapter.fetch_stations()
        ts = adapter.get_last_timestamps()
        assert len(ts) == 3
        assert "rocklea" in ts
        # All should have the same timestamp
        values = set(ts.values())
        assert len(values) == 1

    def test_set_last_timestamps_restores_feed_ts(self):
        adapter = _make_adapter()
        adapter.set_last_timestamps({"rocklea": 1000, "bluff": 1000})
        assert adapter._last_feed_timestamp == 1000

    @patch("adapters.qld.requests.Session.get")
    def test_restored_timestamp_skips_old_feed(self, mock_get):
        """After restoring timestamps, a feed with older/equal time is skipped."""
        adapter = _make_adapter()
        # Set a timestamp far in the future
        adapter.set_last_timestamps({"rocklea": 9999999999})

        mock_get.return_value = _mock_response(SAMPLE_XML)
        stations = adapter.fetch_stations()
        assert len(stations) == 0
