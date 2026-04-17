"""Tests for ACT Government air quality adapter."""

import json
from datetime import datetime
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

from adapters.act import (
    ACT_TZ,
    FIELD_MAP,
    ACTAdapter,
    _parse_act_timestamp,
)

# --- Sample JSON for testing ---

SAMPLE_RECORDS = [
    {
        "name": "Civic",
        "gps": {"latitude": "-35.285307", "longitude": "149.131579"},
        "datetime": "2024-06-15T14:00:00.000",
        "pm10": "17.72",
        "pm2_5": "8.17",
        "pm10_1_hr": "7.77",
        "pm2_5_1_hr": "1.76",
        "o3_1hr": "0.018",
        "o3_4hr": "0.021",
        "o3_8hr": "0.022",
        "co": "0.1",
        "no2": "0.005",
    },
    {
        "name": "Monash",
        "gps": {"latitude": "-35.415", "longitude": "149.089"},
        "datetime": "2024-06-15T14:00:00.000",
        "pm10": "19.55",
        "pm2_5": "13.38",
        "pm10_1_hr": "10.2",
        "pm2_5_1_hr": "5.3",
        "o3_1hr": "0.025",
        "co": "0.17",
    },
    {
        "name": "Florey",
        "gps": {"latitude": "-35.228", "longitude": "149.047"},
        "datetime": "2024-06-15T14:00:00.000",
        "pm10": "15.0",
        "pm2_5": "9.5",
    },
    {
        "name": "Civic",
        "gps": {"latitude": "-35.285307", "longitude": "149.131579"},
        "datetime": "2024-06-15T13:00:00.000",
        "pm10": "16.5",
        "pm2_5": "7.8",
        "o3_1hr": "0.020",
    },
]

SAMPLE_RECORDS_WITH_NULLS = [
    {
        "name": "Civic",
        "gps": {"latitude": "-35.285307", "longitude": "149.131579"},
        "datetime": "2024-06-15T14:00:00.000",
        "pm10": "17.72",
        "pm2_5": None,
        "o3_1hr": "",
        "co": "not_a_number",
    },
]


def _make_adapter(**overrides):
    """Create an ACTAdapter with test defaults."""
    config = {"api_url": "https://example.com/api/id/test.json"}
    config.update(overrides)
    return ACTAdapter("act", config)


def _mock_response(data, status_code=200):
    """Create a mock requests.Response."""
    resp = MagicMock()
    resp.json.return_value = data
    resp.status_code = status_code
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = Exception(f"HTTP {status_code}")
    return resp


class TestParseACTTimestamp:
    """Tests for _parse_act_timestamp.

    ACT API returns datetimes like '2026-04-17T16:00:00.000' in
    Australian Eastern Time (AEST/AEDT via Australia/Sydney).
    """

    def test_basic_timestamp(self):
        """Standard timestamp with milliseconds."""
        ts = _parse_act_timestamp("2024-06-15T14:00:00.000")
        assert ts is not None
        dt = datetime.fromtimestamp(ts, tz=ZoneInfo("UTC"))
        # 14:00 AEST (winter, UTC+10) = 04:00 UTC
        assert dt.hour == 4
        assert dt.day == 15

    def test_without_milliseconds(self):
        """Timestamp without milliseconds should also parse."""
        ts = _parse_act_timestamp("2024-06-15T14:00:00")
        assert ts is not None
        dt = datetime.fromtimestamp(ts, tz=ZoneInfo("UTC"))
        assert dt.hour == 4

    def test_midnight(self):
        """Midnight AEST."""
        ts = _parse_act_timestamp("2024-06-15T00:00:00.000")
        assert ts is not None
        dt = datetime.fromtimestamp(ts, tz=ZoneInfo("UTC"))
        # 00:00 AEST = 14:00 UTC previous day
        assert dt.day == 14
        assert dt.hour == 14

    def test_daylight_saving_summer(self):
        """During AEDT (summer), offset is UTC+11."""
        ts = _parse_act_timestamp("2024-01-15T14:00:00.000")
        assert ts is not None
        dt = datetime.fromtimestamp(ts, tz=ZoneInfo("UTC"))
        # 14:00 AEDT (UTC+11) = 03:00 UTC
        assert dt.hour == 3

    def test_daylight_saving_winter(self):
        """During AEST (winter), offset is UTC+10."""
        ts = _parse_act_timestamp("2024-06-15T14:00:00.000")
        assert ts is not None
        dt = datetime.fromtimestamp(ts, tz=ZoneInfo("UTC"))
        # 14:00 AEST (UTC+10) = 04:00 UTC
        assert dt.hour == 4

    def test_empty_string(self):
        assert _parse_act_timestamp("") is None

    def test_none(self):
        assert _parse_act_timestamp(None) is None

    def test_bad_format(self):
        assert _parse_act_timestamp("15/06/2024 14:00") is None

    def test_not_a_date(self):
        assert _parse_act_timestamp("not-a-date") is None


class TestFieldMap:
    """Tests for FIELD_MAP — verified against live API response."""

    def test_all_live_fields_mapped(self):
        """All field names observed in the live API should be mapped."""
        live_fields = [
            "pm10", "pm2_5", "pm10_1_hr", "pm2_5_1_hr",
            "o3_1hr", "o3_4hr", "o3_8hr", "no2", "co",
        ]
        for field in live_fields:
            assert field in FIELD_MAP, f"'{field}' not in FIELD_MAP"

    def test_pm25_mapping(self):
        assert FIELD_MAP["pm2_5"] == ("pm2_5", "ug/m3")

    def test_pm10_mapping(self):
        assert FIELD_MAP["pm10"] == ("pm10", "ug/m3")

    def test_pm10_1hr_mapping(self):
        assert FIELD_MAP["pm10_1_hr"] == ("pm10_1hr", "ug/m3")

    def test_pm2_5_1hr_mapping(self):
        assert FIELD_MAP["pm2_5_1_hr"] == ("pm2_5_1hr", "ug/m3")

    def test_ozone_1hr_mapping(self):
        assert FIELD_MAP["o3_1hr"] == ("ozone_1hr", "ppm")

    def test_ozone_4hr_mapping(self):
        assert FIELD_MAP["o3_4hr"] == ("ozone_4hr", "ppm")

    def test_ozone_8hr_mapping(self):
        assert FIELD_MAP["o3_8hr"] == ("ozone_8hr", "ppm")

    def test_no2_mapping(self):
        assert FIELD_MAP["no2"] == ("no2", "ppm")

    def test_co_mapping(self):
        assert FIELD_MAP["co"] == ("co", "ppm")

    def test_all_values_are_tuples(self):
        for key, value in FIELD_MAP.items():
            assert isinstance(value, tuple), f"{key} value is not a tuple"
            assert len(value) == 2
            assert isinstance(value[0], str)
            assert isinstance(value[1], str)


class TestACTAdapterParsing:
    """Tests for ACTAdapter JSON parsing via fetch_stations()."""

    @patch("adapters.act.requests.Session.get")
    def test_parses_stations(self, mock_get):
        mock_get.return_value = _mock_response(SAMPLE_RECORDS)
        adapter = _make_adapter()
        stations = adapter.fetch_stations()
        assert len(stations) == 3
        names = {s["name"] for s in stations}
        assert names == {"Civic", "Monash", "Florey"}

    @patch("adapters.act.requests.Session.get")
    def test_station_coordinates(self, mock_get):
        mock_get.return_value = _mock_response(SAMPLE_RECORDS)
        adapter = _make_adapter()
        stations = adapter.fetch_stations()
        civic = next(s for s in stations if s["name"] == "Civic")
        assert civic["latitude"] == -35.285307
        assert civic["longitude"] == 149.131579

    @patch("adapters.act.requests.Session.get")
    def test_station_id_format(self, mock_get):
        mock_get.return_value = _mock_response(SAMPLE_RECORDS)
        adapter = _make_adapter()
        stations = adapter.fetch_stations()
        ids = {s["station_id"] for s in stations}
        assert "civic" in ids
        assert "monash" in ids
        assert "florey" in ids

    @patch("adapters.act.requests.Session.get")
    def test_readings_cached(self, mock_get):
        mock_get.return_value = _mock_response(SAMPLE_RECORDS)
        adapter = _make_adapter()
        stations = adapter.fetch_stations()
        civic = next(s for s in stations if s["name"] == "Civic")
        readings = adapter.fetch_readings(civic)
        # Civic has 2 records (14:00 and 13:00), each with multiple fields
        # 14:00: pm10, pm2_5, pm10_1_hr, pm2_5_1_hr, o3_1hr, o3_4hr, o3_8hr, co, no2 = 9
        # 13:00: pm10, pm2_5, o3_1hr = 3
        assert len(readings) == 12

    @patch("adapters.act.requests.Session.get")
    def test_reading_values(self, mock_get):
        mock_get.return_value = _mock_response(SAMPLE_RECORDS)
        adapter = _make_adapter()
        stations = adapter.fetch_stations()
        civic = next(s for s in stations if s["name"] == "Civic")
        readings = adapter.fetch_readings(civic)
        pm10_readings = [r for r in readings if r["reading_type"] == "pm10"]
        # Should have 2 pm10 readings (14:00 and 13:00)
        assert len(pm10_readings) == 2
        values = {r["value"] for r in pm10_readings}
        assert 17.72 in values
        assert 16.5 in values
        assert pm10_readings[0]["unit"] == "ug/m3"

    @patch("adapters.act.requests.Session.get")
    def test_reading_timestamp(self, mock_get):
        mock_get.return_value = _mock_response(SAMPLE_RECORDS)
        adapter = _make_adapter()
        stations = adapter.fetch_stations()
        monash = next(s for s in stations if s["name"] == "Monash")
        readings = adapter.fetch_readings(monash)
        # Monash has one record at 14:00 AEST
        expected = _parse_act_timestamp("2024-06-15T14:00:00.000")
        for r in readings:
            assert r["timestamp"] == expected

    @patch("adapters.act.requests.Session.get")
    def test_null_and_empty_values_skipped(self, mock_get):
        mock_get.return_value = _mock_response(SAMPLE_RECORDS_WITH_NULLS)
        adapter = _make_adapter()
        stations = adapter.fetch_stations()
        assert len(stations) == 1
        civic = stations[0]
        readings = adapter.fetch_readings(civic)
        # Only pm10 should parse — pm2_5 is None, o3_1hr is "", co is non-numeric
        assert len(readings) == 1
        assert readings[0]["reading_type"] == "pm10"
        assert readings[0]["value"] == 17.72

    @patch("adapters.act.requests.Session.get")
    def test_no_readings_for_unknown_station(self, mock_get):
        mock_get.return_value = _mock_response(SAMPLE_RECORDS)
        adapter = _make_adapter()
        adapter.fetch_stations()
        readings = adapter.fetch_readings({"station_id": "nonexistent", "name": "Nowhere"})
        assert readings == []

    @patch("adapters.act.requests.Session.get")
    def test_http_error_returns_empty(self, mock_get):
        mock_get.return_value = _mock_response([], status_code=500)
        adapter = _make_adapter()
        stations = adapter.fetch_stations()
        assert stations == []

    @patch("adapters.act.requests.Session.get")
    def test_non_array_response_returns_empty(self, mock_get):
        mock_get.return_value = _mock_response({"error": "bad request"})
        adapter = _make_adapter()
        stations = adapter.fetch_stations()
        assert stations == []

    @patch("adapters.act.requests.Session.get")
    def test_missing_gps_skips_station(self, mock_get):
        records = [
            {
                "name": "NoGPS",
                "datetime": "2024-06-15T14:00:00.000",
                "pm10": "10.0",
            },
        ]
        mock_get.return_value = _mock_response(records)
        adapter = _make_adapter()
        stations = adapter.fetch_stations()
        assert len(stations) == 0

    @patch("adapters.act.requests.Session.get")
    def test_missing_name_skips_record(self, mock_get):
        records = [
            {
                "gps": {"latitude": "-35.0", "longitude": "149.0"},
                "datetime": "2024-06-15T14:00:00.000",
                "pm10": "10.0",
            },
        ]
        mock_get.return_value = _mock_response(records)
        adapter = _make_adapter()
        stations = adapter.fetch_stations()
        assert len(stations) == 0

    @patch("adapters.act.requests.Session.get")
    def test_missing_datetime_skips_record(self, mock_get):
        records = [
            {
                "name": "Civic",
                "gps": {"latitude": "-35.285", "longitude": "149.131"},
                "pm10": "10.0",
            },
        ]
        mock_get.return_value = _mock_response(records)
        adapter = _make_adapter()
        stations = adapter.fetch_stations()
        assert len(stations) == 0


class TestACTAdapterDedup:
    """Tests for timestamp-based dedup."""

    @patch("adapters.act.requests.Session.get")
    def test_dedup_skips_old_readings(self, mock_get):
        """Second call with same data returns no new readings."""
        mock_get.return_value = _mock_response(SAMPLE_RECORDS)
        adapter = _make_adapter()

        # First call — all readings new
        stations1 = adapter.fetch_stations()
        assert len(stations1) == 3
        civic = next(s for s in stations1 if s["name"] == "Civic")
        readings1 = adapter.fetch_readings(civic)
        assert len(readings1) > 0

        # Second call with same data — all readings deduped
        mock_get.return_value = _mock_response(SAMPLE_RECORDS)
        stations2 = adapter.fetch_stations()
        # Stations with no new readings should still appear but have empty readings
        for s in stations2:
            readings = adapter.fetch_readings(s)
            assert len(readings) == 0

    @patch("adapters.act.requests.Session.get")
    def test_dedup_allows_newer_readings(self, mock_get):
        """Readings with timestamps after last-seen are accepted."""
        mock_get.return_value = _mock_response(SAMPLE_RECORDS)
        adapter = _make_adapter()
        adapter.fetch_stations()

        # New data with a later timestamp
        newer_records = [
            {
                "name": "Civic",
                "gps": {"latitude": "-35.285307", "longitude": "149.131579"},
                "datetime": "2024-06-15T15:00:00.000",
                "pm10": "20.0",
            },
        ]
        mock_get.return_value = _mock_response(newer_records)
        stations = adapter.fetch_stations()
        assert len(stations) == 1
        readings = adapter.fetch_readings(stations[0])
        assert len(readings) == 1
        assert readings[0]["value"] == 20.0


class TestACTAdapterTimestampPersistence:
    """Tests for get_last_timestamps / set_last_timestamps."""

    @patch("adapters.act.requests.Session.get")
    def test_get_last_timestamps_after_fetch(self, mock_get):
        mock_get.return_value = _mock_response(SAMPLE_RECORDS)
        adapter = _make_adapter()
        adapter.fetch_stations()
        ts = adapter.get_last_timestamps()
        assert len(ts) == 3
        assert "civic" in ts
        assert "monash" in ts
        assert "florey" in ts

    def test_set_last_timestamps(self):
        adapter = _make_adapter()
        adapter.set_last_timestamps({"civic": 1000, "monash": 2000})
        ts = adapter.get_last_timestamps()
        assert ts == {"civic": 1000, "monash": 2000}

    @patch("adapters.act.requests.Session.get")
    def test_restored_timestamps_skip_old_data(self, mock_get):
        """After restoring timestamps, old data is deduped."""
        adapter = _make_adapter()
        # Set timestamps far in the future
        adapter.set_last_timestamps({"civic": 9999999999, "monash": 9999999999, "florey": 9999999999})

        mock_get.return_value = _mock_response(SAMPLE_RECORDS)
        stations = adapter.fetch_stations()
        for s in stations:
            readings = adapter.fetch_readings(s)
            assert len(readings) == 0
