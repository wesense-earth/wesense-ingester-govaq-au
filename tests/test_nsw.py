"""Tests for NSW DPIE air quality adapter."""

from datetime import datetime
from zoneinfo import ZoneInfo

from adapters.nsw import PARAMETER_MAP, _parse_nsw_timestamp, AEST


class TestParseNSWTimestamp:
    """Tests for _parse_nsw_timestamp.

    NSW API returns Hour 1-24 where Hour N = average for (N-1):00 to N:00.
    The function converts to start-of-period timestamp in UTC.
    """

    def test_hour_1_is_midnight(self):
        """Hour 1 = 12am-1am, timestamp should be 00:00 AEST."""
        ts = _parse_nsw_timestamp("2024-06-15", 1)
        assert ts is not None
        dt = datetime.fromtimestamp(ts, tz=ZoneInfo("UTC"))
        # June is AEST (UTC+10), so 00:00 AEST = 14:00 UTC previous day
        assert dt.day == 14
        assert dt.hour == 14

    def test_hour_24_is_23h(self):
        """Hour 24 = 11pm-12am, timestamp should be 23:00 AEST."""
        ts = _parse_nsw_timestamp("2024-06-15", 24)
        assert ts is not None
        dt = datetime.fromtimestamp(ts, tz=ZoneInfo("UTC"))
        # 23:00 AEST = 13:00 UTC same day
        assert dt.day == 15
        assert dt.hour == 13

    def test_hour_12_is_11h(self):
        """Hour 12 = 11am-12pm, timestamp should be 11:00 AEST."""
        ts = _parse_nsw_timestamp("2024-06-15", 12)
        assert ts is not None
        dt = datetime.fromtimestamp(ts, tz=ZoneInfo("UTC"))
        # 11:00 AEST = 01:00 UTC same day
        assert dt.hour == 1

    def test_none_date(self):
        ts = _parse_nsw_timestamp(None, 5)
        assert ts is None

    def test_none_hour(self):
        ts = _parse_nsw_timestamp("2024-01-15", None)
        assert ts is None

    def test_both_none(self):
        ts = _parse_nsw_timestamp(None, None)
        assert ts is None

    def test_bad_date_format(self):
        ts = _parse_nsw_timestamp("15/01/2024", 5)
        assert ts is None

    def test_invalid_date_string(self):
        ts = _parse_nsw_timestamp("not-a-date", 5)
        assert ts is None

    def test_hour_as_string(self):
        """Hour may come as string from JSON -- should still parse."""
        ts = _parse_nsw_timestamp("2024-06-01", "10")
        assert ts is not None

    def test_hour_0_invalid(self):
        """Hour 0 is out of range (API uses 1-24)."""
        ts = _parse_nsw_timestamp("2024-06-01", 0)
        assert ts is None

    def test_hour_25_invalid(self):
        """Hour 25 is out of range."""
        ts = _parse_nsw_timestamp("2024-06-01", 25)
        assert ts is None

    def test_aest_winter(self):
        """In winter (June), Sydney is AEST = UTC+10."""
        ts = _parse_nsw_timestamp("2024-06-15", 13)
        assert ts is not None
        dt = datetime.fromtimestamp(ts, tz=ZoneInfo("UTC"))
        # Hour 13 -> actual_hour 12, 12:00 AEST = 02:00 UTC
        assert dt.hour == 2

    def test_aedt_summer(self):
        """In summer (January), Sydney is AEDT = UTC+11."""
        ts = _parse_nsw_timestamp("2024-01-15", 13)
        assert ts is not None
        dt = datetime.fromtimestamp(ts, tz=ZoneInfo("UTC"))
        # Hour 13 -> actual_hour 12, 12:00 AEDT = 01:00 UTC
        assert dt.hour == 1


class TestParameterMap:
    """Tests for PARAMETER_MAP coverage.

    Parameter codes verified against live API response.
    """

    def test_pm25_exists(self):
        assert "PM2.5" in PARAMETER_MAP
        assert PARAMETER_MAP["PM2.5"] == ("pm2_5", "ug/m3")

    def test_pm10_exists(self):
        assert "PM10" in PARAMETER_MAP
        assert PARAMETER_MAP["PM10"] == ("pm10", "ug/m3")

    def test_ozone_code(self):
        """NSW API uses 'OZONE' not 'O3'."""
        assert "OZONE" in PARAMETER_MAP
        assert PARAMETER_MAP["OZONE"][0] == "ozone"

    def test_co_units(self):
        """CO is reported in ppm by the NSW API."""
        assert PARAMETER_MAP["CO"] == ("co", "ppm")

    def test_humidity_code(self):
        """NSW API uses 'HUMID' not 'HUM'."""
        assert "HUMID" in PARAMETER_MAP
        assert PARAMETER_MAP["HUMID"][0] == "humidity"

    def test_wind_speed_code(self):
        """NSW API uses 'WSP' not 'WS'."""
        assert "WSP" in PARAMETER_MAP
        assert PARAMETER_MAP["WSP"][0] == "wind_speed"

    def test_wind_direction_code(self):
        """NSW API uses 'WDR' not 'WD'."""
        assert "WDR" in PARAMETER_MAP
        assert PARAMETER_MAP["WDR"][0] == "wind_direction"

    def test_gas_units_are_pphm(self):
        """NO, NO2, SO2, OZONE are all in pphm from the NSW API."""
        for code in ["NO", "NO2", "SO2", "OZONE"]:
            assert PARAMETER_MAP[code][1] == "pphm", f"{code} should be pphm"

    def test_all_keys_are_strings(self):
        for key in PARAMETER_MAP:
            assert isinstance(key, str)

    def test_all_values_are_tuples(self):
        for value in PARAMETER_MAP.values():
            assert isinstance(value, tuple)
            assert len(value) == 2
            assert isinstance(value[0], str)
            assert isinstance(value[1], str)
