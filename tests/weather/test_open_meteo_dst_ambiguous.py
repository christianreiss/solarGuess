import pytest

from solarpredict.weather.open_meteo import OpenMeteoWeatherProvider


def test_open_meteo_parses_dst_fall_back_hour_without_ambiguous_time_error():
    provider = OpenMeteoWeatherProvider()
    payload = {
        "timezone": "Europe/Berlin",
        "hourly": {
            "time": [
                "2025-10-26T00:00",
                "2025-10-26T01:00",
                "2025-10-26T02:00",  # ambiguous local time during DST fall-back
                "2025-10-26T03:00",
            ],
            "temperature_2m": [10.0, 9.0, 8.0, 7.0],
            "wind_speed_10m": [1.0, 1.0, 1.0, 1.0],
            "shortwave_radiation": [0.0, 0.0, 0.0, 0.0],
            "diffuse_radiation": [0.0, 0.0, 0.0, 0.0],
            "direct_normal_irradiance": [0.0, 0.0, 0.0, 0.0],
            "cloudcover": [0.0, 0.0, 0.0, 0.0],
        },
        "hourly_units": {"wind_speed_10m": "m/s"},
    }

    df = provider._parse_single(payload)  # intentionally tests the parser directly
    assert df.index.tz is not None
    assert str(df.index.tz) == "Europe/Berlin"
    assert len(df) == 4

