import datetime as dt

from solarpredict.weather.open_meteo import OpenMeteoWeatherProvider


def test_build_params_multi_location():
    provider = OpenMeteoWeatherProvider()
    start = dt.date(2025, 1, 1).isoformat()
    end = dt.date(2025, 1, 2).isoformat()
    locations = [
        {"id": "loc1", "lat": 52.52, "lon": 13.41},
        {"id": "loc2", "lat": 48.85, "lon": 2.35},
    ]
    params = provider._build_params(locations, start, end, "1h")

    assert params["latitude"] == "52.52,48.85"
    assert params["longitude"] == "13.41,2.35"
    assert params["hourly"].split(",") == [
        "temperature_2m",
        "wind_speed_10m",
        "shortwave_radiation",
        "diffuse_radiation",
        "direct_normal_irradiance",
    ]
    assert params["timezone"] == "auto"
    assert params["start_date"] == start
    assert params["end_date"] == end
    assert params["wind_speed_unit"] == "ms"
    assert params["location_ids"] == "loc1,loc2"


def test_build_params_15m_sets_minutely():
    provider = OpenMeteoWeatherProvider()
    params = provider._build_params([], "2025-01-01", "2025-01-02", "15m")
    assert "minutely_15" in params
    assert "hourly" not in params
