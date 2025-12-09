import json
from pathlib import Path

import pandas as pd

from solarpredict.weather.open_meteo import OpenMeteoWeatherProvider

FIXTURE = Path(__file__).parents[1] / "fixtures" / "open_meteo_multi_location.json"


def test_parse_fixture_tz_and_columns():
    data = json.loads(FIXTURE.read_text())
    provider = OpenMeteoWeatherProvider()

    results = provider._parse_single(data[0])
    assert isinstance(results.index, pd.DatetimeIndex)
    assert results.index.tz is not None
    # Ensure we didn't mistakenly shift local timestamps by converting from UTC
    first = results.index[0]
    assert first.hour == 0 and first.day == 9 and first.month == 12 and first.year == 2025
    expected_cols = {"temp_air_c", "wind_ms", "ghi_wm2", "dhi_wm2", "dni_wm2"}
    assert expected_cols.issubset(results.columns)
    assert results.notna().all().all()


def test_parse_converts_wind_kmh_to_ms_when_needed():
    data = json.loads(FIXTURE.read_text())
    provider = OpenMeteoWeatherProvider()
    df = provider._parse_single(data[0])
    # Fixture provides 10 km/h at first slot; expecting ~2.777... m/s when unit is km/h.
    assert abs(df.iloc[0].wind_ms - 2.7777777) < 1e-6


def test_parse_multi_location_map_ids():
    data = json.loads(FIXTURE.read_text())
    provider = OpenMeteoWeatherProvider()
    locs = [
        {"id": "loc1", "lat": data[0]["latitude"], "lon": data[0]["longitude"]},
        {"id": "loc2", "lat": data[1]["latitude"], "lon": data[1]["longitude"]},
    ]
    params = provider._build_params(locs, "2025-01-01", "2025-01-02", "1h")
    provider.debug.emit = lambda *args, **kwargs: None  # silence
    provider.session = None  # not used
    # simulate get_forecast parse path
    provider.debug = type("D", (), {"emit": lambda *a, **k: None})()
    results = {}
    for idx, loc_payload in enumerate(data):
        df = provider._parse_single(loc_payload)
        results[params["location_ids"].split(",")[idx]] = df
    assert set(results.keys()) == {"loc1", "loc2"}
    assert all(not df.empty for df in results.values())
