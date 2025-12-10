import datetime as dt
import json
from pathlib import Path

import pandas as pd

from solarpredict.weather.pvgis import PVGISWeatherProvider

FIXTURE = Path(__file__).parents[1] / "fixtures" / "pvgis_tmy_sample.json"


def test_build_params_basic():
    provider = PVGISWeatherProvider()
    params = provider._build_params({"lat": 45.0, "lon": 8.0})
    assert params["lat"] == "45.0"
    assert params["lon"] == "8.0"
    assert params["outputformat"] == "json"
    assert params["browser"] == "0"


def test_parse_fixture_restamps_year_and_columns():
    data = json.loads(FIXTURE.read_text())
    provider = PVGISWeatherProvider()
    target_year = 2025
    df = provider._parse_single(data, target_year=target_year)

    assert isinstance(df.index, pd.DatetimeIndex)
    assert df.index.tz is not None
    first = df.index[0]
    assert first.year == target_year
    # original fixture starts at Jan 1 00:00 UTC
    assert first.month == 1 and first.day == 1 and first.hour == 0

    expected_cols = {"temp_air_c", "wind_ms", "ghi_wm2", "dni_wm2", "dhi_wm2"}
    assert expected_cols.issubset(df.columns)
    assert df.notna().all().all()


def test_get_forecast_emits_and_slices_single_location(monkeypatch):
    data = json.loads(FIXTURE.read_text())
    calls = []

    class DummyResp:
        def __init__(self, payload):
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class DummySession:
        def __init__(self, payload):
            self.payload = payload

        def get(self, url, params=None, timeout=None):
            calls.append({"url": url, "params": params})
            return DummyResp(self.payload)

    collector = []

    class CollectDebug:
        def emit(self, stage, payload, *, ts, site=None, array=None):
            collector.append((stage, site))

    provider = PVGISWeatherProvider(debug=CollectDebug(), session=DummySession(data))
    start = dt.date(2025, 1, 1).isoformat()
    end = dt.date(2025, 1, 2).isoformat()
    result = provider.get_forecast([{"id": "loc1", "lat": 45.0, "lon": 8.0}], start=start, end=end, timestep="1h")

    assert "loc1" in result
    df = result["loc1"]
    assert not df.empty
    # ensure debug stages were emitted
    stages = [s for s, _ in collector]
    assert "weather.request" in stages
    assert "weather.response_meta" in stages
    assert "weather.summary" in stages
    # ensure we called the right URL
    assert calls[0]["url"].endswith("/tmy")
