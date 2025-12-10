import json

from solarpredict.weather.open_meteo import OpenMeteoWeatherProvider
from solarpredict.weather.pvgis import PVGISWeatherProvider


def test_open_meteo_retries(monkeypatch):
    calls = {"count": 0}

    class Resp:
        def __init__(self, ok=True):
            self.ok = ok

        def raise_for_status(self):
            if not self.ok:
                raise ValueError("boom")

        def json(self):
            return []

    def fake_get(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] < 2:
            raise ValueError("boom")
        # minimal Open-Meteo-like payload with coords so matching succeeds
        r = Resp()
        r.json = lambda: [{"latitude": 0.0, "longitude": 0.0, "hourly": {"time": [], "shortwave_radiation": []}}]
        return r

    provider = OpenMeteoWeatherProvider()
    provider.session.get = fake_get
    provider.debug = type("D", (), {"emit": lambda *a, **k: None})()
    provider._parse_single = lambda payload: __import__("pandas").DataFrame(index=__import__("pandas").DatetimeIndex([]))
    res = provider.get_forecast([{"id": "1", "lat": 0, "lon": 0}], "2025-01-01", "2025-01-02")
    assert calls["count"] == 2
    assert "1" in res


def test_pvgis_retries(monkeypatch, tmp_path):
    calls = {"count": 0}

    class Resp:
        def __init__(self, ok=True):
            self.ok = ok

        def raise_for_status(self):
            if not self.ok:
                raise ValueError("boom")

        def json(self):
            return {"outputs": {"tmy_hourly": [{"time": "20250101:0000", "T2m": 0, "WS10m": 0, "G(h)": 0, "Gb(n)": 0, "Gd(h)": 0}]}}

    def fake_get(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] < 2:
            raise ValueError("boom")
        return Resp()

    provider = PVGISWeatherProvider(cache_dir=tmp_path)
    provider.session.get = fake_get
    provider.debug = type("D", (), {"emit": lambda *a, **k: None})()
    res = provider.get_forecast([{"id": "1", "lat": 0, "lon": 0}], "2025-01-01", "2025-01-02")
    assert calls["count"] == 2
    assert "1" in res
