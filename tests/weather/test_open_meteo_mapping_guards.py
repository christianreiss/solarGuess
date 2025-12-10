import pytest

from solarpredict.weather.open_meteo import OpenMeteoWeatherProvider


def test_mapping_raises_on_missing_coordinate():
    provider = OpenMeteoWeatherProvider()
    provider.debug = type("D", (), {"emit": lambda *a, **k: None})()
    class Resp:
        def raise_for_status(self):
            return None

        def json(self):
            return [{"latitude": 1.0, "longitude": 1.0}]

    provider.session.get = lambda *a, **k: Resp()
    with pytest.raises(ValueError):
        provider.get_forecast(
            [{"id": "a", "lat": 0.0, "lon": 0.0}, {"id": "b", "lat": 1.0, "lon": 1.0}],
            start="2025-01-01",
            end="2025-01-02",
        )
