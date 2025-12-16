import pandas as pd
import pytest

from solarpredict.weather.prefetched import PrefetchedWeatherProvider


def _frame(start: str, periods: int = 48, tz: str | None = "UTC"):
    idx = pd.date_range(start, periods=periods, freq="1h", tz=tz)
    data = {
        "ghi_wm2": range(periods),
        "dni_wm2": range(periods),
        "dhi_wm2": range(periods),
        "temp_air_c": [20.0] * periods,
        "wind_ms": [1.0] * periods,
    }
    return pd.DataFrame(data, index=idx)


def test_prefetched_weather_slices_by_date_window():
    provider = PrefetchedWeatherProvider({"site1": _frame("2025-12-01", periods=72)})
    out = provider.get_forecast([{"id": "site1"}], start="2025-12-01", end="2025-12-03", timestep="1h")
    df = out["site1"]
    assert len(df) == 48  # two days hourly, end exclusive
    assert df.index.min() == pd.Timestamp("2025-12-01T00:00:00+00:00")
    assert df.index.max() == pd.Timestamp("2025-12-02T23:00:00+00:00", tz="UTC")


def test_prefetched_weather_missing_location_raises():
    provider = PrefetchedWeatherProvider({"site1": _frame("2025-12-01")})
    with pytest.raises(ValueError):
        provider.get_forecast([{"id": "site2"}], start="2025-12-01", end="2025-12-02", timestep="1h")


def test_prefetched_weather_returns_full_on_bad_dates():
    provider = PrefetchedWeatherProvider({"site1": _frame("2025-12-01", periods=10)})
    out = provider.get_forecast([{"id": "site1"}], start="not-a-date", end="also-bad")
    df = out["site1"]
    assert len(df) == 10
