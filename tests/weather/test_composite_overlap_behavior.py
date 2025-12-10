import pandas as pd
import pytest

from solarpredict.weather.composite import CompositeWeatherProvider


class DummyProvider:
    def __init__(self, data_by_id):
        self.data_by_id = data_by_id

    def get_forecast(self, locations, start, end, timestep="1h"):
        return {loc["id"]: self.data_by_id[loc["id"]] for loc in locations}


def make_idx(start, periods):
    return pd.date_range(start=start, periods=periods, freq="1h", tz="UTC")


def test_composite_no_overlap_raises():
    primary = DummyProvider(
        {"a": pd.DataFrame({"ghi_wm2": [None]}, index=make_idx("2025-01-02", 1))}
    )
    secondary = DummyProvider(
        {"a": pd.DataFrame({"ghi_wm2": [100]}, index=make_idx("2025-01-01", 1))}
    )
    comp = CompositeWeatherProvider(primary, secondary)
    with pytest.raises(ValueError):
        comp.get_forecast([{"id": "a", "lat": 0, "lon": 0}], start="2025-01-02", end="2025-01-03")
