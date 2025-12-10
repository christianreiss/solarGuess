import pandas as pd

from solarpredict.weather.composite import CompositeWeatherProvider


class DummyProvider:
    def __init__(self, data_by_id):
        self.data_by_id = data_by_id

    def get_forecast(self, locations, start, end, timestep="1h"):
        return {loc["id"]: self.data_by_id[loc["id"]] for loc in locations}


def make_idx(start, periods):
    return pd.date_range(start=start, periods=periods, freq="1h", tz="UTC")


def test_secondary_not_carried_outside_overlap():
    primary = DummyProvider({
        "a": pd.DataFrame({
            "ghi_wm2": [None, None, None],
            "dhi_wm2": [None, None, None],
            "dni_wm2": [None, None, None],
            "temp_air_c": [None, None, None],
            "wind_ms": [None, None, None],
        }, index=make_idx("2025-01-02 00:00", 3)),
    })
    secondary = DummyProvider({
        "a": pd.DataFrame({
            "ghi_wm2": [100, 110],
            "dhi_wm2": [50, 55],
            "dni_wm2": [80, 90],
            "temp_air_c": [10, 11],
            "wind_ms": [2, 2.5],
        }, index=make_idx("2025-01-01 00:00", 2)),
    })

    comp = CompositeWeatherProvider(primary, secondary)
    try:
        comp.get_forecast([{"id": "a", "lat": 0, "lon": 0}], start="2025-01-02", end="2025-01-03")
        assert False, "expected ValueError due to no overlap"
    except ValueError as exc:
        assert "missing for" in str(exc)


def test_secondary_used_only_within_overlap():
    primary = DummyProvider({
        "a": pd.DataFrame({
            "ghi_wm2": [None, None, None],
            "dhi_wm2": [None, None, None],
            "dni_wm2": [None, None, None],
            "temp_air_c": [None, None, None],
            "wind_ms": [None, None, None],
        }, index=make_idx("2025-01-01 00:00", 3)),
    })
    secondary = DummyProvider({
        "a": pd.DataFrame({
            "ghi_wm2": [100, 110, 120],
            "dhi_wm2": [50, 55, 60],
            "dni_wm2": [80, 90, 95],
            "temp_air_c": [10, 11, 12],
            "wind_ms": [2, 2.5, 3],
        }, index=make_idx("2025-01-01 00:00", 3)),
    })

    comp = CompositeWeatherProvider(primary, secondary)
    res = comp.get_forecast([{"id": "a", "lat": 0, "lon": 0}], start="2025-01-01", end="2025-01-02")
    df = res["a"]
    assert (df["ghi_wm2"] == [100, 110, 120]).all()
    assert df.index[0].tz is not None

