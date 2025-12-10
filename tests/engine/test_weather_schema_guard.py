import pandas as pd
import pytest

from solarpredict.core.models import Scenario, Site, PVArray, Location
from solarpredict.engine.simulate import simulate_day


class DummyWeather:
    def __init__(self, df):
        self.df = df

    def get_forecast(self, locations, start, end, timestep="1h"):
        return {loc["id"]: self.df for loc in locations}


def minimal_scenario():
    array = PVArray(
        id="a1",
        tilt_deg=30,
        azimuth_deg=180,
        pdc0_w=1000,
        gamma_pdc=-0.004,
        dc_ac_ratio=1.2,
        eta_inv_nom=0.96,
        losses_percent=10.0,
        temp_model="open_rack_cell_glassback",
    )
    site = Site(id="s1", location=Location(id="loc1", lat=0, lon=0), arrays=[array])
    return Scenario(sites=[site])


def test_schema_guard_raises_on_missing_columns():
    idx = pd.date_range("2025-01-01", periods=1, freq="1h", tz="UTC")
    bad_df = pd.DataFrame({"ghi_wm2": [100]}, index=idx)  # missing others
    wx = DummyWeather(bad_df)

    with pytest.raises(ValueError) as exc:
        simulate_day(minimal_scenario(), idx[0].date(), weather_provider=wx)

    assert "missing required columns" in str(exc.value)
