import datetime as dt

import pandas as pd
import pytest

from solarpredict.core.models import Scenario, Site, PVArray, Location
from solarpredict.engine.simulate import simulate_day


class DummyWeather:
    def __init__(self, df):
        self.df = df

    def get_forecast(self, locations, start, end, timestep):
        return {loc["id"]: self.df for loc in locations}


def test_simulate_missing_dhi_column_raises():
    idx = pd.date_range("2025-01-01", periods=1, freq="1h", tz="UTC")
    df = pd.DataFrame(
        {
            "ghi_wm2": [100],
            "dni_wm2": [80],
            "temp_air_c": [20],
            "wind_ms": [1],
        },
        index=idx,
    )
    wx = DummyWeather(df)
    array = PVArray(
        id="a",
        tilt_deg=30,
        azimuth_deg=180,
        pdc0_w=1000,
        gamma_pdc=-0.004,
        dc_ac_ratio=1.2,
        eta_inv_nom=0.96,
        losses_percent=10.0,
        temp_model="open_rack_glass_glass",
    )
    site = Site(id="s", location=Location(id="loc", lat=0, lon=0), arrays=[array])
    scenario = Scenario(sites=[site])

    with pytest.raises(ValueError):
        simulate_day(scenario, idx[0].date(), weather_provider=wx)
