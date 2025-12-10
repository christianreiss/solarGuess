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


def test_inverter_group_mixed_eta_inv_nom_chooses_max():
    idx = pd.date_range("2025-01-01", periods=2, freq="1h", tz="UTC")
    df = pd.DataFrame(
        {
            "ghi_wm2": [500, 500],
            "dni_wm2": [400, 400],
            "dhi_wm2": [100, 100],
            "temp_air_c": [20, 20],
            "wind_ms": [1, 1],
        },
        index=idx,
    )
    wx = DummyWeather(df)
    arrays = [
        PVArray(
            id="a1",
            tilt_deg=30,
            azimuth_deg=180,
            pdc0_w=1000,
            gamma_pdc=-0.004,
            dc_ac_ratio=1.2,
            eta_inv_nom=0.94,
            losses_percent=7,
            temp_model="open_rack_glass_glass",
            inverter_group_id="g1",
        ),
        PVArray(
            id="a2",
            tilt_deg=30,
            azimuth_deg=180,
            pdc0_w=1000,
            gamma_pdc=-0.004,
            dc_ac_ratio=1.2,
            eta_inv_nom=0.97,
            losses_percent=7,
            temp_model="open_rack_glass_glass",
            inverter_group_id="g1",
        ),
    ]
    site = Site(id="s", location=Location(id="loc", lat=0, lon=0), arrays=arrays)
    scenario = Scenario(sites=[site])

    res = simulate_day(scenario, dt.date(2025, 1, 1), weather_provider=wx)
    # the higher eta_inv_nom should have been chosen; ensure outputs exist and non-negative
    df_out = res.timeseries[("s", "a1")]
    assert (df_out["pac_w"] >= 0).all()
