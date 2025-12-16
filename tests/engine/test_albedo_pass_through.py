import datetime as dt

import pandas as pd

from solarpredict.core.models import Location, PVArray, Scenario, Site
from solarpredict.engine import simulate as sim_mod


class FlatWeather:
    def __init__(self):
        idx = pd.date_range("2025-06-01 12:00", periods=2, freq="1h", tz="UTC", inclusive="left")
        self.df = pd.DataFrame(
            {
                "ghi_wm2": [1000.0] * 2,
                "dni_wm2": [800.0] * 2,
                "dhi_wm2": [200.0] * 2,
                "temp_air_c": [25.0] * 2,
                "wind_ms": [1.0] * 2,
            },
            index=idx,
        )

    def get_forecast(self, locations, start, end, timestep):
        return {loc["id"]: self.df for loc in locations}


def test_simulate_day_passes_array_albedo_to_poa(monkeypatch):
    calls = []

    def _fake_poa_irradiance(*args, **kwargs):
        calls.append(kwargs.get("albedo"))
        idx = kwargs["dni"].index
        return pd.DataFrame(
            {
                "poa_global": pd.Series([1000.0] * len(idx), index=idx),
                "poa_direct": pd.Series([800.0] * len(idx), index=idx),
                "poa_diffuse": pd.Series([200.0] * len(idx), index=idx),
                "poa_ground_diffuse": pd.Series([0.0] * len(idx), index=idx),
                "aoi": pd.Series([0.0] * len(idx), index=idx),
            }
        )

    monkeypatch.setattr(sim_mod, "poa_irradiance", _fake_poa_irradiance)

    loc = Location(id="loc", lat=0, lon=0, tz="UTC")
    arr = PVArray(
        id="a",
        tilt_deg=30,
        azimuth_deg=180,
        pdc0_w=1000,
        gamma_pdc=-0.004,
        dc_ac_ratio=1.1,
        eta_inv_nom=0.96,
        losses_percent=0,
        temp_model="close_mount_glass_glass",
        albedo=0.55,
    )
    scenario = Scenario(sites=[Site(id="s", location=loc, arrays=[arr])])
    sim_mod.simulate_day(scenario, dt.date(2025, 6, 1), timestep="1h", weather_provider=FlatWeather())

    assert calls == [0.55]
