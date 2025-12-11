import datetime as dt

import pandas as pd

from solarpredict.core.models import Location, PVArray, Scenario, Site
from solarpredict.engine.simulate import simulate_day


class SimpleWeather:
    def __init__(self):
        # 3h day slice with sun rising; we make DNI constant so masking is obvious.
        self.idx = pd.date_range("2025-06-01T10:00:00Z", periods=6, freq="30min")
        self.df = pd.DataFrame(
            {
                "ghi_wm2": [0, 200, 500, 700, 500, 200],
                "dni_wm2": [0, 300, 600, 800, 600, 300],
                "dhi_wm2": [0, 100, 150, 200, 150, 100],
                "temp_air_c": [15.0] * 6,
                "wind_ms": [1.0] * 6,
            },
            index=self.idx,
        )

    def get_forecast(self, locations, start, end, timestep="30m"):
        return {loc["id"]: self.df for loc in locations}


def _scenario(horizon=None):
    loc = Location(id="loc", lat=0, lon=0, tz="UTC")
    arr = PVArray(
        id="a",
        tilt_deg=30,
        azimuth_deg=180,
        pdc0_w=5000,
        gamma_pdc=-0.004,
        dc_ac_ratio=1.2,
        eta_inv_nom=0.96,
        losses_percent=0,
        temp_model="close_mount_glass_glass",
        horizon_deg=horizon,
    )
    site = Site(id="s", location=loc, arrays=[arr])
    return Scenario(sites=[site])


def test_horizon_reduces_energy_when_blocking_low_sun():
    scenario = _scenario(horizon=[60] * 12)  # blocks most low sun
    wx = SimpleWeather()
    result = simulate_day(scenario, dt.date(2025, 6, 1), timestep="30m", weather_provider=wx, weather_label="end")
    energy_masked = result.daily.iloc[0]["energy_kwh"]

    scenario_clear = _scenario(horizon=None)
    result_clear = simulate_day(scenario_clear, dt.date(2025, 6, 1), timestep="30m", weather_provider=wx, weather_label="end")
    energy_clear = result_clear.daily.iloc[0]["energy_kwh"]

    assert energy_masked < energy_clear
    assert energy_masked > 0
