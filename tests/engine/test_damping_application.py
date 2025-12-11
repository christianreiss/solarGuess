import datetime as dt

import pandas as pd

from solarpredict.core.models import Location, PVArray, Scenario, Site
from solarpredict.engine.simulate import simulate_day


class FlatWeather:
    def __init__(self):
        # Symmetric day around noon; clear enough to see attenuation effects.
        self.idx = pd.date_range("2025-06-01T04:00:00Z", periods=17, freq="1h")
        self.df = pd.DataFrame(
            {
                "ghi_wm2": [0, 0, 50, 150, 300, 500, 650, 800, 900, 800, 650, 500, 300, 150, 50, 0, 0],
                "dni_wm2": [0, 0, 100, 200, 400, 600, 750, 900, 950, 900, 750, 600, 400, 200, 100, 0, 0],
                "dhi_wm2": [0, 0, 30, 50, 80, 120, 150, 180, 200, 180, 150, 120, 80, 50, 30, 0, 0],
                "temp_air_c": [15.0] * 17,
                "wind_ms": [1.0] * 17,
            },
            index=self.idx,
        )

    def get_forecast(self, locations, start, end, timestep="1h"):
        return {loc["id"]: self.df for loc in locations}


def _scenario(damping_morning=1.0, damping_evening=1.0):
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
        damping_morning=damping_morning,
        damping_evening=damping_evening,
    )
    site = Site(id="s", location=loc, arrays=[arr])
    return Scenario(sites=[site])


def test_damping_reduces_morning_evening_energy_only():
    wx = FlatWeather()
    scenario = _scenario(damping_morning=0.5, damping_evening=0.5)
    result_damped = simulate_day(scenario, dt.date(2025, 6, 1), timestep="1h", weather_provider=wx, weather_label="end")

    scenario_clear = _scenario(damping_morning=1.0, damping_evening=1.0)
    result_clear = simulate_day(scenario_clear, dt.date(2025, 6, 1), timestep="1h", weather_provider=wx, weather_label="end")

    energy_damped = result_damped.daily.iloc[0]["energy_kwh"]
    energy_clear = result_clear.daily.iloc[0]["energy_kwh"]

    assert energy_damped < energy_clear

    # Midday power should remain effectively untouched; check the noon slot (index 8).
    noon_key = ("s", "a")
    noon_power_clear = result_clear.timeseries[noon_key].iloc[8]["pac_net_w"]
    noon_power_damped = result_damped.timeseries[noon_key].iloc[8]["pac_net_w"]
    assert abs(noon_power_clear - noon_power_damped) / noon_power_clear < 1e-6


def test_damping_defaults_to_one_when_unspecified():
    wx = FlatWeather()
    scenario_default = _scenario()
    result_default = simulate_day(scenario_default, dt.date(2025, 6, 1), timestep="1h", weather_provider=wx, weather_label="end")

    scenario_explicit_one = _scenario(damping_morning=1.0, damping_evening=1.0)
    result_explicit = simulate_day(
        scenario_explicit_one, dt.date(2025, 6, 1), timestep="1h", weather_provider=wx, weather_label="end"
    )

    assert result_default.daily.iloc[0]["energy_kwh"] == result_explicit.daily.iloc[0]["energy_kwh"]
