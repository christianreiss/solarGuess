import datetime as dt

import pandas as pd

from solarpredict.engine.simulate import simulate_day
from solarpredict.core.models import Scenario, Site, Location, PVArray
from solarpredict.weather.cloud_scaled import CloudScaledWeatherProvider
from solarpredict.weather.base import WeatherProvider


class FixedIrradianceProvider(WeatherProvider):
    """Baseline provider with fixed irradiance (no clouds)."""

    def __init__(self):
        idx = pd.date_range("2025-06-01 10:00", periods=4, freq="1h", tz="UTC")
        self.df = pd.DataFrame(
            {
                "ghi_wm2": [800, 800, 800, 800],
                "dni_wm2": [700, 700, 700, 700],
                "dhi_wm2": [100, 100, 100, 100],
                "temp_air_c": [20, 20, 20, 20],
                "wind_ms": [1, 1, 1, 1],
            },
            index=idx,
        )

    def get_forecast(self, locations, start, end, timestep="1h"):
        return {loc["id"]: self.df.copy() for loc in locations}


class CloudOnlyProvider(WeatherProvider):
    """Provides cloud cover for cloud-scaled mode; lacks irradiance columns by design."""

    def __init__(self):
        idx = pd.date_range("2025-06-01 10:00", periods=4, freq="1h", tz="UTC")
        self.df = pd.DataFrame(
            {
                "temp_air_c": [20, 20, 20, 20],
                "wind_ms": [1, 1, 1, 1],
                "cloudcover": [0, 0, 80, 100],
            },
            index=idx,
        )

    def get_forecast(self, locations, start, end, timestep="1h"):
        return {loc["id"]: self.df.copy() for loc in locations}


def _scenario():
    loc = Location(id="loc", lat=0, lon=0, tz="UTC")
    site = Site(
        id="s",
        location=loc,
        arrays=[
            PVArray(
                id="a",
                tilt_deg=0,
                azimuth_deg=0,
                pdc0_w=1000,
                gamma_pdc=-0.004,
                dc_ac_ratio=1.0,
                eta_inv_nom=0.96,
                losses_percent=0,
                temp_model="close_mount_glass_glass",
            )
        ],
    )
    return Scenario(sites=[site])


def test_cloud_scaled_cuts_energy_vs_clear():
    scenario = _scenario()
    clear_provider = FixedIrradianceProvider()
    clear_result = simulate_day(
        scenario,
        dt.date(2025, 6, 1),
        timestep="1h",
        weather_provider=clear_provider,
        weather_mode="standard",
    )

    cloud_provider = CloudScaledWeatherProvider(base_provider=CloudOnlyProvider())
    cloud_scaled_result = simulate_day(
        scenario,
        dt.date(2025, 6, 1),
        timestep="1h",
        weather_provider=cloud_provider,
        weather_mode="cloud-scaled",
    )

    clear_energy = clear_result.daily.iloc[0].energy_kwh
    cloudy_energy = cloud_scaled_result.daily.iloc[0].energy_kwh

    assert cloudy_energy < clear_energy
    # Heavy clouds second half should cut at least ~10% with default mapping.
    assert cloudy_energy <= clear_energy * 0.9
