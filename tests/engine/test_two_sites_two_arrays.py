import datetime as dt

import numpy as np
import pandas as pd

from solarpredict.core.models import Location, PVArray, Scenario, Site
from solarpredict.engine.simulate import simulate_day


class DummyWeatherProvider:
    def __init__(self, ghi=800.0, dni=500.0, dhi=300.0, temp=20.0, wind=1.0):
        self.ghi = ghi
        self.dni = dni
        self.dhi = dhi
        self.temp = temp
        self.wind = wind

    def get_forecast(self, locations, start, end, timestep="1h"):
        idx = pd.date_range(start, end, freq=timestep, tz="UTC", inclusive="left")
        data = {
            "ghi_wm2": pd.Series(self.ghi, index=idx),
            "dni_wm2": pd.Series(self.dni, index=idx),
            "dhi_wm2": pd.Series(self.dhi, index=idx),
            "temp_air_c": pd.Series(self.temp, index=idx),
            "wind_ms": pd.Series(self.wind, index=idx),
        }
        return {loc["id"]: pd.DataFrame(data, index=idx) for loc in locations}


def _scenario():
    loc1 = Location(id="site1", lat=0, lon=0, tz="UTC")
    loc2 = Location(id="site2", lat=10, lon=10, tz="UTC")
    array_a = PVArray(
        id="a",
        tilt_deg=30,
        azimuth_deg=0,
        pdc0_w=5000,
        gamma_pdc=-0.004,
        dc_ac_ratio=1.2,
        eta_inv_nom=0.96,
        losses_percent=10,
        temp_model="close_mount_glass_glass",
    )
    array_b = PVArray(
        id="b",
        tilt_deg=20,
        azimuth_deg=0,
        pdc0_w=6000,
        gamma_pdc=-0.004,
        dc_ac_ratio=1.1,
        eta_inv_nom=0.96,
        losses_percent=5,
        temp_model="close_mount_glass_glass",
    )
    site1 = Site(id="site1", location=loc1, arrays=[array_a])
    site2 = Site(id="site2", location=loc2, arrays=[array_b])
    return Scenario(sites=[site1, site2])


def test_two_sites_two_arrays_energy_matches_constant_weather():
    scenario = _scenario()
    date = dt.date(2025, 6, 1)
    wx = DummyWeatherProvider(ghi=800.0, dni=500.0, dhi=300.0, temp=25.0, wind=1.0)

    result = simulate_day(scenario, date, timestep="1h", weather_provider=wx)

    # With constant inputs, expect same number of records and non-zero energy.
    assert len(result.daily) == 2
    assert all(result.daily["energy_kwh"] > 0)

    # Check energy proportional to array pdc0_w roughly
    energies = dict(zip(result.daily["array"], result.daily["energy_kwh"]))
    assert energies["b"] > energies["a"]

    # Timeseries present for both arrays
    assert ("site1", "a") in result.timeseries
    assert ("site2", "b") in result.timeseries


def test_debug_event_order():
    from solarpredict.core.debug import ListDebugCollector

    scenario = _scenario()
    date = dt.date(2025, 6, 1)
    wx = DummyWeatherProvider()
    debug = ListDebugCollector()

    simulate_day(scenario, date, timestep="1h", weather_provider=wx, debug=debug)

    stages = [e["stage"] for e in debug.events]
    # ensure ordering from weather to aggregate appears
    expected_order = [
        "weather.request",
        "weather.response_meta",
        "weather.response_meta",
        "weather.summary",
        "stage.solarpos",
        "poa.summary",
        "stage.poa",
        "temp_model.params",
        "temp_cell.summary",
        "stage.temp",
        "pv.dc.summary",
        "stage.dc",
        "pv.ac.summary",
        "stage.ac",
        "pv.losses.summary",
        "stage.aggregate",
    ]
    # compare subsequence presence
    idx = 0
    for stage in expected_order:
        try:
            pos = stages.index(stage, idx)
        except ValueError:
            raise AssertionError(f"Stage {stage} missing or out of order")
        idx = pos
