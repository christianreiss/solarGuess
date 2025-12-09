import datetime as dt
import pandas as pd
import pytest

from solarpredict.engine.simulate import simulate_day
from solarpredict.core.models import Scenario, Site, Location, PVArray


class FlatWeather:
    def __init__(self):
        idx = pd.date_range("2025-06-01 12:00", periods=4, freq="1h", tz="UTC", inclusive="left")
        self.df = pd.DataFrame(
            {
                "ghi_wm2": [1000.0] * 4,
                "dni_wm2": [800.0] * 4,
                "dhi_wm2": [200.0] * 4,
                "temp_air_c": [25.0] * 4,
                "wind_ms": [1.0] * 4,
            },
            index=idx,
        )

    def get_forecast(self, locations, start, end, timestep):
        return {loc["id"]: self.df for loc in locations}


def _scenario(shared: bool):
    loc = Location(id="loc", lat=0, lon=0, tz="UTC")
    arrays = [
        PVArray(id="a1", tilt_deg=30, azimuth_deg=180, pdc0_w=4000, gamma_pdc=-0.004, dc_ac_ratio=1.1,
                eta_inv_nom=0.96, losses_percent=0, temp_model="close_mount_glass_glass",
                inverter_group_id="g1" if shared else None,
                inverter_pdc0_w=3000 if shared else None),
        PVArray(id="a2", tilt_deg=30, azimuth_deg=180, pdc0_w=4000, gamma_pdc=-0.004, dc_ac_ratio=1.1,
                eta_inv_nom=0.96, losses_percent=0, temp_model="close_mount_glass_glass",
                inverter_group_id="g1" if shared else None,
                inverter_pdc0_w=3000 if shared else None),
    ]
    return Scenario(sites=[Site(id="s", location=loc, arrays=arrays)])


def test_shared_inverter_reduces_peak_clipping():
    wx = FlatWeather()
    separate = simulate_day(_scenario(shared=False), dt.date(2025, 6, 1), timestep="1h", weather_provider=wx)
    shared = simulate_day(_scenario(shared=True), dt.date(2025, 6, 1), timestep="1h", weather_provider=wx)

    # Shared inverter is smaller (explicit 3 kW DC) so peaks/energy drop vs separate 2x3.6kW derived
    peak_sep = max(separate.daily["peak_kw"])
    peak_shared = max(shared.daily["peak_kw"])
    assert peak_shared < peak_sep

    energy_sep = separate.daily["energy_kwh"].sum()
    energy_shared = shared.daily["energy_kwh"].sum()
    assert energy_shared < energy_sep


def test_shared_inverter_uses_explicit_pdc0():
    wx = FlatWeather()
    shared = simulate_day(_scenario(shared=True), dt.date(2025, 6, 1), timestep="1h", weather_provider=wx)

    # Explicit inverter_pdc0_w=3000 with eta_inv_nom=0.96 => pac0=2880, split across two arrays
    expected_peak_kw = 2880 / 2 / 1000
    peak_shared = max(shared.daily["peak_kw"])
    assert peak_shared == pytest.approx(expected_peak_kw, rel=0.02)


def test_shared_inverter_conflicting_sizes_raise():
    wx = FlatWeather()
    loc = Location(id="loc", lat=0, lon=0, tz="UTC")
    arrays = [
        PVArray(id="a1", tilt_deg=30, azimuth_deg=180, pdc0_w=4000, gamma_pdc=-0.004, dc_ac_ratio=1.1,
                eta_inv_nom=0.96, losses_percent=0, temp_model="close_mount_glass_glass",
                inverter_group_id="g1", inverter_pdc0_w=7000),
        PVArray(id="a2", tilt_deg=30, azimuth_deg=180, pdc0_w=4000, gamma_pdc=-0.004, dc_ac_ratio=1.1,
                eta_inv_nom=0.96, losses_percent=0, temp_model="close_mount_glass_glass",
                inverter_group_id="g1", inverter_pdc0_w=6500),
    ]
    scenario = Scenario(sites=[Site(id="s", location=loc, arrays=arrays)])

    with pytest.raises(ValueError):
        simulate_day(scenario, dt.date(2025, 6, 1), timestep="1h", weather_provider=wx)
