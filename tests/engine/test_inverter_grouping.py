import datetime as dt
import pandas as pd

from solarpredict.engine.simulate import simulate_day
from solarpredict.core.models import Scenario, Site, Location, PVArray


class FlatWeather:
    def __init__(self):
        idx = pd.date_range("2025-06-01", periods=4, freq="1h", tz="UTC", inclusive="left")
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
                inverter_group_id="g1" if shared else None),
        PVArray(id="a2", tilt_deg=30, azimuth_deg=180, pdc0_w=4000, gamma_pdc=-0.004, dc_ac_ratio=1.1,
                eta_inv_nom=0.96, losses_percent=0, temp_model="close_mount_glass_glass",
                inverter_group_id="g1" if shared else None),
    ]
    return Scenario(sites=[Site(id="s", location=loc, arrays=arrays)])


def test_shared_inverter_reduces_peak_clipping():
    wx = FlatWeather()
    separate = simulate_day(_scenario(shared=False), dt.date(2025, 6, 1), timestep="1h", weather_provider=wx)
    shared = simulate_day(_scenario(shared=True), dt.date(2025, 6, 1), timestep="1h", weather_provider=wx)

    # With shared inverter, each array should see lower peak due to combined clipping
    peak_sep = max(separate.daily["peak_kw"])
    peak_shared = max(shared.daily["peak_kw"])
    assert peak_shared <= peak_sep

    # Energy should also reduce slightly when sharing an inverter
    energy_sep = separate.daily["energy_kwh"].sum()
    energy_shared = shared.daily["energy_kwh"].sum()
    assert energy_shared <= energy_sep
