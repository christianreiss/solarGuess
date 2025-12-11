import datetime as dt
import pandas as pd
from solarpredict.engine.simulate import simulate_day
from solarpredict.core.models import Scenario, Site, Location, PVArray


class GapWeather:
    """Weather provider that returns uneven steps (0.5h then 1.5h) to test per-interval integration."""

    def __init__(self):
        # Midday times to avoid night zeros: 12:00, 12:30, 14:00 (gap of 1.5h)
        self.index = pd.to_datetime(
            ["2025-01-01T12:00:00Z", "2025-01-01T12:30:00Z", "2025-01-01T14:00:00Z"]
        )
        vals = [500.0, 500.0, 500.0]
        self.df = pd.DataFrame(
            {
                "ghi_wm2": vals,
                "dni_wm2": vals,
                "dhi_wm2": vals,
                "temp_air_c": [20.0] * 3,
                "wind_ms": [1.0] * 3,
            },
            index=self.index,
        )

    def get_forecast(self, locations, start, end, timestep):
        # Return same weather for all locations
        return {loc["id"]: self.df for loc in locations}


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
                horizon_deg=[0] * 12,
            )
        ],
    )
    return Scenario(sites=[site])


def test_per_interval_energy_counts_gaps():
    scenario = _scenario()
    wx = GapWeather()
    result = simulate_day(scenario, dt.date(2025, 1, 1), timestep="30m", weather_provider=wx, weather_label="end")
    row = result.daily.iloc[0]
    # With end-labeled samples the intervals are: 0.5h (filler for first), 0.5h, 1.5h = 2.5h total.
    # Power is ~0.75 kW across those intervals, so total energy should land ~2.25 kWh.
    assert row["energy_kwh"] > 0
    assert 2.1 <= row["energy_kwh"] <= 2.5
