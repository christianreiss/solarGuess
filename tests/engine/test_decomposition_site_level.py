import datetime as dt

import pandas as pd

from solarpredict.core.debug import ListDebugCollector
from solarpredict.core.models import Location, PVArray, Scenario, Site
from solarpredict.engine.simulate import simulate_day


class DummyWeatherProvider:
    def get_forecast(self, locations, start, end, timestep="1h"):
        idx = pd.date_range(start, end, freq=timestep, tz="UTC", inclusive="left")
        df = pd.DataFrame(
            {
                "ghi_wm2": 800.0,
                # Provide NaNs to force decomposition, but keep schema satisfied.
                "dni_wm2": float("nan"),
                "dhi_wm2": float("nan"),
                "temp_air_c": 20.0,
                "wind_ms": 1.0,
            },
            index=idx,
        )
        return {loc["id"]: df.copy() for loc in locations}


def test_decomposition_runs_once_per_site_not_per_array():
    loc = Location(id="loc", lat=0, lon=0, tz="UTC")
    site = Site(
        id="s",
        location=loc,
        arrays=[
            PVArray(
                id="a1",
                tilt_deg=30,
                azimuth_deg=180,
                pdc0_w=1000,
                gamma_pdc=-0.004,
                dc_ac_ratio=1.1,
                eta_inv_nom=0.96,
                losses_percent=0,
                temp_model="close_mount_glass_glass",
            ),
            PVArray(
                id="a2",
                tilt_deg=30,
                azimuth_deg=180,
                pdc0_w=1000,
                gamma_pdc=-0.004,
                dc_ac_ratio=1.1,
                eta_inv_nom=0.96,
                losses_percent=0,
                temp_model="close_mount_glass_glass",
            ),
        ],
    )
    scenario = Scenario(sites=[site])

    debug = ListDebugCollector()
    simulate_day(
        scenario,
        dt.date(2025, 6, 1),
        timestep="1h",
        weather_provider=DummyWeatherProvider(),
        debug=debug,
        weather_label="end",
    )

    decompose = [e for e in debug.events if e["stage"] == "weather.decompose"]
    assert len(decompose) == 1
    assert decompose[0]["site"] == "s"
    assert decompose[0]["array"] is None

