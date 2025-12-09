import datetime as dt
import pandas as pd
from solarpredict.engine.simulate import simulate_day
from solarpredict.core.models import Scenario, Site, Location, PVArray
from solarpredict.core.debug import ListDebugCollector


def test_debug_uses_site_id_not_location_id():
    loc = Location(id="loc-should-not-leak", lat=0, lon=0, tz="UTC")
    site = Site(id="site-1", location=loc, arrays=[
        PVArray(id="arr", tilt_deg=0, azimuth_deg=0, pdc0_w=1000, gamma_pdc=-0.004,
                dc_ac_ratio=1.0, eta_inv_nom=0.96, losses_percent=0, temp_model="close_mount_glass_glass")
    ])
    scenario = Scenario(sites=[site])

    idx = pd.date_range("2025-01-01", periods=2, freq="1h", tz="UTC", inclusive="left")
    df = pd.DataFrame({
        "ghi_wm2": [500.0, 500.0],
        "dni_wm2": [400.0, 400.0],
        "dhi_wm2": [100.0, 100.0],
        "temp_air_c": [20.0, 20.0],
        "wind_ms": [1.0, 1.0],
    }, index=idx)

    class DummyWeather:
        def get_forecast(self, locations, start, end, timestep):
            return {site.id: df}

    debug = ListDebugCollector()
    simulate_day(scenario, dt.date(2025, 1, 1), timestep="1h", weather_provider=DummyWeather(), debug=debug)

    sites = {e.get("site") for e in debug.events if "site" in e and e.get("stage") != "weather.request"}
    assert sites == {"site-1"}
    assert "loc-should-not-leak" not in sites
