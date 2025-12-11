import pandas as pd

from solarpredict.solar.clear_sky import clear_sky_irradiance


def test_clear_sky_shapes_and_units():
    times = pd.date_range("2025-06-01", periods=3, freq="1h", tz="UTC")
    cs = clear_sky_irradiance(lat=0.0, lon=0.0, times=times, tz="UTC", elevation_m=0)
    assert list(cs.columns) == ["ghi_wm2", "dni_wm2", "dhi_wm2"]
    assert len(cs) == 3
    assert (cs >= 0).all().all()
