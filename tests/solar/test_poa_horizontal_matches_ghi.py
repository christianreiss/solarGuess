import pandas as pd
import numpy as np

from solarpredict.solar.irradiance import poa_irradiance


def test_poa_horizontal_matches_ghi():
    times = pd.date_range("2025-06-01 12:00", periods=3, freq="1h", tz="UTC")

    solar_zenith = pd.Series([30, 30, 30], index=times)
    solar_azimuth = pd.Series([180, 180, 180], index=times)

    dni = pd.Series([800.0, 800.0, 800.0], index=times)
    dhi = pd.Series([100.0, 100.0, 100.0], index=times)
    ghi_val = 800.0 * np.cos(np.deg2rad(30)) + 100.0
    ghi = pd.Series([ghi_val] * 3, index=times)

    df = poa_irradiance(
        surface_tilt=0,
        surface_azimuth=180,
        dni=dni,
        ghi=ghi,
        dhi=dhi,
        solar_zenith=solar_zenith,
        solar_azimuth=solar_azimuth,
        albedo=0.2,
    )

    assert np.allclose(df["poa_global"], ghi, rtol=1e-6, atol=1e-6)
