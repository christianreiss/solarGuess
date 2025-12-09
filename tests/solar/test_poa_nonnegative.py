import pandas as pd
import numpy as np

from solarpredict.solar.irradiance import poa_irradiance


def test_poa_nonnegative_clipped():
    times = pd.date_range("2025-01-01 12:00", periods=3, freq="1h", tz="UTC")

    solar_zenith = pd.Series([120, 120, 120], index=times)  # below horizon -> possible negative direct term
    solar_azimuth = pd.Series([0, 0, 0], index=times)

    dni = pd.Series([-5.0, -1.0, 0.0], index=times)  # malformed negative
    dhi = pd.Series([0.0, 0.0, 0.0], index=times)
    ghi = pd.Series([0.0, 0.0, 0.0], index=times)

    df = poa_irradiance(
        surface_tilt=30,
        surface_azimuth=180,
        dni=dni,
        ghi=ghi,
        dhi=dhi,
        solar_zenith=solar_zenith,
        solar_azimuth=solar_azimuth,
        albedo=0.2,
    )

    assert (df[["poa_global", "poa_direct", "poa_diffuse", "poa_ground_diffuse"]] >= 0).all().all()
    assert df["poa_global"].max() == 0.0
