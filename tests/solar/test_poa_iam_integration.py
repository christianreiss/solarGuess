import pandas as pd
import pvlib

from solarpredict.core.debug import ListDebugCollector
from solarpredict.solar.irradiance import poa_irradiance


def test_poa_irradiance_computes_aoi_and_applies_iam():
    idx = pd.date_range("2025-06-01 12:00", periods=2, freq="1h", tz="UTC")

    # Choose a simple geometry where AOI is non-zero (horizontal surface -> AOI=zenith).
    solar_zenith = pd.Series([60.0, 60.0], index=idx)
    solar_azimuth = pd.Series([180.0, 180.0], index=idx)

    dni = pd.Series([800.0, 800.0], index=idx)
    ghi = pd.Series([1000.0, 1000.0], index=idx)
    dhi = pd.Series([200.0, 200.0], index=idx)

    dbg = ListDebugCollector()
    out_no_iam = poa_irradiance(
        surface_tilt=0.0,
        surface_azimuth=180.0,
        dni=dni,
        ghi=ghi,
        dhi=dhi,
        solar_zenith=solar_zenith,
        solar_azimuth=solar_azimuth,
        iam_model=None,
        debug=dbg,
    )

    dbg2 = ListDebugCollector()
    out_iam = poa_irradiance(
        surface_tilt=0.0,
        surface_azimuth=180.0,
        dni=dni,
        ghi=ghi,
        dhi=dhi,
        solar_zenith=solar_zenith,
        solar_azimuth=solar_azimuth,
        iam_model="ashrae",
        iam_coefficient=0.05,
        debug=dbg2,
    )

    # IAM should have been applied (not skipped due to missing AOI).
    stages = [e["stage"] for e in dbg2.events]
    assert "iam.applied" in stages
    assert "iam.skip" not in stages

    # Direct component should be reduced by the ASHRAE factor at AOI=60°.
    aoi = pvlib.irradiance.aoi(0.0, 180.0, solar_zenith, solar_azimuth)
    expected = pvlib.iam.ashrae(aoi, b=0.05).clip(lower=0.0)
    ratio = (out_iam["poa_direct"] / out_no_iam["poa_direct"]).astype(float)
    assert (ratio - expected).abs().max() < 1e-6
