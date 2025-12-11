import pandas as pd

from solarpredict.solar.irradiance import _interp_horizon, poa_irradiance


def test_interp_horizon_wraps_and_interpolates():
    horizon = [0, 10, 20, 30] * 3  # 12 bins, 30 deg each
    az = pd.Series([0, 15, 30, 345], index=pd.date_range("2025-01-01", periods=4, freq="1h", tz="UTC"))
    interp = _interp_horizon(horizon, az)
    assert interp.iloc[0] == 0  # exact bin
    assert interp.iloc[1] == 5  # halfway between 0 and 10
    assert interp.iloc[2] == 10  # next bin start
    # wrap: 345 between last bin (330->0) and first -> halfway gives 15 (30→0 wrap short path)
    assert abs(interp.iloc[3] - 15) < 1e-6


def test_poa_direct_zeroed_when_below_horizon():
    times = pd.date_range("2025-01-01 12:00", periods=3, freq="1h", tz="UTC")
    solar_zenith = pd.Series([100, 70, 100], index=times)  # elevations -10, 20, -10
    solar_azimuth = pd.Series([0, 90, 180], index=times)
    horizon = [0] * 12  # flat horizon
    # Only middle point should pass (elevation 20 > horizon 0)
    df = poa_irradiance(
        surface_tilt=30,
        surface_azimuth=180,
        dni=pd.Series([500, 500, 500], index=times),
        ghi=pd.Series([600, 600, 600], index=times),
        dhi=pd.Series([100, 100, 100], index=times),
        solar_zenith=solar_zenith,
        solar_azimuth=solar_azimuth,
        horizon_deg=horizon,
    )
    assert df["poa_direct"].iloc[0] == 0
    assert df["poa_direct"].iloc[2] == 0
    assert df["poa_direct"].iloc[1] > 0
