import numpy as np
import pandas as pd

from solarpredict.solar.decomposition import fill_dni_dhi


def test_fill_dni_dhi_uses_existing():
    idx = pd.date_range("2025-06-01", periods=3, freq="1h", tz="UTC")
    ghi = pd.Series([500, 600, 700], index=idx)
    dni = pd.Series([400, 500, 600], index=idx)
    dhi = pd.Series([100, 120, 130], index=idx)

    out_dni, out_dhi = fill_dni_dhi(ghi, solar_zenith_deg=pd.Series([30, 40, 50], index=idx), dni_wm2=dni, dhi_wm2=dhi)

    pd.testing.assert_series_equal(out_dni, dni)
    pd.testing.assert_series_equal(out_dhi, dhi)


def test_fill_dni_dhi_erbs_when_missing():
    idx = pd.date_range("2025-06-01", periods=3, freq="1h", tz="UTC")
    ghi = pd.Series([0.0, 500.0, 0.0], index=idx)
    zenith = pd.Series([80.0, 40.0, 85.0], index=idx)

    dni, dhi = fill_dni_dhi(ghi, solar_zenith_deg=zenith, dni_wm2=None, dhi_wm2=None)

    assert (dni >= 0).all()
    assert (dhi >= 0).all()
    mid_cosz = np.cos(np.deg2rad(40.0))
    assert abs((dhi.iloc[1] + dni.iloc[1] * mid_cosz) - ghi.iloc[1]) < 1e-3

