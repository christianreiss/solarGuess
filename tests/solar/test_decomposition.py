import numpy as np
import pandas as pd
import pytest

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


def test_fill_dni_dhi_fills_missing_dhi_by_closure_when_dni_present():
    idx = pd.date_range("2025-06-01", periods=3, freq="1h", tz="UTC")
    ghi = pd.Series([500.0, 600.0, 700.0], index=idx)
    zenith = pd.Series([30.0, 40.0, 50.0], index=idx)
    dni = pd.Series([400.0, 500.0, 600.0], index=idx)

    out_dni, out_dhi = fill_dni_dhi(ghi, solar_zenith_deg=zenith, dni_wm2=dni, dhi_wm2=None)

    cosz = np.cos(np.deg2rad(zenith.astype(float)))
    expected_dhi = (ghi - dni * cosz).clip(lower=0.0)
    pd.testing.assert_series_equal(out_dni, dni)
    pd.testing.assert_series_equal(out_dhi, expected_dhi)


def test_fill_dni_dhi_fills_missing_dni_by_closure_when_dhi_present():
    idx = pd.date_range("2025-06-01", periods=3, freq="1h", tz="UTC")
    ghi = pd.Series([500.0, 600.0, 700.0], index=idx)
    zenith = pd.Series([30.0, 40.0, 50.0], index=idx)
    dhi = pd.Series([100.0, 120.0, 130.0], index=idx)

    out_dni, out_dhi = fill_dni_dhi(ghi, solar_zenith_deg=zenith, dni_wm2=None, dhi_wm2=dhi)

    cosz = np.cos(np.deg2rad(zenith.astype(float)))
    expected_dni = ((ghi - dhi) / cosz).where(cosz > 1e-6, 0.0).clip(lower=0.0)
    pd.testing.assert_series_equal(out_dhi, dhi)
    pd.testing.assert_series_equal(out_dni, expected_dni)


def test_fill_dni_dhi_preserves_existing_and_only_fills_nans():
    idx = pd.date_range("2025-06-01", periods=3, freq="1h", tz="UTC")
    ghi = pd.Series([500.0, 600.0, 700.0], index=idx)
    zenith = pd.Series([30.0, 40.0, 50.0], index=idx)
    dni = pd.Series([400.0, np.nan, 600.0], index=idx)
    dhi = pd.Series([100.0, 120.0, np.nan], index=idx)

    out_dni, out_dhi = fill_dni_dhi(ghi, solar_zenith_deg=zenith, dni_wm2=dni, dhi_wm2=dhi)

    cosz = np.cos(np.deg2rad(zenith.astype(float)))
    # Row 2: dni was NaN, dhi present -> closure for dni
    expected_dni_1 = ((ghi.iloc[1] - dhi.iloc[1]) / cosz.iloc[1]) if cosz.iloc[1] > 1e-6 else 0.0
    assert out_dni.iloc[1] == pytest.approx(max(0.0, expected_dni_1))
    # Row 3: dhi was NaN, dni present -> closure for dhi
    expected_dhi_2 = ghi.iloc[2] - dni.iloc[2] * cosz.iloc[2]
    assert out_dhi.iloc[2] == pytest.approx(max(0.0, expected_dhi_2))
    # Row 1: values preserved
    assert out_dni.iloc[0] == 400.0
    assert out_dhi.iloc[0] == 100.0
