import numpy as np
import pandas as pd

from solarpredict.solar.temperature import cell_temperature


def test_sapm_example_matches_pvlib_docs():
    # Example from pvlib docs: sapm_cell(1000, 10, 0, params) ≈ 44.117
    ts = pd.date_range("2025-06-01", periods=1, freq="h", tz="UTC")
    poa = pd.Series([1000.0], index=ts)
    tair = pd.Series([10.0], index=ts)
    wind = pd.Series([0.0], index=ts)

    out = cell_temperature(poa, tair, wind)

    assert np.isclose(out.iloc[0], 44.117, atol=0.01)


def test_temp_monotonic_with_poa():
    ts = pd.date_range("2025-06-01", periods=3, freq="h", tz="UTC")
    poa = pd.Series([0.0, 500.0, 1000.0], index=ts)
    tair = pd.Series([20.0, 20.0, 20.0], index=ts)
    wind = pd.Series([1.0, 1.0, 1.0], index=ts)

    temps = cell_temperature(poa, tair, wind)

    assert temps.is_monotonic_increasing
