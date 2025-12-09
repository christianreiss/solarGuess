import numpy as np
import pandas as pd

from solarpredict.pv.power import pvwatts_dc


def test_pvwatts_dc_matches_formula():
    # Arrange: simple single-point inputs from pvlib docs
    ei = pd.Series([1000.0], index=pd.date_range("2025-06-01", periods=1, freq="h", tz="UTC"))
    temp_cell = pd.Series([25.0], index=ei.index)
    pdc0 = 5000.0
    gamma = -0.004

    # Act
    pdc = pvwatts_dc(ei, temp_cell, pdc0_w=pdc0, gamma_pdc=gamma)

    # Assert: PVWatts formula: pdc = G/1000 * pdc0 * (1 + gamma*(Tc-25))
    expected = ei.iloc[0] * 0.001 * pdc0 * (1 + gamma * (temp_cell.iloc[0] - 25.0))
    assert np.isclose(pdc.iloc[0], expected)
