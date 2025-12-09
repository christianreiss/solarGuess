import pandas as pd

from solarpredict.pv.power import inverter_pdc0_from_dc_ac_ratio, pvwatts_ac


def test_inverter_clipping():
    # Oversized DC should clip at pac0
    dc_ac_ratio = 1.2
    pdc0_array = 6000.0
    eta_nom = 0.96
    pdc0_inv = inverter_pdc0_from_dc_ac_ratio(pdc0_array, dc_ac_ratio, eta_nom)

    # Two samples: one at nameplate, one above
    pdc = pd.Series([pdc0_inv, pdc0_inv * 1.5], index=pd.date_range("2025-06-01", periods=2, freq="h", tz="UTC"))

    pac = pvwatts_ac(pdc, pdc0_inv_w=pdc0_inv, eta_inv_nom=eta_nom)

    pac0 = eta_nom * pdc0_inv
    assert pac.max() <= pac0 + 1e-9
    # First point should be below or equal to pac0, second clipped exactly
    assert pac.iloc[1] == pac0
