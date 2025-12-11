import numpy as np
import pandas as pd

from solarpredict.solar.incidence import apply_iam


def test_apply_iam_no_model_passthrough():
    idx = pd.date_range("2025-06-01", periods=2, freq="1h", tz="UTC")
    df = pd.DataFrame({"poa_direct": [100.0, 200.0], "poa_diffuse": [50.0, 60.0], "poa_ground_diffuse": [10.0, 10.0]}, index=idx)
    out = apply_iam(df, iam_model=None, aoi=None)
    pd.testing.assert_frame_equal(out, df)


def test_apply_iam_ashrae_scales_direct():
    idx = pd.date_range("2025-06-01", periods=2, freq="1h", tz="UTC")
    df = pd.DataFrame({"poa_direct": [100.0, 200.0], "poa_diffuse": [50.0, 60.0], "poa_ground_diffuse": [10.0, 10.0]}, index=idx)
    aoi = pd.Series([0.0, 60.0], index=idx)
    out = apply_iam(df, iam_model="ashrae", iam_coefficient=0.05, aoi=aoi)
    assert (out["poa_direct"] <= df["poa_direct"]).all()
    assert np.isclose(out.loc[idx[0], "poa_global"], out.loc[idx[0], "poa_direct"] + 50 + 10)
