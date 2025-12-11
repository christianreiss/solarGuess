import pandas as pd

from solarpredict.weather.cloud_scaled import default_cloud_to_clearness


def test_clearness_edges_and_midpoints():
    cloud = pd.Series([0.0, 0.5, 1.0, -0.2, 1.5])
    kt = default_cloud_to_clearness(cloud)
    assert kt.iloc[0] == 1.0  # clear sky
    assert 0.9 < kt.iloc[1] < 0.95  # mid coverage reduces modestly
    assert round(float(kt.iloc[2]), 2) == 0.25  # 1 - 0.75*1**3.4
    # clamps
    assert kt.iloc[3] == 1.0
    assert kt.iloc[4] == 0.25
