import pandas as pd
import pytest

from solarpredict.core.debug import ListDebugCollector
from solarpredict.solar.snow import snow_cover_loss


def test_snow_cover_loss_uses_depth_linear_mapping():
    idx = pd.date_range("2025-01-01", periods=3, freq="1h", tz="UTC")
    df = pd.DataFrame(
        {
            "snow_depth_cm": [0.0, 2.75, 5.0],
            "temp_air_c": [0.0, 0.0, 0.0],
        },
        index=idx,
    )
    debug = ListDebugCollector()
    res = snow_cover_loss(df, debug=debug)

    assert res.factor.iloc[0] == pytest.approx(1.0)
    assert res.factor.iloc[1] == pytest.approx(0.65)
    assert res.factor.iloc[2] == pytest.approx(0.3)
    assert any(e["stage"] == "snow.loss.summary" for e in debug.events)


def test_snow_cover_loss_falls_back_to_precip_below_temp():
    idx = pd.date_range("2025-01-01", periods=2, freq="1h", tz="UTC")
    df = pd.DataFrame(
        {
            "precip_mm": [1.0, 1.0],
            "temp_air_c": [2.0, -2.0],
        },
        index=idx,
    )
    res = snow_cover_loss(df)

    assert res.factor.iloc[0] == pytest.approx(1.0)
    assert res.factor.iloc[1] < 1.0
