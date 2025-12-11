import pytest

from solarpredict.core.config import ConfigError, _parse_array


def test_horizon_csv_parse():
    arr = _parse_array(
        {
            "id": "a",
            "tilt_deg": 10,
            "azimuth_deg": 0,
            "pdc0_w": 1000,
            "gamma_pdc": -0.004,
            "dc_ac_ratio": 1.1,
            "eta_inv_nom": 0.96,
            "losses_percent": 5,
            "temp_model": "x",
            "horizon_deg": "0,10,20,30,40,50,60,70,80,80,70,60",
        }
    )
    assert len(arr.horizon_deg) == 12
    assert arr.horizon_deg[1] == 10.0


def test_horizon_invalid_shape_raises():
    with pytest.raises(ConfigError):
        _parse_array(
            {
                "id": "a",
                "tilt_deg": 10,
                "azimuth_deg": 0,
                "pdc0_w": 1000,
                "gamma_pdc": -0.004,
                "dc_ac_ratio": 1.1,
                "eta_inv_nom": 0.96,
                "losses_percent": 5,
                "temp_model": "x",
                "horizon_deg": {"bad": "type"},
            }
        )
