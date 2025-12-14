import pandas as pd
import pytest

from solarpredict.core.debug import ListDebugCollector
from solarpredict.engine.simulate import SimulationResult, apply_array_scale_factors, apply_output_scale


def test_apply_output_scale_scales_power_and_energy_only():
    idx = pd.date_range("2025-06-01T00:00:00Z", periods=3, freq="1h")
    ts_df = pd.DataFrame(
        {
            "poa_global": [100.0, 200.0, 300.0],
            "temp_cell_c": [25.0, 25.0, 25.0],
            "pdc_w": [1000.0, 2000.0, 3000.0],
            "pac_w": [900.0, 1800.0, 2700.0],
            "pac_net_w": [850.0, 1700.0, 2550.0],
            "interval_h": [1.0, 1.0, 1.0],
        },
        index=idx,
    )
    daily = pd.DataFrame([{"site": "s", "array": "a", "date": "2025-06-01", "energy_kwh": 5.0, "peak_kw": 3.0}])
    result = SimulationResult(daily=daily, timeseries={("s", "a"): ts_df})

    debug = ListDebugCollector()
    scaled = apply_output_scale(result, 0.5, debug=debug)

    assert result.daily.iloc[0].energy_kwh == 5.0
    assert scaled.daily.iloc[0].energy_kwh == pytest.approx(2.5)
    assert scaled.daily.iloc[0].peak_kw == pytest.approx(1.5)

    ts_scaled = scaled.timeseries[("s", "a")]
    assert ts_scaled["pdc_w"].tolist() == pytest.approx([500.0, 1000.0, 1500.0])
    assert ts_scaled["pac_w"].tolist() == pytest.approx([450.0, 900.0, 1350.0])
    assert ts_scaled["pac_net_w"].tolist() == pytest.approx([425.0, 850.0, 1275.0])
    # Weather-driven columns unchanged.
    assert ts_scaled["poa_global"].tolist() == pytest.approx([100.0, 200.0, 300.0])
    assert ts_scaled["interval_h"].tolist() == pytest.approx([1.0, 1.0, 1.0])

    applied = [e for e in debug.events if e["stage"] == "calibration.scale_factor"]
    assert applied
    assert applied[-1]["payload"]["applied"] is True


def test_apply_output_scale_rejects_non_positive():
    debug = ListDebugCollector()
    result = SimulationResult(daily=pd.DataFrame(), timeseries={})
    with pytest.raises(ValueError):
        apply_output_scale(result, 0.0, debug=debug)


def test_apply_array_scale_factors_scales_matching_arrays_only():
    idx = pd.date_range("2025-06-01T00:00:00Z", periods=2, freq="1h")
    ts_a = pd.DataFrame(
        {
            "poa_global": [100.0, 200.0],
            "temp_cell_c": [25.0, 25.0],
            "pdc_w": [1000.0, 2000.0],
            "pac_w": [900.0, 1800.0],
            "pac_net_w": [850.0, 1700.0],
            "interval_h": [1.0, 1.0],
        },
        index=idx,
    )
    ts_b = ts_a.copy()

    daily = pd.DataFrame(
        [
            {"site": "s", "array": "a", "date": "2025-06-01", "energy_kwh": 5.0, "peak_kw": 2.0},
            {"site": "s", "array": "b", "date": "2025-06-01", "energy_kwh": 6.0, "peak_kw": 3.0},
        ]
    )
    result = SimulationResult(daily=daily, timeseries={("s", "a"): ts_a, ("s", "b"): ts_b})

    debug = ListDebugCollector()
    scaled = apply_array_scale_factors(result, {"a": 0.5}, debug=debug)

    # Array a scaled.
    assert scaled.daily.loc[scaled.daily["array"] == "a", "energy_kwh"].iloc[0] == pytest.approx(2.5)
    assert scaled.daily.loc[scaled.daily["array"] == "a", "peak_kw"].iloc[0] == pytest.approx(1.0)
    assert scaled.timeseries[("s", "a")]["pac_net_w"].tolist() == pytest.approx([425.0, 850.0])

    # Array b untouched.
    assert scaled.daily.loc[scaled.daily["array"] == "b", "energy_kwh"].iloc[0] == pytest.approx(6.0)
    assert scaled.timeseries[("s", "b")]["pac_net_w"].tolist() == pytest.approx([850.0, 1700.0])
    assert scaled.timeseries[("s", "b")]["poa_global"].tolist() == pytest.approx([100.0, 200.0])


def test_apply_array_scale_factors_site_specific_key_takes_precedence():
    idx = pd.date_range("2025-06-01T00:00:00Z", periods=1, freq="1h")
    ts = pd.DataFrame(
        {
            "poa_global": [100.0],
            "temp_cell_c": [25.0],
            "pdc_w": [1000.0],
            "pac_w": [900.0],
            "pac_net_w": [850.0],
            "interval_h": [1.0],
        },
        index=idx,
    )
    daily = pd.DataFrame([{"site": "s", "array": "a", "date": "2025-06-01", "energy_kwh": 5.0, "peak_kw": 2.0}])
    result = SimulationResult(daily=daily, timeseries={("s", "a"): ts})

    debug = ListDebugCollector()
    scaled = apply_array_scale_factors(result, {"a": 0.5, "s/a": 0.25}, debug=debug)
    assert scaled.daily.iloc[0].energy_kwh == pytest.approx(1.25)
    assert scaled.timeseries[("s", "a")]["pac_net_w"].iloc[0] == pytest.approx(212.5)


def test_apply_array_scale_factors_rejects_non_positive():
    idx = pd.date_range("2025-06-01T00:00:00Z", periods=1, freq="1h")
    ts = pd.DataFrame(
        {
            "poa_global": [100.0],
            "temp_cell_c": [25.0],
            "pdc_w": [1000.0],
            "pac_w": [900.0],
            "pac_net_w": [850.0],
            "interval_h": [1.0],
        },
        index=idx,
    )
    daily = pd.DataFrame([{"site": "s", "array": "a", "date": "2025-06-01", "energy_kwh": 5.0, "peak_kw": 2.0}])
    result = SimulationResult(daily=daily, timeseries={("s", "a"): ts})

    debug = ListDebugCollector()
    with pytest.raises(ValueError):
        apply_array_scale_factors(result, {"a": 0.0}, debug=debug)
