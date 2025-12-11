import datetime as dt

import pandas as pd
import pytest

from solarpredict.engine.simulate import SimulationResult, apply_actual_adjustment
from solarpredict.core.debug import ListDebugCollector


def _ts_df(start="2025-06-01T00:00:00Z", periods=4, freq="1h", watts=(0, 1000, 1000, 1000)):
    idx = pd.date_range(start, periods=periods, freq=freq)
    df = pd.DataFrame(
        {
            "poa_global": [500.0] * periods,
            "temp_cell_c": [25.0] * periods,
            "pdc_w": watts,
            "pac_w": watts,
            "pac_net_w": watts,
            "interval_h": [1.0] * periods,
        },
        index=idx,
    )
    return df


def _result(df):
    return SimulationResult(
        daily=pd.DataFrame([{"site": "s", "array": "a", "date": df.index[0].date().isoformat(), "energy_kwh": 3.0}]),
        timeseries={("s", "a"): df},
    )


def test_scaling_only_future_samples():
    df = _ts_df()
    result = _result(df)
    debug = ListDebugCollector()
    # Actual to-now (1.5 kWh vs 1.0 predicted) -> scale future by 1.5x
    adjusted = apply_actual_adjustment(result, actual_kwh_today=1.5, debug=debug, now_ts=df.index[1])
    scaled = adjusted.timeseries[("s", "a")]
    # First two samples unchanged
    assert scaled.iloc[0]["pac_net_w"] == 0
    assert scaled.iloc[1]["pac_net_w"] == 1000
    # Future samples scaled by 1.5x
    assert scaled.iloc[2]["pac_net_w"] == 1500
    assert scaled.iloc[3]["pac_net_w"] == 1500
    # Daily energy recomputed
    energy = float(((scaled["pac_net_w"] * scaled["interval_h"]) / 1000.0).sum())
    assert pytest.approx(4.0) == energy
    assert any(e["stage"] == "actual.adjust.applied" for e in debug.events)


def test_zero_actual_resets_without_scaling():
    df = _ts_df()
    result = _result(df)
    debug = ListDebugCollector()
    adjusted = apply_actual_adjustment(result, actual_kwh_today=0.0, debug=debug, now_ts=df.index[1])
    assert adjusted.timeseries[("s", "a")].equals(df)
    assert any(e["payload"]["reason"] == "reset" for e in debug.events)


def test_negative_actual_raises():
    df = _ts_df()
    result = _result(df)
    with pytest.raises(ValueError):
        apply_actual_adjustment(result, actual_kwh_today=-1.0, debug=ListDebugCollector(), now_ts=df.index[0])


def test_no_future_samples_skips():
    df = _ts_df(periods=2, watts=(500, 500))
    result = _result(df)
    debug = ListDebugCollector()
    adjusted = apply_actual_adjustment(result, actual_kwh_today=1.0, debug=debug, now_ts=df.index[-1])
    assert adjusted.timeseries[("s", "a")].equals(df)
    reasons = [e["payload"]["reason"] for e in debug.events if e["stage"] == "actual.adjust.skip"]
    assert "no_future_samples" in reasons


def test_now_clamped_to_window():
    df = _ts_df()
    result = _result(df)
    debug = ListDebugCollector()
    # now before simulation start; should clamp to first ts and scale future
    adjusted = apply_actual_adjustment(result, actual_kwh_today=1.5, debug=debug, now_ts=df.index[0] - pd.Timedelta(hours=2))
    scaled = adjusted.timeseries[("s", "a")]
    # Because first interval has zero energy, we skip scaling (zero_predicted guard)
    assert scaled.equals(df)
    reasons = [e["payload"]["reason"] for e in debug.events if e["stage"] == "actual.adjust.skip"]
    assert "zero_predicted" in reasons
