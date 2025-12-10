import pandas as pd
import pytest

from solarpredict.engine.simulate import _interval_hours


def test_center_label_single_point_uses_declared_step():
    idx = pd.DatetimeIndex([pd.Timestamp("2025-01-01T00:00Z")])
    hours = _interval_hours(idx, step_seconds=3600, label="center")
    assert hours.iloc[0] == 1.0


def test_center_label_raises_when_no_step_and_nan():
    idx = pd.DatetimeIndex([pd.Timestamp("2025-01-01T00:00Z")])
    with pytest.raises(ValueError):
        _interval_hours(idx, step_seconds=0, label="center")


def test_irregular_center_uses_inferred_steps_when_available():
    idx = pd.DatetimeIndex(["2025-01-01T00:00Z", "2025-01-01T03:00Z", "2025-01-01T04:00Z"])
    hours = _interval_hours(idx, step_seconds=0, label="center")
    assert list(hours) == [3.0, 1.0, 1.0]  # last uses fallback
