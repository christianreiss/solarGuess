import pandas as pd
from solarpredict.engine.simulate import _apply_time_label


def test_apply_time_label_end_shifts_backwards():
    times = pd.to_datetime([
        "2025-01-01T00:00:00Z",
        "2025-01-01T01:00:00Z",
    ])
    shifted = _apply_time_label(times, 3600, "end")
    assert list(shifted) == list(pd.to_datetime(["2024-12-31T23:30:00Z", "2025-01-01T00:30:00Z"]))


def test_apply_time_label_start_shifts_forwards():
    times = pd.to_datetime([
        "2025-01-01T00:00:00Z",
        "2025-01-01T01:00:00Z",
    ])
    shifted = _apply_time_label(times, 3600, "start")
    assert list(shifted) == list(pd.to_datetime(["2025-01-01T00:30:00Z", "2025-01-01T01:30:00Z"]))


def test_apply_time_label_center_noop():
    times = pd.to_datetime([
        "2025-01-01T00:00:00Z",
        "2025-01-01T01:00:00Z",
    ])
    shifted = _apply_time_label(times, 3600, "center")
    assert list(shifted) == list(times)


def test_apply_time_label_invalid_raises():
    times = pd.to_datetime(["2025-01-01T00:00:00Z"])
    try:
        _apply_time_label(times, 3600, "bogus")
    except ValueError:
        return
    assert False, "expected ValueError"
