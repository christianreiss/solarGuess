import pandas as pd

from solarpredict.engine.load_window import compute_load_windows, find_windows_for_site


def test_find_windows_single_window():
    idx = pd.date_range("2025-01-01", periods=4, freq="15min", tz="UTC")
    pac = pd.Series([100, 600, 700, 100], index=idx)
    interval_h = pd.Series([0.25] * 4, index=idx)

    windows = find_windows_for_site(pac, interval_h, base_load_w=500, min_duration_min=15)
    assert len(windows) == 1
    w = windows[0]
    assert w.duration_min == 30
    assert round(w.energy_wh, 2) == round((600 + 700) * 0.25, 2)
    assert w.start == idx[1]
    assert w.end == idx[3]


def test_compute_load_windows_multiple_and_best():
    idx = pd.date_range("2025-01-01", periods=6, freq="15min", tz="UTC")
    # Two arrays contributing to same site; second array boosts the middle window energy
    pac_a = pd.Series([0, 600, 600, 100, 900, 0], index=idx)
    pac_b = pd.Series([0, 0, 300, 0, 700, 0], index=idx)
    interval_h = pd.Series([0.25] * 6, index=idx)

    timeseries = {
        ("site1", "a"): pd.DataFrame({"pac_net_w": pac_a, "interval_h": interval_h}),
        ("site1", "b"): pd.DataFrame({"pac_net_w": pac_b, "interval_h": interval_h}),
    }

    windows = compute_load_windows(timeseries, base_load_w=500, min_duration_min=15)
    win_list = windows["site1"]["windows"]
    assert len(win_list) == 2
    assert windows["site1"]["earliest"]["start"] == idx[1].isoformat()
    assert windows["site1"]["latest"]["end"] == (idx[4] + pd.Timedelta(minutes=15)).isoformat()
    # Best should be the second window (higher energy)
    assert windows["site1"]["best"]["start"] == idx[4].isoformat()


def test_compute_load_windows_none():
    idx = pd.date_range("2025-01-01", periods=4, freq="15min", tz="UTC")
    pac = pd.Series([100, 200, 300, 400], index=idx)
    interval_h = pd.Series([0.25] * 4, index=idx)

    timeseries = {("s", "a"): pd.DataFrame({"pac_net_w": pac, "interval_h": interval_h})}
    windows = compute_load_windows(timeseries, base_load_w=500, min_duration_min=15)
    assert windows["s"] == {}
