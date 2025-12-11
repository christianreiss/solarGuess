"""Load window finder for controllable loads.

Find contiguous time windows where site net AC power exceeds a base load.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd

from solarpredict.core.debug import DebugCollector, NullDebugCollector


@dataclass(frozen=True)
class LoadWindow:
    start: pd.Timestamp
    end: pd.Timestamp
    duration_min: float
    energy_wh: float
    avg_w: float
    max_w: float

    def to_dict(self) -> dict:
        return {
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "duration_min": self.duration_min,
            "energy_wh": self.energy_wh,
            "avg_w": self.avg_w,
            "max_w": self.max_w,
        }


def _contiguous_runs(mask: pd.Series) -> List[pd.Index]:
    """Return list of index slices representing contiguous True runs.

    The mask and resulting indices are assumed to be time-ordered; a run's end
    is inclusive of the last timestamp where the mask was True.
    """

    runs: List[pd.Index] = []
    if mask.empty:
        return runs

    in_run = False
    start = None
    last_true = None

    for ts, val in mask.items():
        if val:
            if not in_run:
                start = ts
                in_run = True
            last_true = ts
            continue
        # val is False
        if in_run and start is not None:
            runs.append(mask.loc[start:last_true].index)
        in_run = False
        start = None
        last_true = None

    if in_run and start is not None and last_true is not None:
        runs.append(mask.loc[start:last_true].index)

    return runs


def _calc_end_ts(index_slice: pd.Index, interval_h: pd.Series) -> pd.Timestamp:
    last_ts = index_slice[-1]
    last_width = float(interval_h.loc[last_ts]) if last_ts in interval_h.index else 0.0
    return last_ts + pd.to_timedelta(last_width, unit="h")


def find_windows_for_site(
    pac_w: pd.Series,
    interval_h: pd.Series,
    base_load_w: float,
    min_duration_min: float,
    required_wh: Optional[float] = None,
) -> List[LoadWindow]:
    """Identify qualifying load windows for a site."""
    if pac_w.empty:
        return []
    mask = pac_w >= base_load_w
    runs = _contiguous_runs(mask)
    windows: List[LoadWindow] = []
    for run_idx in runs:
        duration_h = float(interval_h.loc[run_idx].sum())
        duration_min = duration_h * 60.0
        if duration_min < min_duration_min:
            continue
        energy_wh = float((pac_w.loc[run_idx] * interval_h.loc[run_idx]).sum())
        if required_wh is not None and energy_wh < required_wh:
            continue
        avg_w = energy_wh / duration_h if duration_h > 0 else 0.0
        max_w = float(pac_w.loc[run_idx].max())
        start = run_idx[0]
        end = _calc_end_ts(run_idx, interval_h)
        windows.append(
            LoadWindow(
                start=start,
                end=end,
                duration_min=duration_min,
                energy_wh=energy_wh,
                avg_w=avg_w,
                max_w=max_w,
            )
        )
    return windows


def summarize_windows(windows: List[LoadWindow]) -> dict:
    if not windows:
        return {}
    earliest = windows[0]
    latest = windows[-1]
    # Prefer longer/higher-energy windows; tie-break by average power then earliest start.
    best = max(windows, key=lambda w: (w.energy_wh, w.duration_min, w.avg_w, w.start))
    return {
        "earliest": earliest.to_dict(),
        "latest": latest.to_dict(),
        "best": best.to_dict(),
        "windows": [w.to_dict() for w in windows],
    }


def compute_load_windows(
    timeseries: Dict[tuple, pd.DataFrame],
    base_load_w: float,
    min_duration_min: float,
    required_wh: Optional[float] = None,
    debug: Optional[DebugCollector] = None,
) -> Dict[str, dict]:
    debug = debug or NullDebugCollector()
    site_windows: Dict[str, dict] = {}

    # Aggregate pac_net per site
    site_series: Dict[str, pd.Series] = {}
    site_interval: Dict[str, pd.Series] = {}

    for (site_id, _array_id), df in timeseries.items():
        if df is None or df.empty:
            continue
        pac = df.get("pac_net_w")
        interval = df.get("interval_h")
        if pac is None or interval is None:
            continue
        if site_id not in site_series:
            site_series[site_id] = pac.copy()
            site_interval[site_id] = interval.copy()
        else:
            # align on union index to avoid misalignment; fill missing with 0 / ffill interval_h
            combined_idx = site_series[site_id].index.union(pac.index)
            site_series[site_id] = site_series[site_id].reindex(combined_idx).fillna(0) + pac.reindex(combined_idx).fillna(0)
            site_interval[site_id] = site_interval[site_id].reindex(combined_idx).fillna(method="ffill").fillna(method="bfill")

    for site_id, series in site_series.items():
        interval_h = site_interval[site_id]
        windows = find_windows_for_site(series, interval_h, base_load_w, min_duration_min, required_wh)
        summary = summarize_windows(windows)
        site_windows[site_id] = summary
        debug.emit(
            "load_window.summary",
            {
                "site": site_id,
                "base_load_w": base_load_w,
                "min_duration_min": min_duration_min,
                "required_wh": required_wh,
                "window_count": len(windows),
                "earliest_start": summary.get("earliest", {}).get("start") if summary else None,
                "best_energy_wh": summary.get("best", {}).get("energy_wh") if summary else None,
            },
            ts=series.index[0] if len(series.index) else None,
        )

    return site_windows


__all__ = [
    "LoadWindow",
    "compute_load_windows",
    "find_windows_for_site",
]
