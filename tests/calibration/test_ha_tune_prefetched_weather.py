import json

import pytest

from solarpredict.calibration.ha_tune import build_prefetched_weather_from_debug_jsonl
from solarpredict.core.debug import ListDebugCollector


def _write_debug(tmp_path, rows):
    path = tmp_path / "weather.jsonl"
    path.write_text("\n".join(json.dumps(r) for r in rows))
    return path


def test_build_prefetched_weather_filters_sites_and_sets_tz(tmp_path):
    path = _write_debug(
        tmp_path,
        [
            {"stage": "weather.response_meta", "site": "s1", "payload": {"timezone": "America/Denver"}},
            {"stage": "weather.raw", "site": "s1", "payload": {"data": [
                {"ts": "2025-12-01T00:00:00+00:00", "ghi_wm2": 800, "dni_wm2": 600, "dhi_wm2": 200, "temp_air_c": 12, "wind_ms": 1},
                {"ts": "2025-12-01T01:00:00+00:00", "ghi_wm2": 500, "dni_wm2": 400, "dhi_wm2": 150, "temp_air_c": 10, "wind_ms": 2},
                {"ts": "2025-12-01T01:00:00+00:00", "ghi_wm2": 400, "dni_wm2": 300, "dhi_wm2": 120, "temp_air_c": 11, "wind_ms": 3},
            ]}},
            {"stage": "weather.raw", "site": "s2", "payload": {"data": [
                {"ts": "2025-12-01T00:00:00+00:00", "ghi_wm2": 300, "dni_wm2": 200, "dhi_wm2": 100, "temp_air_c": 5, "wind_ms": 1},
            ]}},
        ],
    )
    collector = ListDebugCollector()
    frames = build_prefetched_weather_from_debug_jsonl(path, site_ids=["s1"], debug=collector)
    assert set(frames.keys()) == {"s1"}
    df = frames["s1"]
    assert not df.empty
    assert {"ghi_wm2", "dni_wm2", "dhi_wm2", "temp_air_c", "wind_ms"}.issubset(set(df.columns))
    # duplicates dropped and tz converted
    assert len(df) == 2
    assert str(df.index.tz) == "America/Denver"
    assert any(ev["stage"] == "calibration.weather_debug.loaded" for ev in collector.events)


def test_build_prefetched_weather_requires_ts(tmp_path):
    path = _write_debug(
        tmp_path,
        [
            {"stage": "weather.raw", "site": "s1", "payload": {"data": [
                {"ghi_wm2": 800},
            ]}},
        ],
    )
    with pytest.raises(ValueError):
        build_prefetched_weather_from_debug_jsonl(path, site_ids=None)
