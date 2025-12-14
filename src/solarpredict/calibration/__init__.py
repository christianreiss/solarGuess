"""Calibration helpers for aligning forecasts with measured production."""

from .ha_tune import (
    CalibrationGroup,
    auto_calibration_groups,
    build_prefetched_weather_from_debug_jsonl,
    normalize_ha_entity_id,
)

__all__ = [
    "CalibrationGroup",
    "auto_calibration_groups",
    "build_prefetched_weather_from_debug_jsonl",
    "normalize_ha_entity_id",
]
