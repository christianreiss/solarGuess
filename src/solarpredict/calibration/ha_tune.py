"""Home-Assistant-backed tuning helpers.

This module provides:
- A small heuristic mapper from HA "energy today" sensors to scenario arrays.
- Helpers to reuse cached weather embedded in debug JSONL (stage=weather.raw) so
  training can run offline/deterministically.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple

import pandas as pd

from solarpredict.core.debug import DebugCollector, NullDebugCollector
from solarpredict.core.models import Scenario


@dataclass(frozen=True)
class CalibrationGroup:
    """Maps one or more HA entities to one or more arrays (site/array pairs)."""

    name: str
    ha_entities: List[str]
    arrays: List[Tuple[str, str]]  # (site_id, array_id)


_GENERIC_TOKENS = {
    "sensor",
    "energy",
    "today",
    "total",
    "pv",
    "solar",
    "production",
    "kwh",
    "wh",
}


def _split_tokens(s: str) -> list[str]:
    parts = re.split(r"[^a-z0-9]+", (s or "").lower())
    return [p for p in parts if p and p not in _GENERIC_TOKENS]


def normalize_ha_entity_id(entity_id: str) -> str:
    """Normalize HA entity_id into a stable base name for grouping.

    Examples
    --------
    - sensor.solarfarm_one_energy_today -> solarfarm
    - sensor.playhouse_phase_b_energy_today -> playhouse
    - sensor.house_north_energy_today -> house_north
    """

    ent = (entity_id or "").strip()
    if ent.startswith("sensor."):
        ent = ent.split(".", 1)[1]

    tokens = [t for t in ent.split("_") if t]
    # Drop common suffixes.
    while tokens and tokens[-1] in {"today", "energy", "kwh", "wh"}:
        tokens.pop()
    if tokens and tokens[-1] == "energy":
        tokens.pop()

    # If the sensor encodes phase (phase_a/phase_b/phase_c), drop it.
    if "phase" in tokens:
        i = tokens.index("phase")
        # remove "phase" and its immediate token if present
        tokens = tokens[:i] + tokens[i + 2 :]

    # Collapse trailing enumerators (one/two/1/2/etc) into the base key.
    while tokens and tokens[-1] in {"one", "two", "three", "1", "2", "3"}:
        tokens.pop()

    return "_".join(tokens)


def auto_calibration_groups(
    scenario: Scenario,
    ha_entities: Sequence[str],
    *,
    include_total: bool = True,
) -> list[CalibrationGroup]:
    """Heuristically map HA sensors to scenario arrays.

    The goal is to get a useful default mapping with zero user input. We:
    - Normalize each HA sensor id into a base group name.
    - Match that name's tokens against array ids.
    - Merge sensors that normalize to the same group name (e.g. solarfarm_one/two).

    Notes
    -----
    This is intentionally conservative and only creates a group when at least
    one array token matches.
    """

    if not isinstance(scenario, Scenario):
        raise ValueError("scenario must be a Scenario")

    arrays: list[tuple[str, str, set[str]]] = []
    for site in scenario.sites:
        for arr in site.arrays:
            arrays.append((str(site.id), str(arr.id), set(_split_tokens(str(arr.id)))))

    # Base name -> group accumulators
    groups: dict[str, dict[str, Any]] = {}
    for ent in ha_entities:
        if not isinstance(ent, str) or not ent:
            continue
        if not include_total and "total" in ent.lower():
            continue

        base = normalize_ha_entity_id(ent)
        if not base:
            continue

        # Special-case totals so we don't accidentally match them to a single array.
        if "total" in base.split("_"):
            continue

        base_tokens = set(_split_tokens(base))
        if not base_tokens:
            continue

        matched: list[tuple[str, str]] = []
        for site_id, array_id, toks in arrays:
            if base_tokens.intersection(toks):
                matched.append((site_id, array_id))
        if not matched:
            continue

        g = groups.setdefault(base, {"name": base, "ha_entities": [], "arrays": set()})
        g["ha_entities"].append(ent)
        for pair in matched:
            g["arrays"].add(pair)

    out: list[CalibrationGroup] = []
    for base, g in groups.items():
        out.append(
            CalibrationGroup(
                name=str(g["name"]),
                ha_entities=sorted(set(g["ha_entities"])),
                arrays=sorted(g["arrays"]),
            )
        )

    # Optionally add a total PV group for reference, but only if the entity exists.
    if include_total:
        total_entities = [e for e in ha_entities if isinstance(e, str) and "total_pv_energy_today" in e]
        if total_entities:
            all_arrays = [(str(site.id), str(arr.id)) for site in scenario.sites for arr in site.arrays]
            out.append(CalibrationGroup(name="total_pv", ha_entities=sorted(set(total_entities)), arrays=sorted(all_arrays)))

    return sorted(out, key=lambda g: g.name)


def build_prefetched_weather_from_debug_jsonl(
    debug_path: str | Path,
    *,
    site_ids: Iterable[str] | None = None,
    debug: DebugCollector | None = None,
) -> Dict[str, pd.DataFrame]:
    """Extract weather.raw rows from a debug JSONL into a PrefetchedWeatherProvider payload."""

    debug = debug or NullDebugCollector()
    p = Path(debug_path)
    wanted = set(site_ids) if site_ids is not None else None

    per_site_rows: dict[str, list[dict[str, Any]]] = {}
    tz_by_site: dict[str, str] = {}
    bad_lines = 0
    weather_lines = 0
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                bad_lines += 1
                continue
            stage = obj.get("stage")
            if stage not in {"weather.raw", "weather.response_meta"}:
                continue
            site = obj.get("site")
            if not isinstance(site, str) or not site:
                continue
            if wanted is not None and site not in wanted:
                continue
            if stage == "weather.response_meta":
                payload = obj.get("payload") or {}
                tz = payload.get("timezone")
                if isinstance(tz, str) and tz and tz.lower() != "none":
                    tz_by_site.setdefault(site, tz)
                continue
            payload = obj.get("payload") or {}
            rows = payload.get("data")
            if not isinstance(rows, list):
                continue
            weather_lines += 1
            per_site_rows.setdefault(site, []).extend([r for r in rows if isinstance(r, dict)])

    out: Dict[str, pd.DataFrame] = {}
    for site, rows in per_site_rows.items():
        df = pd.DataFrame(rows)
        if df.empty:
            out[site] = df
            continue
        if "ts" not in df.columns:
            raise ValueError(f"Debug weather rows for site {site} missing 'ts' field")

        # Normalize timestamps to a single tz per site to avoid mixed-offset (DST) object indices.
        #
        # Debug rows store ISO timestamps with an offset, so:
        #   - parse with utc=True for a uniform dtype
        #   - convert back to the provider/site timezone when known
        df["ts"] = pd.to_datetime(df["ts"], errors="coerce", utc=True)
        tz = tz_by_site.get(site)
        if tz:
            try:
                df["ts"] = df["ts"].dt.tz_convert(tz)
            except Exception:
                # Best effort: keep UTC if tz conversion fails
                pass
        df = df.dropna(subset=["ts"])
        df = df.set_index("ts").sort_index()
        # Drop duplicate timestamps (keep last) to handle repeated runs in a single debug file.
        df = df[~df.index.duplicated(keep="last")]
        out[site] = df

    debug.emit(
        "calibration.weather_debug.loaded",
        {
            "path": str(p),
            "sites": sorted(out.keys()),
            "tz_by_site": tz_by_site,
            "weather_lines": weather_lines,
            "bad_lines": bad_lines,
        },
        ts=None,
    )
    return out


__all__ = [
    "CalibrationGroup",
    "auto_calibration_groups",
    "build_prefetched_weather_from_debug_jsonl",
    "normalize_ha_entity_id",
]
