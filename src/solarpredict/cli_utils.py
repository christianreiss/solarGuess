"""Shared CLI helpers to avoid circular imports."""
from __future__ import annotations

import json
from pathlib import Path
from typing import List

import yaml

from solarpredict.core.config import ConfigError, load_scenario, _load_raw
from solarpredict.core.models import Location, PVArray, Scenario, Site


def scenario_to_dict(scenario: Scenario) -> dict:
    def loc_dict(loc: Location) -> dict:
        return {
            "id": loc.id,
            "lat": loc.lat,
            "lon": loc.lon,
            "tz": loc.tz,
            "elevation_m": loc.elevation_m,
        }

    def arr_dict(arr: PVArray) -> dict:
        data = {
            "id": arr.id,
            "tilt_deg": arr.tilt_deg,
            "azimuth_deg": arr.azimuth_deg,
            "pdc0_w": arr.pdc0_w,
            "gamma_pdc": arr.gamma_pdc,
            "dc_ac_ratio": arr.dc_ac_ratio,
            "eta_inv_nom": arr.eta_inv_nom,
            "losses_percent": arr.losses_percent,
            "temp_model": arr.temp_model,
        }
        if arr.inverter_group_id is not None:
            data["inverter_group_id"] = arr.inverter_group_id
        if arr.inverter_pdc0_w is not None:
            data["inverter_pdc0_w"] = arr.inverter_pdc0_w
        if arr.horizon_deg is not None:
            data["horizon_deg"] = arr.horizon_deg
        return data

    return {
        "sites": [
            {
                "id": site.id,
                "location": loc_dict(site.location),
                "arrays": [arr_dict(arr) for arr in site.arrays],
            }
            for site in scenario.sites
        ]
    }


def _merge_mqtt_topics(existing: dict, updates: dict) -> dict:
    """Merge mqtt.publish_topics when caller supplies structured topic flags.

    Legacy configs use a boolean for publish_topics. New structured shape allows
    per-topic toggles (dict). We preserve existing keys and overlay updates.
    """

    result = dict(existing)

    existing_topics = existing.get("publish_topics")
    updates_topics = updates.get("publish_topics")

    # If either side is a dict, normalize to dict and merge.
    if isinstance(existing_topics, dict) or isinstance(updates_topics, dict):
        merged = {}
        if isinstance(existing_topics, dict):
            merged.update(existing_topics)
        if isinstance(updates_topics, dict):
            merged.update(updates_topics)
        result["publish_topics"] = merged
    elif updates_topics is not None:
        result["publish_topics"] = updates_topics

    return result


def write_scenario(path: Path, scenario: Scenario, mqtt: Optional[dict] = None, run: Optional[dict] = None) -> None:
    """Persist scenario while preserving any non-scenario keys in the file.

    This keeps sections like mqtt/run intact when editing only the PV hierarchy.
    """

    data = scenario_to_dict(scenario)
    base: dict[str, Any] = {}
    if path.exists():
        try:
            raw = _load_raw(path)
            if isinstance(raw, dict):
                base = raw
        except Exception:
            base = {}
    base["sites"] = data["sites"]
    if mqtt is not None:
        base_mqtt = base.get("mqtt", {}) if isinstance(base, dict) else {}
        if base_mqtt:
            merged = dict(base_mqtt)
            merged.update({k: v for k, v in mqtt.items() if k != "publish_topics"})
            merged = _merge_mqtt_topics(merged, mqtt)
            base["mqtt"] = merged
        else:
            base["mqtt"] = mqtt
    if run is not None:
        base["run"] = run

    if path.suffix.lower() in {".yaml", ".yml", ""}:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(base, sort_keys=False))
    elif path.suffix.lower() == ".json":
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(base, indent=2, sort_keys=False))
    else:
        raise ConfigError(f"Unsupported config extension: {path.suffix}")


def load_existing(path: Path) -> List[Site]:
    if not path.exists():
        return []
    scenario = load_scenario(path)
    return list(scenario.sites)


def load_mqtt(path: Path) -> dict:
    if not path.exists():
        return {}
    raw = _load_raw(path)
    if isinstance(raw, dict):
        return raw.get("mqtt", {}) or {}
    return {}


def load_run(path: Path) -> dict:
    if not path.exists():
        return {}
    raw = _load_raw(path)
    if isinstance(raw, dict):
        return raw.get("run", {}) or {}
    return {}


__all__ = ["scenario_to_dict", "write_scenario", "load_existing", "load_mqtt", "load_run"]
