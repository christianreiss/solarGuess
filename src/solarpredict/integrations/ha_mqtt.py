"""Publish daily forecast JSON to Home Assistant via MQTT with change/freshness guards.

This module reads the JSON file produced by ``cron.sh`` (e.g. ``live_results.json``),
compares it to the retained state already published on the MQTT broker, and only
publishes when BOTH of these are true:

1. The new data is newer (``generated_at``) than the retained state.
2. The meaningful payload (excluding ``generated_at``) has changed.

It also publishes Home Assistant MQTT discovery config for a single sensor that
exposes the total forecasted energy as the state and attaches the full results
as attributes. This keeps HA setup hands‑free while avoiding churn on the broker.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import re
import os
import threading
import time
from dataclasses import dataclass
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Optional

import paho.mqtt.client as mqtt
import yaml


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------


def _parse_ts(value: str | None) -> Optional[dt.datetime]:
    if not value:
        return None
    try:
        ts = dt.datetime.fromisoformat(value)
    except ValueError:
        return None
    if ts.tzinfo is None:
        # Assume UTC if tz is missing; keeps ordering sane.
        ts = ts.replace(tzinfo=dt.timezone.utc)
    return ts


def _normalize_payload(data: Dict[str, Any]) -> Dict[str, Any]:
    """Return hierarchical payload with meta + sites, handling legacy flat shape."""

    # Already hierarchical; make sure arrays carry `id` and totals exist.
    if "meta" in data and "sites" in data:
        meta = dict(data["meta"])
        sites = []
        for site in data.get("sites", []):
            site_id = site.get("id") or site.get("site")
            arrays = []
            for arr in site.get("arrays", []):
                arr_id = arr.get("id") or arr.get("array")
                arr_clean = dict(arr)
                arr_clean["id"] = arr_id
                arr_clean.pop("array", None)
                arrays.append(arr_clean)
            arrays = sorted(arrays, key=lambda a: a.get("id") or "")
            site_total = site.get("total_energy_kwh")
            if site_total is None:
                site_total = round(sum(float(a.get("energy_kwh", 0) or 0) for a in arrays), 3)
            site_clean = {
                "id": site_id,
                "total_energy_kwh": site_total,
                "arrays": arrays,
            }
            # Preserve optional location info if present.
            if "location" in site:
                site_clean["location"] = site["location"]
            sites.append(site_clean)
        sites = sorted(sites, key=lambda s: s.get("id") or "")

        if meta.get("total_energy_kwh") is None:
            meta["total_energy_kwh"] = round(sum(s["total_energy_kwh"] for s in sites), 3)
        meta.setdefault("site_count", len(sites))
        meta.setdefault("array_count", sum(len(s["arrays"]) for s in sites))

        return {"meta": meta, "sites": sites}

    # Legacy flat format with top-level results list.
    results = data.get("results", [])
    meta = {
        "generated_at": data.get("generated_at"),
        "date": data.get("date"),
        "timestep": data.get("timestep"),
        "provider": data.get("provider"),
        "total_energy_kwh": data.get("total_energy_kwh"),
    }
    by_site: Dict[str, list] = {}
    for rec in results:
        site_id = rec.get("site", "unknown")
        by_site.setdefault(site_id, []).append(rec)

    sites = []
    for site_id, recs in by_site.items():
        arrays = []
        for rec in recs:
            arr_clean = dict(rec)
            arr_clean["id"] = arr_clean.pop("array", None) or arr_clean.get("id")
            arrays.append(arr_clean)
        arrays = sorted(arrays, key=lambda a: a.get("id") or "")
        site_total = round(sum(float(a.get("energy_kwh", 0) or 0) for a in arrays), 3)
        sites.append({"id": site_id, "total_energy_kwh": site_total, "arrays": arrays})
    sites = sorted(sites, key=lambda s: s["id"])

    if meta["total_energy_kwh"] is None:
        meta["total_energy_kwh"] = round(sum(s["total_energy_kwh"] for s in sites), 3)
    meta["site_count"] = len(sites)
    meta["array_count"] = sum(len(s["arrays"]) for s in sites)

    return {"meta": meta, "sites": sites}


def _canonical_payload(data: Dict[str, Any]) -> Dict[str, Any]:
    """Strip non-semantic fields (generated_at) and sort to make hashing stable."""
    normalized = _normalize_payload(data)

    meta = dict(normalized.get("meta", {}))
    meta.pop("generated_at", None)

    sites = []
    for site in normalized.get("sites", []):
        site_copy = dict(site)
        arrays = site_copy.pop("arrays", [])
        arrays = sorted(arrays, key=lambda a: a.get("id") or "")
        site_copy["arrays"] = arrays
        sites.append(site_copy)
    sites = sorted(sites, key=lambda s: s.get("id") or "")

    return {"meta": meta, "sites": sites}


def _iter_topics(base: str, payload: Dict[str, Any]):
    """Yield (topic, value) for meta and per-site/array metrics.

    Topics use the following layout under the provided base prefix (no leading slash):

    - {base}/forecast/meta/<key>
    - {base}/{site_id}/total_energy_kwh
    - {base}/{site_id}/{array_id}/<metric>

    Keys are kept as-is to avoid surprise renames; callers may want to normalize names upstream.
    """

    meta = payload.get("meta", {})
    for key, val in meta.items():
        if val is None:
            continue
        yield f"{base}/forecast/meta/{key}", val

    for site in payload.get("sites", []):
        site_id = site.get("id") or site.get("site") or "unknown"
        yield f"{base}/{site_id}/total_energy_kwh", site.get("total_energy_kwh")
        for arr in site.get("arrays", []):
            arr_id = arr.get("id") or arr.get("array") or "array"
            for key, val in arr.items():
                if key in {"id", "array"}:  # skip identifiers
                    continue
                if val is None:
                    continue
                # Preserve solarGuess naming; callers can alias downstream in HA.
                yield f"{base}/{site_id}/{arr_id}/{key}", val
                # If array has pv area metadata, publish energy-per-m2 for convenience.
                if key == "energy_kwh" and arr.get("area_m2"):
                    area = arr.get("area_m2")
                    try:
                        per_m2 = float(val) / float(area) if area else None
                    except Exception:
                        per_m2 = None
                    if per_m2 is not None:
                        yield f"{base}/{site_id}/{arr_id}/energy_kwh_per_m2", round(per_m2, 3)


def _hash_payload(data: Dict[str, Any]) -> str:
    canonical_json = json.dumps(data, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def _should_publish(local: Dict[str, Any], remote: Optional[Dict[str, Any]]) -> bool:
    """Return True only when data is newer (based on generated_at)."""

    def _extract_ts(payload: Optional[Dict[str, Any]]) -> Optional[dt.datetime]:
        if not payload:
            return None
        if "meta" in payload:
            return _parse_ts(payload["meta"].get("generated_at"))
        return _parse_ts(payload.get("generated_at"))

    local_ts = _extract_ts(local)
    remote_ts = _extract_ts(remote)

    # If we cannot determine timestamps, err on publishing to avoid missing updates.
    if local_ts is None:
        return True
    if remote_ts is None:
        return True
    return local_ts > remote_ts


# ---------------------------------------------------------------------------
# MQTT plumbing
# ---------------------------------------------------------------------------


@dataclass
class MqttConfig:
    host: str = "localhost"
    port: int = 1883
    username: Optional[str] = None
    password: Optional[str] = None
    base_topic: str = "solarguess"
    discovery_prefix: str = "homeassistant"
    keepalive: int = 30
    connect_retries: int = 3
    retry_delay_sec: float = 1.0
    verbose: bool = False
    publish_state: bool = True
    publish_topics: bool = False
    publish_discovery: bool = True

    @property
    def state_topic(self) -> str:
        return f"{self.base_topic}/forecast"

    def discovery_topics(self) -> Dict[str, str]:
        object_id = f"{self.base_topic}_forecast"
        base = f"{self.discovery_prefix}/sensor/{object_id}"
        return {
            "config": f"{base}/config",
            "state": self.state_topic,
            "availability": f"{self.base_topic}/availability",
        }


class PahoBridge:
    """Minimal synchronous wrapper around paho-mqtt suitable for tests."""

    def __init__(self, cfg: MqttConfig):
        self.cfg = cfg
        # Use modern callback API to silence deprecation warnings on paho>=2.0.
        self.client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        if cfg.username:
            self.client.username_pw_set(cfg.username, cfg.password)
        self._loop_running = False

    def _connect(self) -> None:
        last_exc: Exception | None = None
        for attempt in range(1, self.cfg.connect_retries + 1):
            try:
                if self.cfg.verbose:
                    print(f"[ha_mqtt] connect attempt {attempt}/{self.cfg.connect_retries} to {self.cfg.host}:{self.cfg.port}")
                self.client.connect(self.cfg.host, self.cfg.port, keepalive=self.cfg.keepalive)
                return
            except Exception as exc:  # pragma: no cover - exercised via live broker
                last_exc = exc
                if attempt >= self.cfg.connect_retries:
                    raise
                time.sleep(self.cfg.retry_delay_sec)
        if last_exc:
            raise last_exc

    def _ensure_connected(self) -> None:
        if not self.client.is_connected():
            self._connect()
        if not self._loop_running:
            self.client.loop_start()
            self._loop_running = True

    def _disconnect(self) -> None:
        if self._loop_running:
            self.client.loop_stop()
            self._loop_running = False
        if self.client.is_connected():
            self.client.disconnect()

    @contextmanager
    def session(self):
        """Connect once, keep loop running, and cleanly disconnect afterward."""
        self._ensure_connected()
        try:
            yield
        finally:
            self._disconnect()

    def get_retained_json(self, topic: str, timeout: float = 3.0) -> Optional[Dict]:
        """Subscribe and return retained JSON if present, else None."""
        payload: dict | None = None
        event = threading.Event()

        def on_message(client, userdata, msg):
            nonlocal payload
            try:
                payload = json.loads(msg.payload.decode("utf-8"))
            except json.JSONDecodeError:
                payload = None
            finally:
                event.set()

        self.client.on_message = on_message
        self._ensure_connected()
        self.client.subscribe(topic)
        event.wait(timeout)
        self._disconnect()
        if self.cfg.verbose:
            print(f"[ha_mqtt] retained {topic}: {'found' if payload is not None else 'missing'}")
        return payload

    def get_retained_value(self, topic: str, timeout: float = 3.0) -> Optional[str]:
        """Subscribe and return retained payload as text if present, else None."""
        payload: str | None = None
        event = threading.Event()

        def on_message(client, userdata, msg):
            nonlocal payload
            payload = msg.payload.decode("utf-8") if msg.payload else ""
            event.set()

        self.client.on_message = on_message
        self._ensure_connected()
        self.client.subscribe(topic)
        event.wait(timeout)
        self._disconnect()
        if self.cfg.verbose:
            print(f"[ha_mqtt] retained text {topic}: {'found' if payload is not None else 'missing'}")
        return payload

    def publish_json(self, topic: str, payload: Dict[str, Any], retain: bool = True, qos: int = 1):
        if self.cfg.verbose:
            size = len(json.dumps(payload).encode("utf-8"))
            print(f"[ha_mqtt] publish topic={topic} retain={retain} qos={qos} bytes={size}")
        self._ensure_connected()
        self.client.publish(topic, json.dumps(payload), retain=retain, qos=qos)
        # Give the network loop a moment to flush.
        time.sleep(0.05)

    def publish_value(self, topic: str, payload: Any, retain: bool = True, qos: int = 1):
        body: str
        if isinstance(payload, (dict, list)):
            body = json.dumps(payload, separators=(",", ":"))
        elif payload is None:
            body = ""
        else:
            body = str(payload)
        if self.cfg.verbose:
            size = len(body.encode("utf-8"))
            print(f"[ha_mqtt] publish value topic={topic} retain={retain} qos={qos} bytes={size}")
        self._ensure_connected()
        self.client.publish(topic, body, retain=retain, qos=qos)
        time.sleep(0.05)

    def publish_availability(self, available: bool) -> None:
        payload = "online" if available else "offline"
        topic = f"{self.cfg.base_topic}/availability"
        if self.cfg.verbose:
            print(f"[ha_mqtt] publish availability {payload} -> {topic}")
        self._ensure_connected()
        self.client.publish(topic, payload, retain=True, qos=1)
        time.sleep(0.05)


# ---------------------------------------------------------------------------
# Home Assistant discovery payloads
# ---------------------------------------------------------------------------


def build_discovery_config(cfg: MqttConfig) -> Dict[str, Any]:
    topics = cfg.discovery_topics()
    return {
        "name": "SolarGuess Forecast",
        "uniq_id": f"{cfg.base_topic}_forecast",
        "stat_t": topics["state"],
        "avty_t": topics["availability"],
        "pl_avail": "online",
        "pl_not_avail": "offline",
        "val_tpl": "{{ value_json.meta.total_energy_kwh }}",
        "unit_of_meas": "kWh",
        "dev_cla": "energy",
        "stat_cla": "measurement",
        "json_attr_t": topics["state"],
        "json_attr_tpl": "{{ value_json.sites | tojson }}",
        "dev": {
            "name": "SolarGuess",
            "ids": [cfg.base_topic],
            "mf": "solarGuess",
            "mdl": "forecast",
        },
    }


# ---------------------------------------------------------------------------
# Topic fan-out
# ---------------------------------------------------------------------------


def _publish_topics(cfg: MqttConfig, bridge: PahoBridge, payload: Dict[str, Any]) -> None:
    """Publish retained scalar metrics to individual MQTT topics.

    This is useful for consumers that prefer simple topics over a single JSON blob.
    """

    for topic, value in _iter_topics(cfg.base_topic, payload):
        bridge.publish_value(topic, value, retain=True, qos=1)


def _verify_topics(cfg: MqttConfig, bridge: PahoBridge, payload: Dict[str, Any]) -> list[str]:
    """Lightweight verification: ensure a few retained scalar topics match."""
    mismatches: list[str] = []
    for topic, value in _iter_topics(cfg.base_topic, payload):
        # Sample a small subset: all meta, plus first array metric per array.
        if "/forecast/meta/" in topic or topic.count("/") <= 3:
            retained = bridge.get_retained_value(topic)
            if retained is None:
                mismatches.append(f"missing:{topic}")
                continue
            if value is None and retained == "":
                continue
            # Compare as strings for simplicity.
            if str(value) != retained:
                mismatches.append(f"mismatch:{topic} expected={value} got={retained}")
    return mismatches


# ---------------------------------------------------------------------------
# Main script
# ---------------------------------------------------------------------------


def publish_forecast(
    input_path: Path,
    cfg: MqttConfig,
    bridge: Optional[PahoBridge] = None,
    force: bool = False,
    verify: bool = False,
    publish_retries: int = 1,
    retry_delay_sec: float = 1.0,
    skip_if_fresh: bool = False,
    debug: Optional[dict] = None,
) -> bool:
    """Return True if a publish occurred, with optional verification and retries.

    When cfg.publish_state is False, only scalar topics are published/verified.
    """
    last_exc: Exception | None = None
    for attempt in range(1, max(1, publish_retries) + 1):
        try:
            data = json.loads(input_path.read_text())
            if isinstance(data, list):
                # Wrap legacy flat list into expected dict form.
                data = {
                    "results": data,
                    "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                }
            normalized = _normalize_payload(data)
            if cfg.verbose:
                print(f"[ha_mqtt] loaded input {input_path}")
            bridge = bridge or PahoBridge(cfg)

            with bridge.session():
                remote = bridge.get_retained_json(cfg.state_topic) if cfg.publish_state else None

                if skip_if_fresh and remote is not None:
                    local_ts = normalized.get("meta", {}).get("generated_at")
                    remote_ts = remote.get("meta", {}).get("generated_at") if "meta" in remote else remote.get("generated_at")
                    if local_ts and remote_ts and local_ts <= remote_ts and not force:
                        if cfg.verbose:
                            print(f"[ha_mqtt] skip_if_fresh: remote is newer or equal (remote={remote_ts}, local={local_ts})")
                        return False
                bridge.publish_availability(True)

                def extract_ts(payload):
                    if not payload:
                        return None
                    if "meta" in payload:
                        return payload["meta"].get("generated_at")
                    return payload.get("generated_at")

                # Decide whether to emit the retained forecast blob.
                should_publish_state = False
                if cfg.publish_state:
                    should_publish_state = force or _should_publish(normalized, remote)
                # Scalar topics flow when publish_topics enabled; same freshness gate (force bypasses).
                should_publish_topics = cfg.publish_topics and (force or _should_publish(normalized, remote))
                if cfg.verbose:
                    local_hash = _hash_payload(_canonical_payload(normalized))
                    remote_hash = _hash_payload(_canonical_payload(remote)) if remote is not None else None
                    print(
                        "[ha_mqtt] decision "
                        f"force={force} "
                        f"publish_state={cfg.publish_state} publish_topics={cfg.publish_topics} "
                        f"state_should={should_publish_state} topics_should={should_publish_topics} "
                        f"local_ts={extract_ts(normalized)} "
                        f"remote_ts={extract_ts(remote)} "
                        f"local_hash={local_hash} remote_hash={remote_hash}"
                    )

                if not (should_publish_state or should_publish_topics):
                    if cfg.verbose:
                        print("[ha_mqtt] skip publish (not newer or unchanged)")
                    return False

                if should_publish_state:
                    if cfg.publish_discovery:
                        disc = build_discovery_config(cfg)
                        bridge.publish_json(cfg.discovery_topics()["config"], disc, retain=True)
                    bridge.publish_json(cfg.state_topic, normalized, retain=True)

                if should_publish_topics:
                    _publish_topics(cfg, bridge, normalized)

                if verify:
                    checks = []
                    if cfg.publish_state:
                        remote_after = bridge.get_retained_json(cfg.state_topic)
                        if remote_after is None:
                            raise RuntimeError("MQTT verify failed: no retained state after publish")
                        local_hash = _hash_payload(_canonical_payload(normalized))
                        remote_hash = _hash_payload(_canonical_payload(remote_after))
                        if local_hash != remote_hash:
                            raise RuntimeError("MQTT verify failed: retained payload mismatch")
                        checks.append("state")
                    if cfg.publish_topics:
                        # spot-check one meta field and one array field to ensure scalar topics landed
                        topics = list(_iter_topics(cfg.base_topic, normalized))
                        sample_meta = next((t for t, _ in topics if "/forecast/meta/total_energy_kwh" in t), None)
                        sample_arr = next((t for t, _ in topics if t.count("/") >= 3), None)
                        if sample_meta:
                            val = bridge.get_retained_value(sample_meta)
                            if val is None:
                                raise RuntimeError(f"MQTT verify failed: missing retained {sample_meta}")
                        if sample_arr:
                            val = bridge.get_retained_value(sample_arr)
                            if val is None:
                                raise RuntimeError(f"MQTT verify failed: missing retained {sample_arr}")
                        checks.append("topics")
                        # Broader verification for meta + one metric per array.
                        mismatches = _verify_topics(cfg, bridge, normalized)
                        if mismatches:
                            raise RuntimeError(f"MQTT verify failed: {'; '.join(mismatches)}")
                    if cfg.verbose and checks:
                        print(f"[ha_mqtt] verify success ({', '.join(checks)})")
                if debug is not None:
                    debug.update(
                        {
                            "mqtt_published": True,
                            "mqtt_force": force,
                            "mqtt_verify": bool(verify),
                            "mqtt_publish_state": cfg.publish_state,
                            "mqtt_publish_topics": cfg.publish_topics,
                            "mqtt_publish_discovery": cfg.publish_discovery,
                        }
                    )

            return True
        except Exception as exc:  # pragma: no cover - exercised via retry tests
            last_exc = exc
            if attempt >= max(1, publish_retries):
                raise
            time.sleep(retry_delay_sec)
    if last_exc:
        raise last_exc
    return False


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish solarGuess forecast JSON to Home Assistant via MQTT"
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("etc/config.yaml"),
        help="Combined config file (scenario + mqtt); defaults to etc/config.yaml",
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("live_results.json"),
        help="Path to forecast JSON produced by cron.sh",
    )
    # Default to None so config file can provide values when CLI flags omitted.
    parser.add_argument("--mqtt-host", default=None)
    parser.add_argument("--mqtt-port", type=int, default=None)
    parser.add_argument("--mqtt-username", default=None)
    parser.add_argument("--mqtt-password", default=None)
    parser.add_argument("--base-topic", default=None)
    parser.add_argument("--discovery-prefix", default=None)
    parser.add_argument("--connect-retries", type=int, default=None, help="MQTT connection retries (default 3)")
    parser.add_argument("--retry-delay", type=float, default=None, help="Delay between retries in seconds")
    parser.add_argument("--verbose", action="store_true", help="Enable chatty logging")
    parser.add_argument("--force", action="store_true", help="Publish even if unchanged or older")
    parser.add_argument(
        "--no-state",
        action="store_true",
        help="Skip publishing the solarguess/forecast state blob (still publishes scalar topics if enabled)",
    )
    parser.add_argument(
        "--publish-topics",
        action="store_true",
        help="Also publish per-site/array metrics under base topic (retained)",
    )
    return parser.parse_args()


def _merge_config(args: argparse.Namespace) -> tuple[Path, MqttConfig]:
    file_cfg: dict[str, Any] = {}
    if args.config and args.config.exists():
        file_cfg = yaml.safe_load(args.config.read_text()) or {}

    mqtt_cfg = file_cfg.get("mqtt", {}) if isinstance(file_cfg, dict) else {}

    def _from_env_string(val):
        """Allow yaml values like ${ENV_VAR} or env:ENV_VAR."""
        if not isinstance(val, str):
            return val
        match = re.fullmatch(r"\$\{([^}]+)\}", val)
        if match:
            return os.getenv(match.group(1), "")
        if val.startswith("env:"):
            return os.getenv(val.split("env:", 1)[1], "")
        return val

    def choose(key: str, default):
        cli_val = getattr(args, key.replace("-", "_"))
        if cli_val is not None:
            return cli_val
        if key in mqtt_cfg:
            return _from_env_string(mqtt_cfg[key])
        if key in file_cfg:
            # fallback for legacy flat keys
            return _from_env_string(file_cfg[key])
        return default

    input_path = Path(mqtt_cfg.get("input", file_cfg.get("input", args.input)))
    publish_state_cfg = bool(mqtt_cfg.get("publish_state", True))
    publish_state = publish_state_cfg and not bool(getattr(args, "no_state", False))

    cfg = MqttConfig(
        host=choose("mqtt_host", mqtt_cfg.get("host", "localhost")),
        port=int(choose("mqtt_port", mqtt_cfg.get("port", 1883))),
        username=choose("mqtt_username", mqtt_cfg.get("username")),
        password=choose("mqtt_password", mqtt_cfg.get("password")),
        base_topic=str(choose("base_topic", mqtt_cfg.get("base_topic", "solarguess"))).rstrip("/"),
        discovery_prefix=str(choose("discovery_prefix", mqtt_cfg.get("discovery_prefix", "homeassistant"))).rstrip("/"),
        connect_retries=int(choose("connect_retries", mqtt_cfg.get("connect_retries", 3))),
        retry_delay_sec=float(choose("retry_delay", mqtt_cfg.get("retry_delay", 1.0))),
        verbose=bool(choose("verbose", mqtt_cfg.get("verbose", False))),
        publish_state=publish_state,
        publish_topics=bool(choose("publish_topics", mqtt_cfg.get("publish_topics", False))),
        publish_discovery=bool(choose("publish_discovery", mqtt_cfg.get("publish_discovery", True))),
    )
    return input_path, cfg


def main() -> None:  # pragma: no cover
    args = _parse_args()
    input_path, cfg = _merge_config(args)
    published = publish_forecast(input_path, cfg, force=args.force)
    if published:
        print("Published new forecast to MQTT.")
    else:
        print("No publish needed (unchanged or not newer).")


__all__ = [
    "MqttConfig",
    "PahoBridge",
    "publish_forecast",
    "build_discovery_config",
]


if __name__ == "__main__":  # pragma: no cover
    main()
