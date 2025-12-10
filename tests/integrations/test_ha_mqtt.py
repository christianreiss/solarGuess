import json
from pathlib import Path

import pytest

from solarpredict.integrations.ha_mqtt import (
    MqttConfig,
    _canonical_payload,
    _hash_payload,
    _iter_topics,
    _normalize_payload,
    _should_publish,
    build_discovery_config,
    publish_forecast,
    _merge_config,
)


def sample_payload(**overrides):
    base = {
        "generated_at": "2025-12-09T12:00:00+00:00",
        "date": "2025-12-09",
        "timestep": "1h",
        "provider": "open-meteo",
        "results": [
            {"site": "s", "array": "a1", "energy_kwh": 1.0},
            {"site": "s", "array": "a2", "energy_kwh": 2.5},
        ],
    }
    base.update(overrides)
    return base


def sample_config(tmp_path: Path, *, with_mqtt_block: bool = True):
    base = {
        "sites": [],
        "mqtt": {
            "host": "mq.example",
            "port": 1883,
            "username": "u",
            "password": "p",
            "base_topic": "sg",
            "discovery_prefix": "ha",
            "input": "live.json",
            "connect_retries": 2,
            "retry_delay": 0.5,
            "verbose": True,
            "publish_state": True,
            "publish_topics": True,
        },
    }
    if not with_mqtt_block:
        base.pop("mqtt")
    path = tmp_path / "config.yaml"
    path.write_text(json.dumps(base))
    return path


def test_canonical_payload_adds_total_and_strips_generated_at():
    payload = sample_payload()
    canonical = _canonical_payload(payload)
    assert canonical["meta"]["total_energy_kwh"] == 3.5
    assert "generated_at" not in canonical["meta"]


def test_hash_changes_on_content_change_only():
    p1 = _hash_payload(_canonical_payload(sample_payload()))
    p2 = _hash_payload(_canonical_payload(sample_payload(results=[{"energy_kwh": 1.1}])))
    p3 = _hash_payload(_canonical_payload(sample_payload(generated_at="2025-12-09T12:01:00+00:00")))
    assert p1 != p2  # changed energy
    assert p1 == p3  # timestamp alone ignored


def test_should_publish_requires_newer_and_changed():
    local = sample_payload(generated_at="2025-12-09T12:00:00+00:00")
    older_remote = sample_payload(generated_at="2025-12-09T11:00:00+00:00")
    newer_remote = sample_payload(generated_at="2025-12-09T13:00:00+00:00")

    assert _should_publish(local, older_remote) is True  # newer wins
    assert _should_publish(local, newer_remote) is False  # not newer


class DummyBridge:
    def __init__(self):
        self.published = []
        self.retained = None

    # Context manager API used by publish_forecast
    def session(self):
        class _Sess:
            def __init__(self, outer):
                self.outer = outer
            def __enter__(self):
                return self.outer
            def __exit__(self, exc_type, exc, tb):
                return False
        return _Sess(self)

    def get_retained_json(self, topic, timeout=1.0):
        return self.retained

    def publish_json(self, topic, payload, retain=True):
        self.published.append((topic, json.loads(json.dumps(payload)), retain))
        if retain:
            if topic.endswith("/forecast"):
                self.retained = payload

    def publish_availability(self, available: bool):
        self.published.append(("availability", available, True))

    def publish_value(self, topic, payload, retain=True, qos=1):
        self.published.append((topic, payload, retain))


def test_publish_forecast_skips_when_unchanged(tmp_path: Path):
    inp = tmp_path / "live.json"
    inp.write_text(json.dumps(sample_payload()))
    bridge = DummyBridge()
    cfg = MqttConfig(base_topic="sg")
    bridge.retained = sample_payload()  # same generated_at -> not newer
    published = publish_forecast(inp, cfg, bridge=bridge)
    assert published is False
    # Availability still published to keep sensor marked online.
    assert bridge.published == [("availability", True, True)]


def test_publish_forecast_pushes_when_newer_and_changed(tmp_path: Path):
    inp = tmp_path / "live.json"
    inp.write_text(json.dumps(sample_payload(results=[{"energy_kwh": 5.0}])))
    bridge = DummyBridge()
    cfg = MqttConfig(base_topic="sg")
    bridge.retained = sample_payload(generated_at="2025-12-09T00:00:00+00:00")
    published = publish_forecast(inp, cfg, bridge=bridge)
    assert published is True
    topics = [t for (t, _, _) in bridge.published]
    assert cfg.discovery_topics()["config"] in topics
    assert cfg.state_topic in topics


def test_normalize_payload_handles_hierarchical():
    payload = {
        "meta": {"generated_at": "2025-12-09T00:00:00+00:00", "total_energy_kwh": None},
        "sites": [
            {
                "id": "site1",
                "arrays": [
                    {"id": "a1", "energy_kwh": 1.0},
                    {"array": "a2", "energy_kwh": 2.0},
                ],
            }
        ],
    }
    norm = _normalize_payload(payload)
    assert norm["meta"]["total_energy_kwh"] == 3.0
    assert norm["sites"][0]["total_energy_kwh"] == 3.0
    arrays = norm["sites"][0]["arrays"]
    assert {a["id"] for a in arrays} == {"a1", "a2"}


def test_build_discovery_config_includes_topics():
    cfg = MqttConfig(base_topic="sg", discovery_prefix="ha")
    disc = build_discovery_config(cfg)
    assert disc["stat_t"] == cfg.state_topic
    assert disc["json_attr_t"] == cfg.state_topic
    assert "uniq_id" in disc


def test_iter_topics_formats_meta_and_arrays():
    payload = _normalize_payload(sample_payload())
    topics = dict(_iter_topics("sg", payload))
    assert topics["sg/forecast/meta/date"] == "2025-12-09"
    # site total and array metrics present
    assert topics["sg/s/total_energy_kwh"] == 3.5
    assert topics["sg/s/a1/energy_kwh"] == 1.0
    assert "sg/s/a1/id" not in topics  # identifiers skipped


def test_iter_topics_includes_pvgis_and_poa_energy_per_m2():
    payload = _normalize_payload(
        sample_payload(
            results=[
                {
                    "site": "s",
                    "array": "a1",
                    "energy_kwh": 1.0,
                    "poa_kwh_m2": 0.8,
                    "pvgis_poa_kwh_m2": 0.6,
                }
            ]
        )
    )
    topics = dict(_iter_topics("sg", payload))
    assert topics["sg/s/a1/poa_kwh_m2"] == 0.8
    assert topics["sg/s/a1/pvgis_poa_kwh_m2"] == 0.6


def test_publish_topics_flag_pushes_scalar_topics(tmp_path: Path):
    inp = tmp_path / "live.json"
    inp.write_text(json.dumps(sample_payload()))
    bridge = DummyBridge()
    cfg = MqttConfig(base_topic="sg", publish_topics=True)
    bridge.retained = None
    publish_forecast(inp, cfg, bridge=bridge, force=True)
    scalar_topics = [t for (t, _, _) in bridge.published if t.startswith("sg/")]
    assert "sg/s/a1/energy_kwh" in scalar_topics


def test_can_skip_state_but_still_publish_topics(tmp_path: Path):
    inp = tmp_path / "live.json"
    inp.write_text(json.dumps(sample_payload()))
    bridge = DummyBridge()
    cfg = MqttConfig(base_topic="sg", publish_topics=True, publish_state=False)
    publish_forecast(inp, cfg, bridge=bridge, force=False)
    # No state publish, but scalar topics should appear.
    topics = [t for (t, _, _) in bridge.published]
    assert cfg.state_topic not in topics
    assert any(t.startswith("sg/s/a1/energy_kwh") for t in topics)


def test_merge_config_prefers_nested_mqtt(tmp_path: Path):
    cfg_path = sample_config(tmp_path)
    args = type("Args", (), {
        "config": cfg_path,
        "input": Path("cli.json"),
        "mqtt_host": None,
        "mqtt_port": None,
        "mqtt_username": None,
        "mqtt_password": None,
        "base_topic": None,
        "discovery_prefix": None,
        "connect_retries": None,
        "retry_delay": None,
        "verbose": None,
        "no_state": None,
        "publish_topics": None,
        "publish_discovery": None,
    })()

    input_path, cfg = _merge_config(args)
    assert input_path.name == "live.json"
    assert cfg.host == "mq.example"
    assert cfg.port == 1883
    assert cfg.username == "u"
    assert cfg.password == "p"
    assert cfg.base_topic == "sg"
    assert cfg.discovery_prefix == "ha"
    assert cfg.connect_retries == 2
    assert cfg.retry_delay_sec == 0.5
    assert cfg.verbose is True
    assert cfg.publish_state is True
    assert cfg.publish_topics is True


def test_merge_config_accepts_legacy_flat_keys(tmp_path: Path):
    path = tmp_path / "legacy.yaml"
    path.write_text(
        """
input: other.json
mqtt_host: legacy
mqtt_port: 11883
base_topic: legacy_sg
publish_topics: true
"""
    )

    args = type("Args", (), {
        "config": path,
        "input": Path("cli.json"),
        "mqtt_host": None,
        "mqtt_port": None,
        "mqtt_username": None,
        "mqtt_password": None,
        "base_topic": None,
        "discovery_prefix": None,
        "connect_retries": None,
        "retry_delay": None,
        "verbose": None,
        "no_state": None,
        "publish_topics": None,
        "publish_discovery": None,
    })()

    input_path, cfg = _merge_config(args)
    assert input_path.name == "other.json"
    assert cfg.host == "legacy"
    assert cfg.port == 11883
    assert cfg.base_topic == "legacy_sg"
    assert cfg.publish_topics is True


def test_publish_verify_reads_back_and_matches(tmp_path: Path):
    """When verify=True, we should read retained state and compare hashes."""
    inp = tmp_path / "live.json"
    inp.write_text(json.dumps(sample_payload()))

    class VerifyingBridge(DummyBridge):
        def __init__(self):
            super().__init__()
            self.verify_calls = 0

        def get_retained_json(self, topic, timeout=1.0):
            # After publish, retained will be set by DummyBridge.publish_json
            self.verify_calls += 1
            return self.retained

    bridge = VerifyingBridge()
    cfg = MqttConfig(base_topic="sg")
    publish_forecast(inp, cfg, bridge=bridge, force=True, verify=True)
    assert bridge.verify_calls >= 2  # once before, once after
