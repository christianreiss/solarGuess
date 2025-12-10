import threading

import paho.mqtt.client as mqtt

from solarpredict.integrations.ha_mqtt import MqttConfig, PahoBridge, _verify_topics, _iter_topics


class DummyBridge(PahoBridge):
    """Fake bridge that captures subscriptions/publishes without a real broker."""

    def __init__(self, cfg, retained=None):
        super().__init__(cfg)
        self.retained = retained or {}
        # monkeypatch client to call on_message immediately with retained payloads
        class DummyClient:
            def __init__(self, outer):
                self.outer = outer
                self.on_message = None
                self.subs = []

            def subscribe(self, topic, qos=0):
                self.subs.append((topic, qos))
                # simulate retained delivery
                if topic in outer.retained and self.on_message:
                    payload = outer.retained[topic]
                    msg = type("Msg", (), {"topic": topic, "payload": payload.encode("utf-8")})()
                    self.on_message(self, None, msg)

        outer = self
        self.client = DummyClient(self)

    def _ensure_connected(self):  # no-op for dummy
        return None

    def _disconnect(self):  # no-op for dummy
        return None


def test_verify_topics_uses_single_session_and_checks_values(monkeypatch):
    cfg = MqttConfig()
    payload = {
        "meta": {"generated_at": "2025-01-01T00:00:00Z", "total_energy_kwh": 10},
        "sites": [{"id": "s1", "total_energy_kwh": 10, "arrays": []}],
    }

    retained = {topic: str(val) for topic, val in _iter_topics(cfg.base_topic, payload)}
    bridge = DummyBridge(cfg, retained=retained)

    mismatches = _verify_topics(cfg, bridge, payload)
    assert mismatches == []
