import json
from pathlib import Path

from solarpredict.core.debug import JsonlDebugWriter, ListDebugCollector, NullDebugCollector


def test_list_collector_records_events(tmp_path):
    collector = ListDebugCollector()
    collector.emit("stage1", {"b": 2, "a": 1}, ts="2025-01-01T00:00:00Z", site="site1", array="arr1")
    assert len(collector.events) == 1
    event = collector.events[0]
    assert event["stage"] == "stage1"
    # payload should be key-sorted for determinism
    assert list(event["payload"].keys()) == ["a", "b"]


def test_jsonl_writer(tmp_path):
    path = tmp_path / "debug.jsonl"
    writer = JsonlDebugWriter(path)
    writer.emit("stage1", {"z": 1, "y": {"b": 1, "a": 2}}, ts=1, site=None, array=None)
    writer.emit("stage2", {"b": [2, 1]}, ts=2, site="s", array="a")

    lines = path.read_text().splitlines()
    assert len(lines) == 2
    # each line should be valid JSON
    events = [json.loads(line) for line in lines]
    assert events[0]["stage"] == "stage1"
    # payload nested dicts should be ordered by key when serialized
    assert list(events[0]["payload"]["y"].keys()) == ["a", "b"]


def test_null_collector_noop():
    NullDebugCollector().emit("stage", {"x": 1}, ts=0)
    # nothing to assert; just ensure no exceptions
