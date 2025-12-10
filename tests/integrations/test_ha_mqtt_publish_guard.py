import datetime as dt

from solarpredict.integrations.ha_mqtt import _should_publish, _canonical_payload


def make_payload(gen_at: str, total: float) -> dict:
    return {
        "meta": {"generated_at": gen_at, "total_energy_kwh": total},
        "sites": [{"id": "s1", "total_energy_kwh": total, "arrays": []}],
    }


def test_should_publish_allows_changed_payload_same_timestamp():
    ts = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc).isoformat()
    remote = make_payload(ts, total=10.0)
    local = make_payload(ts, total=11.0)  # same generated_at, different content

    assert _canonical_payload(remote) != _canonical_payload(local)
    assert _should_publish(local, remote)


def test_should_publish_honors_newer_timestamp_even_if_equal_hash():
    ts_old = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc).isoformat()
    ts_new = dt.datetime(2025, 1, 2, tzinfo=dt.timezone.utc).isoformat()
    remote = make_payload(ts_old, total=10.0)
    local = make_payload(ts_new, total=10.0)

    assert _should_publish(local, remote)


def test_skip_if_fresh_parses_timestamps_not_strings():
    # string compare would say "9" > "10"; parsed datetimes should avoid that
    newer = "2025-01-10T00:00:00+00:00"
    older = "2025-01-09T00:00:00+00:00"
    remote = make_payload(newer, 10)
    local = make_payload(older, 11)
    # _should_publish called downstream should return False because remote newer
    assert _should_publish(local, remote) is False
