"""Deterministic debug collectors for structured JSON events."""
from __future__ import annotations

import json
import datetime as _dt
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol


class DebugCollector(Protocol):
    def emit(self, stage: str, payload: Dict[str, Any], *, ts: Any, site: Optional[str] = None, array: Optional[str] = None) -> None:
        ...


def _json_safe_scalar(val: Any) -> Any:
    """Convert common non-JSON types to safe representations."""
    if hasattr(val, "isoformat"):
        try:
            return val.isoformat()
        except Exception:
            return str(val)
    if isinstance(val, (_dt.datetime, _dt.date, _dt.time)):
        try:
            return val.isoformat()
        except Exception:
            return str(val)
    return val


def _ordered(obj: Any) -> Any:
    """Recursively order mappings for deterministic JSON dumps."""
    if isinstance(obj, dict):
        return {k: _ordered(obj[k]) for k in sorted(obj)}
    if isinstance(obj, list):
        return [_ordered(v) for v in obj]
    return _json_safe_scalar(obj)


class NullDebugCollector:
    def emit(self, stage: str, payload: Dict[str, Any], *, ts: Any, site: Optional[str] = None, array: Optional[str] = None) -> None:  # noqa: D401
        """Discard events (no-op)."""
        return


@dataclass
class ListDebugCollector:
    events: List[Dict[str, Any]] = field(default_factory=list)

    def emit(self, stage: str, payload: Dict[str, Any], *, ts: Any, site: Optional[str] = None, array: Optional[str] = None) -> None:
        event = {
            "stage": stage,
            "ts": ts,
            "site": site,
            "array": array,
            "payload": _ordered(payload),
        }
        self.events.append(event)


class JsonlDebugWriter:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # open in append text mode
        self._fh = self.path.open("a", encoding="utf-8")

    @staticmethod
    def _serialize_ts(ts):
        if hasattr(ts, "isoformat"):
            try:
                return ts.isoformat()
            except Exception:
                return str(ts)
        return ts

    def emit(self, stage: str, payload: Dict[str, Any], *, ts: Any, site: Optional[str] = None, array: Optional[str] = None) -> None:
        event = {
            "stage": stage,
            "ts": self._serialize_ts(ts),
            "site": site,
            "array": array,
            "payload": _ordered(payload),
        }
        json.dump(event, self._fh, sort_keys=True)
        self._fh.write("\n")
        self._fh.flush()

    def __del__(self):  # pragma: no cover - best effort cleanup
        try:
            self._fh.close()
        except Exception:
            pass


class JsonDebugWriter:
    """Collect all events in memory then write a single JSON array.

    Used when callers pass a ``--debug`` path ending with ``.json`` to make
    audits self-contained (one file with every stage payload).
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._events: List[Dict[str, Any]] = []

    @staticmethod
    def _serialize_ts(ts):
        if hasattr(ts, "isoformat"):
            try:
                return ts.isoformat()
            except Exception:
                return str(ts)
        return ts

    def emit(self, stage: str, payload: Dict[str, Any], *, ts: Any, site: Optional[str] = None, array: Optional[str] = None) -> None:
        event = {
            "stage": stage,
            "ts": self._serialize_ts(ts),
            "site": site,
            "array": array,
            "payload": _ordered(payload),
        }
        self._events.append(event)

    def finalize(self) -> None:
        """Write collected events as a single JSON document."""
        ordered = _ordered(self._events)
        self.path.write_text(json.dumps(ordered, indent=2))

    def __del__(self):  # pragma: no cover - best effort
        try:
            self.finalize()
        except Exception:
            pass


def build_debug_collector(path: str | Path) -> DebugCollector:
    """Factory: .json → JsonDebugWriter, otherwise JsonlDebugWriter."""
    suffix = str(path).lower()
    if suffix.endswith(".json"):
        return JsonDebugWriter(path)
    return JsonlDebugWriter(path)


class ScopedDebugCollector:
    """Wrapper that injects fixed site/array context into every emit."""

    def __init__(self, inner: DebugCollector, *, site: Optional[str] = None, array: Optional[str] = None):
        self.inner = inner
        self.site = site
        self.array = array

    def emit(self, stage: str, payload: Dict[str, Any], *, ts: Any, site: Optional[str] = None, array: Optional[str] = None) -> None:
        # Prefer explicit overrides, otherwise fall back to scoped defaults.
        eff_site = site if site is not None else self.site
        eff_array = array if array is not None else self.array
        self.inner.emit(stage, payload, ts=ts, site=eff_site, array=eff_array)


__all__ = [
    "DebugCollector",
    "NullDebugCollector",
    "ListDebugCollector",
    "JsonlDebugWriter",
    "ScopedDebugCollector",
]
