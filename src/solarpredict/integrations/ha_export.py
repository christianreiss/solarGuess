"""Helpers to parse Home Assistant PV exports.

This project frequently uses HA "energy today" sensors (kWh) as the ground-truth
for validating the forecast model. Christian's exporter currently writes a JSON
blob shaped like:

{
  "year": 2025,
  "timezone": "SYSTEM",
  "sensors": ["sensor.total_pv_energy_today", ...],
  "data": {
    "sensor.total_pv_energy_today": [{"day": "2025-12-12", "max": 9.24}, ...]
  }
}
"""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

from solarpredict.core.debug import DebugCollector, NullDebugCollector


class HaExportError(ValueError):
    """Raised when the Home Assistant export is invalid or unsupported."""


@dataclass(frozen=True)
class HaDailyMaxExport:
    """Daily max export for HA cumulative 'today' energy sensors (kWh)."""

    year: int
    timezone: str
    generated_at: Optional[str]
    db: Optional[str]
    sensors: List[str]
    data: Dict[str, List[Dict[str, Any]]]

    @staticmethod
    def from_path(path: str | Path, *, debug: DebugCollector | None = None) -> "HaDailyMaxExport":
        debug = debug or NullDebugCollector()
        p = Path(path)
        try:
            payload = json.loads(p.read_text())
        except Exception as exc:
            raise HaExportError(f"Failed to read HA export JSON: {p}") from exc
        if not isinstance(payload, dict):
            raise HaExportError("HA export must be a JSON object")

        year = payload.get("year")
        timezone = payload.get("timezone")
        sensors = payload.get("sensors")
        data = payload.get("data")

        if not isinstance(year, int):
            raise HaExportError("HA export missing integer 'year'")
        if not isinstance(timezone, str) or not timezone:
            raise HaExportError("HA export missing string 'timezone'")
        if not isinstance(sensors, list) or not all(isinstance(s, str) for s in sensors):
            raise HaExportError("HA export missing list 'sensors' (entity_id strings)")
        if not isinstance(data, dict):
            raise HaExportError("HA export missing object 'data' mapping entity_id -> rows")

        # Validate row shape lightly; strict parsing happens in to_frame().
        for ent in sensors:
            if ent not in data:
                debug.emit("ha.export.sensor_missing", {"entity_id": ent}, ts=payload.get("generated_at"))
        debug.emit(
            "ha.export.loaded",
            {"path": str(p), "year": year, "timezone": timezone, "sensor_count": len(sensors), "series_count": len(data)},
            ts=payload.get("generated_at"),
        )

        return HaDailyMaxExport(
            year=year,
            timezone=timezone,
            generated_at=payload.get("generated_at"),
            db=payload.get("db"),
            sensors=list(sensors),
            data=data,  # type: ignore[assignment]
        )

    def to_frame(
        self,
        *,
        entities: Iterable[str] | None = None,
        debug: DebugCollector | None = None,
    ) -> pd.DataFrame:
        """Return a normalized dataframe with columns: entity_id, day, energy_kwh."""
        debug = debug or NullDebugCollector()
        wanted = set(entities) if entities is not None else set(self.data.keys())

        rows_out: list[dict[str, Any]] = []
        for ent, rows in self.data.items():
            if ent not in wanted:
                continue
            if not isinstance(rows, list):
                raise HaExportError(f"Expected list of rows for {ent}")
            for r in rows:
                if not isinstance(r, dict):
                    raise HaExportError(f"Expected object rows for {ent}")
                day_s = r.get("day")
                val = r.get("max")
                if not isinstance(day_s, str):
                    raise HaExportError(f"{ent}: row missing 'day' string")
                try:
                    day = dt.date.fromisoformat(day_s)
                except ValueError as exc:
                    raise HaExportError(f"{ent}: invalid day '{day_s}'") from exc
                try:
                    energy_kwh = float(val)
                except Exception as exc:
                    raise HaExportError(f"{ent}: invalid max value '{val}'") from exc
                rows_out.append({"entity_id": ent, "day": day, "energy_kwh": energy_kwh})

        df = pd.DataFrame(rows_out)
        if df.empty:
            return df
        df = df.sort_values(["entity_id", "day"]).reset_index(drop=True)
        debug.emit(
            "ha.export.normalized",
            {"rows": int(len(df)), "entities": sorted(df["entity_id"].unique().tolist())},
            ts=self.generated_at,
        )
        return df


__all__ = ["HaDailyMaxExport", "HaExportError"]

