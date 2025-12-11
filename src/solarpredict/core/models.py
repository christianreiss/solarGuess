"""Domain models for solar prediction core.

Provides data structures with validation for locations, PV arrays, sites and scenarios.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional


class ValidationError(ValueError):
    """Raised when model inputs violate constraints."""


@dataclass(frozen=True)
class Location:
    id: str
    lat: float
    lon: float
    tz: str = "auto"
    elevation_m: Optional[float] = None

    def __post_init__(self):
        if not self.id:
            raise ValidationError("Location id is required")
        if not (-90.0 <= self.lat <= 90.0):
            raise ValidationError("Latitude must be between -90 and 90 degrees")
        if not (-180.0 <= self.lon <= 180.0):
            raise ValidationError("Longitude must be between -180 and 180 degrees")
        if self.elevation_m is not None and self.elevation_m < -430.0:
            raise ValidationError("Elevation seems invalid (below Dead Sea floor)")


@dataclass(frozen=True)
class PVArray:
    id: str
    tilt_deg: float
    azimuth_deg: float
    pdc0_w: float
    gamma_pdc: float
    dc_ac_ratio: float
    eta_inv_nom: float
    losses_percent: float
    temp_model: str
    inverter_pdc0_w: Optional[float] = None
    inverter_group_id: Optional[str] = None
    horizon_deg: Optional[list[float]] = None

    def __post_init__(self):
        if not self.id:
            raise ValidationError("PVArray id is required")
        if not (0.0 <= self.tilt_deg <= 90.0):
            raise ValidationError("Tilt must be between 0 and 90 degrees")
        # Accept common 0..360 inputs and normalize into [-180, 180] to keep geometry stable.
        if not (-180.0 <= self.azimuth_deg <= 180.0):
            normalized = ((self.azimuth_deg + 180.0) % 360.0) - 180.0
            object.__setattr__(self, "azimuth_deg", normalized)
        if not (-180.0 <= self.azimuth_deg <= 180.0):
            raise ValidationError("Azimuth must be between -180 and 180 degrees after normalization")
        if self.pdc0_w < 0:
            raise ValidationError("pdc0_w must be non-negative")
        if self.gamma_pdc > 0:
            raise ValidationError("gamma_pdc should typically be negative (power coefficient)")
        if self.dc_ac_ratio <= 0:
            raise ValidationError("dc_ac_ratio must be positive")
        if not (0 < self.eta_inv_nom <= 1):
            raise ValidationError("eta_inv_nom must be in (0, 1]")
        if self.inverter_pdc0_w is not None and self.inverter_pdc0_w < 0:
            raise ValidationError("inverter_pdc0_w must be non-negative when set")
        if not (0 <= self.losses_percent <= 100):
            raise ValidationError("losses_percent must be between 0 and 100")
        if not self.temp_model:
            raise ValidationError("temp_model is required")
        if self.horizon_deg is not None:
            if len(self.horizon_deg) < 12:
                raise ValidationError("horizon_deg must have at least 12 values (30° bins or finer)")
            cleaned = []
            for val in self.horizon_deg:
                try:
                    fval = float(val)
                except Exception as exc:  # pragma: no cover - defensive
                    raise ValidationError(f"horizon_deg contains non-numeric entry: {val}") from exc
                if not (0.0 <= fval <= 90.0):
                    raise ValidationError("horizon_deg values must be between 0 and 90 degrees")
                cleaned.append(fval)
            object.__setattr__(self, "horizon_deg", cleaned)


@dataclass(frozen=True)
class Site:
    id: str
    location: Location
    arrays: List[PVArray] = field(default_factory=list)

    def __post_init__(self):
        if not self.id:
            raise ValidationError("Site id is required")
        if not isinstance(self.location, Location):
            raise ValidationError("location must be a Location instance")
        if not self.arrays:
            raise ValidationError("Site must contain at least one PVArray")
        for arr in self.arrays:
            if not isinstance(arr, PVArray):
                raise ValidationError("arrays must contain PVArray instances")


@dataclass(frozen=True)
class Scenario:
    sites: List[Site] = field(default_factory=list)

    def __post_init__(self):
        if not self.sites:
            raise ValidationError("Scenario must include at least one Site")
        for site in self.sites:
            if not isinstance(site, Site):
                raise ValidationError("sites must contain Site instances")


__all__ = [
    "ValidationError",
    "Location",
    "PVArray",
    "Site",
    "Scenario",
]
