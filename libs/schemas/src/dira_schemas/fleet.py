from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, field_validator


class FleetGPSPoint(BaseModel):
    vehicle_id_hash: str
    provider: str
    lat: float
    lon: float
    speed_kmh: float
    heading: float | None = None
    timestamp: datetime

    @field_validator("speed_kmh")
    @classmethod
    def validate_speed(cls, value: float) -> float:
        if value < 0:
            raise ValueError("speed_kmh must be non-negative")
        return value

    @field_validator("heading")
    @classmethod
    def validate_heading(cls, value: float | None) -> float | None:
        if value is not None and not 0.0 <= value <= 360.0:
            raise ValueError("heading must be between 0 and 360")
        return value
