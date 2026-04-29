from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, field_validator


DSM_BBOX = (-7.0, 39.1, -6.6, 39.4)


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


class TelecomPing(BaseModel):
    device_id_hash: str
    tower_id: str
    lat: float
    lon: float
    timestamp: datetime
    signal_strength: int | None = None

    @field_validator("lat")
    @classmethod
    def clamp_lat(cls, value: float) -> float:
        return clamp(value, DSM_BBOX[0], DSM_BBOX[2])

    @field_validator("lon")
    @classmethod
    def clamp_lon(cls, value: float) -> float:
        return clamp(value, DSM_BBOX[1], DSM_BBOX[3])
