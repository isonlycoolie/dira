from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, field_validator


class CVDetection(BaseModel):
    camera_id: str
    frame_timestamp: datetime
    vehicle_count: int
    avg_speed_kmh: float | None = None
    lane_occupancy: float
    queue_length_m: float | None = None

    @field_validator("vehicle_count")
    @classmethod
    def validate_vehicle_count(cls, value: int) -> int:
        if value < 0:
            raise ValueError("vehicle_count must be non-negative")
        return value

    @field_validator("avg_speed_kmh")
    @classmethod
    def validate_avg_speed(cls, value: float | None) -> float | None:
        if value is not None and value < 0:
            raise ValueError("avg_speed_kmh must be non-negative")
        return value

    @field_validator("lane_occupancy")
    @classmethod
    def validate_lane_occupancy(cls, value: float) -> float:
        if not 0.0 <= value <= 1.0:
            raise ValueError("lane_occupancy must be between 0 and 1")
        return value

    @field_validator("queue_length_m")
    @classmethod
    def validate_queue_length(cls, value: float | None) -> float | None:
        if value is not None and value < 0:
            raise ValueError("queue_length_m must be non-negative")
        return value
