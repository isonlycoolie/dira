from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, field_validator

from dira_schemas.enums import WeatherCondition


class WeatherReading(BaseModel):
    station_id: str
    timestamp: datetime
    condition: WeatherCondition
    rainfall_mm: float
    visibility_m: float
    temperature_c: float

    @field_validator("rainfall_mm", "visibility_m")
    @classmethod
    def validate_non_negative(cls, value: float) -> float:
        if value < 0:
            raise ValueError("value must be non-negative")
        return value
