from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator

from dira_schemas.enums import CongestionLevel, DataSourceType, PipelineStage, WeatherCondition


class UnifiedTrafficEvent(BaseModel):
    road_segment_id: int
    event_time: datetime
    pipeline_stage: PipelineStage
    source_type: DataSourceType
    vehicle_count: int | None = None
    avg_speed_kmh: float | None = None
    flow_rate: float | None = None
    density: float | None = None
    congestion_score: float
    incident_flag: bool = False
    weather_factor: WeatherCondition | None = None
    raw_attributes: dict[str, Any] = Field(default_factory=dict)

    @field_validator("congestion_score")
    @classmethod
    def validate_congestion_score(cls, value: float) -> float:
        if not 0.0 <= value <= 1.0:
            raise ValueError("congestion_score must be between 0 and 1")
        return value

    @property
    def congestion_level(self) -> CongestionLevel:
        score = self.congestion_score
        if score < 0.30:
            return CongestionLevel.FREE_FLOW
        if score < 0.50:
            return CongestionLevel.LIGHT
        if score < 0.70:
            return CongestionLevel.MODERATE
        if score < 0.90:
            return CongestionLevel.HEAVY
        return CongestionLevel.SEVERE

    def to_kafka_dict(self) -> dict[str, Any]:
        payload = self.model_dump(mode="json")
        payload["congestion_level"] = self.congestion_level.value
        return payload
