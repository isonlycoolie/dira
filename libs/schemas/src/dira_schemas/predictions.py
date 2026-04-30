from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Mapping
from uuid import UUID

from pydantic import BaseModel, field_validator


class CongestionPrediction(BaseModel):
    id: UUID
    road_segment_id: int
    predicted_at: datetime
    horizon_minutes: Literal[10, 20, 30]
    congestion_prob: float
    predicted_speed_kmh: float | None = None
    model_version: str
    confidence: float

    @field_validator("congestion_prob", "confidence")
    @classmethod
    def validate_probability(cls, value: float) -> float:
        if not 0.0 <= value <= 1.0:
            raise ValueError("value must be between 0 and 1")
        return value

    @classmethod
    def from_db_row(cls, row: Mapping[str, Any]) -> "CongestionPrediction":
        return cls(
            id=row["id"],
            road_segment_id=row["road_segment_id"],
            predicted_at=row["predicted_at"],
            horizon_minutes=row["horizon_minutes"],
            congestion_prob=row["congestion_prob"],
            predicted_speed_kmh=row.get("predicted_speed_kmh"),
            model_version=row["model_version"],
            confidence=row["confidence"],
        )
