from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, model_validator

from dira_schemas.enums import DataSourceType


class GeoPoint(BaseModel):
    lat: float
    lon: float


class RawMessage(BaseModel):
    source: DataSourceType
    timestamp: datetime
    geo: GeoPoint
    attributes: dict[str, Any] = Field(default_factory=dict)
    message_id: UUID = Field(default_factory=uuid4)
    schema_version: str = "1.0"

    @model_validator(mode="after")
    def normalize_timestamp_to_utc(self) -> "RawMessage":
        if self.timestamp.tzinfo is None:
            self.timestamp = self.timestamp.replace(tzinfo=UTC)
        else:
            self.timestamp = self.timestamp.astimezone(UTC)
        return self
