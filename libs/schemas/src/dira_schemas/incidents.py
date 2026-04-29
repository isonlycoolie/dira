from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, field_validator

from dira_schemas.enums import IncidentType


class IncidentReport(BaseModel):
    id: UUID
    incident_type: IncidentType
    lat: float
    lon: float
    reported_at: datetime
    source: str
    description: str | None = None
    severity: int

    @field_validator("severity")
    @classmethod
    def validate_severity(cls, value: int) -> int:
        if not 1 <= value <= 5:
            raise ValueError("severity must be between 1 and 5")
        return value
