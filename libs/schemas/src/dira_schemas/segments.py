from __future__ import annotations

from typing import Any, Mapping

from pydantic import BaseModel

from dira_schemas.enums import CongestionLevel, RoadType


class RoadSegment(BaseModel):
    id: int
    osm_id: int
    name: str | None = None
    road_type: RoadType
    length_m: float
    speed_limit_kmh: int
    congestion_score: float | None = None
    congestion_level: CongestionLevel | None = None
    avg_speed_kmh: float | None = None
    flow_rate: float | None = None
    density: float | None = None
    geom: str | None = None
    buffer_geom: str | None = None
    h3_index: str | None = None

    @classmethod
    def from_db_row(cls, row: Mapping[str, Any]) -> "RoadSegment":
        congestion_level = row.get("congestion_level")
        road_type = row.get("road_type")
        return cls(
            id=row["id"],
            osm_id=row["osm_id"],
            name=row.get("name"),
            road_type=RoadType(road_type) if road_type is not None else RoadType.RESIDENTIAL,
            length_m=row["length_m"],
            speed_limit_kmh=row["speed_limit_kmh"],
            congestion_score=row.get("congestion_score"),
            congestion_level=CongestionLevel(congestion_level) if congestion_level is not None else None,
            avg_speed_kmh=row.get("avg_speed_kmh"),
            flow_rate=row.get("flow_rate"),
            density=row.get("density"),
            geom=row.get("geom"),
            buffer_geom=row.get("buffer_geom"),
            h3_index=row.get("h3_index"),
        )
