from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends, Query

from apps.api.src.dependencies import get_db

router = APIRouter(prefix="", tags=["segments"])


@router.get("/segments")
async def get_segments(road_type: Optional[str] = Query(None, description="Filter by road_type"), db=Depends(get_db)) -> list[dict[str, Any]]:
    """Return list of road segments joined with latest silver traffic metrics.

    Uses a lateral join to fetch the latest 5-minute window per segment.
    """
    sql = """
    SELECT
        re.id as road_segment_id,
        re.name as name,
        re.road_type as road_type,
        st.avg_speed_kmh,
        st.congestion_score,
        st.congestion_level
    FROM road_edges re
    LEFT JOIN LATERAL (
        SELECT avg_speed_kmh, congestion_score, congestion_level
        FROM segment_traffic_5min st
        WHERE st.road_segment_id = re.id
        ORDER BY event_time DESC
        LIMIT 1
    ) st ON true
    """

    params = []
    if road_type:
        sql = sql + " WHERE re.road_type = $1"
        params = [road_type]

    rows = await db.fetch(sql, *params)  # type: ignore[attr-defined]
    results: list[dict[str, Any]] = []
    for row in rows:
        results.append(
            {
                "road_segment_id": row.get("road_segment_id") if hasattr(row, "get") else row["road_segment_id"],
                "name": row.get("name") if hasattr(row, "get") else row["name"],
                "road_type": row.get("road_type") if hasattr(row, "get") else row["road_type"],
                "avg_speed_kmh": row.get("avg_speed_kmh") if hasattr(row, "get") else row["avg_speed_kmh"],
                "congestion_score": row.get("congestion_score") if hasattr(row, "get") else row["congestion_score"],
                "congestion_level": row.get("congestion_level") if hasattr(row, "get") else row["congestion_level"],
            }
        )
    return results


__all__ = ["router", "get_segments"]
