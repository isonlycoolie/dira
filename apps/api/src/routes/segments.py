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


@router.get("/segments/{segment_id}/traffic")
async def get_segment_traffic(
    segment_id: int,
    hours: int = 1,
    db=Depends(get_db),
) -> list[dict[str, Any]]:
    """Return traffic events for a segment for the last `hours` hours (default 1)."""
    if hours <= 0 or hours > 24:
        hours = 1

    sql = """
    SELECT road_segment_id, event_time, vehicle_count, avg_speed_kmh, flow_rate, congestion_score
    FROM segment_traffic_5min
    WHERE road_segment_id = $1
      AND event_time >= now() - ($2::int * interval '1 hour')
    ORDER BY event_time DESC
    LIMIT 500
    """
    rows = await db.fetch(sql, segment_id, hours)  # type: ignore[attr-defined]
    results: list[dict[str, Any]] = []
    for row in rows:
        results.append({
            "road_segment_id": row.get("road_segment_id") if hasattr(row, "get") else row["road_segment_id"],
            "event_time": row.get("event_time") if hasattr(row, "get") else row["event_time"],
            "vehicle_count": row.get("vehicle_count") if hasattr(row, "get") else row["vehicle_count"],
            "avg_speed_kmh": row.get("avg_speed_kmh") if hasattr(row, "get") else row["avg_speed_kmh"],
            "flow_rate": row.get("flow_rate") if hasattr(row, "get") else row["flow_rate"],
            "congestion_score": row.get("congestion_score") if hasattr(row, "get") else row["congestion_score"],
        })
    return results
