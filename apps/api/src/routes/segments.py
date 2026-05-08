from __future__ import annotations

from typing import Any, Optional
import json

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


@router.get("/congestion/heatmap")
async def get_congestion_heatmap(
    bbox: Optional[str] = Query(None, description="BBox as minx,miny,maxx,maxy"),
    db=Depends(get_db),
) -> dict[str, Any]:
    """Return a GeoJSON FeatureCollection of segment centroids with congestion scores.

    - `bbox` (optional): comma-separated minx,miny,maxx,maxy to filter segments.
    """
    sql = """
    SELECT
        re.id as road_segment_id,
        ST_AsGeoJSON(ST_Centroid(re.geom)) as centroid,
        st.avg_speed_kmh,
        st.congestion_score
    FROM road_edges re
    LEFT JOIN LATERAL (
        SELECT avg_speed_kmh, congestion_score
        FROM segment_traffic_5min st
        WHERE st.road_segment_id = re.id
        ORDER BY event_time DESC
        LIMIT 1
    ) st ON true
    """

    params: list[Any] = []
    if bbox:
        try:
            minx, miny, maxx, maxy = [float(x) for x in bbox.split(",")]
            sql = sql + "\n WHERE ST_Intersects(re.geom, ST_MakeEnvelope($1, $2, $3, $4, 4326))"
            params = [minx, miny, maxx, maxy]
        except Exception:
            params = []

    rows = await db.fetch(sql, *params)  # type: ignore[attr-defined]

    def _congestion_level(score: Optional[float]) -> str:
        if score is None:
            return "unknown"
        if score >= 0.75:
            return "severe"
        if score >= 0.5:
            return "heavy"
        if score >= 0.25:
            return "moderate"
        return "free_flow"

    features: list[dict[str, Any]] = []
    for row in rows:
        centroid_raw = row.get("centroid") if hasattr(row, "get") else row["centroid"]
        try:
            geometry = json.loads(centroid_raw) if isinstance(centroid_raw, str) else centroid_raw
        except Exception:
            geometry = None

        score = row.get("congestion_score") if hasattr(row, "get") else row["congestion_score"]
        avg_speed = row.get("avg_speed_kmh") if hasattr(row, "get") else row["avg_speed_kmh"]

        prop = {
            "road_segment_id": row.get("road_segment_id") if hasattr(row, "get") else row["road_segment_id"],
            "congestion_score": score,
            "congestion_level": _congestion_level(score),
            "avg_speed_kmh": avg_speed,
        }

        feature = {
            "type": "Feature",
            "geometry": geometry,
            "properties": prop,
        }
        features.append(feature)

    return {"type": "FeatureCollection", "features": features}


@router.get("/segments/{segment_id}/predictions")
async def get_segment_predictions(
    segment_id: int,
    db=Depends(get_db),
) -> dict[str, Any]:
    """Return the latest ML prediction for a segment or a historical average fallback.

    Response shape includes `model` indicating source (e.g., 'xgboost' or 'historical_avg').
    """
    # try to fetch latest model prediction
    sql_pred = """
    SELECT predicted_at, model_name, predicted_speed_kmh, confidence
    FROM segment_predictions
    WHERE road_segment_id = $1
    ORDER BY predicted_at DESC
    LIMIT 1
    """
    preds = await db.fetch(sql_pred, segment_id)  # type: ignore[attr-defined]
    if preds:
        row = preds[0]
        return {
            "road_segment_id": row.get("road_segment_id") if hasattr(row, "get") else row.get("road_segment_id", segment_id),
            "predicted_at": row.get("predicted_at") if hasattr(row, "get") else row.get("predicted_at"),
            "predicted_speed_kmh": row.get("predicted_speed_kmh") if hasattr(row, "get") else row.get("predicted_speed_kmh"),
            "confidence": row.get("confidence") if hasattr(row, "get") else row.get("confidence"),
            "model": row.get("model_name") if hasattr(row, "get") else row.get("model_name"),
        }

    # fallback: compute historical average speed for last 7 days
    sql_hist = """
    SELECT avg(avg_speed_kmh) as predicted_speed_kmh
    FROM segment_traffic_5min
    WHERE road_segment_id = $1
      AND event_time >= now() - interval '7 days'
    """
    hist = await db.fetch(sql_hist, segment_id)  # type: ignore[attr-defined]
    predicted_speed = None
    if hist and len(hist) > 0:
        predicted_speed = hist[0].get("predicted_speed_kmh") if hasattr(hist[0], "get") else hist[0].get("predicted_speed_kmh")

    return {
        "road_segment_id": segment_id,
        "predicted_at": None,
        "predicted_speed_kmh": predicted_speed,
        "confidence": None,
        "model": "historical_avg",
    }
