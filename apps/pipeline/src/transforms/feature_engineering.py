from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, Any

from dira_schemas.events import UnifiedTrafficEvent

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
else:
    DataFrame = Any


class CongestionIndexCalculator:
    def calculate(
        self,
        avg_speed_kmh: float,
        free_flow_speed_kmh: float,
        vehicle_count: int,
        capacity: int,
    ) -> float:
        if free_flow_speed_kmh <= 0:
            raise ValueError("free_flow_speed_kmh must be positive")
        if capacity <= 0:
            raise ValueError("capacity must be positive")

        speed_ratio = max(0.0, min(1.0, avg_speed_kmh / free_flow_speed_kmh))
        density_ratio = max(0.0, min(1.0, vehicle_count / capacity))
        score = (1.0 - speed_ratio) * 0.6 + density_ratio * 0.4
        return max(0.0, min(1.0, score))


class DwellTimeDetector:
    WINDOW_DURATION = timedelta(seconds=30)
    MIN_CONSECUTIVE_WINDOWS = 3
    LOW_SPEED_THRESHOLD_KMH = 5.0

    def is_congestion_anchor(
        self,
        segment_id: int,
        recent_windows: list[UnifiedTrafficEvent],
    ) -> bool:
        matching_windows = [window for window in recent_windows if window.road_segment_id == segment_id]
        if len(matching_windows) < self.MIN_CONSECUTIVE_WINDOWS:
            return False

        trailing_windows = sorted(matching_windows, key=lambda window: window.event_time)[-
            self.MIN_CONSECUTIVE_WINDOWS :
        ]
        if not self._are_consecutive(trailing_windows):
            return False

        if not all(self._is_slow(window) for window in trailing_windows):
            return False

        for window in trailing_windows:
            window.incident_flag = True
        return True

    def _are_consecutive(self, windows: list[UnifiedTrafficEvent]) -> bool:
        if len(windows) < self.MIN_CONSECUTIVE_WINDOWS:
            return False

        ordered_windows = sorted(windows, key=lambda window: window.event_time)
        deltas = [
            ordered_windows[index].event_time - ordered_windows[index - 1].event_time
            for index in range(1, len(ordered_windows))
        ]
        return all(delta == self.WINDOW_DURATION for delta in deltas)

    def _is_slow(self, window: UnifiedTrafficEvent) -> bool:
        return (window.avg_speed_kmh or 0.0) < self.LOW_SPEED_THRESHOLD_KMH


class UpstreamDownstreamSpeedFeatureTransform:
    def __init__(
        self,
        road_graph: DataFrame,
        road_segment_column: str = "road_segment_id",
        graph_segment_column: str = "id",
        from_node_column: str = "from_node_id",
        to_node_column: str = "to_node_id",
        speed_column: str = "avg_speed_kmh",
    ) -> None:
        self._road_graph = road_graph
        self._road_segment_column = road_segment_column
        self._graph_segment_column = graph_segment_column
        self._from_node_column = from_node_column
        self._to_node_column = to_node_column
        self._speed_column = speed_column

    def apply(self, df: DataFrame) -> DataFrame:
        from pyspark.sql.functions import broadcast, col

        road_graph = broadcast(
            self._road_graph.selectExpr(
                f"CAST({self._graph_segment_column} AS BIGINT) AS {self._road_segment_column}",
                self._from_node_column,
                self._to_node_column,
            )
        )
        upstream_neighbors = road_graph.selectExpr(
            f"{self._road_segment_column} AS upstream_road_segment_id",
            f"{self._to_node_column} AS upstream_to_node_id",
        )
        downstream_neighbors = road_graph.selectExpr(
            f"{self._road_segment_column} AS downstream_road_segment_id",
            f"{self._from_node_column} AS downstream_from_node_id",
        )

        upstream_map = road_graph.join(
            upstream_neighbors,
            col(self._from_node_column) == col("upstream_to_node_id"),
            "left",
        ).selectExpr(
            self._road_segment_column,
            "upstream_road_segment_id",
        )
        downstream_map = road_graph.join(
            downstream_neighbors,
            col(self._to_node_column) == col("downstream_from_node_id"),
            "left",
        ).selectExpr(
            self._road_segment_column,
            "downstream_road_segment_id",
        )

        upstream_speed_lookup = df.selectExpr(
            f"{self._road_segment_column} AS upstream_road_segment_id",
            f"{self._speed_column} AS upstream_speed_kmh",
        )
        downstream_speed_lookup = df.selectExpr(
            f"{self._road_segment_column} AS downstream_road_segment_id",
            f"{self._speed_column} AS downstream_speed_kmh",
        )

        enriched = (
            df.join(broadcast(upstream_map), self._road_segment_column, "left")
            .join(upstream_speed_lookup, "upstream_road_segment_id", "left")
            .join(broadcast(downstream_map), self._road_segment_column, "left")
            .join(downstream_speed_lookup, "downstream_road_segment_id", "left")
            .drop("upstream_road_segment_id", "downstream_road_segment_id")
        )
        return enriched


__all__ = ["CongestionIndexCalculator", "DwellTimeDetector", "UpstreamDownstreamSpeedFeatureTransform"]
