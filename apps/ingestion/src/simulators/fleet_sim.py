from __future__ import annotations

import logging
import math
import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from dira_schemas.telecom import DSM_BBOX

logger = logging.getLogger(__name__)

DEFAULT_POINTS_PER_TRAJECTORY = 12
DEFAULT_SPEED_RANGE_KMH = (30.0, 60.0)
DEFAULT_PROVIDER = "fleet-simulator"
DEFAULT_NETWORK_TYPE = "drive"


@dataclass(frozen=True, slots=True)
class RouteSegment:
    start_lat: float
    start_lon: float
    end_lat: float
    end_lon: float
    length_m: float
    bearing_deg: float


def _load_osmnx() -> Any:
    try:
        import osmnx as ox
    except ModuleNotFoundError as exc:
        raise RuntimeError("osmnx is required for FleetGPSSimulator") from exc

    return ox


class FleetGPSSimulator:
    def __init__(
        self,
        graph: Any | None = None,
        bbox: tuple[float, float, float, float] = DSM_BBOX,
        rng: random.Random | Any | None = None,
        points_per_trajectory: int = DEFAULT_POINTS_PER_TRAJECTORY,
        speed_range_kmh: tuple[float, float] = DEFAULT_SPEED_RANGE_KMH,
        provider: str = DEFAULT_PROVIDER,
        network_type: str = DEFAULT_NETWORK_TYPE,
    ) -> None:
        if points_per_trajectory < 2:
            raise ValueError("points_per_trajectory must be at least 2")
        if len(speed_range_kmh) != 2:
            raise ValueError("speed_range_kmh must contain exactly two values")
        if speed_range_kmh[0] <= 0 or speed_range_kmh[1] <= 0:
            raise ValueError("speed_range_kmh values must be positive")
        if speed_range_kmh[0] >= speed_range_kmh[1]:
            raise ValueError("speed_range_kmh minimum must be less than maximum")

        self._graph = graph
        self.bbox = bbox
        self._rng = rng or random.Random()
        self.points_per_trajectory = points_per_trajectory
        self.speed_range_kmh = speed_range_kmh
        self.provider = provider
        self.network_type = network_type

    def generate_trajectories(
        self,
        vehicle_count: int,
        start_timestamp: datetime | None = None,
    ) -> list[list[dict[str, Any]]]:
        if vehicle_count < 0:
            raise ValueError("vehicle_count must be non-negative")

        graph = self._resolve_graph()
        timestamp = self._normalize_timestamp(start_timestamp or datetime.now(UTC))
        trajectories: list[list[dict[str, Any]]] = []
        for trajectory_index in range(vehicle_count):
            route = self._sample_route(graph)
            trajectory = self._build_trajectory(graph, route, trajectory_index, timestamp)
            trajectories.append(trajectory)
        return trajectories

    def generate_batch(
        self,
        vehicle_count: int,
        start_timestamp: datetime | None = None,
    ) -> list[dict[str, Any]]:
        batch: list[dict[str, Any]] = []
        for trajectory in self.generate_trajectories(vehicle_count, start_timestamp=start_timestamp):
            batch.extend(trajectory)
        return batch

    def generate(self, vehicle_count: int, start_timestamp: datetime | None = None) -> list[dict[str, Any]]:
        return self.generate_batch(vehicle_count, start_timestamp=start_timestamp)

    def _resolve_graph(self) -> Any:
        if self._graph is not None:
            return self._graph

        ox = _load_osmnx()
        self._graph = self._graph_from_bbox(ox)
        return self._graph

    def _graph_from_bbox(self, ox: Any) -> Any:
        south, west, north, east = self.bbox
        attempts = (
            lambda: ox.graph_from_bbox(
                north=north,
                south=south,
                east=east,
                west=west,
                network_type=self.network_type,
                simplify=True,
                retain_all=False,
                truncate_by_edge=True,
            ),
            lambda: ox.graph_from_bbox(
                north,
                south,
                east,
                west,
                network_type=self.network_type,
                simplify=True,
                retain_all=False,
                truncate_by_edge=True,
            ),
            lambda: ox.graph_from_bbox(
                (north, south, east, west),
                network_type=self.network_type,
                simplify=True,
                retain_all=False,
                truncate_by_edge=True,
            ),
        )

        last_error: Exception | None = None
        for attempt in attempts:
            try:
                return attempt()
            except TypeError as exc:
                last_error = exc

        raise TypeError("unsupported osmnx graph_from_bbox signature") from last_error

    def _sample_route(self, graph: Any) -> list[Any]:
        ox = _load_osmnx()
        node_ids = list(graph.nodes)
        if len(node_ids) < 2:
            raise ValueError("graph must contain at least two nodes")

        last_error: Exception | None = None
        for _ in range(20):
            origin, destination = self._rng.sample(node_ids, 2)
            try:
                route = ox.shortest_path(graph, origin, destination, weight="length")
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                continue

            if route is not None and len(route) >= 2:
                return list(route)

        raise RuntimeError("unable to sample a valid shortest path") from last_error

    def _build_trajectory(
        self,
        graph: Any,
        route: list[Any],
        trajectory_index: int,
        start_timestamp: datetime,
    ) -> list[dict[str, Any]]:
        segments = self._route_segments(graph, route)
        total_length = sum(segment.length_m for segment in segments)
        if total_length <= 0:
            raise RuntimeError("sampled route has no measurable length")

        point_count = max(2, self.points_per_trajectory)
        sample_distances = [total_length * index / (point_count - 1) for index in range(point_count)]
        speed_profile = self._speed_profile(point_count)
        timestamps = [start_timestamp]
        for interval_index in range(point_count - 1):
            interval_distance = sample_distances[interval_index + 1] - sample_distances[interval_index]
            interval_speed_kmh = speed_profile[interval_index]
            interval_speed_mps = interval_speed_kmh * 1000.0 / 3600.0
            timestamps.append(timestamps[-1] + timedelta(seconds=interval_distance / interval_speed_mps))

        vehicle_id = f"vehicle-{trajectory_index + 1:04d}"
        points: list[dict[str, Any]] = []
        for point_index, distance in enumerate(sample_distances):
            lat, lon, heading = self._point_at_distance(segments, distance)
            points.append(
                {
                    "vehicle_id": vehicle_id,
                    "provider": self.provider,
                    "lat": lat,
                    "lon": lon,
                    "speed_kmh": round(speed_profile[point_index], 2),
                    "heading": round(heading, 2) if heading is not None else None,
                    "timestamp": timestamps[point_index].isoformat(),
                }
            )

        return points

    def _route_segments(self, graph: Any, route: list[Any]) -> list[RouteSegment]:
        segments: list[RouteSegment] = []
        for start_node, end_node in zip(route, route[1:]):
            start_lat, start_lon = self._node_coordinates(graph, start_node)
            end_lat, end_lon = self._node_coordinates(graph, end_node)
            length_m = self._segment_length(graph, start_node, end_node, start_lat, start_lon, end_lat, end_lon)
            bearing_deg = self._bearing(start_lat, start_lon, end_lat, end_lon)
            segments.append(
                RouteSegment(
                    start_lat=start_lat,
                    start_lon=start_lon,
                    end_lat=end_lat,
                    end_lon=end_lon,
                    length_m=length_m,
                    bearing_deg=bearing_deg,
                )
            )

        return segments

    def _speed_profile(self, point_count: int) -> list[float]:
        minimum_speed, maximum_speed = self.speed_range_kmh
        current_speed = self._rng.uniform(minimum_speed, maximum_speed)
        speeds = [current_speed]
        for _ in range(point_count - 1):
            current_speed = self._clamp(current_speed + self._rng.uniform(-4.0, 4.0), minimum_speed, maximum_speed)
            speeds.append(current_speed)
        return speeds

    def _point_at_distance(self, segments: list[RouteSegment], distance_m: float) -> tuple[float, float, float]:
        remaining_distance = distance_m
        for index, segment in enumerate(segments):
            if segment.length_m <= 0:
                continue

            if remaining_distance <= segment.length_m or index == len(segments) - 1:
                ratio = self._clamp(remaining_distance / segment.length_m if segment.length_m else 0.0, 0.0, 1.0)
                lat = segment.start_lat + (segment.end_lat - segment.start_lat) * ratio
                lon = segment.start_lon + (segment.end_lon - segment.start_lon) * ratio
                return lat, lon, segment.bearing_deg

            remaining_distance -= segment.length_m

        last_segment = segments[-1]
        return last_segment.end_lat, last_segment.end_lon, last_segment.bearing_deg

    def _node_coordinates(self, graph: Any, node_id: Any) -> tuple[float, float]:
        node_attrs = graph.nodes[node_id]
        latitude = node_attrs.get("y", node_attrs.get("lat")) if hasattr(node_attrs, "get") else None
        longitude = node_attrs.get("x", node_attrs.get("lon")) if hasattr(node_attrs, "get") else None
        if latitude is None or longitude is None:
            raise ValueError(f"node {node_id!r} is missing coordinate attributes")
        return float(latitude), float(longitude)

    def _segment_length(
        self,
        graph: Any,
        start_node: Any,
        end_node: Any,
        start_lat: float,
        start_lon: float,
        end_lat: float,
        end_lon: float,
    ) -> float:
        get_edge_data = getattr(graph, "get_edge_data", None)
        if callable(get_edge_data):
            try:
                edge_data = get_edge_data(start_node, end_node)
            except TypeError:
                edge_data = get_edge_data(start_node, end_node, default=None)
            lengths = self._extract_edge_lengths(edge_data)
            if lengths:
                return min(lengths)

        return self._haversine_distance(start_lat, start_lon, end_lat, end_lon)

    @staticmethod
    def _extract_edge_lengths(edge_data: Any) -> list[float]:
        if not isinstance(edge_data, dict):
            return []

        if "length" in edge_data and not all(isinstance(value, dict) for value in edge_data.values()):
            try:
                return [float(edge_data["length"])]
            except (TypeError, ValueError):
                return []

        lengths: list[float] = []
        for value in edge_data.values():
            if not isinstance(value, dict):
                continue
            length = value.get("length")
            if length is None:
                continue
            try:
                lengths.append(float(length))
            except (TypeError, ValueError):
                continue
        return lengths

    @staticmethod
    def _bearing(start_lat: float, start_lon: float, end_lat: float, end_lon: float) -> float:
        start_lat_rad = math.radians(start_lat)
        end_lat_rad = math.radians(end_lat)
        delta_lon_rad = math.radians(end_lon - start_lon)

        y = math.sin(delta_lon_rad) * math.cos(end_lat_rad)
        x = math.cos(start_lat_rad) * math.sin(end_lat_rad) - math.sin(start_lat_rad) * math.cos(end_lat_rad) * math.cos(delta_lon_rad)
        return (math.degrees(math.atan2(y, x)) + 360.0) % 360.0

    @staticmethod
    def _haversine_distance(start_lat: float, start_lon: float, end_lat: float, end_lon: float) -> float:
        earth_radius_m = 6_371_000.0
        delta_lat = math.radians(end_lat - start_lat)
        delta_lon = math.radians(end_lon - start_lon)
        lat1 = math.radians(start_lat)
        lat2 = math.radians(end_lat)
        a = (
            math.sin(delta_lat / 2.0) ** 2
            + math.cos(lat1) * math.cos(lat2) * math.sin(delta_lon / 2.0) ** 2
        )
        return 2.0 * earth_radius_m * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))

    @staticmethod
    def _clamp(value: float, lower: float, upper: float) -> float:
        return max(lower, min(upper, value))

    @staticmethod
    def _normalize_timestamp(timestamp: datetime) -> datetime:
        if timestamp.tzinfo is None:
            return timestamp.replace(tzinfo=UTC)
        return timestamp


__all__ = ["FleetGPSSimulator", "RouteSegment"]
