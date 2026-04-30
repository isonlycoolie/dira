from __future__ import annotations

import math
import random
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from dira_schemas.telecom import DSM_BBOX, TelecomPing

DEFAULT_ROAD_CENTROIDS: tuple[tuple[float, float], ...] = (
    (-6.7929, 39.2128),
    (-6.7968, 39.2241),
    (-6.8026, 39.2304),
    (-6.8081, 39.2652),
    (-6.8195, 39.2748),
    (-6.8294, 39.2861),
)


class TelecomDataSimulator:
    def __init__(
        self,
        noise_radius_meters: float = 75.0,
        engine: Any | None = None,
        road_centroids: Sequence[tuple[float, float]] | None = None,
        rng: random.Random | None = None,
        road_edge_probability: float = 0.8,
    ) -> None:
        if noise_radius_meters < 0:
            raise ValueError("noise_radius_meters must be non-negative")
        if not 0.0 <= road_edge_probability <= 1.0:
            raise ValueError("road_edge_probability must be between 0 and 1")

        self.noise_radius_meters = noise_radius_meters
        self.road_edge_probability = road_edge_probability
        self.road_centroids = self._load_road_centroids(engine, road_centroids)
        if not self.road_centroids:
            raise ValueError("road_centroids must contain at least one point")
        self._rng = rng or random.Random()

    def generate(self, count: int) -> list[TelecomPing]:
        if count < 0:
            raise ValueError("count must be non-negative")

        return [self._generate_ping(index) for index in range(count)]

    def generate_batch(self, count: int) -> list[TelecomPing]:
        return self.generate(count)

    def _generate_ping(self, index: int) -> TelecomPing:
        if self._rng.random() < self.road_edge_probability:
            centroid_lat, centroid_lon = self._rng.choice(self.road_centroids)
            lat, lon = self._sample_road_edge_point(centroid_lat, centroid_lon)
        else:
            lat, lon = self._sample_random_point()

        return TelecomPing(
            device_id_hash=f"device-{uuid4().hex}",
            tower_id=f"tower-{(index % 7) + 1}",
            lat=lat,
            lon=lon,
            timestamp=datetime.now(UTC),
            signal_strength=self._rng.randint(-108, -55),
        )

    def _load_road_centroids(
        self,
        engine: Any | None,
        road_centroids: Sequence[tuple[float, float]] | None,
    ) -> tuple[tuple[float, float], ...]:
        if road_centroids is not None:
            return tuple(road_centroids)

        if engine is None:
            return DEFAULT_ROAD_CENTROIDS

        try:
            from sqlalchemy import text
        except ModuleNotFoundError:
            return DEFAULT_ROAD_CENTROIDS

        query = text(
            "SELECT ST_Y(ST_Centroid(geom)) AS lat, ST_X(ST_Centroid(geom)) AS lon FROM road_edges WHERE geom IS NOT NULL"
        )
        try:
            connect_method = getattr(engine, "connect", None)
            if callable(connect_method):
                with engine.connect() as connection:
                    rows = connection.execute(query).fetchall()
            else:
                rows = engine.execute(query).fetchall()
        except Exception:  # noqa: BLE001
            return DEFAULT_ROAD_CENTROIDS

        centroids = tuple((float(row[0]), float(row[1])) for row in rows if row[0] is not None and row[1] is not None)
        return centroids or DEFAULT_ROAD_CENTROIDS

    def _sample_road_edge_point(self, centroid_lat: float, centroid_lon: float) -> tuple[float, float]:
        for _ in range(100):
            lat_offset_meters = self._rng.gauss(0.0, self.noise_radius_meters)
            lon_offset_meters = self._rng.gauss(0.0, self.noise_radius_meters)
            if math.hypot(lat_offset_meters, lon_offset_meters) <= 100.0:
                latitude = centroid_lat + lat_offset_meters / 111_320.0
                longitude_scale = max(math.cos(math.radians(centroid_lat)), 0.1)
                longitude = centroid_lon + lon_offset_meters / (111_320.0 * longitude_scale)
                return latitude, longitude

        return centroid_lat, centroid_lon

    def _sample_random_point(self) -> tuple[float, float]:
        latitude = self._rng.uniform(DSM_BBOX[0], DSM_BBOX[2])
        longitude = self._rng.uniform(DSM_BBOX[1], DSM_BBOX[3])
        return latitude, longitude