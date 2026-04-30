from __future__ import annotations

import math
import random
from collections.abc import Sequence
from datetime import UTC, datetime
from uuid import uuid4

from dira_schemas.telecom import TelecomPing

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
        road_centroids: Sequence[tuple[float, float]] | None = None,
        rng: random.Random | None = None,
    ) -> None:
        if noise_radius_meters < 0:
            raise ValueError("noise_radius_meters must be non-negative")

        self.noise_radius_meters = noise_radius_meters
        self.road_centroids = tuple(road_centroids or DEFAULT_ROAD_CENTROIDS)
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
        centroid_lat, centroid_lon = self._rng.choice(self.road_centroids)
        lat, lon = self._sample_point(centroid_lat, centroid_lon)
        return TelecomPing(
            device_id_hash=f"device-{uuid4().hex}",
            tower_id=f"tower-{(index % 7) + 1}",
            lat=lat,
            lon=lon,
            timestamp=datetime.now(UTC),
            signal_strength=self._rng.randint(-108, -55),
        )

    def _sample_point(self, centroid_lat: float, centroid_lon: float) -> tuple[float, float]:
        lat_offset_meters = self._rng.gauss(0.0, self.noise_radius_meters)
        lon_offset_meters = self._rng.gauss(0.0, self.noise_radius_meters)
        latitude = centroid_lat + lat_offset_meters / 111_320.0
        longitude_scale = max(math.cos(math.radians(centroid_lat)), 0.1)
        longitude = centroid_lon + lon_offset_meters / (111_320.0 * longitude_scale)
        return latitude, longitude
