from __future__ import annotations


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


__all__ = ["CongestionIndexCalculator"]
