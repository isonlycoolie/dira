from __future__ import annotations

from datetime import timedelta

from dira_schemas.events import UnifiedTrafficEvent


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


__all__ = ["CongestionIndexCalculator", "DwellTimeDetector"]
