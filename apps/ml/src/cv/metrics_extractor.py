from __future__ import annotations

from datetime import UTC, datetime
from statistics import mean
from typing import Any

from dira_schemas.cv import CVDetection

from cv.yolo_detector import Detection


class CVMetricsExtractor:
    def __init__(self, pixel_per_meter: float | None = None, frame_interval_seconds: float = 1.0) -> None:
        if pixel_per_meter is not None and pixel_per_meter <= 0:
            raise ValueError("pixel_per_meter must be positive")
        if frame_interval_seconds <= 0:
            raise ValueError("frame_interval_seconds must be positive")

        self.pixel_per_meter = pixel_per_meter
        self.frame_interval_seconds = frame_interval_seconds

    def extract(
        self,
        detections: list[Detection],
        prev_detections: list[Detection],
        frame_metadata: dict[str, Any],
    ) -> CVDetection:
        frame_area = self._frame_area(frame_metadata)
        pixel_per_meter = self._pixel_per_meter(frame_metadata)
        frame_interval_seconds = float(frame_metadata.get("frame_interval_seconds", self.frame_interval_seconds))
        if frame_interval_seconds <= 0:
            raise ValueError("frame_interval_seconds must be positive")

        return CVDetection(
            camera_id=str(frame_metadata.get("camera_id", "unknown")),
            frame_timestamp=self._frame_timestamp(frame_metadata),
            vehicle_count=len(detections),
            avg_speed_kmh=self._average_speed_kmh(detections, prev_detections, pixel_per_meter, frame_interval_seconds),
            lane_occupancy=min(self._detection_area(detections) / frame_area, 1.0),
            queue_length_m=self._queue_length_m(detections, pixel_per_meter),
        )

    @staticmethod
    def _frame_timestamp(frame_metadata: dict[str, Any]) -> datetime:
        timestamp = frame_metadata.get("frame_timestamp")
        if timestamp is None:
            return datetime.now(UTC)
        return timestamp

    @staticmethod
    def _frame_area(frame_metadata: dict[str, Any]) -> float:
        frame_area = frame_metadata.get("frame_area")
        if frame_area is not None:
            frame_area_value = float(frame_area)
            if frame_area_value <= 0:
                raise ValueError("frame_area must be positive")
            return frame_area_value

        frame_width = frame_metadata.get("frame_width")
        frame_height = frame_metadata.get("frame_height")
        if frame_width is None or frame_height is None:
            frame_shape = frame_metadata.get("frame_shape")
            if isinstance(frame_shape, (list, tuple)) and len(frame_shape) >= 2:
                frame_height, frame_width = frame_shape[:2]
            else:
                raise ValueError("frame metadata must include frame_area or frame_width/frame_height")

        frame_area_value = float(frame_width) * float(frame_height)
        if frame_area_value <= 0:
            raise ValueError("frame area must be positive")
        return frame_area_value

    def _pixel_per_meter(self, frame_metadata: dict[str, Any]) -> float:
        pixel_per_meter = self.pixel_per_meter or frame_metadata.get("pixel_per_meter")
        if pixel_per_meter is None:
            raise ValueError("pixel_per_meter is required")

        pixel_per_meter_value = float(pixel_per_meter)
        if pixel_per_meter_value <= 0:
            raise ValueError("pixel_per_meter must be positive")
        return pixel_per_meter_value

    @staticmethod
    def _centroid(detection: Detection) -> tuple[float, float]:
        x1, y1, x2, y2 = detection.bbox
        return ((x1 + x2) / 2.0, (y1 + y2) / 2.0)

    def _average_speed_kmh(
        self,
        detections: list[Detection],
        prev_detections: list[Detection],
        pixel_per_meter: float,
        frame_interval_seconds: float,
    ) -> float | None:
        if not detections or not prev_detections:
            return None

        speeds: list[float] = []
        for current_detection, previous_detection in zip(detections, prev_detections):
            current_x, current_y = self._centroid(current_detection)
            previous_x, previous_y = self._centroid(previous_detection)
            pixel_distance = ((current_x - previous_x) ** 2 + (current_y - previous_y) ** 2) ** 0.5
            meters_travelled = pixel_distance / pixel_per_meter
            speed_mps = meters_travelled / frame_interval_seconds
            speeds.append(speed_mps * 3.6)

        return mean(speeds) if speeds else None

    def _queue_length_m(self, detections: list[Detection], pixel_per_meter: float) -> float | None:
        if not detections:
            return None

        centroids = [self._centroid(detection) for detection in detections]
        x_values = [centroid[0] for centroid in centroids]
        y_values = [centroid[1] for centroid in centroids]
        span_pixels = max(max(x_values) - min(x_values), max(y_values) - min(y_values))
        return span_pixels / pixel_per_meter

    @staticmethod
    def _detection_area(detections: list[Detection]) -> float:
        return sum(max(0.0, detection.bbox[2] - detection.bbox[0]) * max(0.0, detection.bbox[3] - detection.bbox[1]) for detection in detections)
