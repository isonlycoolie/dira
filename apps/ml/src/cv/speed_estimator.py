from __future__ import annotations

from statistics import mean
from typing import TYPE_CHECKING, Any

from cv.yolo_detector import Detection

if TYPE_CHECKING:
    import numpy as np


DEFAULT_PIXEL_PER_METER = 1.0
DEFAULT_FRAME_INTERVAL_SECONDS = 1.0


def _load_cv2() -> Any:
    try:
        import cv2
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError("opencv-python is required to use SpeedEstimator") from exc

    return cv2


def _build_points(detections: list[Detection]) -> Any:
    centroids = [
        ((detection.bbox[0] + detection.bbox[2]) / 2.0, (detection.bbox[1] + detection.bbox[3]) / 2.0)
        for detection in detections
    ]

    try:
        import numpy as np
    except ModuleNotFoundError:
        return [[x, y] for x, y in centroids]

    return np.array(centroids, dtype=np.float32).reshape(-1, 1, 2)


class SpeedEstimator:
    def __init__(self, pixel_per_meter: float = DEFAULT_PIXEL_PER_METER, frame_interval_seconds: float = DEFAULT_FRAME_INTERVAL_SECONDS) -> None:
        if pixel_per_meter <= 0:
            raise ValueError("pixel_per_meter must be positive")
        if frame_interval_seconds <= 0:
            raise ValueError("frame_interval_seconds must be positive")

        self.pixel_per_meter = pixel_per_meter
        self.frame_interval_seconds = frame_interval_seconds

    def estimate(self, prev_frame: "np.ndarray", curr_frame: "np.ndarray", detections: list[Detection]) -> float | None:
        if not detections:
            return None

        cv2 = _load_cv2()
        prev_gray = self._to_grayscale(cv2, prev_frame)
        curr_gray = self._to_grayscale(cv2, curr_frame)
        points = _build_points(detections)
        if len(points) == 0:
            return None

        next_points, status, _error = cv2.calcOpticalFlowPyrLK(
            prev_gray,
            curr_gray,
            points,
            None,
        )
        if next_points is None or status is None:
            return None

        speeds_kmh: list[float] = []
        for source_point, tracked_point, tracked_status in zip(points, next_points, status):
            if not self._is_tracked(tracked_status):
                continue

            source_x, source_y = self._coerce_point(source_point)
            tracked_x, tracked_y = self._coerce_point(tracked_point)
            pixel_distance = ((tracked_x - source_x) ** 2 + (tracked_y - source_y) ** 2) ** 0.5
            meters_travelled = pixel_distance / self.pixel_per_meter
            speed_mps = meters_travelled / self.frame_interval_seconds
            speeds_kmh.append(speed_mps * 3.6)

        return mean(speeds_kmh) if speeds_kmh else None

    @staticmethod
    def _to_grayscale(cv2: Any, frame: Any) -> Any:
        frame_ndim = getattr(frame, "ndim", None)
        if frame_ndim == 2:
            return frame
        return cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)

    @staticmethod
    def _coerce_point(point: Any) -> tuple[float, float]:
        if hasattr(point, "tolist"):
            point = point.tolist()

        while isinstance(point, (list, tuple)) and len(point) == 1 and isinstance(point[0], (list, tuple)):
            point = point[0]

        if isinstance(point, (list, tuple)) and len(point) >= 2:
            return float(point[0]), float(point[1])

        raise ValueError("point is not a valid 2D coordinate")

    @staticmethod
    def _is_tracked(status: Any) -> bool:
        if hasattr(status, "item"):
            return bool(status.item())

        while isinstance(status, (list, tuple)) and status:
            status = status[0]

        return bool(status)
