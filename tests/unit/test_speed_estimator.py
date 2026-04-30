from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
for package_path in (
    PROJECT_ROOT / "apps" / "ml" / "src",
):
    package_path_str = str(package_path)
    if package_path_str not in sys.path:
        sys.path.insert(0, package_path_str)

import cv.speed_estimator as speed_estimator_module
from cv.speed_estimator import SpeedEstimator
from cv.yolo_detector import Detection


class _FakeCv2:
    COLOR_BGR2GRAY = 6

    def cvtColor(self, frame, code):
        return frame

    def calcOpticalFlowPyrLK(self, prev_gray, curr_gray, points, _next_points, **kwargs):
        tracked_points = []
        status = []
        for point in points:
            if hasattr(point, "tolist"):
                point = point.tolist()
            while isinstance(point, (list, tuple)) and len(point) == 1 and isinstance(point[0], (list, tuple)):
                point = point[0]
            x, y = float(point[0]), float(point[1])
            tracked_points.append([[x + 10.0, y]])
            status.append([1])
        return tracked_points, status, None


def test_speed_estimator_uses_lucas_kanade_flow(monkeypatch) -> None:
    monkeypatch.setattr(speed_estimator_module, "_load_cv2", lambda: _FakeCv2())

    estimator = SpeedEstimator(pixel_per_meter=2.0, frame_interval_seconds=1.0)
    speed_kmh = estimator.estimate(
        prev_frame="prev-frame",
        curr_frame="curr-frame",
        detections=[Detection(bbox=(0.0, 0.0, 10.0, 10.0), class_name="car", confidence=0.9)],
    )

    assert speed_kmh == 18.0


def test_speed_estimator_returns_none_without_detections(monkeypatch) -> None:
    monkeypatch.setattr(speed_estimator_module, "_load_cv2", lambda: _FakeCv2())

    estimator = SpeedEstimator(pixel_per_meter=2.0, frame_interval_seconds=1.0)

    assert estimator.estimate(prev_frame="prev-frame", curr_frame="curr-frame", detections=[]) is None
