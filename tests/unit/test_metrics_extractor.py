from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
for package_path in (
    PROJECT_ROOT / "apps" / "ml" / "src",
    PROJECT_ROOT / "libs" / "schemas" / "src",
):
    package_path_str = str(package_path)
    if package_path_str not in sys.path:
        sys.path.insert(0, package_path_str)

from cv.metrics_extractor import CVMetricsExtractor
from cv.yolo_detector import Detection


def test_metrics_extractor_computes_vehicle_count_speed_occupancy_and_queue_length() -> None:
    extractor = CVMetricsExtractor(pixel_per_meter=2.0, frame_interval_seconds=1.0)
    frame_timestamp = datetime(2026, 4, 30, 8, 15, tzinfo=UTC)
    detections = [
        Detection(bbox=(10.0, 10.0, 30.0, 30.0), class_name="car", confidence=0.91),
        Detection(bbox=(40.0, 40.0, 60.0, 60.0), class_name="truck", confidence=0.77),
    ]
    prev_detections = [
        Detection(bbox=(0.0, 10.0, 20.0, 30.0), class_name="car", confidence=0.9),
        Detection(bbox=(30.0, 40.0, 50.0, 60.0), class_name="truck", confidence=0.75),
    ]

    cv_detection = extractor.extract(
        detections=detections,
        prev_detections=prev_detections,
        frame_metadata={
            "camera_id": "cam-1",
            "frame_timestamp": frame_timestamp,
            "frame_width": 100,
            "frame_height": 100,
        },
    )

    assert cv_detection.camera_id == "cam-1"
    assert cv_detection.frame_timestamp == frame_timestamp
    assert cv_detection.vehicle_count == 2
    assert cv_detection.avg_speed_kmh == 18.0
    assert cv_detection.lane_occupancy == 0.08
    assert cv_detection.queue_length_m == 15.0


def test_metrics_extractor_returns_none_speed_and_queue_for_empty_detections() -> None:
    extractor = CVMetricsExtractor(pixel_per_meter=2.0, frame_interval_seconds=1.0)

    cv_detection = extractor.extract(
        detections=[],
        prev_detections=[],
        frame_metadata={
            "camera_id": "cam-1",
            "frame_timestamp": datetime(2026, 4, 30, 8, 15, tzinfo=UTC),
            "frame_width": 100,
            "frame_height": 100,
        },
    )

    assert cv_detection.vehicle_count == 0
    assert cv_detection.avg_speed_kmh is None
    assert cv_detection.lane_occupancy == 0.0
    assert cv_detection.queue_length_m is None
