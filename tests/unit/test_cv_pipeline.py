from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
for package_path in (
    PROJECT_ROOT / "apps" / "ml" / "src",
    PROJECT_ROOT / "libs" / "schemas" / "src",
):
    package_path_str = str(package_path)
    if package_path_str not in sys.path:
        sys.path.insert(0, package_path_str)

import cv.frame_processor as frame_processor_module
import cv.speed_estimator as speed_estimator_module
from cv.frame_processor import FrameProcessor
from cv.metrics_extractor import CVMetricsExtractor
from cv.speed_estimator import SpeedEstimator
from cv.yolo_detector import Detection, YOLODetector


class _FakeBoxes:
    def __init__(self, xyxy, conf, cls) -> None:
        self.xyxy = xyxy
        self.conf = conf
        self.cls = cls


class _FakeResult:
    def __init__(self, names, boxes) -> None:
        self.names = names
        self.boxes = boxes


class _FakeModel:
    def __init__(self, results=None) -> None:
        self.results = results or []
        self.predict_calls = []

    def predict(self, frame, **kwargs):
        self.predict_calls.append((frame, kwargs))
        return self.results


class _FakeCapture:
    def __init__(self, frames) -> None:
        self.frames = list(frames)
        self.index = 0
        self.released = False

    def isOpened(self) -> bool:
        return True

    def read(self):
        if self.index >= len(self.frames):
            return False, None
        frame = self.frames[self.index]
        self.index += 1
        return True, frame

    def release(self) -> None:
        self.released = True


class _FakeCv2ForFrames:
    def __init__(self, capture: _FakeCapture) -> None:
        self.capture = capture
        self.video_source = None

    def VideoCapture(self, video_source):
        self.video_source = video_source
        return self.capture


class _FakeCv2ForFlow:
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


class _FakeFrame:
    shape = (100, 200, 3)


class _FakeDetector:
    def __init__(self) -> None:
        self.frames = []

    def detect(self, frame):
        self.frames.append(frame)
        return [Detection(bbox=(0.0, 0.0, 10.0, 10.0), class_name="car", confidence=0.9)]


def test_yolo_detector_filters_synthetic_vehicle_boxes() -> None:
    fake_model = _FakeModel(
        [
            _FakeResult(
                {
                    0: "person",
                    1: "car",
                    2: "truck",
                    3: "bus",
                    4: "motorcycle",
                    5: "dog",
                    6: "bicycle",
                    7: "traffic light",
                    8: "chair",
                    9: "cat",
                },
                _FakeBoxes(
                    xyxy=[(i, i, i + 10, i + 10) for i in range(10)],
                    conf=[0.1 + i * 0.08 for i in range(10)],
                    cls=list(range(10)),
                ),
            )
        ]
    )

    detector = YOLODetector(model=fake_model, device="cuda")
    detections = detector.detect(frame="synthetic-frame")

    assert [detection.class_name for detection in detections] == ["car", "truck", "bus", "motorcycle"]
    assert len(detections) == 4


def test_metrics_extractor_combines_detections_into_cv_detection() -> None:
    extractor = CVMetricsExtractor(pixel_per_meter=2.0, frame_interval_seconds=1.0)
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
            "frame_timestamp": "2026-04-30T08:15:00+00:00",
            "frame_width": 100,
            "frame_height": 100,
        },
    )

    assert cv_detection.vehicle_count == 2
    assert cv_detection.avg_speed_kmh == 18.0
    assert cv_detection.lane_occupancy == 0.08
    assert cv_detection.queue_length_m == 15.0


def test_frame_processor_samples_every_fifteenth_frame_from_local_file(tmp_path, monkeypatch) -> None:
    video_path = tmp_path / "sample.mp4"
    video_path.write_bytes(b"fake-video")

    capture = _FakeCapture([f"frame-{index}" for index in range(1, 31)])
    fake_cv2 = _FakeCv2ForFrames(capture)
    detector = _FakeDetector()
    monkeypatch.setattr(frame_processor_module, "_load_cv2", lambda: fake_cv2)

    processor = FrameProcessor(detector=detector, frame_skip_interval=15)
    results = list(processor.process(str(video_path)))

    assert fake_cv2.video_source == str(video_path)
    assert capture.released is True
    assert detector.frames == ["frame-15", "frame-30"]
    assert [result.frame_index for result in results] == [15, 30]


def test_speed_estimator_uses_mock_optical_flow(monkeypatch) -> None:
    monkeypatch.setattr(speed_estimator_module, "_load_cv2", lambda: _FakeCv2ForFlow())

    estimator = SpeedEstimator(pixel_per_meter=2.0, frame_interval_seconds=1.0)
    speed_kmh = estimator.estimate(
        prev_frame="prev-frame",
        curr_frame="curr-frame",
        detections=[
            Detection(bbox=(0.0, 0.0, 10.0, 10.0), class_name="car", confidence=0.9),
            Detection(bbox=(10.0, 10.0, 20.0, 20.0), class_name="truck", confidence=0.8),
        ],
    )

    assert speed_kmh == 18.0
