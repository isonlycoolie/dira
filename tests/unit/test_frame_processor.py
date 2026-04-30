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

import cv.frame_processor as frame_processor_module
from cv.frame_processor import FrameProcessor
from cv.yolo_detector import Detection


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


class _FakeCv2:
    def __init__(self, capture: _FakeCapture) -> None:
        self.capture = capture

    def VideoCapture(self, video_source):
        self.video_source = video_source
        return self.capture


class _FakeDetector:
    def __init__(self) -> None:
        self.frames = []

    def detect(self, frame):
        self.frames.append(frame)
        return [Detection(bbox=(0.0, 1.0, 2.0, 3.0), class_name="car", confidence=0.9)]


def test_frame_processor_samples_every_fifteenth_frame(monkeypatch, caplog) -> None:
    frames = [f"frame-{index}" for index in range(1, 31)]
    capture = _FakeCapture(frames)
    fake_cv2 = _FakeCv2(capture)
    detector = _FakeDetector()
    monkeypatch.setattr(frame_processor_module, "_load_cv2", lambda: fake_cv2)

    processor = FrameProcessor(detector=detector, frame_skip_interval=15)

    with caplog.at_level("INFO"):
        results = list(processor.process("camera.mp4"))

    assert fake_cv2.video_source == "camera.mp4"
    assert capture.released is True
    assert detector.frames == ["frame-15", "frame-30"]
    assert [result.frame_index for result in results] == [15, 30]
    assert [result.frame for result in results] == ["frame-15", "frame-30"]
    assert all(result.detections for result in results)
    assert "Processed frame 15 from camera.mp4" in caplog.text
    assert "Processed frame 30 from camera.mp4" in caplog.text


def test_frame_processor_skips_frame_after_high_latency(monkeypatch, caplog) -> None:
    frames = [f"frame-{index}" for index in range(1, 46)]
    capture = _FakeCapture(frames)
    fake_cv2 = _FakeCv2(capture)
    detector = _FakeDetector()
    latency_values = iter([0.0, 0.1, 0.2, 0.22])

    monkeypatch.setattr(frame_processor_module, "_load_cv2", lambda: fake_cv2)
    monkeypatch.setattr(frame_processor_module, "perf_counter", lambda: next(latency_values))

    processor = FrameProcessor(detector=detector, frame_skip_interval=15, max_latency_ms=50.0)

    with caplog.at_level("DEBUG"):
        results = list(processor.process("camera.mp4"))

    assert capture.released is True
    assert detector.frames == ["frame-15", "frame-45"]
    assert [result.frame_index for result in results] == [15, 45]
    assert processor.frames_processed == 2
    assert "Skipping frame 30 from camera.mp4" in caplog.text
