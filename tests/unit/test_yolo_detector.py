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

import cv.yolo_detector as yolo_detector_module
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


def test_yolo_detector_filters_vehicle_classes() -> None:
    frame = object()
    fake_model = _FakeModel(
        [
            _FakeResult(
                {0: "person", 2: "car", 7: "truck"},
                _FakeBoxes(
                    xyxy=[(1, 2, 3, 4), (10, 20, 30, 40), (5, 6, 7, 8)],
                    conf=[0.2, 0.91, 0.77],
                    cls=[0, 2, 7],
                ),
            )
        ]
    )

    detector = YOLODetector(model=fake_model)
    detections = detector.detect(frame=frame)

    assert detections == [
        Detection(bbox=(10.0, 20.0, 30.0, 40.0), class_name="car", confidence=0.91),
        Detection(bbox=(5.0, 6.0, 7.0, 8.0), class_name="truck", confidence=0.77),
    ]
    assert fake_model.predict_calls == [(frame, {"conf": 0.25, "verbose": False})]


def test_yolo_detector_uses_default_model_path(monkeypatch) -> None:
    captured = {}

    def fake_loader(model_path: str):
        captured["model_path"] = model_path
        return _FakeModel([_FakeResult({}, _FakeBoxes([], [], []))])

    monkeypatch.setattr(yolo_detector_module, "_load_yolo_model", fake_loader)

    detector = YOLODetector()

    assert captured["model_path"] == "yolo11n.pt"
    assert detector.model_path == "yolo11n.pt"
