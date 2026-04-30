from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
for package_path in (
    PROJECT_ROOT / "apps" / "ingestion" / "src",
    PROJECT_ROOT / "apps" / "ml" / "src",
    PROJECT_ROOT / "libs" / "common" / "src",
    PROJECT_ROOT / "libs" / "schemas" / "src",
):
    package_path_str = str(package_path)
    if package_path_str not in sys.path:
        sys.path.insert(0, package_path_str)

from connectors import cctv as cctv_module
from connectors.base import BaseConnector
from connectors.cctv import CCTVConnector
from cv.frame_processor import FrameResult
from cv.yolo_detector import Detection
from dira_schemas.cv import CVDetection


class _FakeFrame:
    shape = (100, 200, 3)


class _FakeFrameProcessor:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def process(self, video_source: str):
        self.calls.append(video_source)
        yield FrameResult(
            frame_index=15,
            frame=_FakeFrame(),
            detections=[Detection(bbox=(10.0, 10.0, 30.0, 30.0), class_name="car", confidence=0.9)],
            inference_latency_ms=12.5,
        )


class _FakeMetricsExtractor:
    def __init__(self) -> None:
        self.calls: list[tuple[list[Detection], list[Detection], dict[str, object]]] = []

    def extract(self, detections, prev_detections, frame_metadata):
        self.calls.append((detections, prev_detections, frame_metadata))
        return CVDetection(
            camera_id=frame_metadata["camera_id"],
            frame_timestamp=frame_metadata["frame_timestamp"],
            vehicle_count=len(detections),
            avg_speed_kmh=18.0,
            lane_occupancy=0.08,
            queue_length_m=15.0,
        )


class _FakeProducer:
    def __init__(self) -> None:
        self.calls: list[tuple[str, CVDetection]] = []

    def publish(self, topic, message):
        self.calls.append((topic, message))
        return {"topic": topic}

    def is_healthy(self):
        return True

    def close(self):
        return None


def test_cctv_connector_processes_and_publishes_frame(monkeypatch) -> None:
    fake_processor = _FakeFrameProcessor()
    fake_extractor = _FakeMetricsExtractor()
    fake_producer = _FakeProducer()

    monkeypatch.setattr(BaseConnector, "_connect_producer", lambda self: setattr(self, "_producer", fake_producer))

    connector = CCTVConnector(frame_processor=fake_processor, metrics_extractor=fake_extractor)
    connector.run("cam-dsm-01", "file.mp4")

    assert fake_processor.calls == ["file.mp4"]
    assert len(fake_extractor.calls) == 1
    detections, prev_detections, frame_metadata = fake_extractor.calls[0]
    assert detections[0].class_name == "car"
    assert prev_detections == []
    assert frame_metadata["camera_id"] == "cam-dsm-01"
    assert frame_metadata["camera_registry"]["road_segment_id"] == 101
    assert frame_metadata["pixel_per_meter"] == 4.5
    assert frame_metadata["frame_width"] == 200
    assert frame_metadata["frame_height"] == 100
    assert isinstance(frame_metadata["frame_timestamp"], datetime)
    assert frame_metadata["frame_timestamp"].tzinfo == UTC
    assert fake_producer.calls[0][0] == "dira.raw.cctv"
    assert fake_producer.calls[0][1].camera_id == "cam-dsm-01"


def test_cctv_connector_fails_fast_for_unknown_camera() -> None:
    connector = CCTVConnector(frame_processor=_FakeFrameProcessor(), metrics_extractor=_FakeMetricsExtractor())

    try:
        connector.run("cam-missing", "file.mp4")
    except ValueError as exc:
        assert "Unknown camera_id" in str(exc)
    else:
        raise AssertionError("unknown camera should fail fast")
