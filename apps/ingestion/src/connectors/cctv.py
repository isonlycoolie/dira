from __future__ import annotations

import json
import logging
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dira_schemas.cv import CVDetection

from cv.frame_processor import FrameProcessor, FrameResult
from cv.metrics_extractor import CVMetricsExtractor
from cv.yolo_detector import Detection

from .base import BaseConnector

logger = logging.getLogger(__name__)

DEFAULT_CAMERA_REGISTRY_PATH = Path(__file__).resolve().parents[4] / "infra" / "camera_registry.json"
DEFAULT_CCTV_TOPIC = "dira.raw.cctv"


class CCTVConnector(BaseConnector):
    def __init__(
        self,
        brokers: Sequence[str] | None = None,
        camera_registry_path: str | Path | None = None,
        frame_processor: FrameProcessor | None = None,
        metrics_extractor: CVMetricsExtractor | None = None,
        reconnect_delay_seconds: float = 5.0,
    ) -> None:
        super().__init__(brokers=brokers)
        self._camera_registry_path = Path(camera_registry_path or DEFAULT_CAMERA_REGISTRY_PATH)
        self._camera_registry = self._load_camera_registry(self._camera_registry_path)
        self._frame_processor = frame_processor
        self._metrics_extractor = metrics_extractor
        self._reconnect_delay_seconds = reconnect_delay_seconds

    def connect(self) -> None:
        self._connect_producer()

    def disconnect(self) -> None:
        self._disconnect_producer()

    def health_check(self) -> bool:
        producer = self._producer
        if producer is None:
            return False
        return producer.is_healthy()

    def run(self, camera_id: str, video_source: str) -> None:
        camera_config = self._camera_config(camera_id)
        self.connect()

        is_rtsp_source = video_source.lower().startswith("rtsp://")
        while True:
            try:
                self._run_once(camera_id, camera_config, video_source)
            except Exception as exc:  # noqa: BLE001
                if not is_rtsp_source:
                    raise
                logger.warning("Reconnecting CCTV stream for %s after error: %s", camera_id, exc)
                time.sleep(self._reconnect_delay_seconds)
                continue

            if not is_rtsp_source:
                return

    def _run_once(self, camera_id: str, camera_config: dict[str, Any], video_source: str) -> None:
        frame_processor = self._frame_processor or FrameProcessor()
        metrics_extractor = self._metrics_extractor or CVMetricsExtractor()
        prev_detections: list[Detection] = []

        for frame_result in frame_processor.process(video_source):
            frame_metadata = self._frame_metadata(camera_id, camera_config, frame_result)
            cv_detection = metrics_extractor.extract(frame_result.detections, prev_detections, frame_metadata)
            self.publish(cv_detection, DEFAULT_CCTV_TOPIC)
            prev_detections = frame_result.detections

    def _camera_config(self, camera_id: str) -> dict[str, Any]:
        try:
            camera_config = self._camera_registry[camera_id]
        except KeyError as exc:
            available_cameras = ", ".join(sorted(self._camera_registry)) or "<none>"
            raise ValueError(f"Unknown camera_id {camera_id!r}; available cameras: {available_cameras}") from exc

        return camera_config

    @staticmethod
    def _load_camera_registry(camera_registry_path: Path) -> dict[str, dict[str, Any]]:
        if not camera_registry_path.exists():
            raise FileNotFoundError(f"camera registry not found: {camera_registry_path}")

        with camera_registry_path.open("r", encoding="utf-8") as handle:
            registry = json.load(handle)

        if not isinstance(registry, dict):
            raise ValueError("camera registry must be a JSON object keyed by camera_id")

        return registry

    @staticmethod
    def _frame_metadata(camera_id: str, camera_config: dict[str, Any], frame_result: FrameResult) -> dict[str, Any]:
        frame_shape = getattr(frame_result.frame, "shape", None)
        frame_height = None
        frame_width = None
        if isinstance(frame_shape, (list, tuple)) and len(frame_shape) >= 2:
            frame_height = int(frame_shape[0])
            frame_width = int(frame_shape[1])

        return {
            "camera_id": camera_id,
            "camera_registry": camera_config,
            "frame_timestamp": datetime.now(UTC),
            "frame_index": frame_result.frame_index,
            "frame_latency_ms": frame_result.inference_latency_ms,
            "frame_shape": frame_shape,
            "frame_height": frame_height,
            "frame_width": frame_width,
            "frame_interval_seconds": 1.0,
            "pixel_per_meter": camera_config.get("pixel_per_meter"),
        }
