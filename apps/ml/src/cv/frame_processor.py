from __future__ import annotations

import logging
from dataclasses import dataclass
from time import perf_counter
from typing import TYPE_CHECKING, Any, Iterator

from cv.yolo_detector import Detection, YOLODetector

if TYPE_CHECKING:
    import numpy as np


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class FrameResult:
    frame_index: int
    frame: Any
    detections: list[Detection]
    inference_latency_ms: float


def _load_cv2() -> Any:
    try:
        import cv2
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError("opencv-python is required to use FrameProcessor") from exc

    return cv2


class FrameProcessor:
    def __init__(self, detector: YOLODetector | None = None, frame_skip_interval: int = 15) -> None:
        if frame_skip_interval <= 0:
            raise ValueError("frame_skip_interval must be positive")

        self.detector = detector or YOLODetector()
        self.frame_skip_interval = frame_skip_interval

    def process(self, video_source: str) -> Iterator[FrameResult]:
        cv2 = _load_cv2()
        capture = cv2.VideoCapture(video_source)
        if not capture.isOpened():
            raise RuntimeError(f"Unable to open video source: {video_source}")

        frame_index = 0
        try:
            while True:
                has_frame, frame = capture.read()
                if not has_frame:
                    break

                frame_index += 1
                if frame_index % self.frame_skip_interval != 0:
                    continue

                inference_started = perf_counter()
                detections = self.detector.detect(frame)
                inference_latency_ms = (perf_counter() - inference_started) * 1000.0
                logger.info(
                    "Processed frame %s from %s in %.2f ms",
                    frame_index,
                    video_source,
                    inference_latency_ms,
                )
                yield FrameResult(
                    frame_index=frame_index,
                    frame=frame,
                    detections=detections,
                    inference_latency_ms=inference_latency_ms,
                )
        finally:
            capture.release()
