from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import numpy as np


VEHICLE_CLASSES = {"car", "truck", "bus", "motorcycle"}
DEFAULT_MODEL_PATH = "yolo11n.pt"
DEFAULT_CONFIDENCE_THRESHOLD = 0.25
CPU_CONFIDENCE_THRESHOLD = 0.15

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Detection:
    bbox: tuple[float, float, float, float]
    class_name: str
    confidence: float


def _load_yolo_model(model_path: str = DEFAULT_MODEL_PATH) -> Any:
    try:
        from ultralytics import YOLO
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError("ultralytics is required to use YOLODetector") from exc

    return YOLO(model_path)


def _detect_device() -> str:
    try:
        import torch
    except ModuleNotFoundError:
        return "cpu"

    return "cuda" if torch.cuda.is_available() else "cpu"


class YOLODetector:
    def __init__(
        self,
        model: Any | None = None,
        model_path: str = DEFAULT_MODEL_PATH,
        confidence_threshold: float = DEFAULT_CONFIDENCE_THRESHOLD,
        device: str | None = None,
    ) -> None:
        resolved_device = device or _detect_device()
        resolved_confidence_threshold = confidence_threshold
        if device is None and resolved_device == "cpu" and confidence_threshold >= DEFAULT_CONFIDENCE_THRESHOLD:
            resolved_confidence_threshold = CPU_CONFIDENCE_THRESHOLD

        logger.info("Selected YOLO device: %s (confidence_threshold=%.2f)", resolved_device, resolved_confidence_threshold)

        self._model = model or _load_yolo_model(model_path)
        self.model_path = model_path
        self.confidence_threshold = resolved_confidence_threshold
        self.device = resolved_device

    def detect(self, frame: "np.ndarray") -> list[Detection]:
        predict_kwargs: dict[str, Any] = {"conf": self.confidence_threshold, "verbose": False}
        if self.device is not None:
            predict_kwargs["device"] = self.device

        results = self._model.predict(frame, **predict_kwargs)
        if not isinstance(results, (list, tuple)):
            results = [results]

        detections: list[Detection] = []
        for result in results:
            detections.extend(self._parse_result(result))
        return detections

    def _parse_result(self, result: Any) -> list[Detection]:
        boxes = getattr(result, "boxes", None)
        if boxes is None:
            return []

        xyxy = getattr(boxes, "xyxy", None)
        confidence_values = getattr(boxes, "conf", None)
        class_ids = getattr(boxes, "cls", None)
        if xyxy is None or confidence_values is None or class_ids is None:
            return []

        names = getattr(result, "names", getattr(self._model, "names", {}))
        detections: list[Detection] = []
        for bbox, confidence, class_id in zip(xyxy, confidence_values, class_ids):
            class_index = int(class_id)
            class_name = self._resolve_class_name(names, class_index)
            if class_name not in VEHICLE_CLASSES:
                continue

            detections.append(
                Detection(
                    bbox=tuple(float(value) for value in bbox),
                    class_name=class_name,
                    confidence=float(confidence),
                )
            )

        return detections

    @staticmethod
    def _resolve_class_name(names: Any, class_index: int) -> str:
        if isinstance(names, dict):
            return str(names.get(class_index, class_index))
        if isinstance(names, (list, tuple)) and 0 <= class_index < len(names):
            return str(names[class_index])
        return str(class_index)
