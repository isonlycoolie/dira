from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

import structlog

try:
    from kafka import KafkaProducer as KafkaPythonProducer
except ModuleNotFoundError:
    KafkaPythonProducer = None

from pydantic import BaseModel

from dira_common.exceptions import IngestionError


class _LocalKafkaFuture:
    def get(self, timeout: float | None = None) -> dict[str, Any]:
        return {"timeout": timeout, "status": "sent"}


class _LocalKafkaProducer:
    def __init__(self, brokers: Sequence[str]) -> None:
        self.brokers = list(brokers)
        self.sent_messages: list[tuple[str, bytes]] = []

    def send(self, topic: str, value: bytes) -> _LocalKafkaFuture:
        self.sent_messages.append((topic, value))
        return _LocalKafkaFuture()

    def bootstrap_connected(self) -> bool:
        return True

    def close(self) -> None:
        return None


class KafkaProducerWrapper:
    def __init__(self, brokers: Sequence[str] | None = None, producer: Any | None = None) -> None:
        broker_list = list(brokers) if brokers is not None else ["localhost:9092"]
        self._logger = structlog.get_logger(__name__)
        if producer is not None:
            self._producer = producer
        elif KafkaPythonProducer is not None:
            self._producer = KafkaPythonProducer(
                bootstrap_servers=broker_list,
                value_serializer=lambda payload: payload,
                acks="all",
                retries=0,
            )
        else:
            self._producer = _LocalKafkaProducer(broker_list)

    def publish(self, topic: str, message: BaseModel) -> dict[str, Any]:
        try:
            payload = json.dumps(message.model_dump(mode="json")).encode("utf-8")
            future = self._producer.send(topic, value=payload)
            result = future.get(timeout=10) if hasattr(future, "get") else None
            self._logger.info("published message", topic=topic)
            return {"topic": topic, "payload": payload, "result": result}
        except Exception as exc:  # noqa: BLE001
            raise IngestionError(
                "failed to publish message",
                details={"topic": topic, "error": str(exc)},
                source="ingestion",
            ) from exc

    def is_healthy(self) -> bool:
        try:
            return bool(self._producer.bootstrap_connected())
        except Exception:
            return False

    def close(self) -> None:
        close_method = getattr(self._producer, "close", None)
        if callable(close_method):
            close_method()
