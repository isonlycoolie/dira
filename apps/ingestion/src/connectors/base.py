from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any

from pydantic import BaseModel

from dira_common.exceptions import IngestionError
from dira_common.kafka import KafkaProducerWrapper
from dira_common.metrics import PrometheusRegistry


class BaseConnector(ABC):
    def __init__(self, brokers: Sequence[str] | None = None) -> None:
        self.brokers = tuple(brokers or ())
        self._producer: KafkaProducerWrapper | None = None

    @abstractmethod
    def connect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def disconnect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def health_check(self) -> bool:
        raise NotImplementedError

    def publish(self, message: BaseModel, topic: str) -> dict[str, Any]:
        if self._producer is None:
            self.connect()

        producer = self._producer
        if producer is None:
            raise IngestionError(
                "connector is not connected",
                details={"topic": topic, "connector": type(self).__name__},
                source="ingestion",
            )

        try:
            result = producer.publish(topic, message)
        except IngestionError:
            PrometheusRegistry.messages_failed.labels(topic=topic, status="failure").inc()
            raise
        except Exception as exc:  # noqa: BLE001
            PrometheusRegistry.messages_failed.labels(topic=topic, status="failure").inc()
            raise IngestionError(
                "failed to publish message",
                details={"topic": topic, "connector": type(self).__name__, "error": str(exc)},
                source="ingestion",
            ) from exc

        PrometheusRegistry.messages_published.labels(topic=topic, status="success").inc()
        return result

    def _connect_producer(self) -> KafkaProducerWrapper:
        self._producer = KafkaProducerWrapper(self.brokers or None)
        return self._producer

    def _disconnect_producer(self) -> None:
        if self._producer is not None:
            self._producer.close()
            self._producer = None
