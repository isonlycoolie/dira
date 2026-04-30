from __future__ import annotations

import json
import time
from collections.abc import Iterator, Sequence
from typing import Any

import structlog

try:
    from kafka import KafkaProducer as KafkaPythonProducer
    from kafka import KafkaConsumer as KafkaPythonConsumer
    from kafka.errors import CommitFailedError
except ModuleNotFoundError:
    KafkaPythonProducer = None
    KafkaPythonConsumer = None

    class CommitFailedError(Exception):
        pass

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


class _LocalKafkaConsumer:
    def __init__(self, topics: Sequence[str], group_id: str, auto_offset_reset: str) -> None:
        self.topics = list(topics)
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self._queued_batches: list[list[dict[str, Any]]] = []
        self.commit_calls = 0

    def queue_batch(self, messages: list[dict[str, Any]]) -> None:
        self._queued_batches.append(messages)

    def poll(self, timeout_ms: int = 1000) -> dict[str, list[Any]]:
        if not self._queued_batches:
            return {}
        batch = self._queued_batches.pop(0)
        return {"local": [type("Record", (), {"value": json.dumps(message).encode("utf-8")}) for message in batch]}

    def commit(self) -> None:
        self.commit_calls += 1

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


class KafkaConsumerWrapper:
    def __init__(
        self,
        topics: Sequence[str] | None = None,
        group_id: str = "dira-pipeline",
        auto_offset_reset: str = "earliest",
        consumer: Any | None = None,
    ) -> None:
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self._logger = structlog.get_logger(__name__)
        if consumer is not None:
            self._consumer = consumer
        elif KafkaPythonConsumer is not None:
            topic_list = list(topics or [])
            self._consumer = KafkaPythonConsumer(
                *topic_list,
                group_id=group_id,
                auto_offset_reset=auto_offset_reset,
                enable_auto_commit=False,
                value_deserializer=lambda payload: json.loads(payload.decode("utf-8")),
            )
        else:
            self._consumer = _LocalKafkaConsumer(topics or [], group_id, auto_offset_reset)

    def _commit_with_retry(self, max_attempts: int = 3) -> None:
        commit_method = getattr(self._consumer, "commit", None)
        if not callable(commit_method):
            return

        for attempt_number in range(1, max_attempts + 1):
            try:
                commit_method()
                return
            except CommitFailedError as exc:
                if attempt_number >= max_attempts:
                    raise IngestionError(
                        "failed to commit consumer offset",
                        details={"group_id": self.group_id, "error": str(exc)},
                        source="ingestion",
                    ) from exc

                self._logger.warning(
                    "retrying consumer commit",
                    group_id=self.group_id,
                    attempt=attempt_number,
                    error=str(exc),
                )
                time.sleep(min(2 ** (attempt_number - 1), 60))
            except Exception as exc:  # noqa: BLE001
                raise IngestionError(
                    "failed to commit consumer offset",
                    details={"group_id": self.group_id, "error": str(exc)},
                    source="ingestion",
                ) from exc

    def poll(self, timeout_ms: int = 1000) -> Iterator[dict[str, Any]]:
        records_by_partition = self._consumer.poll(timeout_ms=timeout_ms)
        messages: list[dict[str, Any]] = []
        for records in records_by_partition.values():
            for record in records:
                value = getattr(record, "value", record)
                if isinstance(value, bytes):
                    value = json.loads(value.decode("utf-8"))
                elif isinstance(value, str):
                    value = json.loads(value)
                messages.append(value)

        self._commit_with_retry()
        yield from messages

    def close(self) -> None:
        close_method = getattr(self._consumer, "close", None)
        if callable(close_method):
            close_method()
