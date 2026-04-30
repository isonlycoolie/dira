from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import structlog
from pydantic import BaseModel
from pydantic import Field

from connectors.base import BaseConnector
from dira_common.exceptions import IngestionError
from dira_schemas.enums import DataSourceType
from dira_schemas.raw import RawMessage


logger = structlog.get_logger(__name__)


class DLQMessage(BaseModel):
    original_payload: dict[str, Any]
    error_type: str
    error_message: str
    source_topic: str
    failed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class KafkaProducerService(BaseConnector):
    TOPIC_MAP: dict[DataSourceType, str] = {
        DataSourceType.TELECOM: "dira.raw.telecom",
        DataSourceType.CCTV: "dira.raw.cctv",
        DataSourceType.FLEET_GPS: "dira.raw.fleet",
        DataSourceType.INCIDENT: "dira.raw.incidents",
        DataSourceType.WEATHER: "dira.raw.weather",
        DataSourceType.FUSED: "dira.processed.events",
    }
    DLQ_TOPIC = "dira.dlq"

    def connect(self) -> None:
        self._connect_producer()

    def disconnect(self) -> None:
        self._disconnect_producer()

    def health_check(self) -> bool:
        producer = self._producer
        if producer is None:
            return False
        return producer.is_healthy()

    def publish_raw(self, source_type: DataSourceType, message: BaseModel) -> dict[str, Any]:
        topic = self.TOPIC_MAP.get(source_type)
        if topic is None:
            error_message = f"unsupported data source type: {source_type}"
            self._route_to_dlq(message, type(source_type).__name__, error_message, str(source_type))
            raise IngestionError(
                error_message,
                details={"source_type": str(source_type), "dlq_topic": self.DLQ_TOPIC},
                source="ingestion",
            )

        try:
            wrapped_message = self._validate_and_wrap(message)
            return self.publish(wrapped_message, topic)
        except IngestionError as exc:
            self._route_to_dlq(message, type(exc).__name__, str(exc), topic)
            raise
        except Exception as exc:  # noqa: BLE001
            self._route_to_dlq(message, type(exc).__name__, str(exc), topic)
            raise IngestionError(
                "failed to publish raw message",
                details={"source_type": str(source_type), "topic": topic, "error": str(exc)},
                source="ingestion",
            ) from exc

    def _validate_and_wrap(self, message: BaseModel) -> RawMessage:
        if isinstance(message, RawMessage):
            return message

        try:
            return RawMessage.model_validate(message.model_dump(mode="json"))
        except Exception as exc:  # noqa: BLE001
            raise IngestionError(
                "invalid raw message payload",
                details={"error": str(exc)},
                source="ingestion",
            ) from exc

    def _route_to_dlq(self, message: BaseModel, error_type: str, error_message: str, source_topic: str) -> None:
        logger.error(
            "routing message to dlq",
            source_topic=source_topic,
            error_type=error_type,
            error_message=error_message,
        )
        dlq_message = DLQMessage(
            original_payload=message.model_dump(mode="json"),
            error_type=error_type,
            error_message=error_message,
            source_topic=source_topic,
        )
        self.publish(dlq_message, self.DLQ_TOPIC)
