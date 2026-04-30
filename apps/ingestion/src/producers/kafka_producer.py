from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from connectors.base import BaseConnector
from dira_common.exceptions import IngestionError
from dira_schemas.enums import DataSourceType
from dira_schemas.raw import RawMessage


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
        try:
            topic = self.TOPIC_MAP[source_type]
        except KeyError as exc:
            self.publish(message, self.DLQ_TOPIC)
            raise IngestionError(
                "unsupported data source type",
                details={"source_type": str(source_type), "dlq_topic": self.DLQ_TOPIC},
                source="ingestion",
            ) from exc

        wrapped_message = self._validate_and_wrap(message)
        return self.publish(wrapped_message, topic)

    def _validate_and_wrap(self, message: BaseModel) -> RawMessage:
        if isinstance(message, RawMessage):
            return message

        try:
            return RawMessage.model_validate(message.model_dump(mode="json"))
        except Exception as exc:  # noqa: BLE001
            self.publish(message, self.DLQ_TOPIC)
            raise IngestionError(
                "invalid raw message payload",
                details={"dlq_topic": self.DLQ_TOPIC, "error": str(exc)},
                source="ingestion",
            ) from exc
