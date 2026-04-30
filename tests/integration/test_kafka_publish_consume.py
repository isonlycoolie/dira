from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import BaseModel

PROJECT_ROOT = Path(__file__).resolve().parents[2]
for package_path in (
    PROJECT_ROOT / "libs" / "common" / "src",
    PROJECT_ROOT / "libs" / "schemas" / "src",
    PROJECT_ROOT / "apps" / "ingestion" / "src",
):
    package_path_str = str(package_path)
    if package_path_str not in sys.path:
        sys.path.insert(0, package_path_str)

from dira_schemas.enums import DataSourceType
from dira_schemas.raw import GeoPoint, RawMessage
from dira_schemas.telecom import TelecomPing
from dira_common.exceptions import IngestionError
from dira_common.kafka import KafkaConsumerWrapper, KafkaProducerWrapper
from producers.kafka_producer import KafkaProducerService


class InvalidPayload(BaseModel):
    value: int


@pytest.mark.integration
def test_kafka_publish_consume_round_trip_and_dlq() -> None:
    pytest.importorskip("kafka")
    pytest.importorskip("testcontainers.kafka")

    from testcontainers.kafka import KafkaContainer

    def collect_messages(consumer: object, expected_count: int, timeout_ms: int = 1000) -> list[dict[str, object]]:
        collected: list[dict[str, object]] = []
        for _ in range(5):
            batch = list(consumer.poll(timeout_ms=timeout_ms))
            collected.extend(batch)
            if len(collected) >= expected_count:
                break
        return collected

    with KafkaContainer("confluentinc/cp-kafka:7.6.0") as kafka_container:
        bootstrap_server = kafka_container.get_bootstrap_server()

        producer = KafkaProducerWrapper([bootstrap_server])

        for index in range(10):
            ping = TelecomPing(
                device_id_hash=f"device-{index}",
                tower_id=f"tower-{index % 3}",
                lat=-6.8 + (index * 0.0001),
                lon=39.2 + (index * 0.0001),
                timestamp=datetime(2026, 4, 30, 8, 15, tzinfo=UTC),
                signal_strength=-65 + index,
            )
            producer.publish("dira.raw.telecom", ping)

        producer.close()

        telecom_consumer = KafkaConsumerWrapper(
            ["dira.raw.telecom"],
            group_id=f"dira-telecom-test-{uuid4().hex}",
            auto_offset_reset="earliest",
        )
        telemetry_messages = collect_messages(telecom_consumer, expected_count=10)
        telecom_consumer.close()

        assert len(telemetry_messages) == 10
        round_tripped = [TelecomPing.model_validate(message) for message in telemetry_messages]
        assert round_tripped[0].device_id_hash == "device-0"
        assert round_tripped[-1].tower_id == "tower-0"

        producer_service = KafkaProducerService([bootstrap_server])
        invalid_payload = InvalidPayload(value=42)
        with pytest.raises(IngestionError):
            producer_service.publish_raw(DataSourceType.TELECOM, invalid_payload)

        dlq_consumer = KafkaConsumerWrapper(
            ["dira.dlq"],
            group_id=f"dira-dlq-test-{uuid4().hex}",
            auto_offset_reset="earliest",
        )
        dlq_messages = collect_messages(dlq_consumer, expected_count=1)
        dlq_consumer.close()
        producer_service.disconnect()

        assert len(dlq_messages) == 1
        assert dlq_messages[0]["source_topic"] == "dira.raw.telecom"
        assert dlq_messages[0]["error_type"] == "IngestionError"
        assert dlq_messages[0]["original_payload"] == {"value": 42}
