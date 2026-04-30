#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import Any

SEVEN_DAYS_MS = 7 * 24 * 60 * 60 * 1000


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int = 1
    retention_ms: int = SEVEN_DAYS_MS


TOPIC_SPECS: tuple[TopicSpec, ...] = (
    TopicSpec("dira.raw.telecom"),
    TopicSpec("dira.raw.cctv"),
    TopicSpec("dira.raw.fleet"),
    TopicSpec("dira.raw.incidents"),
    TopicSpec("dira.raw.weather"),
    TopicSpec("dira.processed.events"),
    TopicSpec("dira.alerts.congestion"),
    TopicSpec("dira.dlq"),
)


def _load_kafka_admin() -> tuple[Any, Any, type[Exception]]:
    try:
        from kafka import KafkaAdminClient
        from kafka.admin import NewTopic
        from kafka.errors import TopicAlreadyExistsError
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised when dependency is absent
        raise RuntimeError("kafka-python is required for Kafka topic initialization") from exc
    return KafkaAdminClient, NewTopic, TopicAlreadyExistsError


def _parse_brokers(value: str) -> list[str]:
    brokers = [item.strip() for item in value.split(",") if item.strip()]
    return brokers or ["localhost:9092"]


def _existing_topics(admin_client: Any) -> set[str]:
    list_topics = getattr(admin_client, "list_topics", None)
    if not callable(list_topics):
        return set()

    try:
        return {str(topic) for topic in list_topics()}
    except Exception:  # noqa: BLE001
        return set()


def init_kafka_topics(
    admin_client: Any,
    new_topic_factory: Any,
    topic_already_exists_error: type[Exception] | tuple[type[Exception], ...],
) -> dict[str, list[str]]:
    existing_topics = _existing_topics(admin_client)
    create_topics = getattr(admin_client, "create_topics", None)
    if not callable(create_topics):
        raise RuntimeError("Kafka admin client does not support topic creation")

    created: list[str] = []
    skipped: list[str] = []
    for topic in TOPIC_SPECS:
        if topic.name in existing_topics:
            skipped.append(topic.name)
            continue

        new_topic = new_topic_factory(
            name=topic.name,
            num_partitions=topic.partitions,
            replication_factor=1,
            topic_configs={"cleanup.policy": "delete", "retention.ms": str(topic.retention_ms)},
        )
        try:
            create_topics(new_topics=[new_topic])
        except topic_already_exists_error:
            skipped.append(topic.name)
        else:
            created.append(topic.name)

    return {"created": created, "skipped": skipped}


def main() -> int:
    brokers = _parse_brokers(os.getenv("KAFKA_BROKERS", "localhost:9092"))
    admin_client_cls, new_topic_factory, topic_already_exists_error = _load_kafka_admin()
    admin_client = admin_client_cls(bootstrap_servers=brokers, client_id="dira-topic-init")

    try:
        summary = init_kafka_topics(admin_client, new_topic_factory, topic_already_exists_error)
    except Exception as exc:  # noqa: BLE001
        print(f"failed to initialize kafka topics: {exc}", file=sys.stderr)
        return 1
    finally:
        close_method = getattr(admin_client, "close", None)
        if callable(close_method):
            close_method()

    print(
        f"created {len(summary['created'])} topic(s), skipped {len(summary['skipped'])} existing topic(s)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
