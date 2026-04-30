#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import Any

from init_kafka_topics import TOPIC_SPECS

GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
RESET = "\033[0m"


@dataclass(frozen=True)
class TopicHealth:
    name: str
    present: bool
    partitions: int
    detail: str


def _load_kafka_clients() -> tuple[Any, Any]:
    try:
        from kafka import KafkaAdminClient, KafkaConsumer
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised when dependency is absent
        raise RuntimeError("kafka-python is required for Kafka topic checks") from exc
    return KafkaAdminClient, KafkaConsumer


def _parse_brokers(value: str) -> list[str]:
    brokers = [item.strip() for item in value.split(",") if item.strip()]
    return brokers or ["localhost:9092"]


def check_kafka_topics(brokers: list[str], admin_client_cls: Any, consumer_cls: Any) -> list[TopicHealth]:
    admin_client = admin_client_cls(bootstrap_servers=brokers, client_id="dira-topic-check")
    consumer = consumer_cls(bootstrap_servers=brokers, group_id="dira-topic-check", enable_auto_commit=False)
    try:
        available_topics = {str(topic) for topic in (admin_client.list_topics() or [])}
        checks: list[TopicHealth] = []
        for spec in TOPIC_SPECS:
            present = spec.name in available_topics
            partitions: set[Any] = set()
            if present:
                try:
                    partitions = set(consumer.partitions_for_topic(spec.name) or set())
                except Exception:  # noqa: BLE001
                    partitions = set()
            partition_count = len(partitions)
            if not present:
                detail = "missing"
            elif partition_count < 1:
                detail = "no partitions"
            else:
                detail = "ok"
            checks.append(TopicHealth(spec.name, present, partition_count, detail))
        return checks
    finally:
        close_method = getattr(consumer, "close", None)
        if callable(close_method):
            close_method()
        close_method = getattr(admin_client, "close", None)
        if callable(close_method):
            close_method()


def _print_results(results: list[TopicHealth]) -> None:
    widths = [max(len("Topic"), max(len(result.name) for result in results)), len("Status"), len("Partitions"), len("Detail")]
    print(f"{'Topic'.ljust(widths[0])} | {'Status'.ljust(widths[1])} | {'Partitions'.ljust(widths[2])} | Detail")
    print("-" * (sum(widths) + 9))
    for result in results:
        status = f"{GREEN}OK{RESET}" if result.present and result.partitions >= 1 else f"{RED}FAIL{RESET}"
        print(
            f"{result.name.ljust(widths[0])} | {status.ljust(widths[1] + len(GREEN) + len(RESET) + 2)} | "
            f"{str(result.partitions).ljust(widths[2])} | {result.detail}"
        )


def main() -> int:
    brokers = _parse_brokers(os.getenv("KAFKA_BROKERS", "localhost:9092"))
    try:
        admin_client_cls, consumer_cls = _load_kafka_clients()
    except RuntimeError as exc:
        print(f"{RED}failed to load kafka client: {exc}{RESET}", file=sys.stderr)
        return 1

    results = check_kafka_topics(brokers, admin_client_cls, consumer_cls)
    _print_results(results)

    failures = [result for result in results if not result.present or result.partitions < 1]
    if failures:
        print(f"{YELLOW}{len(failures)} topic(s) failed health check{RESET}")
        return 1

    print(f"{GREEN}all kafka topics healthy{RESET}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
