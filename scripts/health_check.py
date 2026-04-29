#!/usr/bin/env python3

from __future__ import annotations

import os
import socket
import sys
from dataclasses import dataclass
from urllib.parse import urlparse
from urllib.request import urlopen


@dataclass(frozen=True)
class CheckResult:
    name: str
    target: str
    healthy: bool
    detail: str


GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
RESET = "\033[0m"


def parse_host_port(value: str, default_host: str, default_port: int) -> tuple[str, int]:
    if not value:
        return default_host, default_port

    if "://" in value:
        parsed = urlparse(value)
        return parsed.hostname or default_host, parsed.port or default_port

    first_item = value.split(",", 1)[0].strip()
    if not first_item:
        return default_host, default_port

    if ":" in first_item:
        host, port_text = first_item.rsplit(":", 1)
        try:
            return host, int(port_text)
        except ValueError:
            return host, default_port

    return first_item, default_port


def check_tcp(name: str, host: str, port: int, timeout_seconds: float = 2.0) -> CheckResult:
    try:
        with socket.create_connection((host, port), timeout=timeout_seconds):
            return CheckResult(name, f"{host}:{port}", True, "connected")
    except OSError as exc:
        return CheckResult(name, f"{host}:{port}", False, str(exc))


def check_http(name: str, url: str, timeout_seconds: float = 2.0) -> CheckResult:
    try:
        with urlopen(url, timeout=timeout_seconds) as response:
            return CheckResult(name, url, response.status < 400, f"HTTP {response.status}")
    except Exception as exc:  # noqa: BLE001
        return CheckResult(name, url, False, str(exc))


def colorize_status(result: CheckResult) -> str:
    label = "HEALTHY" if result.healthy else "DOWN"
    color = GREEN if result.healthy else RED
    return f"{color}{label}{RESET}"


def print_table(results: list[CheckResult]) -> None:
    headers = ("Service", "Target", "Status", "Detail")
    rows = [(r.name, r.target, "HEALTHY" if r.healthy else "DOWN", r.detail) for r in results]
    widths = [len(headers[i]) for i in range(4)]
    for row in rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(str(cell)))

    def line(left: str, middle: str, right: str, fill: str = "-") -> str:
        pieces = [fill * (width + 2) for width in widths]
        return left + middle.join(pieces) + right

    print(line("+", "+", "+"))
    print(
        "| "
        + " | ".join(headers[index].ljust(widths[index]) for index in range(4))
        + " |"
    )
    print(line("+", "+", "+"))
    for result in results:
        status = colorize_status(result)
        print(
            "| "
            + result.name.ljust(widths[0])
            + " | "
            + result.target.ljust(widths[1])
            + " | "
            + status.ljust(widths[2] + len(RED) + len(RESET) + 9)
            + " | "
            + result.detail.ljust(widths[3])
            + " |"
        )
    print(line("+", "+", "+"))


def main() -> int:
    timeout_seconds = float(os.getenv("HEALTH_CHECK_TIMEOUT", "2.0"))

    database_url = os.getenv("DATABASE_URL", "postgresql://localhost:5432/dira")
    kafka_brokers = os.getenv("KAFKA_BROKERS", "localhost:9092")
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    spark_ui_url = os.getenv("SPARK_UI_URL", "http://localhost:8080")
    airflow_health_url = os.getenv("AIRFLOW_HEALTH_URL", "http://airflow-webserver:8080/health")

    db_host, db_port = parse_host_port(database_url, "localhost", 5432)
    kafka_host, kafka_port = parse_host_port(kafka_brokers, "localhost", 9092)
    redis_host, redis_port = parse_host_port(redis_url, "localhost", 6379)

    results = [
        check_tcp("PostgreSQL", db_host, db_port, timeout_seconds),
        check_tcp("Kafka", kafka_host, kafka_port, timeout_seconds),
        check_tcp("Redis", redis_host, redis_port, timeout_seconds),
        check_http("Spark UI", spark_ui_url, timeout_seconds),
        check_http("Airflow", airflow_health_url, timeout_seconds),
    ]

    print_table(results)
    failed = [result for result in results if not result.healthy]
    if failed:
        print(f"{YELLOW}{len(failed)} service(s) down{RESET}")
        return 1

    print(f"{GREEN}all services healthy{RESET}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
