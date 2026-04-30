from __future__ import annotations

import os
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
else:
    DataFrame = Any
    SparkSession = Any

RAW_KAFKA_TOPICS: tuple[str, ...] = (
    "dira.raw.telecom",
    "dira.raw.cctv",
    "dira.raw.fleet",
    "dira.raw.incidents",
    "dira.raw.weather",
)


def _normalize_bootstrap_servers(bootstrap_servers: str | None) -> str:
    return bootstrap_servers or os.getenv("KAFKA_BROKERS", "localhost:9092")


def _normalize_topics(topics: Sequence[str] | None) -> str:
    return ",".join(topics or RAW_KAFKA_TOPICS)


def build_raw_kafka_stream(
    spark: SparkSession,
    bootstrap_servers: str | None = None,
    topics: Sequence[str] | None = None,
) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", _normalize_bootstrap_servers(bootstrap_servers))
        .option("subscribe", _normalize_topics(topics))
        .option("startingOffsets", "latest")
        .load()
        .selectExpr(
            "CAST(key AS STRING) AS key",
            "CAST(value AS STRING) AS value",
            "topic",
            "partition",
            "offset",
            "timestamp",
        )
    )


__all__ = ["RAW_KAFKA_TOPICS", "build_raw_kafka_stream"]