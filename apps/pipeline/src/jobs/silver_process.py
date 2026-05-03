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


class SegmentAggregationTransform:
    WINDOW_DURATION = "30 seconds"

    def __init__(
        self,
        event_time_column: str = "event_time",
        road_segment_column: str = "road_segment_id",
        speed_column: str = "speed_kmh",
        flow_column: str = "flow",
    ) -> None:
        self._event_time_column = event_time_column
        self._road_segment_column = road_segment_column
        self._speed_column = speed_column
        self._flow_column = flow_column

    def aggregate(self, df: DataFrame) -> DataFrame:
        from pyspark.sql.functions import avg, col, count, sum, window

        grouped = df.groupBy(
            self._road_segment_column,
            window(col(self._event_time_column), self.WINDOW_DURATION),
        ).agg(
            count(col(self._road_segment_column)).alias("vehicle_count"),
            avg(col(self._speed_column)).alias("avg_speed_kmh"),
            sum(col(self._flow_column)).alias("flow_rate"),
        )

        return grouped.selectExpr(
            self._road_segment_column,
            "window.start AS event_time",
            "vehicle_count",
            "avg_speed_kmh",
            "flow_rate",
        )

    def apply(self, df: DataFrame) -> DataFrame:
        return self.aggregate(df)


__all__ = ["RAW_KAFKA_TOPICS", "SegmentAggregationTransform", "build_raw_kafka_stream"]