from __future__ import annotations

import logging
import os
from collections.abc import Sequence
from datetime import datetime
from typing import Callable
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

logger = logging.getLogger(__name__)


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


class SilverPostgresWriter:
    UPSERT_COLUMNS: tuple[str, ...] = (
        "road_segment_id",
        "event_time",
        "vehicle_count",
        "avg_speed_kmh",
        "flow_rate",
    )
    UPSERT_KEY_COLUMNS: tuple[str, ...] = ("road_segment_id", "event_time")

    def __init__(
        self,
        database_url: str,
        table_name: str = "traffic_events",
        batch_size: int = 500,
        connection_factory: Callable[[str, Any | None], Any] | None = None,
        logger: Any | None = None,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")

        self._jdbc_url = self._normalize_jdbc_url(database_url)
        self._table_name = table_name
        self._batch_size = batch_size
        self._connection_factory = connection_factory or self._default_connection_factory
        self._logger = logger or globals()["logger"]

    def foreach_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        rows = self._collect_rows(batch_df)
        if not rows:
            self._logger.info("silver postgres batch empty", batch_id=batch_id, table=self._table_name)
            return

        spark_session = getattr(batch_df, "sparkSession", None)
        connection = self._connection_factory(self._jdbc_url, spark_session)
        statement = None
        total_rows_written = 0

        try:
            self._set_autocommit(connection, False)
            statement = connection.prepareStatement(self._build_upsert_sql())
            for chunk in self._chunk_rows(rows):
                self._execute_chunk(statement, chunk)
                total_rows_written += len(chunk)

            self._commit(connection)
            self._logger.info(
                "wrote silver postgres batch",
                batch_id=batch_id,
                rows_written=total_rows_written,
                table=self._table_name,
            )
        except Exception:
            self._rollback(connection)
            raise
        finally:
            self._close_statement(statement)
            self._close_connection(connection)

    def _collect_rows(self, batch_df: DataFrame) -> list[Any]:
        collected_rows = batch_df.collect()
        return list(collected_rows or [])

    def _chunk_rows(self, rows: list[Any]) -> list[list[Any]]:
        return [rows[start : start + self._batch_size] for start in range(0, len(rows), self._batch_size)]

    def _execute_chunk(self, statement: Any, rows: list[Any]) -> None:
        for row in rows:
            row_values = row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
            for index, column_name in enumerate(self.UPSERT_COLUMNS, start=1):
                statement.setObject(index, self._coerce_value(row_values.get(column_name)))
            statement.addBatch()

        statement.executeBatch()
        clear_batch = getattr(statement, "clearBatch", None)
        if callable(clear_batch):
            clear_batch()

    def _build_upsert_sql(self) -> str:
        columns = ", ".join(self.UPSERT_COLUMNS)
        placeholders = ", ".join("?" for _ in self.UPSERT_COLUMNS)
        update_columns = [column for column in self.UPSERT_COLUMNS if column not in self.UPSERT_KEY_COLUMNS]
        update_clause = ", ".join(f"{column} = EXCLUDED.{column}" for column in update_columns)
        conflict_columns = ", ".join(self.UPSERT_KEY_COLUMNS)

        return (
            f"INSERT INTO {self._table_name} ({columns}) VALUES ({placeholders}) "
            f"ON CONFLICT ({conflict_columns}) DO UPDATE SET {update_clause}"
        )

    @staticmethod
    def _coerce_value(value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        return value

    @staticmethod
    def _normalize_jdbc_url(database_url: str) -> str:
        if database_url.startswith("jdbc:"):
            return database_url
        if database_url.startswith("postgresql://"):
            return f"jdbc:{database_url}"
        if database_url.startswith("postgres://"):
            return f"jdbc:postgresql://{database_url.removeprefix('postgres://')}"
        return database_url if database_url.startswith("jdbc:") else f"jdbc:{database_url}"

    @staticmethod
    def _default_connection_factory(database_url: str, spark_session: Any | None) -> Any:
        if spark_session is None:
            raise RuntimeError("spark session is required to open the JDBC connection")

        spark_context = getattr(spark_session, "sparkContext", None) or getattr(spark_session, "_sc", None)
        if spark_context is None:
            raise RuntimeError("spark session is missing spark context")

        jvm = getattr(spark_context, "_jvm", None)
        if jvm is None:
            raise RuntimeError("spark session is missing a JVM gateway")

        properties = jvm.java.util.Properties()
        return jvm.java.sql.DriverManager.getConnection(database_url, properties)

    @staticmethod
    def _set_autocommit(connection: Any, enabled: bool) -> None:
        setter = getattr(connection, "setAutoCommit", None)
        if callable(setter):
            setter(enabled)

    @staticmethod
    def _commit(connection: Any) -> None:
        commit = getattr(connection, "commit", None)
        if callable(commit):
            commit()

    @staticmethod
    def _rollback(connection: Any) -> None:
        rollback = getattr(connection, "rollback", None)
        if callable(rollback):
            rollback()

    @staticmethod
    def _close_statement(statement: Any | None) -> None:
        if statement is None:
            return
        close = getattr(statement, "close", None)
        if callable(close):
            close()

    @staticmethod
    def _close_connection(connection: Any) -> None:
        close = getattr(connection, "close", None)
        if callable(close):
            close()


def build_silver_postgres_foreach_batch_writer(
    database_url: str,
    table_name: str = "traffic_events",
    batch_size: int = 500,
    connection_factory: Callable[[str, Any | None], Any] | None = None,
    logger: Any | None = None,
) -> Callable[[DataFrame, int], None]:
    writer = SilverPostgresWriter(
        database_url=database_url,
        table_name=table_name,
        batch_size=batch_size,
        connection_factory=connection_factory,
        logger=logger,
    )
    return writer.foreach_batch


__all__ = [
    "RAW_KAFKA_TOPICS",
    "SegmentAggregationTransform",
    "SilverPostgresWriter",
    "build_raw_kafka_stream",
    "build_silver_postgres_foreach_batch_writer",
]