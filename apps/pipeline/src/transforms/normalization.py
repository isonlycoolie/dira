from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from dira_schemas.enums import DataSourceType

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
else:
    DataFrame = Any


def _struct_field(name: str, data_type: Any, nullable: bool = True) -> Any:
    from pyspark.sql.types import StructField

    return StructField(name, data_type, nullable)


def _struct_type(*fields: Any) -> Any:
    from pyspark.sql.types import StructType

    return StructType(list(fields))


def _telecom_schema() -> Any:
    from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType

    return _struct_type(
        _struct_field("device_id_hash", StringType(), False),
        _struct_field("tower_id", StringType(), False),
        _struct_field("lat", DoubleType(), False),
        _struct_field("lon", DoubleType(), False),
        _struct_field("timestamp", TimestampType(), False),
        _struct_field("signal_strength", IntegerType(), True),
    )


def _cctv_schema() -> Any:
    from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType

    return _struct_type(
        _struct_field("camera_id", StringType(), False),
        _struct_field("frame_timestamp", TimestampType(), False),
        _struct_field("vehicle_count", IntegerType(), False),
        _struct_field("avg_speed_kmh", DoubleType(), True),
        _struct_field("lane_occupancy", DoubleType(), False),
        _struct_field("queue_length_m", DoubleType(), True),
    )


def _fleet_schema() -> Any:
    from pyspark.sql.types import DoubleType, StringType, TimestampType

    return _struct_type(
        _struct_field("vehicle_id_hash", StringType(), False),
        _struct_field("provider", StringType(), False),
        _struct_field("lat", DoubleType(), False),
        _struct_field("lon", DoubleType(), False),
        _struct_field("speed_kmh", DoubleType(), False),
        _struct_field("heading", DoubleType(), True),
        _struct_field("timestamp", TimestampType(), False),
    )


def _incident_schema() -> Any:
    from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType

    return _struct_type(
        _struct_field("id", StringType(), False),
        _struct_field("incident_type", StringType(), False),
        _struct_field("lat", DoubleType(), False),
        _struct_field("lon", DoubleType(), False),
        _struct_field("reported_at", TimestampType(), False),
        _struct_field("source", StringType(), False),
        _struct_field("description", StringType(), True),
        _struct_field("severity", IntegerType(), False),
    )


def _weather_schema() -> Any:
    from pyspark.sql.types import DoubleType, StringType, TimestampType

    return _struct_type(
        _struct_field("station_id", StringType(), False),
        _struct_field("timestamp", TimestampType(), False),
        _struct_field("condition", StringType(), False),
        _struct_field("rainfall_mm", DoubleType(), False),
        _struct_field("visibility_m", DoubleType(), False),
        _struct_field("temperature_c", DoubleType(), False),
    )


SOURCE_SCHEMA_BUILDERS: dict[DataSourceType, Callable[[], Any]] = {
    DataSourceType.TELECOM: _telecom_schema,
    DataSourceType.CCTV: _cctv_schema,
    DataSourceType.FLEET_GPS: _fleet_schema,
    DataSourceType.INCIDENT: _incident_schema,
    DataSourceType.WEATHER: _weather_schema,
}


def _schema_for(source_type: DataSourceType) -> Any:
    schema_builder = SOURCE_SCHEMA_BUILDERS.get(source_type)
    if schema_builder is None:
        raise ValueError(f"unsupported source type for normalization: {source_type}")
    return schema_builder()


def deserialize(df: DataFrame, source_type: DataSourceType) -> DataFrame:
    from pyspark.sql.functions import col, coalesce, from_json, lit

    schema = _schema_for(source_type)
    return (
        df.withColumn("source_type", coalesce(col("key"), lit(source_type.value)))
        .withColumn("payload", from_json(col("value"), schema))
        .selectExpr(
            "topic",
            "partition",
            "offset",
            "timestamp as kafka_timestamp",
            "key",
            "source_type",
            "payload.*",
        )
    )


__all__ = ["SOURCE_SCHEMA_BUILDERS", "deserialize"]