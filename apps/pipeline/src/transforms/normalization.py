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


class NormalizationTransform:
    MILES_TO_KILOMETERS = 1.60934
    SOURCE_TIMESTAMP_COLUMNS: dict[DataSourceType, str] = {
        DataSourceType.TELECOM: "timestamp",
        DataSourceType.CCTV: "frame_timestamp",
        DataSourceType.FLEET_GPS: "timestamp",
        DataSourceType.INCIDENT: "reported_at",
        DataSourceType.WEATHER: "timestamp",
        DataSourceType.FUSED: "event_time",
    }

    def __init__(
        self,
        segment_speed_lookup: DataFrame | None = None,
        lookup_speed_column: str = "historical_median_speed_kmh",
        road_segment_column: str = "road_segment_id",
    ) -> None:
        self._segment_speed_lookup = segment_speed_lookup
        self._lookup_speed_column = lookup_speed_column
        self._road_segment_column = road_segment_column

    def normalize(self, df: DataFrame, source_type: DataSourceType) -> DataFrame:
        normalized = df
        normalized = self._standardize_coordinates(normalized)
        normalized = self._standardize_timestamp(normalized, source_type)
        normalized = self._standardize_speed_columns(normalized)
        normalized = self._fill_missing_average_speed(normalized)
        return normalized

    def apply(self, df: DataFrame, source_type: DataSourceType) -> DataFrame:
        return self.normalize(df, source_type)

    def _standardize_coordinates(self, df: DataFrame) -> DataFrame:
        columns = list(getattr(df, "columns", []))
        renames = (("latitude", "lat"), ("longitude", "lon"), ("lng", "lon"))
        normalized = df
        for source_name, target_name in renames:
            if source_name not in columns or target_name in columns:
                continue
            normalized = normalized.withColumnRenamed(source_name, target_name)
            columns = list(getattr(normalized, "columns", []))
        return normalized

    def _standardize_timestamp(self, df: DataFrame, source_type: DataSourceType) -> DataFrame:
        from pyspark.sql.functions import col, to_utc_timestamp

        timestamp_column = self.SOURCE_TIMESTAMP_COLUMNS.get(source_type)
        if timestamp_column is None:
            raise ValueError(f"unsupported source type for normalization: {source_type}")

        normalized = df.withColumn(
            "event_time",
            to_utc_timestamp(col(timestamp_column), "UTC"),
        )
        if timestamp_column != "event_time":
            normalized = normalized.drop(timestamp_column)
        return normalized

    def _standardize_speed_columns(self, df: DataFrame) -> DataFrame:
        from pyspark.sql.functions import col, lit

        columns = list(getattr(df, "columns", []))
        normalized = df
        for column_name in columns:
            if not column_name.endswith("_mph"):
                continue

            kmh_column_name = f"{column_name[:-4]}_kmh"
            if kmh_column_name in columns:
                continue

            normalized = normalized.withColumn(
                kmh_column_name,
                col(column_name) * lit(self.MILES_TO_KILOMETERS),
            ).drop(column_name)
            columns = list(getattr(normalized, "columns", []))

        return normalized

    def _fill_missing_average_speed(self, df: DataFrame) -> DataFrame:
        if self._segment_speed_lookup is None:
            return df

        columns = list(getattr(df, "columns", []))
        if "avg_speed_kmh" not in columns or self._road_segment_column not in columns:
            return df

        lookup_columns = list(getattr(self._segment_speed_lookup, "columns", []))
        required_lookup_columns = {self._road_segment_column, self._lookup_speed_column}
        if not required_lookup_columns.issubset(set(lookup_columns)):
            raise ValueError(
                "segment speed lookup must contain road segment and historical speed columns"
            )

        from pyspark.sql.functions import col, coalesce

        joined = df.join(self._segment_speed_lookup, self._road_segment_column, "left")
        return joined.withColumn(
            "avg_speed_kmh",
            coalesce(col("avg_speed_kmh"), col(self._lookup_speed_column)),
        ).drop(self._lookup_speed_column)


__all__ = ["NormalizationTransform", "SOURCE_SCHEMA_BUILDERS", "deserialize"]