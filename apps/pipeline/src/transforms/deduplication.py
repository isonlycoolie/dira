from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
else:
    DataFrame = Any


class DeduplicationTransform:
    WINDOW_DURATION = "10 seconds"

    def __init__(
        self,
        watermark_duration: str = "30 seconds",
        timestamp_column: str = "timestamp",
        road_segment_column: str = "road_segment_id",
        source_type_column: str = "source_type",
    ) -> None:
        self._watermark_duration = watermark_duration
        self._timestamp_column = timestamp_column
        self._road_segment_column = road_segment_column
        self._source_type_column = source_type_column

    def apply(self, df: DataFrame) -> DataFrame:
        from pyspark.sql.functions import col, window

        rounded_timestamp = window(col(self._timestamp_column), self.WINDOW_DURATION).start
        return (
            df.withColumn("rounded_timestamp", rounded_timestamp)
            .withWatermark(self._timestamp_column, self._watermark_duration)
            .dropDuplicates([self._road_segment_column, self._source_type_column, "rounded_timestamp"])
            .drop("rounded_timestamp")
        )


__all__ = ["DeduplicationTransform"]