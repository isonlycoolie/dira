from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
else:
    DataFrame = Any
    SparkSession = Any


class SpatialFilterTransform:
    def __init__(self, road_buffers: DataFrame) -> None:
        self._road_buffers = road_buffers

    @classmethod
    def from_postgis(
        cls,
        spark: SparkSession,
        jdbc_url: str,
        jdbc_properties: dict[str, str] | None = None,
        road_table: str = "road_edges",
    ) -> "SpatialFilterTransform":
        road_buffers_query = (
            f"(SELECT id, buffer_geom FROM {road_table} WHERE buffer_geom IS NOT NULL) AS road_edges"
        )
        reader = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", road_buffers_query)
        )
        if jdbc_properties:
            reader = reader.options(**jdbc_properties)

        road_buffers = reader.load().selectExpr(
            "CAST(id AS BIGINT) AS road_segment_id",
            "buffer_geom",
        )

        from pyspark.sql.functions import broadcast

        return cls(broadcast(road_buffers))

    def apply(self, df: DataFrame) -> DataFrame:
        from pyspark.sql.functions import col
        from sedona.sql.functions import ST_Point, ST_Within

        point_geometry = ST_Point(col("lon"), col("lat"))
        return (
            df.withColumn("point_geom", point_geometry)
            .join(self._road_buffers, ST_Within(col("point_geom"), col("buffer_geom")), "inner")
            .drop("point_geom", "buffer_geom")
        )


__all__ = ["SpatialFilterTransform"]