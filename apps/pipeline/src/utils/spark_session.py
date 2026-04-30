from __future__ import annotations

import os
from pathlib import Path
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
else:
    SparkSession = Any

PROJECT_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.37.0",
    "org.postgresql:postgresql:42.7.4",
)


def _default_checkpoint_location() -> str:
    checkpoint_directory = PROJECT_ROOT / ".spark-checkpoints"
    return f"file:///{checkpoint_directory.resolve().as_posix().lstrip('/')}"


class SparkSessionFactory:
    @staticmethod
    def create(app_name: str) -> SparkSession:
        try:
            from pyspark.sql import SparkSession as PySparkSession
        except ModuleNotFoundError as exc:
            raise ImportError("pyspark is required to create a Spark session") from exc

        packages = os.getenv("SPARK_JARS_PACKAGES") or ",".join(DEFAULT_SPARK_PACKAGES)
        checkpoint_location = os.getenv("SPARK_STREAMING_CHECKPOINT_LOCATION") or _default_checkpoint_location()
        master_url = os.getenv("SPARK_MASTER_URL") or "local[*]"

        return (
            PySparkSession.builder.master(master_url)
            .appName(app_name)
            .config("spark.jars.packages", packages)
            .config("spark.sql.streaming.checkpointLocation", checkpoint_location)
            .getOrCreate()
        )


__all__ = ["DEFAULT_SPARK_PACKAGES", "SparkSessionFactory"]