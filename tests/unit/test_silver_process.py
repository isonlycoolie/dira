from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
for package_path in (
    PROJECT_ROOT / "apps" / "pipeline" / "src",
):
    package_path_str = str(package_path)
    if package_path_str not in sys.path:
        sys.path.insert(0, package_path_str)

from jobs.silver_process import RAW_KAFKA_TOPICS, build_raw_kafka_stream


class _FakeDataFrame:
    def __init__(self) -> None:
        self.select_exprs: list[str] = []

    def selectExpr(self, *expressions: str) -> _FakeDataFrame:
        self.select_exprs = list(expressions)
        return self


class _FakeReadStreamBuilder:
    def __init__(self) -> None:
        self.format_name: str | None = None
        self.options: dict[str, str] = {}
        self.loaded_frame = _FakeDataFrame()

    def format(self, value: str) -> _FakeReadStreamBuilder:
        self.format_name = value
        return self

    def option(self, key: str, value: str) -> _FakeReadStreamBuilder:
        self.options[key] = value
        return self

    def load(self) -> _FakeDataFrame:
        return self.loaded_frame


class _FakeSparkSession:
    def __init__(self) -> None:
        self.readStream = _FakeReadStreamBuilder()


def test_build_raw_kafka_stream_subscribes_and_decodes_utf8_json() -> None:
    spark = _FakeSparkSession()

    frame = build_raw_kafka_stream(spark, bootstrap_servers="kafka:9092")

    assert frame is spark.readStream.loaded_frame
    assert spark.readStream.format_name == "kafka"
    assert spark.readStream.options["kafka.bootstrap.servers"] == "kafka:9092"
    assert spark.readStream.options["subscribe"] == ",".join(RAW_KAFKA_TOPICS)
    assert spark.readStream.options["startingOffsets"] == "latest"
    assert frame.select_exprs == [
        "CAST(key AS STRING) AS key",
        "CAST(value AS STRING) AS value",
        "topic",
        "partition",
        "offset",
        "timestamp",
    ]