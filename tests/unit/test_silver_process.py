from __future__ import annotations

import sys
import types
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
for package_path in (
    PROJECT_ROOT / "apps" / "pipeline" / "src",
    PROJECT_ROOT / "libs" / "common" / "src",
    PROJECT_ROOT / "libs" / "schemas" / "src",
):
    package_path_str = str(package_path)
    if package_path_str not in sys.path:
        sys.path.insert(0, package_path_str)

from jobs.silver_process import RAW_KAFKA_TOPICS, SegmentAggregationTransform, build_raw_kafka_stream


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


class _FakeAggregationResult:
    def __init__(self, function_name: str, operand: object) -> None:
        self.function_name = function_name
        self.operand = operand

    def alias(self, alias_name: str) -> tuple[str, str, object, str]:
        return ("alias", self.function_name, self.operand, alias_name)


class _FakeWindowResult:
    def __init__(self, expression: object, duration: str) -> None:
        self.expression = expression
        self.duration = duration
        self.start = ("window_start", expression, duration)


class _FakeAggregationFrame:
    def __init__(self) -> None:
        self.group_by_calls: list[tuple[object, ...]] = []
        self.agg_calls: list[tuple[object, ...]] = []
        self.select_exprs: list[str] = []

    def groupBy(self, *keys: object) -> _FakeAggregationFrame:
        self.group_by_calls.append(keys)
        return self

    def agg(self, *expressions: object) -> _FakeAggregationFrame:
        self.agg_calls.append(expressions)
        return self

    def selectExpr(self, *expressions: str) -> _FakeAggregationFrame:
        self.select_exprs = list(expressions)
        return self


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


def test_segment_aggregation_transform_groups_and_projects_windows(monkeypatch) -> None:
    captured: dict[str, object] = {}
    fake_functions = types.ModuleType("pyspark.sql.functions")

    def _col(name: str) -> tuple[str, str]:
        return ("col", name)

    def _count(expression: object) -> _FakeAggregationResult:
        return _FakeAggregationResult("count", expression)

    def _avg(expression: object) -> _FakeAggregationResult:
        return _FakeAggregationResult("avg", expression)

    def _sum(expression: object) -> _FakeAggregationResult:
        return _FakeAggregationResult("sum", expression)

    def _window(expression: object, duration: str) -> _FakeWindowResult:
        captured["window"] = (expression, duration)
        window_result = _FakeWindowResult(expression, duration)
        captured["window_result"] = window_result
        return window_result

    fake_functions.col = _col
    fake_functions.count = _count
    fake_functions.avg = _avg
    fake_functions.sum = _sum
    fake_functions.window = _window

    fake_sql = types.ModuleType("pyspark.sql")
    fake_sql.functions = fake_functions
    fake_pyspark = types.ModuleType("pyspark")
    fake_pyspark.__path__ = []  # type: ignore[attr-defined]
    fake_pyspark.sql = fake_sql

    monkeypatch.setitem(sys.modules, "pyspark", fake_pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", fake_functions)

    frame = _FakeAggregationFrame()
    transform = SegmentAggregationTransform()

    result = transform.aggregate(frame)

    assert result is frame
    assert captured["window"] == (("col", "event_time"), "30 seconds")
    assert frame.group_by_calls == [("road_segment_id", captured["window_result"])]
    assert frame.agg_calls == [
        (
            ("alias", "count", ("col", "road_segment_id"), "vehicle_count"),
            ("alias", "avg", ("col", "speed_kmh"), "avg_speed_kmh"),
            ("alias", "sum", ("col", "flow"), "flow_rate"),
        )
    ]
    assert frame.select_exprs == [
        "road_segment_id",
        "window.start AS event_time",
        "vehicle_count",
        "avg_speed_kmh",
        "flow_rate",
    ]