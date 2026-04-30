from __future__ import annotations

import sys
import types
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
for package_path in (
    PROJECT_ROOT / "apps" / "pipeline" / "src",
):
    package_path_str = str(package_path)
    if package_path_str not in sys.path:
        sys.path.insert(0, package_path_str)

from transforms.deduplication import DeduplicationTransform


class _FakeWindowResult:
    def __init__(self, expression: object, duration: str) -> None:
        self.expression = expression
        self.duration = duration
        self.start = ("window_start", expression, duration)


class _FakeFrame:
    def __init__(self) -> None:
        self.with_column_calls: list[tuple[str, object]] = []
        self.watermark_calls: list[tuple[str, str]] = []
        self.drop_duplicates_calls: list[list[str]] = []
        self.drop_calls: list[tuple[str, ...]] = []

    def withColumn(self, name: str, expression: object) -> _FakeFrame:
        self.with_column_calls.append((name, expression))
        return self

    def withWatermark(self, column: str, duration: str) -> _FakeFrame:
        self.watermark_calls.append((column, duration))
        return self

    def dropDuplicates(self, columns: list[str]) -> _FakeFrame:
        self.drop_duplicates_calls.append(columns)
        return self

    def drop(self, *columns: str) -> _FakeFrame:
        self.drop_calls.append(columns)
        return self


def _install_fake_spark(monkeypatch, captured: dict[str, object]) -> None:
    fake_functions = types.ModuleType("pyspark.sql.functions")

    def _col(name: str) -> tuple[str, str]:
        return ("col", name)

    def _window(expression: object, duration: str) -> _FakeWindowResult:
        captured["window"] = (expression, duration)
        return _FakeWindowResult(expression, duration)

    fake_functions.col = _col
    fake_functions.window = _window

    fake_sql = types.ModuleType("pyspark.sql")
    fake_sql.functions = fake_functions
    fake_pyspark = types.ModuleType("pyspark")
    fake_pyspark.__path__ = []  # type: ignore[attr-defined]
    fake_pyspark.sql = fake_sql

    monkeypatch.setitem(sys.modules, "pyspark", fake_pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", fake_functions)


def test_deduplication_transform_rounds_timestamps_and_deduplicates(monkeypatch) -> None:
    captured: dict[str, object] = {}
    _install_fake_spark(monkeypatch, captured)

    frame = _FakeFrame()
    transform = DeduplicationTransform()

    result = transform.apply(frame)

    assert result is frame
    assert captured["window"] == (("col", "timestamp"), "10 seconds")
    assert frame.with_column_calls == [("rounded_timestamp", ("window_start", ("col", "timestamp"), "10 seconds"))]
    assert frame.watermark_calls == [("timestamp", "30 seconds")]
    assert frame.drop_duplicates_calls == [["road_segment_id", "source_type", "rounded_timestamp"]]
    assert frame.drop_calls == [("rounded_timestamp",)]


def test_deduplication_transform_allows_custom_timestamp_column(monkeypatch) -> None:
    captured: dict[str, object] = {}
    _install_fake_spark(monkeypatch, captured)

    frame = _FakeFrame()
    transform = DeduplicationTransform(
        watermark_duration="45 seconds",
        timestamp_column="event_time",
    )

    transform.apply(frame)

    assert captured["window"] == (("col", "event_time"), "10 seconds")
    assert frame.watermark_calls == [("event_time", "45 seconds")]