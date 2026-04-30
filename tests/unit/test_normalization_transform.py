from __future__ import annotations

import sys
import types
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
for package_path in (
    PROJECT_ROOT / "apps" / "pipeline" / "src",
    PROJECT_ROOT / "libs" / "schemas" / "src",
):
    package_path_str = str(package_path)
    if package_path_str not in sys.path:
        sys.path.insert(0, package_path_str)

from dira_schemas.enums import DataSourceType
from transforms.normalization import NormalizationTransform


class _FakeColumn:
    def __init__(self, expression: object) -> None:
        self.expression = expression

    def __mul__(self, other: object) -> tuple[str, object, object]:
        return ("mul", self.expression, other)


class _FakeFrame:
    def __init__(self, columns: list[str]) -> None:
        self.columns = list(columns)
        self.rename_calls: list[tuple[str, str]] = []
        self.with_column_calls: list[tuple[str, object]] = []
        self.join_calls: list[tuple[object, object, str]] = []
        self.drop_calls: list[tuple[str, ...]] = []

    def withColumnRenamed(self, source: str, target: str) -> _FakeFrame:
        self.rename_calls.append((source, target))
        if source in self.columns:
            self.columns = [target if column == source else column for column in self.columns]
        return self

    def withColumn(self, name: str, expression: object) -> _FakeFrame:
        self.with_column_calls.append((name, expression))
        if name not in self.columns:
            self.columns.append(name)
        return self

    def join(self, other: object, on: object, how: str) -> _FakeFrame:
        self.join_calls.append((other, on, how))
        return self

    def drop(self, *columns: str) -> _FakeFrame:
        self.drop_calls.append(columns)
        for column in columns:
            if column in self.columns:
                self.columns.remove(column)
        return self


def _install_fake_spark(monkeypatch) -> None:
    fake_functions = types.ModuleType("pyspark.sql.functions")

    def _col(name: str) -> _FakeColumn:
        return _FakeColumn(("col", name))

    def _lit(value: object) -> tuple[str, object]:
        return ("lit", value)

    def _coalesce(*expressions: object) -> tuple[str, tuple[object, ...]]:
        normalized = tuple(getattr(expression, "expression", expression) for expression in expressions)
        return ("coalesce", normalized)

    def _to_utc_timestamp(expression: object, timezone: str) -> tuple[str, object, str]:
        normalized_expression = getattr(expression, "expression", expression)
        return ("to_utc_timestamp", normalized_expression, timezone)

    fake_functions.col = _col
    fake_functions.lit = _lit
    fake_functions.coalesce = _coalesce
    fake_functions.to_utc_timestamp = _to_utc_timestamp

    fake_sql = types.ModuleType("pyspark.sql")
    fake_sql.functions = fake_functions
    fake_pyspark = types.ModuleType("pyspark")
    fake_pyspark.__path__ = []  # type: ignore[attr-defined]
    fake_pyspark.sql = fake_sql

    monkeypatch.setitem(sys.modules, "pyspark", fake_pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", fake_functions)


def test_normalization_transform_standardizes_cctv_rows_and_fills_speed(monkeypatch) -> None:
    _install_fake_spark(monkeypatch)

    lookup_frame = _FakeFrame(["road_segment_id", "historical_median_speed_kmh"])
    frame = _FakeFrame(
        [
            "road_segment_id",
            "source_type",
            "latitude",
            "longitude",
            "speed_mph",
            "frame_timestamp",
            "avg_speed_kmh",
        ]
    )
    transform = NormalizationTransform(segment_speed_lookup=lookup_frame)

    result = transform.normalize(frame, DataSourceType.CCTV)

    assert result is frame
    assert frame.rename_calls == [("latitude", "lat"), ("longitude", "lon")]
    assert frame.with_column_calls[0] == (
        "event_time",
        ("to_utc_timestamp", ("col", "frame_timestamp"), "UTC"),
    )
    assert frame.with_column_calls[1] == (
        "speed_kmh",
        ("mul", ("col", "speed_mph"), ("lit", 1.60934)),
    )
    assert frame.join_calls == [(lookup_frame, "road_segment_id", "left")]
    assert frame.with_column_calls[2] == (
        "avg_speed_kmh",
        ("coalesce", (("col", "avg_speed_kmh"), ("col", "historical_median_speed_kmh"))),
    )
    assert frame.drop_calls == [
        ("frame_timestamp",),
        ("speed_mph",),
        ("historical_median_speed_kmh",),
    ]
    assert "event_time" in frame.columns
    assert "lat" in frame.columns
    assert "lon" in frame.columns
    assert "speed_kmh" in frame.columns
    assert "avg_speed_kmh" in frame.columns
    assert "frame_timestamp" not in frame.columns
    assert "speed_mph" not in frame.columns