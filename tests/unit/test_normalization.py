from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
for package_path in (
    PROJECT_ROOT / "apps" / "pipeline" / "src",
    PROJECT_ROOT / "libs" / "schemas" / "src",
):
    package_path_str = str(package_path)
    if package_path_str not in sys.path:
        sys.path.insert(0, package_path_str)

from dira_schemas.enums import DataSourceType
from transforms.normalization import SOURCE_SCHEMA_BUILDERS, deserialize


class _FakeStructField:
    def __init__(self, name: str, data_type: object, nullable: bool = True) -> None:
        self.name = name
        self.dataType = data_type
        self.nullable = nullable


class _FakeStructType:
    def __init__(self, fields: list[_FakeStructField] | None = None) -> None:
        self.fields = list(fields or [])


class _FakeSimpleType:
    pass


class _FakeDataFrame:
    def __init__(self) -> None:
        self.with_column_calls: list[tuple[str, object]] = []
        self.select_exprs: list[str] = []

    def withColumn(self, name: str, expression: object) -> _FakeDataFrame:
        self.with_column_calls.append((name, expression))
        return self

    def selectExpr(self, *expressions: str) -> _FakeDataFrame:
        self.select_exprs = list(expressions)
        return self


def _install_fake_pyspark(monkeypatch, captured: dict[str, object]) -> None:
    fake_functions = types.ModuleType("pyspark.sql.functions")

    def _col(name: str) -> tuple[str, str]:
        return ("col", name)

    def _lit(value: object) -> tuple[str, object]:
        return ("lit", value)

    def _coalesce(*expressions: object) -> tuple[str, tuple[object, ...]]:
        return ("coalesce", expressions)

    def _from_json(expression: object, schema: object) -> tuple[str, object, object]:
        captured["schema"] = schema
        return ("from_json", expression, schema)

    fake_functions.col = _col
    fake_functions.lit = _lit
    fake_functions.coalesce = _coalesce
    fake_functions.from_json = _from_json

    fake_types = types.ModuleType("pyspark.sql.types")
    fake_types.StructField = _FakeStructField
    fake_types.StructType = _FakeStructType
    fake_types.StringType = _FakeSimpleType
    fake_types.DoubleType = _FakeSimpleType
    fake_types.IntegerType = _FakeSimpleType
    fake_types.TimestampType = _FakeSimpleType

    fake_sql = types.ModuleType("pyspark.sql")
    fake_sql.functions = fake_functions
    fake_sql.types = fake_types
    fake_pyspark = types.ModuleType("pyspark")
    fake_pyspark.__path__ = []  # type: ignore[attr-defined]
    fake_pyspark.sql = fake_sql

    monkeypatch.setitem(sys.modules, "pyspark", fake_pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", fake_functions)
    monkeypatch.setitem(sys.modules, "pyspark.sql.types", fake_types)


@pytest.mark.parametrize(
    ("source_type", "expected_field_names"),
    [
        (DataSourceType.TELECOM, ["device_id_hash", "tower_id", "lat", "lon", "timestamp", "signal_strength"]),
        (DataSourceType.CCTV, ["camera_id", "frame_timestamp", "vehicle_count", "avg_speed_kmh", "lane_occupancy", "queue_length_m"]),
        (DataSourceType.FLEET_GPS, ["vehicle_id_hash", "provider", "lat", "lon", "speed_kmh", "heading", "timestamp"]),
        (DataSourceType.INCIDENT, ["id", "incident_type", "lat", "lon", "reported_at", "source", "description", "severity"]),
        (DataSourceType.WEATHER, ["station_id", "timestamp", "condition", "rainfall_mm", "visibility_m", "temperature_c"]),
    ],
)
def test_deserialize_uses_source_specific_schema_and_preserves_metadata(monkeypatch, source_type: DataSourceType, expected_field_names: list[str]) -> None:
    captured: dict[str, object] = {}
    _install_fake_pyspark(monkeypatch, captured)

    frame = _FakeDataFrame()
    result = deserialize(frame, source_type)

    assert result is frame
    assert frame.with_column_calls[0][0] == "source_type"
    assert frame.with_column_calls[0][1][0] == "coalesce"
    assert ("col", "key") in frame.with_column_calls[0][1][1]
    assert ("lit", source_type.value) in frame.with_column_calls[0][1][1]
    assert frame.with_column_calls[1][0] == "payload"
    assert frame.with_column_calls[1][1][0] == "from_json"
    assert frame.with_column_calls[1][1][1] == ("col", "value")
    assert frame.select_exprs == [
        "topic",
        "partition",
        "offset",
        "timestamp as kafka_timestamp",
        "key",
        "source_type",
        "payload.*",
    ]

    schema = captured["schema"]
    assert [field.name for field in schema.fields] == expected_field_names
    assert source_type in SOURCE_SCHEMA_BUILDERS