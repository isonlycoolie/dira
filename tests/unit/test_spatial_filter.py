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

from transforms.spatial_filter import SpatialFilterTransform


class _FakeFrame:
    def __init__(self) -> None:
        self.select_exprs: list[str] = []
        self.with_column_calls: list[tuple[str, object]] = []
        self.join_calls: list[tuple[object, object, str]] = []
        self.drop_calls: list[tuple[str, ...]] = []

    def selectExpr(self, *expressions: str) -> _FakeFrame:
        self.select_exprs = list(expressions)
        return self

    def withColumn(self, name: str, expression: object) -> _FakeFrame:
        self.with_column_calls.append((name, expression))
        return self

    def join(self, other: object, condition: object, how: str) -> _FakeFrame:
        self.join_calls.append((other, condition, how))
        return self

    def drop(self, *columns: str) -> _FakeFrame:
        self.drop_calls.append(columns)
        return self


class _FakeJdbcReader:
    def __init__(self) -> None:
        self.format_name: str | None = None
        self.options_map: dict[str, str] = {}
        self.options_kwargs: dict[str, str] = {}
        self.loaded_frame = _FakeFrame()

    def format(self, value: str) -> _FakeJdbcReader:
        self.format_name = value
        return self

    def option(self, key: str, value: str) -> _FakeJdbcReader:
        self.options_map[key] = value
        return self

    def options(self, **kwargs: str) -> _FakeJdbcReader:
        self.options_kwargs.update(kwargs)
        return self

    def load(self) -> _FakeFrame:
        return self.loaded_frame


class _FakeSparkSession:
    def __init__(self) -> None:
        self.read = _FakeJdbcReader()


def _install_fake_spark(monkeypatch, broadcast_result: object | None = None, include_sedona: bool = False, captured: dict[str, object] | None = None) -> None:
    captured = captured if captured is not None else {}
    fake_functions = types.ModuleType("pyspark.sql.functions")

    def _col(name: str) -> tuple[str, str]:
        return ("col", name)

    def _broadcast(frame: object) -> object:
        captured["broadcast_input"] = frame
        return broadcast_result if broadcast_result is not None else frame

    fake_functions.col = _col
    fake_functions.broadcast = _broadcast

    fake_sql = types.ModuleType("pyspark.sql")
    fake_sql.functions = fake_functions
    fake_pyspark = types.ModuleType("pyspark")
    fake_pyspark.__path__ = []  # type: ignore[attr-defined]
    fake_pyspark.sql = fake_sql

    monkeypatch.setitem(sys.modules, "pyspark", fake_pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", fake_functions)

    if include_sedona:
        fake_sedona_functions = types.ModuleType("sedona.sql.functions")

        def _st_point(lon: object, lat: object) -> tuple[str, object, object]:
            return ("ST_Point", lon, lat)

        def _st_within(point_geometry: object, buffer_geometry: object) -> tuple[str, object, object]:
            return ("ST_Within", point_geometry, buffer_geometry)

        fake_sedona_functions.ST_Point = _st_point
        fake_sedona_functions.ST_Within = _st_within

        fake_sedona = types.ModuleType("sedona")
        fake_sedona.__path__ = []  # type: ignore[attr-defined]
        fake_sedona.sql = types.ModuleType("sedona.sql")
        fake_sedona.sql.functions = fake_sedona_functions

        monkeypatch.setitem(sys.modules, "sedona", fake_sedona)
        monkeypatch.setitem(sys.modules, "sedona.sql", fake_sedona.sql)
        monkeypatch.setitem(sys.modules, "sedona.sql.functions", fake_sedona_functions)


def test_spatial_filter_transform_loads_broadcast_road_buffers(monkeypatch) -> None:
    captured: dict[str, object] = {}
    broadcast_frame = object()
    _install_fake_spark(monkeypatch, broadcast_result=broadcast_frame, captured=captured)

    spark = _FakeSparkSession()
    transform = SpatialFilterTransform.from_postgis(
        spark,
        jdbc_url="jdbc:postgresql://localhost:5432/dira",
        jdbc_properties={"user": "dira", "password": "dira"},
        road_table="road_edges",
    )

    assert spark.read.format_name == "jdbc"
    assert spark.read.options_map["url"] == "jdbc:postgresql://localhost:5432/dira"
    assert spark.read.options_map["dbtable"] == (
        "(SELECT id, buffer_geom FROM road_edges WHERE buffer_geom IS NOT NULL) AS road_edges"
    )
    assert spark.read.options_kwargs == {"user": "dira", "password": "dira"}
    assert spark.read.loaded_frame.select_exprs == ["CAST(id AS BIGINT) AS road_segment_id", "buffer_geom"]
    assert captured["broadcast_input"] is spark.read.loaded_frame
    assert transform._road_buffers is broadcast_frame


def test_spatial_filter_transform_applies_st_within_join(monkeypatch) -> None:
    _install_fake_spark(monkeypatch, include_sedona=True)

    road_buffers = object()
    transform = SpatialFilterTransform(road_buffers)
    source_frame = _FakeFrame()

    result = transform.apply(source_frame)

    assert result is source_frame
    assert source_frame.with_column_calls == [("point_geom", ("ST_Point", ("col", "lon"), ("col", "lat")))]
    assert source_frame.join_calls == [
        (road_buffers, ("ST_Within", ("col", "point_geom"), ("col", "buffer_geom")), "inner")
    ]
    assert source_frame.drop_calls == [("point_geom", "buffer_geom")]