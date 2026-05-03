from __future__ import annotations

import sys
import types
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
for package_path in (
    PROJECT_ROOT / "apps" / "pipeline" / "src",
    PROJECT_ROOT / "libs" / "common" / "src",
    PROJECT_ROOT / "libs" / "schemas" / "src",
):
    package_path_str = str(package_path)
    if package_path_str not in sys.path:
        sys.path.insert(0, package_path_str)

from dira_schemas.enums import DataSourceType
from jobs.silver_process import (
    SegmentAggregationTransform,
    build_raw_kafka_stream,
    build_silver_postgres_foreach_batch_writer,
)
from transforms.deduplication import DeduplicationTransform
from transforms.normalization import NormalizationTransform
from transforms.spatial_filter import SpatialFilterTransform


@dataclass(frozen=True)
class _ColumnRef:
    name: str

    def __mul__(self, other: object) -> _Multiply:
        return _Multiply(self, other)

    def __rmul__(self, other: object) -> _Multiply:
        return _Multiply(other, self)


@dataclass(frozen=True)
class _Literal:
    value: object


@dataclass(frozen=True)
class _Multiply:
    left: object
    right: object


@dataclass(frozen=True)
class _Coalesce:
    expressions: tuple[object, ...]


@dataclass(frozen=True)
class _ToUtcTimestamp:
    expression: object
    timezone: str


@dataclass(frozen=True)
class _WindowStart:
    expression: object
    duration: str


@dataclass(frozen=True)
class _WindowSpec:
    expression: object
    duration: str

    @property
    def start(self) -> _WindowStart:
        return _WindowStart(self.expression, self.duration)


@dataclass(frozen=True, order=True)
class _WindowBucket:
    start: datetime
    end: datetime


@dataclass(frozen=True)
class _AggregationSpec:
    function_name: str
    expression: object
    alias_name: str | None = None

    def alias(self, alias_name: str) -> _AggregationSpec:
        return replace(self, alias_name=alias_name)


@dataclass(frozen=True)
class _PointExpression:
    lon_expression: object
    lat_expression: object


@dataclass(frozen=True)
class _WithinExpression:
    point_expression: object
    buffer_expression: object


def _install_fake_spark(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_functions = types.ModuleType("pyspark.sql.functions")
    fake_functions.col = lambda name: _ColumnRef(name)
    fake_functions.lit = lambda value: _Literal(value)
    fake_functions.coalesce = lambda *expressions: _Coalesce(expressions)
    fake_functions.to_utc_timestamp = lambda expression, timezone: _ToUtcTimestamp(expression, timezone)
    fake_functions.window = lambda expression, duration: _WindowSpec(expression, duration)
    fake_functions.count = lambda expression: _AggregationSpec("count", expression)
    fake_functions.avg = lambda expression: _AggregationSpec("avg", expression)
    fake_functions.sum = lambda expression: _AggregationSpec("sum", expression)
    fake_functions.broadcast = lambda frame: frame

    fake_sql = types.ModuleType("pyspark.sql")
    fake_sql.functions = fake_functions
    fake_pyspark = types.ModuleType("pyspark")
    fake_pyspark.__path__ = []  # type: ignore[attr-defined]
    fake_pyspark.sql = fake_sql

    monkeypatch.setitem(sys.modules, "pyspark", fake_pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", fake_functions)

    fake_sedona_functions = types.ModuleType("sedona.sql.functions")
    fake_sedona_functions.ST_Point = lambda lon, lat: _PointExpression(lon, lat)
    fake_sedona_functions.ST_Within = lambda point, buffer: _WithinExpression(point, buffer)

    fake_sedona = types.ModuleType("sedona")
    fake_sedona.__path__ = []  # type: ignore[attr-defined]
    fake_sedona.sql = types.ModuleType("sedona.sql")
    fake_sedona.sql.functions = fake_sedona_functions

    monkeypatch.setitem(sys.modules, "sedona", fake_sedona)
    monkeypatch.setitem(sys.modules, "sedona.sql", fake_sedona.sql)
    monkeypatch.setitem(sys.modules, "sedona.sql.functions", fake_sedona_functions)


def _duration_to_freq(duration: str) -> str:
    amount, unit = duration.split(maxsplit=1)
    normalized_unit = unit.lower().rstrip("s")
    if normalized_unit == "second":
        return f"{amount}s"
    if normalized_unit == "minute":
        return f"{amount}min"
    raise ValueError(f"unsupported duration: {duration}")


def _as_timestamp(value: object) -> pd.Timestamp:
    return pd.to_datetime(value, utc=True)


def _evaluate_expression(frame: _FakeFrame, expression: object) -> object:
    if isinstance(expression, _ColumnRef):
        return frame._frame[expression.name]
    if isinstance(expression, _Literal):
        return expression.value
    if isinstance(expression, _Multiply):
        left = _evaluate_expression(frame, expression.left)
        right = _evaluate_expression(frame, expression.right)
        return left * right
    if isinstance(expression, _Coalesce):
        values = [_evaluate_expression(frame, part) for part in expression.expressions]
        result = values[0]
        for value in values[1:]:
            if isinstance(result, pd.Series) and isinstance(value, pd.Series):
                result = result.combine_first(value)
            elif isinstance(result, pd.Series):
                result = result.where(result.notna(), value)
            elif pd.isna(result):
                result = value
        return result
    if isinstance(expression, _ToUtcTimestamp):
        series = pd.to_datetime(_evaluate_expression(frame, expression.expression), utc=True)
        return series
    if isinstance(expression, _WindowStart):
        series = pd.to_datetime(_evaluate_expression(frame, expression.expression), utc=True)
        return series.dt.floor(_duration_to_freq(expression.duration))
    if isinstance(expression, _PointExpression):
        lon_values = _evaluate_expression(frame, expression.lon_expression)
        lat_values = _evaluate_expression(frame, expression.lat_expression)
        return pd.Series(list(zip(lon_values, lat_values)), index=frame._frame.index)
    return expression


def _point_within_bounds(point: tuple[float, float], bounds: tuple[float, float, float, float]) -> bool:
    lon, lat = point
    min_lon, min_lat, max_lon, max_lat = bounds
    return min_lon <= lon <= max_lon and min_lat <= lat <= max_lat


def _bucket_timestamp(value: object, duration: str) -> _WindowBucket:
    timestamp = _as_timestamp(value)
    start = timestamp.floor(_duration_to_freq(duration))
    end = start + pd.Timedelta(_duration_to_freq(duration))
    return _WindowBucket(start=start.to_pydatetime(), end=end.to_pydatetime())


class _FakeRow:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = dict(payload)

    def asDict(self, recursive: bool = False) -> dict[str, object]:
        return dict(self._payload)


class _FakeFrame:
    def __init__(self, frame: pd.DataFrame, spark_session: object | None = None) -> None:
        self._frame = frame.copy()
        self.sparkSession = spark_session

    @property
    def columns(self) -> list[str]:
        return list(self._frame.columns)

    def withColumnRenamed(self, source: str, target: str) -> _FakeFrame:
        self._frame = self._frame.rename(columns={source: target})
        return self

    def withColumn(self, name: str, expression: object) -> _FakeFrame:
        values = _evaluate_expression(self, expression)
        self._frame[name] = values
        return self

    def join(self, other: object, condition: object, how: str) -> _FakeFrame:
        if not isinstance(other, _FakeFrame):
            raise TypeError("join expects a fake frame")
        if how != "inner":
            raise NotImplementedError("only inner joins are supported in the integration fake")
        if not isinstance(condition, _WithinExpression):
            raise NotImplementedError("only spatial within joins are supported")

        joined_rows: list[dict[str, object]] = []
        for source_row in self._frame.to_dict(orient="records"):
            point_value = source_row.get(condition.point_expression.name)
            if point_value is None:
                continue
            for buffer_row in other._frame.to_dict(orient="records"):
                buffer_value = buffer_row.get(condition.buffer_expression.name)
                if buffer_value is None:
                    continue
                if _point_within_bounds(point_value, buffer_value):
                    joined_rows.append({**source_row, **buffer_row})

        return _FakeFrame(pd.DataFrame(joined_rows), spark_session=self.sparkSession)

    def drop(self, *columns: str) -> _FakeFrame:
        self._frame = self._frame.drop(columns=list(columns), errors="ignore")
        return self

    def withWatermark(self, column: str, duration: str) -> _FakeFrame:
        return self

    def dropDuplicates(self, columns: list[str]) -> _FakeFrame:
        self._frame = self._frame.drop_duplicates(subset=columns, keep="first").reset_index(drop=True)
        return self

    def groupBy(self, *keys: object) -> _FakeGroupedFrame:
        return _FakeGroupedFrame(self, keys)

    def selectExpr(self, *expressions: str) -> _FakeFrame:
        selected: dict[str, object] = {}
        for expression in expressions:
            source_expression = expression.strip()
            alias = source_expression
            if " AS " in source_expression:
                source_expression, alias = source_expression.split(" AS ", 1)
                source_expression = source_expression.strip()
                alias = alias.strip()

            if source_expression == "window.start":
                selected[alias] = self._frame["window"].map(lambda window: window.start)
            else:
                selected[alias] = self._frame[source_expression]

        return _FakeFrame(pd.DataFrame(selected), spark_session=self.sparkSession)

    def collect(self) -> list[_FakeRow]:
        return [_FakeRow(record) for record in self._frame.to_dict(orient="records")]


class _FakeGroupedFrame:
    def __init__(self, frame: _FakeFrame, keys: tuple[object, ...]) -> None:
        self._frame = frame
        self._keys = keys

    def agg(self, *expressions: _AggregationSpec) -> _FakeFrame:
        working_frame = self._frame._frame.copy()
        group_columns: list[str] = []

        for index, key in enumerate(self._keys):
            if isinstance(key, str):
                group_columns.append(key)
                continue
            if isinstance(key, _WindowSpec):
                window_column = f"__window_{index}"
                working_frame[window_column] = working_frame[key.expression.name].map(
                    lambda value, duration=key.duration: _bucket_timestamp(value, duration)
                )
                group_columns.append(window_column)
                continue
            raise TypeError(f"unsupported grouping key: {key!r}")

        rows: list[dict[str, object]] = []
        grouped = working_frame.groupby(group_columns, sort=True, dropna=False)
        for group_values, group_frame in grouped:
            if not isinstance(group_values, tuple):
                group_values = (group_values,)

            row: dict[str, object] = {}
            for column_name, group_value in zip(group_columns, group_values):
                if column_name.startswith("__window_"):
                    row["window"] = group_value
                else:
                    row[column_name] = group_value

            for expression in expressions:
                alias_name = expression.alias_name or expression.function_name
                source_expression = expression.expression
                if not isinstance(source_expression, _ColumnRef):
                    raise TypeError("aggregation expressions must reference a column")

                series = group_frame[source_expression.name]
                if expression.function_name == "count":
                    row[alias_name] = int(series.count())
                elif expression.function_name == "avg":
                    row[alias_name] = float(series.mean())
                elif expression.function_name == "sum":
                    row[alias_name] = float(series.sum())
                else:
                    raise ValueError(f"unsupported aggregation: {expression.function_name}")

            rows.append(row)

        return _FakeFrame(pd.DataFrame(rows), spark_session=self._frame.sparkSession)


class _FakeReadStreamBuilder:
    def __init__(self) -> None:
        self.format_name: str | None = None
        self.options: dict[str, str] = {}
        self.loaded_frame = _FakeKafkaFrame()

    def format(self, value: str) -> _FakeReadStreamBuilder:
        self.format_name = value
        return self

    def option(self, key: str, value: str) -> _FakeReadStreamBuilder:
        self.options[key] = value
        return self

    def load(self) -> _FakeKafkaFrame:
        return self.loaded_frame


class _FakeKafkaFrame:
    def __init__(self) -> None:
        self.select_exprs: list[str] = []

    def selectExpr(self, *expressions: str) -> _FakeKafkaFrame:
        self.select_exprs = list(expressions)
        return self


class _FakeSparkSession:
    def __init__(self) -> None:
        self.readStream = _FakeReadStreamBuilder()


class _FakePreparedStatement:
    def __init__(self, sql: str) -> None:
        self.sql = sql
        self.current_row: dict[int, object] = {}
        self.executed_batches: list[dict[int, object]] = []

    def setObject(self, index: int, value: object) -> None:
        self.current_row[index] = value

    def addBatch(self) -> None:
        self.executed_batches.append(dict(self.current_row))
        self.current_row = {}

    def executeBatch(self) -> list[int]:
        return [1 for _ in self.executed_batches]

    def clearBatch(self) -> None:
        self.current_row = {}

    def close(self) -> None:
        return None


class _FakeConnection:
    def __init__(self) -> None:
        self.autocommit: bool | None = None
        self.prepared_statement: _FakePreparedStatement | None = None
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def setAutoCommit(self, enabled: bool) -> None:
        self.autocommit = enabled

    def prepareStatement(self, sql: str) -> _FakePreparedStatement:
        self.prepared_statement = _FakePreparedStatement(sql)
        return self.prepared_statement

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


class _FakeCheckpointClient:
    def __init__(self) -> None:
        self.writes: list[tuple[str, pd.DataFrame]] = []

    def write_parquet(self, bucket_path: str, frame: pd.DataFrame) -> None:
        self.writes.append((bucket_path, frame.copy()))


@pytest.mark.integration
def test_silver_job_pipeline_reads_kafka_filters_deduplicates_and_writes_postgres(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_spark(monkeypatch)

    spark = _FakeSparkSession()
    raw_stream = build_raw_kafka_stream(
        spark,
        bootstrap_servers="kafka:9092",
        topics=("dira.raw.fleet",),
    )

    assert raw_stream is spark.readStream.loaded_frame
    assert spark.readStream.format_name == "kafka"
    assert spark.readStream.options == {
        "kafka.bootstrap.servers": "kafka:9092",
        "subscribe": "dira.raw.fleet",
        "startingOffsets": "latest",
    }
    assert raw_stream.select_exprs == [
        "CAST(key AS STRING) AS key",
        "CAST(value AS STRING) AS value",
        "topic",
        "partition",
        "offset",
        "timestamp",
    ]

    road_buffers = _FakeFrame(
        pd.DataFrame(
            [
                {"road_segment_id": 101, "buffer_geom": (39.19, -6.81, 39.21, -6.79)},
                {"road_segment_id": 202, "buffer_geom": (39.26, -6.83, 39.28, -6.81)},
            ]
        )
    )

    source_rows = _FakeFrame(
        pd.DataFrame(
            [
                {
                    "source_type": DataSourceType.FLEET_GPS.value,
                    "latitude": -6.8001,
                    "longitude": 39.2001,
                    "speed_mph": 30.0,
                    "timestamp": datetime(2026, 4, 30, 8, 15, 1, tzinfo=UTC),
                    "flow": 1.0,
                },
                {
                    "source_type": DataSourceType.FLEET_GPS.value,
                    "latitude": -6.8001,
                    "longitude": 39.2001,
                    "speed_mph": 30.0,
                    "timestamp": datetime(2026, 4, 30, 8, 15, 4, tzinfo=UTC),
                    "flow": 1.0,
                },
                {
                    "source_type": DataSourceType.FLEET_GPS.value,
                    "latitude": -6.8002,
                    "longitude": 39.2002,
                    "speed_mph": 42.0,
                    "timestamp": datetime(2026, 4, 30, 8, 15, 22, tzinfo=UTC),
                    "flow": 2.0,
                },
                {
                    "source_type": DataSourceType.FLEET_GPS.value,
                    "latitude": -6.8204,
                    "longitude": 39.2704,
                    "speed_mph": 36.0,
                    "timestamp": datetime(2026, 4, 30, 9, 5, 5, tzinfo=UTC),
                    "flow": 3.0,
                },
            ]
        ),
        spark_session=object(),
    )

    normalized = NormalizationTransform().normalize(source_rows, DataSourceType.FLEET_GPS)
    spatial_filtered = SpatialFilterTransform(road_buffers).apply(normalized)
    deduplicated = DeduplicationTransform(timestamp_column="event_time").apply(spatial_filtered)
    aggregated = SegmentAggregationTransform(
        event_time_column="event_time",
        road_segment_column="road_segment_id",
        speed_column="speed_kmh",
        flow_column="flow",
    ).aggregate(deduplicated)

    connection = _FakeConnection()
    checkpoint_client = _FakeCheckpointClient()

    def connection_factory(database_url: str, spark_session: object | None) -> _FakeConnection:
        assert database_url == "jdbc:postgresql://localhost:5432/dira"
        assert spark_session is not None
        return connection

    writer = build_silver_postgres_foreach_batch_writer(
        "postgresql://localhost:5432/dira",
        connection_factory=connection_factory,
        checkpoint_client=checkpoint_client,
    )
    writer(aggregated, 17)

    assert connection.autocommit is False
    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert connection.closed is True
    assert connection.prepared_statement is not None
    assert connection.prepared_statement.sql == (
        "INSERT INTO traffic_events (road_segment_id, event_time, vehicle_count, avg_speed_kmh, flow_rate) "
        "VALUES (?, ?, ?, ?, ?) ON CONFLICT (road_segment_id, event_time) DO UPDATE SET "
        "vehicle_count = EXCLUDED.vehicle_count, avg_speed_kmh = EXCLUDED.avg_speed_kmh, "
        "flow_rate = EXCLUDED.flow_rate"
    )

    executed_batches = connection.prepared_statement.executed_batches
    assert len(executed_batches) == 2
    first_row = executed_batches[0]
    second_row = executed_batches[1]
    assert first_row[1] == 101
    assert first_row[2] == "2026-04-30T08:15:00+00:00"
    assert first_row[3] == 2
    assert first_row[4] == pytest.approx(57.93624, rel=1e-6)
    assert first_row[5] == pytest.approx(3.0, rel=1e-6)
    assert second_row[1] == 202
    assert second_row[2] == "2026-04-30T09:05:00+00:00"
    assert second_row[3] == 1
    assert second_row[4] == pytest.approx(57.93624, rel=1e-6)
    assert second_row[5] == pytest.approx(3.0, rel=1e-6)

    assert [path for path, _ in checkpoint_client.writes] == [
        "gs://dira-silver/2026-04-30/08/part-17.parquet",
        "gs://dira-silver/2026-04-30/09/part-17.parquet",
    ]
    assert [len(frame) for _, frame in checkpoint_client.writes] == [1, 1]

