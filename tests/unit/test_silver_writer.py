from __future__ import annotations

import sys
from datetime import UTC, datetime
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

from jobs.silver_process import SilverPostgresWriter, build_silver_postgres_foreach_batch_writer


class _FakeRow:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def asDict(self, recursive: bool = False) -> dict[str, object]:
        return dict(self._payload)


class _FakeBatchFrame:
    def __init__(self, rows: list[_FakeRow]) -> None:
        self._rows = rows
        self.sparkSession = object()

    def collect(self) -> list[_FakeRow]:
        return list(self._rows)


class _FakePreparedStatement:
    def __init__(self) -> None:
        self.current_row: list[object] = []
        self.pending_rows: list[tuple[object, ...]] = []
        self.executed_batches: list[list[tuple[object, ...]]] = []
        self.closed = False

    def setObject(self, index: int, value: object) -> None:
        while len(self.current_row) < index - 1:
            self.current_row.append(None)
        if len(self.current_row) == index - 1:
            self.current_row.append(value)
        else:
            self.current_row[index - 1] = value

    def addBatch(self) -> None:
        self.pending_rows.append(tuple(self.current_row))
        self.current_row = []

    def executeBatch(self) -> list[int]:
        self.executed_batches.append(list(self.pending_rows))
        self.pending_rows = []
        return [1] * len(self.executed_batches[-1])

    def clearBatch(self) -> None:
        self.current_row = []
        self.pending_rows = []

    def close(self) -> None:
        self.closed = True


class _FakeConnection:
    def __init__(self) -> None:
        self.auto_commit_enabled: bool | None = None
        self.commits = 0
        self.rollbacks = 0
        self.closed = 0
        self.prepared_sql: str | None = None
        self.statement = _FakePreparedStatement()

    def setAutoCommit(self, enabled: bool) -> None:
        self.auto_commit_enabled = enabled

    def prepareStatement(self, sql: str) -> _FakePreparedStatement:
        self.prepared_sql = sql
        return self.statement

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed += 1


class _FakeLogger:
    def __init__(self) -> None:
        self.info_calls: list[tuple[str, dict[str, object]]] = []

    def info(self, message: str, **kwargs: object) -> None:
        self.info_calls.append((message, kwargs))


class _FakeCheckpointClient:
    def __init__(self) -> None:
        self.writes: list[tuple[str, object]] = []

    def write_parquet(self, bucket_path: str, frame: object) -> None:
        self.writes.append((bucket_path, frame.copy()))


def test_silver_postgres_writer_upserts_in_batches_of_500() -> None:
    connection = _FakeConnection()
    logger = _FakeLogger()

    def connection_factory(database_url: str, spark_session: object | None) -> _FakeConnection:
        assert database_url == "jdbc:postgresql://localhost:5432/dira"
        return connection

    writer = SilverPostgresWriter(
        database_url="postgresql://localhost:5432/dira",
        batch_size=500,
        connection_factory=connection_factory,
        logger=logger,
    )

    rows = [
        _FakeRow(
            {
                "road_segment_id": 7,
                "event_time": datetime(2026, 4, 30, 8, 15, tzinfo=UTC),
                "vehicle_count": index,
                "avg_speed_kmh": 22.5,
                "flow_rate": 9.0,
            }
        )
        for index in range(501)
    ]
    frame = _FakeBatchFrame(rows)

    writer.foreach_batch(frame, 12)

    assert connection.auto_commit_enabled is False
    assert connection.prepared_sql == (
        "INSERT INTO traffic_events (road_segment_id, event_time, vehicle_count, avg_speed_kmh, flow_rate) "
        "VALUES (?, ?, ?, ?, ?) ON CONFLICT (road_segment_id, event_time) DO UPDATE SET vehicle_count = EXCLUDED.vehicle_count, avg_speed_kmh = EXCLUDED.avg_speed_kmh, flow_rate = EXCLUDED.flow_rate"
    )
    assert len(connection.statement.executed_batches) == 2
    assert len(connection.statement.executed_batches[0]) == 500
    assert len(connection.statement.executed_batches[1]) == 1
    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert connection.closed == 1
    assert connection.statement.closed is True
    assert logger.info_calls == [
        (
            "wrote silver postgres batch",
            {"batch_id": 12, "rows_written": 501, "table": "traffic_events"},
        )
    ]


def test_build_silver_postgres_foreach_batch_writer_returns_callback() -> None:
    callback = build_silver_postgres_foreach_batch_writer("postgresql://localhost:5432/dira")

    assert callable(callback)


def test_silver_postgres_writer_writes_hour_partitioned_checkpoint() -> None:
    connection = _FakeConnection()
    checkpoint_client = _FakeCheckpointClient()

    def connection_factory(database_url: str, spark_session: object | None) -> _FakeConnection:
        assert database_url == "jdbc:postgresql://localhost:5432/dira"
        return connection

    writer = SilverPostgresWriter(
        database_url="postgresql://localhost:5432/dira",
        batch_size=500,
        connection_factory=connection_factory,
        checkpoint_client=checkpoint_client,
    )

    rows = [
        _FakeRow(
            {
                "road_segment_id": 7,
                "event_time": datetime(2026, 4, 30, 8, 15, tzinfo=UTC),
                "vehicle_count": 10,
                "avg_speed_kmh": 22.5,
                "flow_rate": 9.0,
            }
        ),
        _FakeRow(
            {
                "road_segment_id": 7,
                "event_time": datetime(2026, 4, 30, 8, 45, tzinfo=UTC),
                "vehicle_count": 11,
                "avg_speed_kmh": 23.0,
                "flow_rate": 9.5,
            }
        ),
        _FakeRow(
            {
                "road_segment_id": 7,
                "event_time": datetime(2026, 4, 30, 9, 5, tzinfo=UTC),
                "vehicle_count": 12,
                "avg_speed_kmh": 24.0,
                "flow_rate": 10.0,
            }
        ),
    ]
    frame = _FakeBatchFrame(rows)

    writer.foreach_batch(frame, 88)

    assert len(checkpoint_client.writes) == 2
    assert checkpoint_client.writes[0][0] == "gs://dira-silver/2026-04-30/08/part-88.parquet"
    assert checkpoint_client.writes[1][0] == "gs://dira-silver/2026-04-30/09/part-88.parquet"
    first_partition = checkpoint_client.writes[0][1]
    second_partition = checkpoint_client.writes[1][1]
    assert list(first_partition["vehicle_count"]) == [10, 11]
    assert list(second_partition["vehicle_count"]) == [12]
    assert connection.commits == 1
    assert connection.rollbacks == 0