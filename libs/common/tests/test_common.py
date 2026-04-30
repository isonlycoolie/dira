from __future__ import annotations

import io
import json
import sys
from contextlib import redirect_stderr
from pathlib import Path
from uuid import uuid4

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from dira_common.config import DiraSettings
from dira_common.exceptions import IngestionError
from dira_common.kafka import CommitFailedError, KafkaConsumerWrapper, KafkaProducerWrapper
from dira_common.logging import setup_logging
from dira_common.storage import GCSParquetClient
from dira_common.utils import retry


class _PayloadModel:
    def __init__(self, value: int) -> None:
        self.value = value

    def model_dump(self, mode: str = "json") -> dict[str, int]:
        return {"value": self.value}


class _ProducerFuture:
    def get(self, timeout: float | None = None) -> dict[str, float | None]:
        return {"timeout": timeout}


class _FakeProducer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes]] = []

    def send(self, topic: str, value: bytes) -> _ProducerFuture:
        self.sent.append((topic, value))
        return _ProducerFuture()

    def bootstrap_connected(self) -> bool:
        return True

    def close(self) -> None:
        return None


class _FakeConsumer:
    def __init__(self) -> None:
        self.commit_calls = 0

    def poll(self, timeout_ms: int = 1000) -> dict[str, list[object]]:
        class _Record:
            def __init__(self, value: object) -> None:
                self.value = value

        return {
            "topic-1": [
                _Record(b'{"value": 1}'),
                _Record('{"value": 2}'),
            ]
        }

    def commit(self) -> None:
        self.commit_calls += 1
        if self.commit_calls == 1:
            raise CommitFailedError("commit failed once")

    def close(self) -> None:
        return None


class _FakeBlob:
    def __init__(self) -> None:
        self.data = b""
        self.content_type: str | None = None

    def upload_from_string(self, data: bytes, content_type: str | None = None) -> None:
        self.data = data
        self.content_type = content_type

    def download_as_bytes(self) -> bytes:
        return self.data


class _FakeBucket:
    def __init__(self) -> None:
        self.blobs: dict[str, _FakeBlob] = {}

    def blob(self, name: str) -> _FakeBlob:
        if name not in self.blobs:
            self.blobs[name] = _FakeBlob()
        return self.blobs[name]


class _FakeStorageClient:
    def __init__(self) -> None:
        self.bucket_obj = _FakeBucket()

    def bucket(self, name: str) -> _FakeBucket:
        return self.bucket_obj


class _FakeTable:
    def __init__(self, frame: pd.DataFrame) -> None:
        self.frame = frame

    @staticmethod
    def from_pandas(frame: pd.DataFrame) -> "_FakeTable":
        return _FakeTable(frame.copy())


class _FakeReadTable:
    def __init__(self, frame: pd.DataFrame) -> None:
        self._frame = frame

    def to_pandas(self) -> pd.DataFrame:
        return self._frame.copy()


class _FakePyArrow:
    Table = _FakeTable


class _FakePyArrowParquet:
    @staticmethod
    def write_table(table: _FakeTable, buffer: io.BytesIO) -> None:
        payload = table.frame.to_json(orient="split")
        buffer.write(payload.encode("utf-8"))

    @staticmethod
    def read_table(buffer: io.BytesIO) -> _FakeReadTable:
        frame = pd.read_json(io.BytesIO(buffer.getvalue()), orient="split")
        return _FakeReadTable(frame)


def test_settings_loading_from_env(monkeypatch: object) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://localhost:5432/dira")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9092")
    monkeypatch.setenv("GCS_BUCKET_PREFIX", "gs://dira-dev")
    monkeypatch.setenv("DSM_BBOX", "-7.0,39.1,-6.6,39.4")
    monkeypatch.setenv("SPARK_MASTER_URL", "spark://localhost:7077")
    monkeypatch.setenv("AIRFLOW_DB_URL", "postgresql://localhost:5432/airflow")
    monkeypatch.setenv("OPENWEATHERMAP_API_KEY", "test-key")
    monkeypatch.setenv("ENV", "dev")

    settings = DiraSettings()

    assert settings.database_url == "postgresql://localhost:5432/dira"
    assert settings.redis_url == "redis://localhost:6379/0"
    assert settings.kafka_brokers == ["kafka-1:9092", "kafka-2:9092"]
    assert settings.gcs_bucket_prefix == "gs://dira-dev"
    assert settings.dsm_bbox == (-7.0, 39.1, -6.6, 39.4)
    assert settings.spark_master_url == "spark://localhost:7077"
    assert settings.airflow_db_url == "postgresql://localhost:5432/airflow"
    assert settings.openweathermap_api_key == "test-key"
    assert settings.env == "dev"


def test_retry_decorator_retries_and_succeeds() -> None:
    stream = io.StringIO()
    with redirect_stderr(stream):
        setup_logging("dev", service_name="dira-test")
        attempts = {"count": 0}

        @retry(max_attempts=3, sleep=lambda _: None)
        def flaky() -> str:
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise ValueError("boom")
            return "ok"

        result = flaky()

    output = stream.getvalue()
    assert result == "ok"
    assert attempts["count"] == 3
    assert "retrying" in output
    assert "dira-test" in output


def test_kafka_wrappers_mocked() -> None:
    producer = _FakeProducer()
    wrapper = KafkaProducerWrapper(producer=producer)
    payload = _PayloadModel(5)

    result = wrapper.publish("topic-1", payload)
    assert result["topic"] == "topic-1"
    assert b'"value": 5' in producer.sent[0][1]
    assert wrapper.is_healthy() is True

    class _BrokenProducer(_FakeProducer):
        def send(self, topic: str, value: bytes) -> _ProducerFuture:
            raise RuntimeError("send failed")

    try:
        KafkaProducerWrapper(producer=_BrokenProducer()).publish("topic-1", payload)
    except IngestionError as exc:
        assert exc.source == "ingestion"
        assert exc.details["topic"] == "topic-1"
    else:
        raise AssertionError("expected IngestionError")

    consumer = _FakeConsumer()
    consumer_wrapper = KafkaConsumerWrapper(consumer=consumer, group_id="group-1")
    records = list(consumer_wrapper.poll())
    assert records == [{"value": 1}, {"value": 2}]
    assert consumer.commit_calls == 2


def test_gcs_client_mocked_storage(monkeypatch: object) -> None:
    import dira_common.storage as storage_module

    monkeypatch.setattr(storage_module, "pa", _FakePyArrow())
    monkeypatch.setattr(storage_module, "pq", _FakePyArrowParquet())

    client = GCSParquetClient(storage_client=_FakeStorageClient())
    frame = pd.DataFrame({"segment_id": [1, 2], "score": [0.5, 0.8]})
    bucket_path = f"gs://dira-bronze/test/{uuid4()}.parquet"

    client.write_parquet(bucket_path, frame)
    restored = client.read_parquet(bucket_path)

    pd.testing.assert_frame_equal(restored, frame)
