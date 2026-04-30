from __future__ import annotations

import sys
from datetime import date
from hashlib import sha256
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
for package_path in (
    PROJECT_ROOT / "apps" / "ingestion" / "src",
    PROJECT_ROOT / "libs" / "common" / "src",
    PROJECT_ROOT / "libs" / "schemas" / "src",
):
    package_path_str = str(package_path)
    if package_path_str not in sys.path:
        sys.path.insert(0, package_path_str)

from connectors import telecom as telecom_module
from connectors.telecom import TelecomConnector
from dira_common.exceptions import IngestionError
from dira_common.metrics import PrometheusRegistry


def _expected_hash(raw_id: str, salt: str) -> str:
    return sha256(f"{raw_id}{salt}".encode("utf-8")).hexdigest()


def test_anonymize_device_id_uses_daily_salt(monkeypatch) -> None:
    connector = TelecomConnector()
    today_salt = date.today().isoformat()

    assert connector._anonymize_device_id("device-123") == _expected_hash("device-123", today_salt)
    assert connector._anonymize_device_id("device-123") == _expected_hash("device-123", today_salt)

    class FakeDate(date):
        @classmethod
        def today(cls) -> date:
            return cls(2026, 4, 29)

    monkeypatch.setattr(telecom_module, "date", FakeDate)

    assert connector._anonymize_device_id("device-123") == _expected_hash("device-123", "2026-04-29")
    assert connector._anonymize_device_id("device-123") != _expected_hash("device-123", today_salt)


class _FakeProducer:
    def __init__(self, brokers=None, producer=None):
        self.calls = []

    def publish(self, topic, message):
        self.calls.append((topic, message.model_dump(mode="json")))
        return {"topic": topic}

    def is_healthy(self):
        return True

    def close(self):
        return None


class _FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    def get(self, key: str) -> str | None:
        return self.values.get(key)

    def set(self, key: str, value: str) -> None:
        self.values[key] = value

    def expire(self, key: str, seconds: int) -> None:
        return None


def _metric_value(metric: object) -> float:
    value = getattr(metric, "value", None)
    if value is not None:
        return float(value)

    for family in metric.collect():  # type: ignore[attr-defined]
        for sample in family.samples:
            if sample.name.endswith("_total") or sample.name.endswith("_count"):
                return float(sample.value)
    raise AssertionError("unable to read metric value")


def _make_connector(monkeypatch: pytest.MonkeyPatch) -> tuple[TelecomConnector, _FakeRedis, _FakeProducer]:
    fake_redis = _FakeRedis()
    fake_producer = _FakeProducer()
    monkeypatch.setattr(telecom_module, "_load_redis_client", lambda redis_url: fake_redis)
    monkeypatch.setattr(telecom_module, "_LocalRedisClient", _FakeRedis)
    monkeypatch.setattr(telecom_module.BaseConnector, "_connect_producer", lambda self: setattr(self, "_producer", fake_producer))
    return TelecomConnector(), fake_redis, fake_producer


def test_telecom_connector_batch_publish_success_tracks_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    connector, _, _ = _make_connector(monkeypatch)
    start_received = _metric_value(PrometheusRegistry.telecom_pings_received_total)
    start_bbox = _metric_value(PrometheusRegistry.telecom_pings_filtered_bbox_total)
    start_residential = _metric_value(PrometheusRegistry.telecom_pings_filtered_residential_total)
    start_published = _metric_value(PrometheusRegistry.telecom_pings_published_total)
    start_latency_samples = len(getattr(PrometheusRegistry.telecom_publish_latency_seconds, "samples", []))

    published, filtered = connector.ingest_batch(
        [
            {
                "device_id_hash": "device-a",
                "tower_id": "tower-1",
                "lat": -8.0,
                "lon": 39.2,
                "timestamp": "2026-04-30T08:00:00+00:00",
            },
            {
                "device_id_hash": "device-b",
                "tower_id": "tower-2",
                "lat": -6.8,
                "lon": 39.2,
                "timestamp": "2026-04-30T08:00:00+00:00",
            },
            {
                "device_id_hash": "device-b",
                "tower_id": "tower-2",
                "lat": -6.8,
                "lon": 39.2,
                "timestamp": "2026-04-30T08:16:00+00:00",
            },
            {
                "device_id_hash": "device-c",
                "tower_id": "tower-2",
                "lat": -6.8,
                "lon": 39.2,
                "timestamp": "2026-04-30T08:20:00+00:00",
            },
        ]
    )

    assert (published, filtered) == (2, 2)
    assert _metric_value(PrometheusRegistry.telecom_pings_received_total) == start_received + 4
    assert _metric_value(PrometheusRegistry.telecom_pings_filtered_bbox_total) == start_bbox + 1
    assert _metric_value(PrometheusRegistry.telecom_pings_filtered_residential_total) == start_residential + 1
    assert _metric_value(PrometheusRegistry.telecom_pings_published_total) == start_published + 2
    assert len(getattr(PrometheusRegistry.telecom_publish_latency_seconds, "samples", [])) == start_latency_samples + 2


def test_telecom_connector_residential_filter_after_fifteen_minutes(monkeypatch: pytest.MonkeyPatch) -> None:
    connector, _, _ = _make_connector(monkeypatch)

    published, filtered = connector.ingest_batch(
        [
            {
                "device_id_hash": "device-b",
                "tower_id": "tower-2",
                "lat": -6.8,
                "lon": 39.2,
                "timestamp": "2026-04-30T08:00:00+00:00",
            },
            {
                "device_id_hash": "device-b",
                "tower_id": "tower-2",
                "lat": -6.8,
                "lon": 39.2,
                "timestamp": "2026-04-30T08:16:00+00:00",
            },
        ]
    )

    assert (published, filtered) == (1, 1)


def test_telecom_connector_bbox_filter_rejects_outside_dsm_bbox(monkeypatch: pytest.MonkeyPatch) -> None:
    connector, _, _ = _make_connector(monkeypatch)

    published, filtered = connector.ingest_batch(
        [
            {
                "device_id_hash": "device-a",
                "tower_id": "tower-1",
                "lat": -8.0,
                "lon": 39.2,
                "timestamp": "2026-04-30T08:00:00+00:00",
            }
        ]
    )

    assert (published, filtered) == (0, 1)


def test_telecom_connector_batch_publish_failure_propagates_ingestion_error(monkeypatch: pytest.MonkeyPatch) -> None:
    connector, _, _ = _make_connector(monkeypatch)

    def fail_publish(message, topic):
        raise IngestionError("boom")

    monkeypatch.setattr(connector, "publish", fail_publish)

    with pytest.raises(IngestionError):
        connector.ingest_batch(
            [
                {
                    "device_id_hash": "device-a",
                    "tower_id": "tower-1",
                    "lat": -6.8,
                    "lon": 39.2,
                    "timestamp": "2026-04-30T08:00:00+00:00",
                }
            ]
        )
