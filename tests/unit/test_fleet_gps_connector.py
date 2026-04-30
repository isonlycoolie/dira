from __future__ import annotations

import sys
from datetime import date, datetime
from hashlib import sha256
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
for package_path in (
    PROJECT_ROOT / "apps" / "ingestion" / "src",
    PROJECT_ROOT / "libs" / "common" / "src",
    PROJECT_ROOT / "libs" / "schemas" / "src",
):
    package_path_str = str(package_path)
    if package_path_str not in sys.path:
        sys.path.insert(0, package_path_str)

from dira_common.exceptions import IngestionError
from connectors.base import BaseConnector
from connectors.fleet_gps import FleetGPSConnector


class _FakeProducer:
    def __init__(self) -> None:
        self.calls = []

    def publish(self, topic, message):
        self.calls.append((topic, message.model_dump(mode="json")))
        return {"topic": topic}

    def is_healthy(self):
        return True

    def close(self):
        return None


def _expected_hash(raw_id: str, salt: str) -> str:
    return sha256(f"{raw_id}{salt}".encode("utf-8")).hexdigest()


def test_fleet_gps_connector_anonymizes_and_publishes(monkeypatch) -> None:
    fake_producer = _FakeProducer()
    monkeypatch.setattr(BaseConnector, "_connect_producer", lambda self: setattr(self, "_producer", fake_producer))
    connector = FleetGPSConnector(
        fetcher=lambda api_url, api_key: [
            {
                "vehicle_id": "veh-1",
                "provider": "provider-a",
                "lat": -6.8,
                "lon": 39.2,
                "speed_kmh": 32.5,
                "heading": 90.0,
                "timestamp": "2026-04-30T08:15:00+00:00",
            }
        ]
    )
    published = connector.ingest_from_api("https://example.test/fleet", "api-key")

    assert published == 1
    assert fake_producer.calls[0][0] == "dira.raw.fleet"
    payload = fake_producer.calls[0][1]
    assert payload["vehicle_id_hash"] == _expected_hash("veh-1", date.today().isoformat())
    assert "vehicle_id" not in payload
    assert payload["provider"] == "provider-a"
    assert payload["speed_kmh"] == 32.5
    assert payload["timestamp"] == "2026-04-30T08:15:00Z"


def test_fleet_gps_connector_rejects_non_array_payload(monkeypatch) -> None:
    fake_producer = _FakeProducer()
    monkeypatch.setattr(BaseConnector, "_connect_producer", lambda self: setattr(self, "_producer", fake_producer))
    connector = FleetGPSConnector(fetcher=lambda api_url, api_key: {"vehicle_id": "veh-1"})

    try:
        connector.ingest_from_api("https://example.test/fleet", "api-key")
    except IngestionError as exc:
        assert "JSON array" in str(exc)
    else:
        raise AssertionError("non-array payload should fail")
