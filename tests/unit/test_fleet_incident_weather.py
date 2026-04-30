from __future__ import annotations

import json
import sys
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

PROJECT_ROOT = Path(__file__).resolve().parents[2]
for package_path in (
    PROJECT_ROOT / "apps" / "ingestion" / "src",
    PROJECT_ROOT / "libs" / "common" / "src",
    PROJECT_ROOT / "libs" / "schemas" / "src",
):
    package_path_str = str(package_path)
    if package_path_str not in sys.path:
        sys.path.insert(0, package_path_str)

from connectors.base import BaseConnector
from connectors.fleet_gps import FleetGPSConnector
from connectors.incidents import IncidentConnector
from connectors.weather import CACHE_TTL_SECONDS, DEFAULT_WEATHER_TOPIC, WEATHER_CACHE_KEY, WeatherConnector
from dira_common.exceptions import IngestionError
from dira_schemas.enums import IncidentType, WeatherCondition
from dira_schemas.incidents import IncidentReport


class _FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.expiries: dict[str, int] = {}

    def get(self, key: str) -> str | None:
        return self.values.get(key)

    def set(self, key: str, value: str) -> None:
        self.values[key] = value

    def expire(self, key: str, seconds: int) -> None:
        self.expiries[key] = seconds


class _FakeProducer:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    def publish(self, topic, message):
        self.calls.append((topic, message.model_dump(mode="json")))
        return {"topic": topic}

    def is_healthy(self):
        return True

    def close(self):
        return None


def _expected_hash(raw_id: str, salt: str) -> str:
    return sha256(f"{raw_id}{salt}".encode("utf-8")).hexdigest()


def _make_report(report_id=None) -> IncidentReport:
    return IncidentReport(
        id=report_id or uuid4(),
        incident_type=IncidentType.ACCIDENT,
        lat=-6.8,
        lon=39.2,
        reported_at=datetime(2026, 4, 30, 8, 15, tzinfo=UTC),
        source="api",
        description="collision near junction",
        severity=3,
    )


def test_fleet_gps_connector_anonymizes_vehicle_id(monkeypatch) -> None:
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
    assert payload["timestamp"] == "2026-04-30T08:15:00Z"


def test_incident_connector_filters_duplicate_reports(monkeypatch) -> None:
    fake_redis = _FakeRedis()
    fake_producer = _FakeProducer()
    monkeypatch.setattr(BaseConnector, "_connect_producer", lambda self: setattr(self, "_producer", fake_producer))

    connector = IncidentConnector(redis_client=fake_redis)
    report = _make_report()

    published = connector.ingest(report)
    duplicate = connector.ingest(report)

    assert published is not None
    assert published.id == report.id
    assert duplicate is None
    assert len(fake_producer.calls) == 1
    assert fake_producer.calls[0][0] == "dira.raw.incidents"
    attributes = fake_producer.calls[0][1]["attributes"]
    assert attributes["incident_id"] == str(report.id)
    assert attributes["incident_type"] == "accident"
    assert fake_redis.expiries[f"dira:incident:{report.id}:dedup"] == 1800


def test_weather_connector_fetches_mocked_api_response(monkeypatch) -> None:
    fake_redis = _FakeRedis()
    fake_producer = _FakeProducer()
    captured: dict[str, str] = {}
    response = {
        "name": "Dar es Salaam",
        "dt": 1714468200,
        "weather": [{"main": "Rain", "description": "heavy rain"}],
        "rain": {"1h": 8.1},
        "visibility": 7000,
        "main": {"temp": 27.4},
    }

    def fake_fetcher(api_url: str, api_key: str):
        captured["api_url"] = api_url
        captured["api_key"] = api_key
        return response

    monkeypatch.setattr(BaseConnector, "_connect_producer", lambda self: setattr(self, "_producer", fake_producer))

    connector = WeatherConnector(redis_client=fake_redis, api_key="test-key", fetcher=fake_fetcher)
    reading = connector.fetch_and_publish()

    assert captured["api_key"] == "test-key"
    assert "lat=" in captured["api_url"]
    assert "lon=" in captured["api_url"]
    assert "appid=test-key" in captured["api_url"]
    assert "units=metric" in captured["api_url"]
    assert reading.station_id == "Dar es Salaam"
    assert reading.condition == WeatherCondition.HEAVY_RAIN
    assert reading.rainfall_mm == 8.1
    assert reading.visibility_m == 7000.0
    assert reading.temperature_c == 27.4
    assert fake_producer.calls[0][0] == DEFAULT_WEATHER_TOPIC
    attributes = fake_producer.calls[0][1]["attributes"]
    assert attributes["condition"] == "heavy_rain"
    assert attributes["temperature_c"] == 27.4
    assert fake_redis.expiries[WEATHER_CACHE_KEY] == CACHE_TTL_SECONDS
    cached_payload = json.loads(fake_redis.values[WEATHER_CACHE_KEY])
    assert cached_payload["station_id"] == "Dar es Salaam"
    assert cached_payload["condition"] == "heavy_rain"


def test_incident_connector_rejects_malformed_whatsapp_payload() -> None:
    connector = IncidentConnector(redis_client=_FakeRedis())
    webhook_payload = {
        "entry": [
            {
                "changes": [
                    {
                        "value": {
                            "messages": [
                                {
                                    "text": {
                                        "body": "just some text that cannot be parsed into fields"
                                    }
                                }
                            ]
                        }
                    }
                ]
            }
        ]
    }

    try:
        connector.ingest_from_whatsapp(webhook_payload)
    except IngestionError as exc:
        assert "unable to parse WhatsApp incident body" in str(exc)
    else:
        raise AssertionError("malformed WhatsApp payload should fail")