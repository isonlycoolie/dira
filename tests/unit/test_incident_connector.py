from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

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
from connectors.incidents import IncidentConnector
from dira_schemas.enums import IncidentType
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


def _make_report(report_id: UUID | None = None) -> IncidentReport:
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


def test_incident_connector_ingest_publishes_once_and_filters_duplicates(monkeypatch) -> None:
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


def test_incident_connector_is_duplicate_uses_redis_cache() -> None:
    fake_redis = _FakeRedis()
    connector = IncidentConnector(redis_client=fake_redis)
    incident_id = uuid4()
    dedup_key = f"dira:incident:{incident_id}:dedup"

    assert connector._is_duplicate(incident_id) is False
    assert fake_redis.values[dedup_key] == "1"
    assert fake_redis.expiries[dedup_key] == 1800

    assert connector._is_duplicate(incident_id) is True


def test_incident_connector_parses_whatsapp_webhook_payload() -> None:
    connector = IncidentConnector(redis_client=_FakeRedis())
    incident_id = uuid4()
    webhook_payload = {
        "entry": [
            {
                "changes": [
                    {
                        "value": {
                            "messages": [
                                {
                                    "id": "wamid.test-123",
                                    "text": {
                                        "body": (
                                            "{"
                                            '"id": "%s", '
                                            '"incident_type": "roadblock", '
                                            '"lat": -6.82, '
                                            '"lon": 39.28, '
                                            '"reported_at": "2026-04-30T08:16:00+00:00", '
                                            '"description": "stalled truck", '
                                            '"severity": 4'
                                            "}"
                                        )
                                        % incident_id
                                    },
                                }
                            ]
                        }
                    }
                ]
            }
        ]
    }

    report = connector.ingest_from_whatsapp(webhook_payload)

    assert report.id == incident_id
    assert report.incident_type == IncidentType.ROADBLOCK
    assert report.lat == -6.82
    assert report.lon == 39.28
    assert report.reported_at == datetime(2026, 4, 30, 8, 16, tzinfo=UTC)
    assert report.source == "whatsapp"
    assert report.description == "stalled truck"
    assert report.severity == 4
