from __future__ import annotations

import sys
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
for package_path in (
    PROJECT_ROOT / "apps" / "api" / "src",
    PROJECT_ROOT / "apps" / "ingestion" / "src",
    PROJECT_ROOT / "libs" / "common" / "src",
    PROJECT_ROOT / "libs" / "schemas" / "src",
):
    package_path_str = str(package_path)
    if package_path_str not in sys.path:
        sys.path.insert(0, package_path_str)

import dependencies as api_dependencies
import routes.incidents as incidents_module
from dira_schemas.enums import IncidentType
from dira_schemas.incidents import IncidentReport
from routes.incidents import ingest_incident


class FakeConnector:
    def __init__(self) -> None:
        self.calls: list[IncidentReport] = []
        self.disconnected = False

    def ingest(self, report: IncidentReport):
        self.calls.append(report)
        return report

    def disconnect(self) -> None:
        self.disconnected = True


def test_ingest_incident_delegates_to_connector(monkeypatch: pytest.MonkeyPatch) -> None:
    token = "test-internal-key"
    monkeypatch.setenv("INTERNAL_API_KEY_HASH", sha256(token.encode("utf-8")).hexdigest())

    incidents_module._RATE_LIMIT_STATE.clear()
    connector = FakeConnector()
    report = IncidentReport(
        id=uuid4(),
        incident_type=IncidentType.ACCIDENT,
        lat=-6.8,
        lon=39.2,
        reported_at=datetime(2026, 4, 30, 8, 15, tzinfo=UTC),
        source="api",
        description="collision near junction",
        severity=3,
    )

    response = ingest_incident(report, api_key_hash=api_dependencies.verify_api_key(token), connector=connector)

    assert response.id == report.id
    assert connector.calls == [report]
    assert connector.disconnected is True


def test_incident_rate_limit_blocks_after_100_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    token_hash = sha256(b"test-internal-key").hexdigest()
    incidents_module._RATE_LIMIT_STATE.clear()
    monkeypatch.setattr(incidents_module.time, "monotonic", lambda: 1.0)

    for _ in range(100):
        assert incidents_module._enforce_incident_rate_limit(token_hash) == token_hash

    with pytest.raises(incidents_module.HTTPException) as exc_info:
        incidents_module._enforce_incident_rate_limit(token_hash)

    assert exc_info.value.status_code == 429
