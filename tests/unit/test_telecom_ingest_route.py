from __future__ import annotations

import sys
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

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
from dira_schemas.telecom import TelecomPing
from routes.telecom import ingest_telecom


def test_verify_api_key_uses_configured_hash(monkeypatch: pytest.MonkeyPatch) -> None:
    token = "test-internal-key"
    monkeypatch.setenv("INTERNAL_API_KEY_HASH", sha256(token.encode("utf-8")).hexdigest())

    assert api_dependencies.verify_api_key(token) == sha256(token.encode("utf-8")).hexdigest()

    with pytest.raises(api_dependencies.HTTPException):
        api_dependencies.verify_api_key("wrong-key")


class FakeConnector:
    def __init__(self) -> None:
        self.calls: list[list[dict[str, object]]] = []
        self.disconnected = False

    def ingest_batch(self, pings):
        self.calls.append(pings)
        return len(pings), 0

    def disconnect(self) -> None:
        self.disconnected = True


def test_ingest_telecom_delegates_to_connector(monkeypatch: pytest.MonkeyPatch) -> None:
    token = "test-internal-key"
    monkeypatch.setenv("INTERNAL_API_KEY_HASH", sha256(token.encode("utf-8")).hexdigest())

    connector = FakeConnector()
    payload = [
        TelecomPing(
            device_id_hash="device-1",
            tower_id="tower-a",
            lat=-6.8,
            lon=39.2,
            timestamp=datetime(2026, 4, 30, 8, 15, tzinfo=UTC),
            signal_strength=-65,
        )
    ]

    response = ingest_telecom(payload, api_key=token, connector=connector)

    assert response.published == 1
    assert response.filtered == 0
    assert connector.calls == [[payload[0].model_dump(mode="python")]]
    assert connector.disconnected is True
