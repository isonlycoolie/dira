from __future__ import annotations

import sys
from datetime import date
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

from connectors import telecom as telecom_module
from connectors.telecom import TelecomConnector


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
