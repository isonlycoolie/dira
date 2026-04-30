from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime, timedelta
from hashlib import sha256
from typing import Any

from dira_common.exceptions import IngestionError
from dira_schemas.enums import DataSourceType
from dira_schemas.raw import GeoPoint, RawMessage
from dira_schemas.telecom import TelecomPing

from .base import BaseConnector


class TelecomConnector(BaseConnector):
    def __init__(self, brokers: Sequence[str] | None = None) -> None:
        super().__init__(brokers=brokers)
        self._tower_dwell_start: dict[tuple[str, str], datetime] = {}

    def connect(self) -> None:
        self._connect_producer()

    def disconnect(self) -> None:
        self._disconnect_producer()

    def health_check(self) -> bool:
        producer = self._producer
        if producer is None:
            return False
        return producer.is_healthy()

    def ingest_batch(self, pings: list[dict[str, Any]]) -> tuple[int, int]:
        published = 0
        filtered = 0

        for ping_payload in pings:
            try:
                ping = TelecomPing.model_validate(ping_payload)
            except Exception:  # noqa: BLE001
                filtered += 1
                continue

            if self._is_residential(ping.device_id_hash, ping.tower_id, ping.timestamp):
                filtered += 1
                continue

            anonymized_device_id = self._anonymize_device_id(ping.device_id_hash)
            raw_message = RawMessage(
                source=DataSourceType.TELECOM,
                timestamp=ping.timestamp,
                geo=GeoPoint(lat=ping.lat, lon=ping.lon),
                attributes={
                    "device_id_hash": anonymized_device_id,
                    "tower_id": ping.tower_id,
                    "signal_strength": ping.signal_strength,
                },
            )
            try:
                self.publish(raw_message, "dira.raw.telecom")
            except IngestionError:
                raise
            else:
                published += 1

        return published, filtered

    def _anonymize_device_id(self, raw_id: str) -> str:
        daily_salt = date.today().isoformat()
        digest = sha256(f"{raw_id}{daily_salt}".encode("utf-8")).hexdigest()
        return digest

    def _is_residential(self, device_hash: str, tower_id: str, timestamp: datetime) -> bool:
        dwell_key = (device_hash, tower_id)
        first_seen = self._tower_dwell_start.get(dwell_key)
        if first_seen is None:
            self._tower_dwell_start[dwell_key] = timestamp
            return False

        if timestamp - first_seen >= timedelta(minutes=15):
            return True

        return False
