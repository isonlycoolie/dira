from __future__ import annotations

import os
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from typing import Any

from dira_common.exceptions import IngestionError
from dira_schemas.enums import DataSourceType
from dira_schemas.raw import GeoPoint, RawMessage
from dira_schemas.telecom import TelecomPing

from .base import BaseConnector


class _LocalRedisClient:
    def __init__(self) -> None:
        self._values: dict[str, str] = {}
        self._ttl_seconds: dict[str, int] = {}

    def get(self, key: str) -> str | None:
        return self._values.get(key)

    def set(self, key: str, value: str) -> None:
        self._values[key] = value

    def expire(self, key: str, seconds: int) -> None:
        self._ttl_seconds[key] = seconds


def _load_redis_client(redis_url: str) -> Any:
    try:
        import redis
    except ModuleNotFoundError:
        return _LocalRedisClient()

    return redis.Redis.from_url(redis_url, decode_responses=True)


class TelecomConnector(BaseConnector):
    def __init__(
        self,
        brokers: Sequence[str] | None = None,
        redis_client: Any | None = None,
        redis_url: str | None = None,
    ) -> None:
        super().__init__(brokers=brokers)
        self._redis = redis_client or _load_redis_client(redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0"))

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
        normalized_timestamp = timestamp if timestamp.tzinfo is not None else timestamp.replace(tzinfo=UTC)
        dwell_key = f"dira:device:{device_hash}:tower_dwell"
        raw_value = self._redis.get(dwell_key)
        if raw_value is None:
            self._redis.set(dwell_key, f"{tower_id}|{normalized_timestamp.isoformat()}")
            self._redis.expire(dwell_key, int(timedelta(minutes=15).total_seconds()))
            return False

        stored_tower_id, _, first_seen_text = str(raw_value).partition("|")
        if stored_tower_id != tower_id:
            self._redis.set(dwell_key, f"{tower_id}|{normalized_timestamp.isoformat()}")
            self._redis.expire(dwell_key, int(timedelta(minutes=15).total_seconds()))
            return False

        try:
            first_seen = datetime.fromisoformat(first_seen_text)
        except ValueError:
            first_seen = normalized_timestamp

        if first_seen.tzinfo is None:
            first_seen = first_seen.replace(tzinfo=UTC)

        is_residential = normalized_timestamp - first_seen >= timedelta(minutes=15)
        self._redis.set(dwell_key, f"{tower_id}|{first_seen.isoformat()}")
        self._redis.expire(dwell_key, int(timedelta(minutes=15).total_seconds()))
        return is_residential
