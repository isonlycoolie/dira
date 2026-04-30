from __future__ import annotations

import json
import logging
import os
import re
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from dira_common.exceptions import IngestionError
from dira_common.metrics import PrometheusRegistry
from dira_schemas.enums import DataSourceType
from dira_schemas.incidents import IncidentReport
from dira_schemas.raw import GeoPoint, RawMessage

from .base import BaseConnector

logger = logging.getLogger(__name__)

DEFAULT_INCIDENT_TOPIC = "dira.raw.incidents"
DEDUP_TTL_SECONDS = int(timedelta(minutes=30).total_seconds())


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


class IncidentConnector(BaseConnector):
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

    def ingest(self, report: IncidentReport) -> IncidentReport | None:
        validated_report = IncidentReport.model_validate(report.model_dump(mode="python"))
        PrometheusRegistry.incident_reports_received_total.inc()
        if self._is_duplicate(validated_report.id):
            logger.debug("filtered duplicate incident report", incident_id=str(validated_report.id))
            return None

        raw_message = RawMessage(
            source=DataSourceType.INCIDENT,
            timestamp=validated_report.reported_at,
            geo=GeoPoint(lat=validated_report.lat, lon=validated_report.lon),
            attributes={
                "incident_id": str(validated_report.id),
                "incident_type": validated_report.incident_type.value,
                "source": validated_report.source,
                "description": validated_report.description,
                "severity": validated_report.severity,
            },
        )
        self.publish(raw_message, DEFAULT_INCIDENT_TOPIC)
        return validated_report

    def ingest_from_whatsapp(self, webhook_payload: dict[str, Any]) -> IncidentReport:
        incident_payload = self._extract_incident_payload(webhook_payload)
        incident_payload.setdefault("id", uuid4())
        incident_payload.setdefault("source", "whatsapp")
        incident_payload.setdefault("reported_at", datetime.now(UTC))
        try:
            return IncidentReport.model_validate(incident_payload)
        except Exception as exc:  # noqa: BLE001
            raise IngestionError(
                "invalid WhatsApp incident payload",
                details={"error": str(exc)},
                source="ingestion",
            ) from exc

    def _is_duplicate(self, incident_id: UUID) -> bool:
        dedup_key = f"dira:incident:{incident_id}:dedup"
        if self._redis.get(dedup_key) is not None:
            PrometheusRegistry.incident_duplicates_filtered_total.inc()
            return True

        self._redis.set(dedup_key, "1")
        self._redis.expire(dedup_key, DEDUP_TTL_SECONDS)
        return False

    def _extract_incident_payload(self, webhook_payload: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(webhook_payload, dict):
            raise IngestionError("WhatsApp webhook payload must be a JSON object", source="ingestion")

        for key in ("incident", "report", "payload"):
            nested_payload = webhook_payload.get(key)
            if isinstance(nested_payload, dict):
                return dict(nested_payload)

        body = self._extract_message_body(webhook_payload)
        if body is not None:
            return self._parse_body_text(body)

        if self._looks_like_incident_payload(webhook_payload):
            return dict(webhook_payload)

        raise IngestionError("unsupported WhatsApp incident payload", source="ingestion")

    def _extract_message_body(self, webhook_payload: dict[str, Any]) -> str | None:
        if isinstance(webhook_payload.get("body"), str):
            return str(webhook_payload["body"])
        if isinstance(webhook_payload.get("message"), str):
            return str(webhook_payload["message"])
        if isinstance(webhook_payload.get("text"), str):
            return str(webhook_payload["text"])

        entries = webhook_payload.get("entry")
        if isinstance(entries, list):
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                changes = entry.get("changes")
                if not isinstance(changes, list):
                    continue
                for change in changes:
                    if not isinstance(change, dict):
                        continue
                    value = change.get("value")
                    if not isinstance(value, dict):
                        continue
                    messages = value.get("messages")
                    if not isinstance(messages, list):
                        continue
                    for message in messages:
                        if not isinstance(message, dict):
                            continue
                        text = message.get("text")
                        if isinstance(text, dict) and isinstance(text.get("body"), str):
                            return str(text["body"])
        return None

    def _parse_body_text(self, body: str) -> dict[str, Any]:
        stripped_body = body.strip()
        if not stripped_body:
            raise IngestionError("WhatsApp incident body was empty", source="ingestion")

        try:
            parsed_body = json.loads(stripped_body)
        except json.JSONDecodeError:
            parsed_body = None
        if isinstance(parsed_body, dict):
            return parsed_body

        normalized: dict[str, Any] = {}
        for part in re.split(r"[;,\n]+", stripped_body):
            fragment = part.strip()
            if not fragment:
                continue
            if "=" in fragment:
                key, value = fragment.split("=", 1)
            elif ":" in fragment:
                key, value = fragment.split(":", 1)
            else:
                continue
            normalized[key.strip()] = value.strip()

        if not normalized:
            raise IngestionError("unable to parse WhatsApp incident body", source="ingestion")

        return normalized

    @staticmethod
    def _looks_like_incident_payload(payload: dict[str, Any]) -> bool:
        return any(key in payload for key in ("incident_type", "lat", "lon", "severity", "reported_at", "id"))
