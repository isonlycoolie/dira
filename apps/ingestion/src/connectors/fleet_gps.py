from __future__ import annotations

import json
import logging
from collections.abc import Callable, Sequence
from datetime import date
from hashlib import sha256
from typing import Any
from urllib import error as urllib_error
from urllib import request as urllib_request

from dira_common.exceptions import IngestionError
from dira_schemas.fleet import FleetGPSPoint

from .base import BaseConnector

logger = logging.getLogger(__name__)

DEFAULT_FLEET_TOPIC = "dira.raw.fleet"


class FleetGPSConnector(BaseConnector):
    def __init__(
        self,
        brokers: Sequence[str] | None = None,
        fetcher: Callable[[str, str], list[dict[str, Any]]] | None = None,
    ) -> None:
        super().__init__(brokers=brokers)
        self._fetcher = fetcher or self._fetch_from_api

    def connect(self) -> None:
        self._connect_producer()

    def disconnect(self) -> None:
        self._disconnect_producer()

    def health_check(self) -> bool:
        producer = self._producer
        if producer is None:
            return False
        return producer.is_healthy()

    def ingest_from_api(self, api_url: str, api_key: str) -> int:
        self.connect()
        published = 0
        payloads = self._fetcher(api_url, api_key)
        if not isinstance(payloads, list):
            raise IngestionError("fleet gps API must return a JSON array", details={"api_url": api_url}, source="ingestion")

        for payload in payloads:
            normalized_payload = self._normalize_payload(payload)
            fleet_point = FleetGPSPoint.model_validate(normalized_payload)
            self.publish(fleet_point, DEFAULT_FLEET_TOPIC)
            published += 1

        return published

    def _normalize_payload(self, payload: Any) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise IngestionError("fleet gps payload must be a JSON object", details={"payload_type": type(payload).__name__}, source="ingestion")

        raw_vehicle_id = payload.get("vehicle_id")
        if raw_vehicle_id is None:
            raw_vehicle_id = payload.get("vehicle_id_hash")
        if raw_vehicle_id is None:
            raise IngestionError("fleet gps payload is missing vehicle_id", details={"payload": payload}, source="ingestion")

        normalized_payload = dict(payload)
        normalized_payload["vehicle_id_hash"] = self._anonymize_vehicle_id(str(raw_vehicle_id))
        normalized_payload.pop("vehicle_id", None)
        return normalized_payload

    @staticmethod
    def _anonymize_vehicle_id(raw_id: str) -> str:
        daily_salt = date.today().isoformat()
        return sha256(f"{raw_id}{daily_salt}".encode("utf-8")).hexdigest()

    @staticmethod
    def _fetch_from_api(api_url: str, api_key: str) -> list[dict[str, Any]]:
        request = urllib_request.Request(
            api_url,
            headers={"X-API-Key": api_key, "Accept": "application/json"},
            method="GET",
        )

        try:
            with urllib_request.urlopen(request, timeout=30) as response:
                response_body = response.read().decode("utf-8")
        except (urllib_error.HTTPError, urllib_error.URLError, TimeoutError) as exc:
            raise IngestionError("failed to fetch fleet gps payloads", details={"api_url": api_url, "error": str(exc)}, source="ingestion") from exc

        try:
            payloads = json.loads(response_body)
        except json.JSONDecodeError as exc:
            raise IngestionError("fleet gps API returned invalid JSON", details={"api_url": api_url}, source="ingestion") from exc

        if not isinstance(payloads, list):
            raise IngestionError("fleet gps API must return a JSON array", details={"api_url": api_url}, source="ingestion")

        return payloads
