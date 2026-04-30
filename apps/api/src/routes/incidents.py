from __future__ import annotations

import os
import sys
import time
from collections import defaultdict, deque
from pathlib import Path
from threading import Lock
from typing import Any
from uuid import UUID

from pydantic import BaseModel

PROJECT_ROOT = Path(__file__).resolve().parents[4]
for package_path in (
    PROJECT_ROOT / "apps" / "ingestion" / "src",
    PROJECT_ROOT / "libs" / "common" / "src",
    PROJECT_ROOT / "libs" / "schemas" / "src",
):
    package_path_str = str(package_path)
    if package_path_str not in sys.path:
        sys.path.insert(0, package_path_str)

try:
    from fastapi import APIRouter, Depends, HTTPException, status
except ModuleNotFoundError:
    class APIRouter:  # type: ignore[no-redef]
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.routes: list[tuple[str, str, Any, dict[str, Any]]] = []

        def post(self, path: str, **kwargs: Any):
            def decorator(func: Any) -> Any:
                self.routes.append(("POST", path, func, kwargs))
                return func

            return decorator

    def Depends(dependency: Any) -> Any:  # type: ignore[no-redef]
        return dependency

    class HTTPException(Exception):  # type: ignore[no-redef]
        def __init__(self, status_code: int, detail: str) -> None:
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail

    class _Status:
        HTTP_201_CREATED = 201
        HTTP_429_TOO_MANY_REQUESTS = 429

    status = _Status()

from connectors.incidents import IncidentConnector
from dira_schemas.incidents import IncidentReport

from dependencies import get_api_key


class IncidentIngestResponse(BaseModel):
    id: UUID


router = APIRouter(prefix="", tags=["ingestion"])
_RATE_LIMIT_WINDOW_SECONDS = 60.0
_RATE_LIMIT_MAX_REQUESTS = 100
_RATE_LIMIT_STATE: dict[str, deque[float]] = defaultdict(deque)
_RATE_LIMIT_LOCK = Lock()


def _parse_brokers(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def get_incident_connector() -> IncidentConnector:
    brokers = _parse_brokers(os.getenv("KAFKA_BROKERS", "localhost:9092"))
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    return IncidentConnector(brokers=brokers, redis_url=redis_url)


def _enforce_incident_rate_limit(api_key_hash: str = Depends(get_api_key)) -> str:
    current_time = time.monotonic()
    with _RATE_LIMIT_LOCK:
        request_times = _RATE_LIMIT_STATE[api_key_hash]
        while request_times and current_time - request_times[0] >= _RATE_LIMIT_WINDOW_SECONDS:
            request_times.popleft()

        if len(request_times) >= _RATE_LIMIT_MAX_REQUESTS:
            raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="rate limit exceeded")

        request_times.append(current_time)

    return api_key_hash


@router.post("/incidents", status_code=status.HTTP_201_CREATED, response_model=IncidentIngestResponse)
def ingest_incident(
    report: IncidentReport,
    api_key_hash: str = Depends(_enforce_incident_rate_limit),
    connector: IncidentConnector = Depends(get_incident_connector),
) -> IncidentIngestResponse:
    _ = api_key_hash
    try:
        connector.ingest(report)
        return IncidentIngestResponse(id=report.id)
    finally:
        connector.disconnect()
