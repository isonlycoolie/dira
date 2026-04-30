from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

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
    from fastapi import APIRouter, Depends
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

from connectors.telecom import TelecomConnector
from dira_schemas.telecom import TelecomPing

from dependencies import get_api_key


class TelecomIngestResponse(BaseModel):
    published: int
    filtered: int


router = APIRouter(prefix="", tags=["ingestion"])


def _parse_brokers(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def get_telecom_connector() -> TelecomConnector:
    brokers = _parse_brokers(os.getenv("KAFKA_BROKERS", "localhost:9092"))
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    return TelecomConnector(brokers=brokers, redis_url=redis_url)


@router.post("/ingest/telecom", response_model=TelecomIngestResponse)
def ingest_telecom(
    pings: list[TelecomPing],
    api_key: str = Depends(get_api_key),
    connector: TelecomConnector = Depends(get_telecom_connector),
) -> TelecomIngestResponse:
    _ = api_key
    published, filtered = connector.ingest_batch([ping.model_dump(mode="python") for ping in pings])
    try:
        return TelecomIngestResponse(published=published, filtered=filtered)
    finally:
        connector.disconnect()
