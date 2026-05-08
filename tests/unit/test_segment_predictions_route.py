from __future__ import annotations

import sys
from types import SimpleNamespace
import pytest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
for package_path in (
    PROJECT_ROOT / "apps" / "api" / "src",
    PROJECT_ROOT / "libs" / "schemas" / "src",
):
    package_path_str = str(package_path)
    if package_path_str not in sys.path:
        sys.path.insert(0, package_path_str)

from apps.api.src.main import create_app
import importlib.util
from pathlib import Path as _P

# Load the segments module directly to avoid importing package __init__ side effects
spec = importlib.util.spec_from_file_location(
    "segments_module",
    str(_P(__file__).resolve().parents[2] / "apps" / "api" / "src" / "routes" / "segments.py"),
)
segments_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(segments_module)  # type: ignore[attr-defined]
segments_router = segments_module.router


class _FakeConn:
    async def fetch(self, query: str, *args):
        q = (query or "").lower()
        # return a model prediction if asking predictions table
        if "from segment_predictions" in q:
            return [
                {
                    "road_segment_id": args[0],
                    "predicted_at": "2026-05-08T00:00:00Z",
                    "model_name": "xgboost",
                    "predicted_speed_kmh": 18.5,
                    "confidence": 0.82,
                }
            ]
        # historical average query
        if "avg(" in q:
            return [{"predicted_speed_kmh": 25.0}]
        return []


class _FakePool:
    def __init__(self):
        pass

    async def acquire(self):
        # emulate async context manager
        class _CM:
            async def __aenter__(self_inner):
                return _FakeConn()

            async def __aexit__(self_inner, exc_type, exc, tb):
                return False

        return _CM()


@pytest.mark.asyncio
async def test_prediction_returns_model_prediction():
    app = create_app()
    app.include_router(segments_router)
    app.state.db_pool = _FakePool()

    conn_cm = await app.state.db_pool.acquire()
    conn = await conn_cm.__aenter__()
    try:
        result = await segments_module.get_segment_predictions(42, db=conn)
    finally:
        await conn_cm.__aexit__(None, None, None)

    assert isinstance(result, dict)
    assert result["model"] == "xgboost"
    assert result["predicted_speed_kmh"] == 18.5


@pytest.mark.asyncio
async def test_prediction_fallback_historical_avg():
    # fake conn that returns no model predictions but returns historical avg
    class _NoModelConn(_FakeConn):
        async def fetch(self, query: str, *args):
            q = (query or "").lower()
            if "from segment_predictions" in q:
                return []
            if "avg(" in q:
                return [{"predicted_speed_kmh": 30.0}]
            return []

    class _NoModelPool(_FakePool):
        async def acquire(self):
            class _CM:
                async def __aenter__(self_inner):
                    return _NoModelConn()

                async def __aexit__(self_inner, exc_type, exc, tb):
                    return False

            return _CM()

    app = create_app()
    app.include_router(segments_router)
    app.state.db_pool = _NoModelPool()

    conn_cm = await app.state.db_pool.acquire()
    conn = await conn_cm.__aenter__()
    try:
        result = await segments_module.get_segment_predictions(7, db=conn)
    finally:
        await conn_cm.__aexit__(None, None, None)

    assert result["model"] == "historical_avg"
    assert result["predicted_speed_kmh"] == 30.0
