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
        return [
            {
                "road_segment_id": 1,
                "name": "Main St",
                "road_type": "primary",
                "avg_speed_kmh": 30.5,
                "congestion_score": 0.12,
                "congestion_level": "free_flow",
            }
        ]


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
async def test_get_segments_route_returns_data():
    app = create_app()
    # register router for this test
    app.include_router(segments_router)
    app.state.db_pool = _FakePool()
    request = SimpleNamespace(app=app)

    # call the endpoint function directly via dependency injection
    get_segments = segments_module.get_segments

    conn_cm = await app.state.db_pool.acquire()
    conn = await conn_cm.__aenter__()
    try:
        result = await get_segments(None, db=conn)
    finally:
        await conn_cm.__aexit__(None, None, None)
    assert isinstance(result, list)
    assert result and result[0]["name"] == "Main St"
