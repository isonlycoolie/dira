from __future__ import annotations

import sys
from types import SimpleNamespace
import pytest
from pathlib import Path
import importlib.util

PROJECT_ROOT = Path(__file__).resolve().parents[2]
for package_path in (
    PROJECT_ROOT / "apps" / "api" / "src",
    PROJECT_ROOT / "libs" / "schemas" / "src",
):
    package_path_str = str(package_path)
    if package_path_str not in sys.path:
        sys.path.insert(0, package_path_str)

from apps.api.src.main import create_app

# load segments module directly
spec = importlib.util.spec_from_file_location(
    "segments_module",
    str(PROJECT_ROOT / "apps" / "api" / "src" / "routes" / "segments.py"),
)
segments_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(segments_module)  # type: ignore[attr-defined]
get_segment_traffic = segments_module.get_segment_traffic


class _FakeConn:
    async def fetch(self, query: str, *args):
        return [
            {
                "road_segment_id": 42,
                "event_time": "2026-05-08T12:00:00Z",
                "vehicle_count": 5,
                "avg_speed_kmh": 12.3,
                "flow_rate": 60.0,
                "congestion_score": 0.7,
            }
        ]


class _FakePool:
    async def acquire(self):
        class _CM:
            async def __aenter__(self_inner):
                return _FakeConn()

            async def __aexit__(self_inner, exc_type, exc, tb):
                return False

        return _CM()


@pytest.mark.asyncio
async def test_get_segment_traffic_returns_list():
    app = create_app()
    app.state.db_pool = _FakePool()

    # call function directly
    conn_cm = await app.state.db_pool.acquire()
    conn = await conn_cm.__aenter__()
    try:
        result = await get_segment_traffic(42, hours=1, db=conn)
    finally:
        await conn_cm.__aexit__(None, None, None)

    assert isinstance(result, list)
    assert result and result[0]["road_segment_id"] == 42
