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
                "centroid": '{"type":"Point","coordinates":[-122.0,47.6]}',
                "avg_speed_kmh": 22.0,
                "congestion_score": 0.6,
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
async def test_heatmap_returns_featurecollection():
    app = create_app()
    app.include_router(segments_router)
    app.state.db_pool = _FakePool()

    conn_cm = await app.state.db_pool.acquire()
    conn = await conn_cm.__aenter__()
    try:
        result = await segments_module.get_congestion_heatmap(None, db=conn)
    finally:
        await conn_cm.__aexit__(None, None, None)

    assert isinstance(result, dict)
    assert result.get("type") == "FeatureCollection"
    features = result.get("features")
    assert isinstance(features, list) and len(features) == 1
    feat = features[0]
    assert feat["type"] == "Feature"
    assert feat["geometry"]["type"] == "Point"
    assert feat["properties"]["congestion_level"] == "heavy"
