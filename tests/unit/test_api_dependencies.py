from __future__ import annotations

import sys
from types import SimpleNamespace
from contextlib import asynccontextmanager
import pytest

PROJECT_ROOT = __file__
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
from apps.api.src.dependencies import get_db


class _FakeConn:
    async def fetchval(self, query: str):
        return 1


class _FakePool:
    def __init__(self):
        self.acquired = False

    @asynccontextmanager
    async def acquire(self):
        conn = _FakeConn()
        try:
            yield conn
        finally:
            pass


@pytest.mark.asyncio
async def test_get_db_yields_connection():
    app = create_app()
    app.state.db_pool = _FakePool()
    request = SimpleNamespace(app=app)

    async with get_db(request) as conn:
        assert hasattr(conn, "fetchval")

