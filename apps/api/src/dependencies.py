from __future__ import annotations

import sys
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from pathlib import Path

from fastapi import HTTPException, Request

# Ensure local libs are importable in dev/test environments
PROJECT_ROOT = Path(__file__).resolve().parents[3]
for package_path in (
    PROJECT_ROOT / "libs" / "common" / "src",
    PROJECT_ROOT / "libs" / "schemas" / "src",
):
    package_path_str = str(package_path)
    if package_path_str not in sys.path:
        sys.path.insert(0, package_path_str)


@asynccontextmanager
async def get_db(request: Request) -> AsyncGenerator[object, None]:
    """FastAPI dependency that yields a connection from the app's asyncpg pool.

    Expects `request.app.state.db_pool` to be an `asyncpg.pool.Pool` or an object
    implementing `acquire()` as an async context manager.
    """
    pool = getattr(request.app.state, "db_pool", None)
    if pool is None:
        raise HTTPException(status_code=503, detail="database pool not available")

    # `pool.acquire()` should be an async context manager that yields a connection
    async with pool.acquire() as conn:  # type: ignore[attr-defined]
        yield conn


__all__ = ["get_db"]
