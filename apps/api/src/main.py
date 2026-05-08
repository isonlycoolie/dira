from __future__ import annotations

import asyncio
import hashlib
import os
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import AsyncGenerator

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

# Insert local package paths for imports when running in tests or dev
PROJECT_ROOT = (__file__).resolve().parents[3]
for package_path in (
    PROJECT_ROOT / "libs" / "common" / "src",
    PROJECT_ROOT / "libs" / "schemas" / "src",
):
    package_path_str = str(package_path)
    if package_path_str not in sys.path:
        sys.path.insert(0, package_path_str)

try:
    import asyncpg
except Exception:  # pragma: no cover - optional in test environments
    asyncpg = None  # type: ignore

try:
    import aioredis
except Exception:  # pragma: no cover - optional in test environments
    aioredis = None  # type: ignore

try:
    import structlog
except Exception:  # pragma: no cover - optional
    structlog = None  # type: ignore

try:
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
except Exception:  # pragma: no cover - optional
    generate_latest = None  # type: ignore
    CONTENT_TYPE_LATEST = "text/plain"


class RequestIDMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get("X-Request-ID")
        if not request_id:
            request_id = hashlib.sha256(f"{datetime.now(timezone.utc).isoformat()}".encode()).hexdigest()
        request.state.request_id = request_id
        response: Response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        return response


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # Initialize logging
    if structlog is not None:
        structlog.configure(processors=[structlog.processors.JSONRenderer()])
        app.state.log = structlog.get_logger("apps.api")
    else:
        app.state.log = None

    # Initialize asyncpg pool
    app.state.db_pool = None
    db_dsn = os.getenv("DATABASE_DSN")
    if asyncpg is not None and db_dsn:
        try:
            app.state.db_pool = await asyncpg.create_pool(dsn=db_dsn, min_size=1, max_size=5)
            if app.state.log:
                app.state.log.info("asyncpg pool created")
        except Exception as exc:  # pragma: no cover - best-effort in dev
            if app.state.log:
                app.state.log.error("failed to create asyncpg pool", exc=exc)

    # Initialize Redis
    app.state.redis = None
    redis_url = os.getenv("REDIS_URL")
    if aioredis is not None and redis_url:
        try:
            app.state.redis = await aioredis.from_url(redis_url)
            if app.state.log:
                app.state.log.info("redis connected")
        except Exception as exc:  # pragma: no cover - best-effort
            if app.state.log:
                app.state.log.error("failed to connect redis", exc=exc)

    yield

    # Teardown
    if app.state.db_pool is not None:
        await app.state.db_pool.close()
    if app.state.redis is not None:
        await app.state.redis.close()


def create_app() -> FastAPI:
    app = FastAPI(title="DIRA API", version="0.1.0", lifespan=lifespan)

    # CORS
    origins = os.getenv("CORS_ORIGINS", "*")
    origin_list = [o.strip() for o in origins.split(",")] if origins else ["*"]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origin_list,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Request ID middleware
    app.add_middleware(RequestIDMiddleware)

    # Prometheus metrics endpoint
    @app.get("/metrics")
    async def metrics() -> Response:
        if generate_latest is None:
            return Response("", media_type="text/plain")
        payload = generate_latest()
        return Response(payload, media_type=CONTENT_TYPE_LATEST)

    # Health endpoint
    @app.get("/health")
    async def health() -> dict:
        db_ok = False
        redis_ok = False
        try:
            if getattr(app.state, "db_pool", None) is not None:
                # lightweight check
                async with app.state.db_pool.acquire() as conn:  # type: ignore[attr-defined]
                    await conn.fetchval("SELECT 1")
                db_ok = True
        except Exception:
            db_ok = False
        try:
            if getattr(app.state, "redis", None) is not None:
                await app.state.redis.ping()  # type: ignore[attr-defined]
                redis_ok = True
        except Exception:
            redis_ok = False
        return {
            "status": "ok" if (db_ok and redis_ok) else "degraded",
            "version": app.version,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "dependencies": {"postgres": db_ok, "redis": redis_ok},
        }

    return app


app = create_app()


__all__ = ["app", "create_app"]
