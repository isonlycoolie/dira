from __future__ import annotations

import os
from hashlib import sha256
from typing import Any

try:
    from fastapi import Header, HTTPException, status
except ModuleNotFoundError:
    class HTTPException(Exception):
        def __init__(self, status_code: int, detail: str) -> None:
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail

    class _Status:
        HTTP_401_UNAUTHORIZED = 401

    status = _Status()

    def Header(default: Any = ..., alias: str | None = None) -> Any:
        return default

DEV_API_KEY_HASH = "ef7abf0aab9a10c171d92e8f72d7227d299818aa39a4f9a626a4e1c7834c4ffa"


def _expected_api_key_hash() -> str:
    return os.getenv("INTERNAL_API_KEY_HASH", DEV_API_KEY_HASH)


def verify_api_key(api_key: str) -> str:
    hashed_api_key = sha256(api_key.encode("utf-8")).hexdigest()
    expected_hash = _expected_api_key_hash()
    if hashed_api_key != expected_hash:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid api key")
    return hashed_api_key


def get_api_key(api_key: str = Header(..., alias="X-API-Key")) -> str:
    return verify_api_key(api_key)
