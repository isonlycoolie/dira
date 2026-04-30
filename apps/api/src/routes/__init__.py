from __future__ import annotations

from .incidents import router as incidents_router
from .telecom import router as telecom_router

__all__ = ["incidents_router", "telecom_router"]
