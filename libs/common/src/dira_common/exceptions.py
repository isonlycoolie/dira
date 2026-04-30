from __future__ import annotations

from typing import Any


class DiraBaseException(Exception):
    default_source = "dira"

    def __init__(self, message: str, details: dict[str, Any] | None = None, source: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}
        self.source = source or self.default_source

    def __str__(self) -> str:
        return f"{self.source}: {self.message}"


class IngestionError(DiraBaseException):
    default_source = "ingestion"


class SpatialError(DiraBaseException):
    default_source = "spatial"


class PipelineError(DiraBaseException):
    default_source = "pipeline"


class MLError(DiraBaseException):
    default_source = "ml"


class ConfigError(DiraBaseException):
    default_source = "config"
