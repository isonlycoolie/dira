from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

try:
    from pydantic_settings import BaseSettings as PydanticBaseSettings
    from pydantic_settings import SettingsConfigDict
except ModuleNotFoundError:
    PydanticBaseSettings = None
    SettingsConfigDict = ConfigDict

    def _parse_env_file(path: Path) -> dict[str, str]:
        values: dict[str, str] = {}
        if not path.exists():
            return values

        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip().strip('"').strip("'")
        return values

    class BaseSettings(BaseModel):
        model_config = ConfigDict(extra="ignore", populate_by_name=True)

        def __init__(self, **data: Any) -> None:
            payload = self._load_settings_values()
            payload.update(data)
            super().__init__(**payload)

        @classmethod
        def _load_settings_values(cls) -> dict[str, Any]:
            env_values: dict[str, str] = {}
            env_file = cls.model_config.get("env_file") if isinstance(cls.model_config, dict) else None
            if env_file:
                env_paths = env_file if isinstance(env_file, (list, tuple)) else [env_file]
                for env_path in env_paths:
                    env_values.update(_parse_env_file(Path(env_path)))

            env_values.update(os.environ)

            resolved: dict[str, Any] = {}
            for field_name, field_info in cls.model_fields.items():
                candidates: list[str] = []
                alias = field_info.validation_alias
                if isinstance(alias, str):
                    candidates.append(alias)
                candidates.append(field_name.upper())
                candidates.append(field_name)

                for candidate in candidates:
                    if candidate in env_values:
                        resolved[candidate] = env_values[candidate]
                        break
            return resolved

else:
    BaseSettings = PydanticBaseSettings


class DiraSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore", populate_by_name=True)

    database_url: str = Field(validation_alias="DATABASE_URL")
    redis_url: str = Field(validation_alias="REDIS_URL")
    kafka_brokers: list[str] = Field(validation_alias="KAFKA_BROKERS")
    gcs_bucket_prefix: str = Field(validation_alias="GCS_BUCKET_PREFIX")
    dsm_bbox: tuple[float, float, float, float] = Field(validation_alias="DSM_BBOX")
    spark_master_url: str = Field(validation_alias="SPARK_MASTER_URL")
    airflow_db_url: str = Field(validation_alias="AIRFLOW_DB_URL")
    openweathermap_api_key: str = Field(validation_alias="OPENWEATHERMAP_API_KEY")
    road_buffer_meters: int = Field(default=50, validation_alias="ROAD_BUFFER_METERS")
    log_level: str = Field(default="INFO", validation_alias="LOG_LEVEL")
    env: Literal["dev", "prod"] = Field(default="dev", validation_alias="ENV")

    @field_validator("kafka_brokers", mode="before")
    @classmethod
    def parse_kafka_brokers(cls, value: object) -> list[str]:
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        if isinstance(value, (list, tuple)):
            return [str(item).strip() for item in value if str(item).strip()]
        raise TypeError("KAFKA_BROKERS must be a comma-separated string or sequence")

    @field_validator("dsm_bbox", mode="before")
    @classmethod
    def parse_dsm_bbox(cls, value: object) -> tuple[float, float, float, float]:
        if isinstance(value, str):
            parts = [item.strip() for item in value.split(",") if item.strip()]
        elif isinstance(value, (list, tuple)):
            parts = list(value)
        else:
            raise TypeError("DSM_BBOX must be a comma-separated string or sequence")

        if len(parts) != 4:
            raise ValueError("DSM_BBOX must contain four values")

        return tuple(float(part) for part in parts)

    @field_validator("road_buffer_meters")
    @classmethod
    def validate_road_buffer_meters(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("ROAD_BUFFER_METERS must be positive")
        return value
