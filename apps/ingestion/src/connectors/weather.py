from __future__ import annotations

import json
import logging
import os
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from dira_common.exceptions import IngestionError
from dira_schemas.enums import DataSourceType, WeatherCondition
from dira_schemas.raw import GeoPoint, RawMessage
from dira_schemas.telecom import DSM_BBOX
from dira_schemas.weather import WeatherReading

from .base import BaseConnector

logger = logging.getLogger(__name__)

DEFAULT_WEATHER_TOPIC = "dira.raw.weather"
WEATHER_CACHE_KEY = "dira:weather:dsm:latest"
CACHE_TTL_SECONDS = int(timedelta(minutes=10).total_seconds())
DEFAULT_STATION_ID = "dsm"
DEFAULT_WEATHER_API_URL = "https://api.openweathermap.org/data/2.5/weather"


class _LocalRedisClient:
    def __init__(self) -> None:
        self._values: dict[str, str] = {}
        self._ttl_seconds: dict[str, int] = {}

    def get(self, key: str) -> str | None:
        return self._values.get(key)

    def set(self, key: str, value: str) -> None:
        self._values[key] = value

    def expire(self, key: str, seconds: int) -> None:
        self._ttl_seconds[key] = seconds


def _load_redis_client(redis_url: str) -> Any:
    try:
        import redis
    except ModuleNotFoundError:
        return _LocalRedisClient()

    return redis.Redis.from_url(redis_url, decode_responses=True)


class WeatherConnector(BaseConnector):
    def __init__(
        self,
        brokers: Sequence[str] | None = None,
        redis_client: Any | None = None,
        redis_url: str | None = None,
        api_key: str | None = None,
        fetcher: Any | None = None,
        station_id: str = DEFAULT_STATION_ID,
        dsm_coordinates: tuple[float, float] | None = None,
    ) -> None:
        super().__init__(brokers=brokers)
        self._redis = redis_client or _load_redis_client(redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0"))
        self._api_key = api_key or os.getenv("OPENWEATHERMAP_API_KEY")
        self._fetcher = fetcher or self._fetch_from_api
        self._station_id = station_id
        self._dsm_coordinates = dsm_coordinates or self._resolve_dsm_coordinates()

    def connect(self) -> None:
        self._connect_producer()

    def disconnect(self) -> None:
        self._disconnect_producer()

    def health_check(self) -> bool:
        producer = self._producer
        if producer is None:
            return False
        return producer.is_healthy()

    def fetch_and_publish(self) -> WeatherReading:
        if not self._api_key:
            raise IngestionError("OPENWEATHERMAP_API_KEY is required", source="config")

        api_url = self._build_api_url()
        weather_payload = self._fetcher(api_url, self._api_key)
        weather_reading = self._map_weather_reading(weather_payload)

        raw_message = RawMessage(
            source=DataSourceType.WEATHER,
            timestamp=weather_reading.timestamp,
            geo=GeoPoint(lat=self._dsm_coordinates[0], lon=self._dsm_coordinates[1]),
            attributes={
                "station_id": weather_reading.station_id,
                "condition": weather_reading.condition.value,
                "rainfall_mm": weather_reading.rainfall_mm,
                "visibility_m": weather_reading.visibility_m,
                "temperature_c": weather_reading.temperature_c,
            },
        )
        self.publish(raw_message, DEFAULT_WEATHER_TOPIC)
        self._cache_latest_weather(weather_reading)
        return weather_reading

    def _build_api_url(self) -> str:
        params = {
            "lat": self._dsm_coordinates[0],
            "lon": self._dsm_coordinates[1],
            "appid": self._api_key,
            "units": "metric",
        }
        return f"{DEFAULT_WEATHER_API_URL}?{urlencode(params)}"

    def _cache_latest_weather(self, weather_reading: WeatherReading) -> None:
        self._redis.set(WEATHER_CACHE_KEY, json.dumps(weather_reading.model_dump(mode="json")))
        self._redis.expire(WEATHER_CACHE_KEY, CACHE_TTL_SECONDS)

    def _map_weather_reading(self, payload: dict[str, Any]) -> WeatherReading:
        if not isinstance(payload, dict):
            raise IngestionError("weather API must return a JSON object", source="ingestion")

        weather_items = payload.get("weather")
        weather_item = weather_items[0] if isinstance(weather_items, list) and weather_items else {}
        main_section = payload.get("main") if isinstance(payload.get("main"), dict) else {}
        rain_section = payload.get("rain") if isinstance(payload.get("rain"), dict) else {}

        rainfall_mm = self._first_float(rain_section, ("1h", "3h"), default=0.0)
        visibility_m = float(payload.get("visibility", 0.0) or 0.0)
        temperature_c = float(main_section.get("temp", 0.0) or 0.0)
        condition = self._map_condition(weather_item, rainfall_mm)
        timestamp = self._parse_timestamp(payload.get("dt") or payload.get("timestamp"))
        station_id = str(payload.get("name") or self._station_id)

        return WeatherReading(
            station_id=station_id,
            timestamp=timestamp,
            condition=condition,
            rainfall_mm=rainfall_mm,
            visibility_m=visibility_m,
            temperature_c=temperature_c,
        )

    @staticmethod
    def _map_condition(weather_item: Any, rainfall_mm: float) -> WeatherCondition:
        main = ""
        description = ""
        if isinstance(weather_item, dict):
            main = str(weather_item.get("main", "")).lower()
            description = str(weather_item.get("description", "")).lower()

        if rainfall_mm >= 7.5 or "heavy" in description:
            return WeatherCondition.HEAVY_RAIN
        if "fog" in main or "mist" in description or "fog" in description:
            return WeatherCondition.FOG
        if "rain" in main or "rain" in description:
            return WeatherCondition.RAIN
        return WeatherCondition.CLEAR

    @staticmethod
    def _first_float(source: dict[str, Any], keys: tuple[str, ...], default: float) -> float:
        for key in keys:
            if key not in source:
                continue
            try:
                return float(source[key])
            except (TypeError, ValueError):
                continue
        return default

    @staticmethod
    def _parse_timestamp(value: Any) -> datetime:
        if isinstance(value, datetime):
            return value if value.tzinfo is not None else value.replace(tzinfo=UTC)

        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value), tz=UTC)

        if isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                try:
                    return datetime.fromtimestamp(float(value), tz=UTC)
                except (TypeError, ValueError):
                    return datetime.now(UTC)
            return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)

        return datetime.now(UTC)

    def _resolve_dsm_coordinates(self) -> tuple[float, float]:
        south, west, north, east = DSM_BBOX
        return ((south + north) / 2.0, (west + east) / 2.0)

    @staticmethod
    def _fetch_from_api(api_url: str, api_key: str) -> dict[str, Any]:
        request = Request(api_url, headers={"Accept": "application/json"}, method="GET")
        try:
            with urlopen(request, timeout=30) as response:
                response_body = response.read().decode("utf-8")
        except Exception as exc:  # noqa: BLE001
            raise IngestionError("failed to fetch weather payload", details={"api_url": api_url, "error": str(exc)}, source="ingestion") from exc

        try:
            payload = json.loads(response_body)
        except json.JSONDecodeError as exc:
            raise IngestionError("weather API returned invalid JSON", details={"api_url": api_url}, source="ingestion") from exc

        if not isinstance(payload, dict):
            raise IngestionError("weather API must return a JSON object", details={"api_url": api_url}, source="ingestion")

        return payload
