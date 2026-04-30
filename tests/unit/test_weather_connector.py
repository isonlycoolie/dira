from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
for package_path in (
    PROJECT_ROOT / "apps" / "ingestion" / "src",
    PROJECT_ROOT / "libs" / "common" / "src",
    PROJECT_ROOT / "libs" / "schemas" / "src",
):
    package_path_str = str(package_path)
    if package_path_str not in sys.path:
        sys.path.insert(0, package_path_str)

from connectors.base import BaseConnector
from connectors.weather import CACHE_TTL_SECONDS, DEFAULT_WEATHER_TOPIC, WEATHER_CACHE_KEY, WeatherConnector
from dira_schemas.enums import WeatherCondition


class _FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.expiries: dict[str, int] = {}

    def get(self, key: str) -> str | None:
        return self.values.get(key)

    def set(self, key: str, value: str) -> None:
        self.values[key] = value

    def expire(self, key: str, seconds: int) -> None:
        self.expiries[key] = seconds


class _FakeProducer:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    def publish(self, topic, message):
        self.calls.append((topic, message.model_dump(mode="json")))
        return {"topic": topic}

    def is_healthy(self):
        return True

    def close(self):
        return None


def test_weather_connector_fetches_maps_publishes_and_caches(monkeypatch) -> None:
    fake_redis = _FakeRedis()
    fake_producer = _FakeProducer()
    captured = {}
    response = {
        "name": "Dar es Salaam",
        "dt": 1714468200,
        "weather": [{"main": "Rain", "description": "heavy rain"}],
        "rain": {"1h": 8.1},
        "visibility": 7000,
        "main": {"temp": 27.4},
    }

    def fake_fetcher(api_url: str, api_key: str):
        captured["api_url"] = api_url
        captured["api_key"] = api_key
        return response

    monkeypatch.setattr(BaseConnector, "_connect_producer", lambda self: setattr(self, "_producer", fake_producer))

    connector = WeatherConnector(redis_client=fake_redis, api_key="test-key", fetcher=fake_fetcher)
    reading = connector.fetch_and_publish()

    assert captured["api_key"] == "test-key"
    assert "lat=" in captured["api_url"]
    assert "lon=" in captured["api_url"]
    assert "appid=test-key" in captured["api_url"]
    assert "units=metric" in captured["api_url"]
    assert reading.station_id == "Dar es Salaam"
    assert reading.condition == WeatherCondition.HEAVY_RAIN
    assert reading.rainfall_mm == 8.1
    assert reading.visibility_m == 7000.0
    assert reading.temperature_c == 27.4
    assert fake_producer.calls[0][0] == DEFAULT_WEATHER_TOPIC
    attributes = fake_producer.calls[0][1]["attributes"]
    assert attributes["condition"] == "heavy_rain"
    assert attributes["temperature_c"] == 27.4
    assert fake_redis.expiries[WEATHER_CACHE_KEY] == CACHE_TTL_SECONDS
    cached_payload = json.loads(fake_redis.values[WEATHER_CACHE_KEY])
    assert cached_payload["station_id"] == "Dar es Salaam"
    assert cached_payload["condition"] == "heavy_rain"
    assert cached_payload["timestamp"] == datetime.fromtimestamp(1714468200, tz=UTC).isoformat().replace("+00:00", "Z")
