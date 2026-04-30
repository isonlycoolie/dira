from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
for package_path in (
    PROJECT_ROOT / "infra" / "airflow" / "dags",
    PROJECT_ROOT / "apps" / "ingestion" / "src",
    PROJECT_ROOT / "libs" / "common" / "src",
    PROJECT_ROOT / "libs" / "schemas" / "src",
):
    package_path_str = str(package_path)
    if package_path_str not in sys.path:
        sys.path.insert(0, package_path_str)

import fleet_gps_ingestion as dag_module


class _FakeSimulator:
    def __init__(self) -> None:
        self.batch_sizes: list[int] = []

    def generate_batch(self, count: int):
        self.batch_sizes.append(count)
        return [
            {
                "vehicle_id": f"veh-{index + 1}",
                "provider": "fleet-simulator",
                "lat": -6.8,
                "lon": 39.2,
                "speed_kmh": 42.0,
                "heading": 90.0,
                "timestamp": "2026-04-30T08:00:00+00:00",
            }
            for index in range(count)
        ]


class _FakeConnector:
    instances: list["_FakeConnector"] = []

    def __init__(self, brokers=None, redis_url=None, fetcher=None):
        self.brokers = brokers
        self.redis_url = redis_url
        self.fetcher = fetcher
        self.disconnect_called = False
        self.calls: list[tuple[str, str]] = []
        self.payloads: list[dict[str, object]] = []
        self.__class__.instances.append(self)

    def ingest_from_api(self, api_url: str, api_key: str):
        self.calls.append((api_url, api_key))
        self.payloads = self.fetcher(api_url, api_key)
        return len(self.payloads)

    def disconnect(self):
        self.disconnect_called = True


def test_fleet_gps_ingestion_dag_shape() -> None:
    assert dag_module.dag.dag_id == "fleet_gps_ingestion"
    assert dag_module.dag.schedule_interval.total_seconds() == 3600
    assert dag_module.dag.catchup is False
    assert dag_module.dag.tags == ["ingestion", "fleet"]
    assert len(dag_module.dag.tasks) == 1
    assert dag_module.dag.tasks[0].task_id == "ingest_fleet_gps_batch"
    assert dag_module.dag.tasks[0].kwargs["retries"] == 3
    assert dag_module.dag.tasks[0].kwargs["retry_delay"].total_seconds() == 300


def test_run_fleet_gps_ingestion_uses_simulator_as_mock_endpoint(monkeypatch) -> None:
    fake_simulator = _FakeSimulator()
    _FakeConnector.instances = []
    monkeypatch.setattr(dag_module, "FleetGPSSimulator", lambda: fake_simulator)
    monkeypatch.setattr(dag_module, "FleetGPSConnector", _FakeConnector)

    result = dag_module.run_fleet_gps_ingestion(batch_size=4)

    assert fake_simulator.batch_sizes == [4]
    assert result == {"generated": 4, "published": 4}
    assert len(_FakeConnector.instances) == 1
    connector = _FakeConnector.instances[0]
    assert connector.calls == [("mock://fleet-gps", "mock-api-key")]
    assert connector.disconnect_called is True
    assert len(connector.payloads) == 4
    assert connector.payloads[0]["vehicle_id"] == "veh-1"
