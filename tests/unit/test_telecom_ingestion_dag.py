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

import telecom_ingestion as dag_module


class FakeSimulator:
    def __init__(self) -> None:
        self.batch_sizes: list[int] = []

    def generate_batch(self, count: int):
        self.batch_sizes.append(count)
        return [type("Ping", (), {"model_dump": lambda self, mode="python": {"value": 1}})() for _ in range(count)]


class FakeConnector:
    def __init__(self) -> None:
        self.received: list[list[dict[str, object]]] = []
        self.disconnected = False

    def ingest_batch(self, payloads):
        self.received.append(payloads)
        return len(payloads), 0

    def disconnect(self) -> None:
        self.disconnected = True


def test_telecom_ingestion_dag_shape() -> None:
    assert dag_module.dag.dag_id == "telecom_ingestion"
    assert dag_module.dag.schedule_interval.total_seconds() == 30
    assert dag_module.dag.catchup is False
    assert dag_module.dag.tags == ["ingestion", "realtime"]
    assert len(dag_module.dag.tasks) == 1
    assert dag_module.dag.tasks[0].task_id == "ingest_telecom_batch"


def test_run_telecom_ingestion_delegates_to_simulator_and_connector(monkeypatch) -> None:
    fake_simulator = FakeSimulator()
    fake_connector = FakeConnector()
    monkeypatch.setattr(dag_module, "TelecomDataSimulator", lambda: fake_simulator)
    monkeypatch.setattr(
        dag_module,
        "TelecomConnector",
        lambda brokers=None, redis_url=None: fake_connector,
    )

    result = dag_module.run_telecom_ingestion(batch_size=3)

    assert fake_simulator.batch_sizes == [3]
    assert len(fake_connector.received) == 1
    assert len(fake_connector.received[0]) == 3
    assert result == {"generated": 3, "published": 3, "filtered": 0}
    assert fake_connector.disconnected is True
