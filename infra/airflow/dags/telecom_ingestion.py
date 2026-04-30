from __future__ import annotations

import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]
for package_path in (
    PROJECT_ROOT / "apps" / "ingestion" / "src",
    PROJECT_ROOT / "libs" / "common" / "src",
    PROJECT_ROOT / "libs" / "schemas" / "src",
):
    package_path_str = str(package_path)
    if package_path_str not in sys.path:
        sys.path.insert(0, package_path_str)

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ModuleNotFoundError:
    class DAG:  # type: ignore[no-redef]
        def __init__(
            self,
            dag_id: str,
            start_date: datetime | None = None,
            schedule_interval: timedelta | None = None,
            catchup: bool = False,
            tags: list[str] | None = None,
            **kwargs: Any,
        ) -> None:
            self.dag_id = dag_id
            self.start_date = start_date
            self.schedule_interval = schedule_interval
            self.catchup = catchup
            self.tags = tags or []
            self.kwargs = kwargs
            self.tasks: list[Any] = []

        def __enter__(self) -> DAG:
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

    class PythonOperator:  # type: ignore[no-redef]
        def __init__(self, task_id: str, python_callable: Any, dag: Any = None, **kwargs: Any) -> None:
            self.task_id = task_id
            self.python_callable = python_callable
            self.dag = dag
            self.kwargs = kwargs
            if dag is not None and hasattr(dag, "tasks"):
                dag.tasks.append(self)

from connectors.telecom import TelecomConnector
from simulators.telecom_sim import TelecomDataSimulator


def _parse_brokers(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def run_telecom_ingestion(batch_size: int = 50) -> dict[str, int]:
    simulator = TelecomDataSimulator()
    connector = TelecomConnector(
        brokers=_parse_brokers(os.getenv("KAFKA_BROKERS", "localhost:9092")),
        redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
    )

    pings = simulator.generate_batch(batch_size)
    payloads = [ping.model_dump(mode="python") for ping in pings]
    try:
        published, filtered = connector.ingest_batch(payloads)
        return {"generated": len(payloads), "published": published, "filtered": filtered}
    finally:
        connector.disconnect()


with DAG(
    dag_id="telecom_ingestion",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule_interval=timedelta(seconds=30),
    catchup=False,
    tags=["ingestion", "realtime"],
) as dag:
    ingest_telecom_batch = PythonOperator(
        task_id="ingest_telecom_batch",
        python_callable=run_telecom_ingestion,
        dag=dag,
    )
