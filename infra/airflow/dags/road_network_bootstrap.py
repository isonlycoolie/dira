from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
for path in (PROJECT_ROOT, SCRIPTS_DIR):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ModuleNotFoundError:
    class DAG:  # type: ignore[no-redef]
        def __init__(
            self,
            dag_id: str,
            start_date: datetime | None = None,
            schedule: str | None = None,
            catchup: bool = False,
            tags: list[str] | None = None,
            **kwargs: Any,
        ) -> None:
            self.dag_id = dag_id
            self.start_date = start_date
            self.schedule = schedule
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


def run_bootstrap_road_network() -> Any:
    from bootstrap_road_network import main as bootstrap_main

    return bootstrap_main()


with DAG(
    dag_id="road_network_bootstrap",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule="@once",
    catchup=False,
    tags=["infrastructure"],
) as dag:
    bootstrap_road_network_task = PythonOperator(
        task_id="bootstrap_road_network",
        python_callable=run_bootstrap_road_network,
        dag=dag,
    )
