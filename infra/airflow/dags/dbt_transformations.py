from __future__ import annotations

import logging
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DBT_PROJECT_DIR = PROJECT_ROOT / "data" / "dbt"

if str(DBT_PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(DBT_PROJECT_DIR))

try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
except ModuleNotFoundError:
    class DAG:  # type: ignore[no-redef]
        def __init__(
            self,
            dag_id: str,
            start_date: datetime | None = None,
            schedule_interval: timedelta | None = None,
            catchup: bool = False,
            tags: list[str] | None = None,
            default_args: dict[str, Any] | None = None,
            **kwargs: Any,
        ) -> None:
            self.dag_id = dag_id
            self.start_date = start_date
            self.schedule_interval = schedule_interval
            self.catchup = catchup
            self.tags = tags or []
            self.default_args = default_args or {}
            self.kwargs = kwargs
            self.tasks: list[Any] = []

        def __enter__(self) -> DAG:
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

    class BashOperator:  # type: ignore[no-redef]
        def __init__(self, task_id: str, bash_command: str, dag: Any = None, **kwargs: Any) -> None:
            self.task_id = task_id
            self.bash_command = bash_command
            self.dag = dag
            self.kwargs = kwargs
            if dag is not None and hasattr(dag, "tasks"):
                dag.tasks.append(self)

        def execute(self) -> subprocess.CompletedProcess[str]:
            return subprocess.run(self.bash_command, shell=True, check=True, cwd=str(DBT_PROJECT_DIR), text=True)

        def __rshift__(self, other: Any) -> Any:
            return other


LOGGER = logging.getLogger(__name__)


def _notify_on_failure(context: dict[str, Any]) -> None:
    task_instance = context.get("task_instance")
    task_id = getattr(task_instance, "task_id", "unknown")
    LOGGER.error("dbt transformation task failed", extra={"task_id": task_id, "dag_id": "dbt_transformations"})
    LOGGER.info("Slack alert placeholder: configure webhook in Phase 2")


DEFAULT_ARGS: dict[str, Any] = {
    "owner": "dira",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": _notify_on_failure,
}

with DAG(
    dag_id="dbt_transformations",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    tags=["dbt", "transformations", "analytics"],
    default_args=DEFAULT_ARGS,
) as dag:
    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command="dbt run --models silver gold",
        dag=dag,
    )

    test_dbt_models = BashOperator(
        task_id="test_dbt_models",
        bash_command="dbt test",
        dag=dag,
    )

    run_dbt_models >> test_dbt_models
