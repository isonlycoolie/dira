from __future__ import annotations

import sys
import types
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
for package_path in (
    PROJECT_ROOT / "apps" / "pipeline" / "src",
    PROJECT_ROOT / "libs" / "common" / "src",
    PROJECT_ROOT / "libs" / "schemas" / "src",
):
    package_path_str = str(package_path)
    if package_path_str not in sys.path:
        sys.path.insert(0, package_path_str)

from utils.spark_session import DEFAULT_SPARK_PACKAGES, SparkSessionFactory


def test_spark_session_factory_configures_expected_packages(monkeypatch) -> None:
    captured: dict[str, object] = {}
    fake_session = object()

    class _FakeBuilder:
        def __init__(self) -> None:
            self.configs: dict[str, str] = {}

        def master(self, value: str) -> _FakeBuilder:
            captured["master"] = value
            return self

        def appName(self, value: str) -> _FakeBuilder:
            captured["app_name"] = value
            return self

        def config(self, key: str, value: str) -> _FakeBuilder:
            self.configs[key] = value
            return self

        def getOrCreate(self) -> object:
            captured["configs"] = dict(self.configs)
            return fake_session

    fake_builder = _FakeBuilder()
    fake_sql_module = types.ModuleType("pyspark.sql")
    fake_sql_module.SparkSession = types.SimpleNamespace(builder=fake_builder)
    fake_pyspark_module = types.ModuleType("pyspark")
    fake_pyspark_module.__path__ = []  # type: ignore[attr-defined]
    fake_pyspark_module.sql = fake_sql_module

    monkeypatch.setitem(sys.modules, "pyspark", fake_pyspark_module)
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql_module)
    monkeypatch.setenv("SPARK_MASTER_URL", "local[2]")
    monkeypatch.setenv("SPARK_STREAMING_CHECKPOINT_LOCATION", "file:///tmp/dira-checkpoints")
    monkeypatch.delenv("SPARK_JARS_PACKAGES", raising=False)

    session = SparkSessionFactory.create("dira-pipeline")

    assert session is fake_session
    assert captured["master"] == "local[2]"
    assert captured["app_name"] == "dira-pipeline"
    assert captured["configs"]["spark.jars.packages"] == ",".join(DEFAULT_SPARK_PACKAGES)
    assert DEFAULT_SPARK_PACKAGES[0].startswith("org.apache.spark:spark-sql-kafka-0-10_2.12:")
    assert DEFAULT_SPARK_PACKAGES[1].startswith("com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:")
    assert DEFAULT_SPARK_PACKAGES[2].startswith("org.postgresql:postgresql:")
    assert captured["configs"]["spark.sql.streaming.checkpointLocation"] == "file:///tmp/dira-checkpoints"