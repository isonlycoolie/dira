from __future__ import annotations

import os
from pathlib import Path

import pytest

pytest.importorskip("alembic")
pytest.importorskip("sqlalchemy")
pytest.importorskip("testcontainers.postgres")

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from testcontainers.postgres import PostgresContainer

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ALEMBIC_INI = PROJECT_ROOT / "data" / "migrations" / "alembic.ini"


@pytest.mark.integration
def test_migrations_run_cleanly() -> None:
    postgres = PostgresContainer("postgis/postgis:16-3.4")
    try:
        postgres.start()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Docker unavailable for migration test: {exc}")

    engine = None
    try:
        connection_url = postgres.get_connection_url()
        os.environ["DATABASE_URL"] = connection_url

        alembic_config = Config(str(ALEMBIC_INI))
        alembic_config.set_main_option("sqlalchemy.url", connection_url)
        command.upgrade(alembic_config, "head")

        engine = create_engine(connection_url)
        with engine.begin() as connection:
            tables = {
                row[0]
                for row in connection.execute(
                    text(
                        """
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = 'public'
                        """
                    )
                )
            }
            expected_tables = {
                "road_nodes",
                "road_edges",
                "road_corridors",
                "traffic_events",
                "traffic_events_default",
                "incidents",
                "congestion_predictions",
                "ml_models",
                "api_keys",
            }
            assert expected_tables.issubset(tables)

            postgis_enabled = connection.execute(
                text("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'postgis')")
            ).scalar_one()
            assert postgis_enabled is True

            enum_types = {
                row[0]
                for row in connection.execute(
                    text(
                        """
                        SELECT typname
                        FROM pg_type
                        WHERE typname IN (
                            'road_type_enum',
                            'source_type_enum',
                            'pipeline_stage_enum',
                            'incident_type_enum',
                            'congestion_level_enum',
                            'weather_condition_enum',
                            'model_type_enum'
                        )
                        """
                    )
                )
            }
            assert enum_types == {
                "road_type_enum",
                "source_type_enum",
                "pipeline_stage_enum",
                "incident_type_enum",
                "congestion_level_enum",
                "weather_condition_enum",
                "model_type_enum",
            }

            index_count = connection.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM pg_indexes
                    WHERE schemaname = 'public'
                    AND tablename IN (
                        'road_nodes',
                        'road_edges',
                        'road_corridors',
                        'traffic_events',
                        'traffic_events_default',
                        'incidents',
                        'congestion_predictions',
                        'ml_models',
                        'api_keys'
                    )
                    """
                )
            ).scalar_one()
            assert index_count > 0
    finally:
        if engine is not None:
            engine.dispose()
        postgres.stop()
