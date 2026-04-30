from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
for package_path in (
    PROJECT_ROOT / "libs" / "common" / "src",
    PROJECT_ROOT / "libs" / "geospatial" / "src",
):
    package_path_str = str(package_path)
    if package_path_str not in sys.path:
        sys.path.insert(0, package_path_str)

from dira_geo.road_network import OsmRoadNetworkExtractor, RoadNetworkLoader

KARIAKOO_SUB_BBOX = (-6.8225, 39.2665, -6.8135, 39.2755)


def _install_execute_compat(monkeypatch: pytest.MonkeyPatch, engine_cls: type[Any]) -> None:
    def execute(self: Any, statement: object, *args: Any, **kwargs: Any):
        # GeoPandas still hands the loader a real Engine, so preserve the older execute entrypoint.
        from sqlalchemy import text as sa_text

        sql = statement if isinstance(statement, str) else str(statement)
        with self.connect() as connection:
            return connection.execute(sa_text(sql), *args, **kwargs)

    monkeypatch.setattr(engine_cls, "execute", execute, raising=False)


@pytest.mark.integration
def test_road_bootstrap_loads_buffer_and_h3(monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("geoalchemy2")
    pytest.importorskip("geopandas")
    pytest.importorskip("osmnx")
    pytest.importorskip("shapely")
    pytest.importorskip("sqlalchemy")
    pytest.importorskip("testcontainers.postgres")

    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import Engine
    from testcontainers.postgres import PostgresContainer

    postgres = PostgresContainer("postgis/postgis:16-3.4")
    try:
        postgres.start()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Docker unavailable for road bootstrap test: {exc}")

    engine = None
    try:
        connection_url = postgres.get_connection_url()
        engine = create_engine(connection_url)
        _install_execute_compat(monkeypatch, Engine)

        with engine.begin() as connection:
            postgis_enabled = connection.execute(
                text("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'postgis')")
            ).scalar_one()
            assert postgis_enabled is True

            try:
                connection.execute(text("CREATE EXTENSION IF NOT EXISTS h3"))
            except Exception:  # noqa: BLE001
                pass

            h3_function_exists = connection.execute(
                text(
                    "SELECT to_regprocedure('h3_lat_lng_to_cell(double precision, double precision, integer)') IS NOT NULL"
                )
            ).scalar_one()
            if not h3_function_exists:
                pytest.skip("H3 extension unavailable in road bootstrap test container")

        road_edges = OsmRoadNetworkExtractor().extract(KARIAKOO_SUB_BBOX)
        assert len(road_edges) > 0

        RoadNetworkLoader().load(road_edges, engine=engine)

        with engine.begin() as connection:
            row_count = connection.execute(text("SELECT COUNT(*) FROM road_edges")).scalar_one()
            buffer_count = connection.execute(
                text("SELECT COUNT(*) FROM road_edges WHERE buffer_geom IS NOT NULL")
            ).scalar_one()
            h3_count = connection.execute(
                text("SELECT COUNT(*) FROM road_edges WHERE h3_index IS NOT NULL AND h3_index <> ''")
            ).scalar_one()

        assert row_count > 0
        assert buffer_count == row_count
        assert h3_count == row_count
    finally:
        if engine is not None:
            engine.dispose()
        postgres.stop()