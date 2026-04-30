#!/usr/bin/env python3

from __future__ import annotations

import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import structlog

PROJECT_ROOT = Path(__file__).resolve().parents[1]
for package_path in (
    PROJECT_ROOT / "libs" / "common" / "src",
    PROJECT_ROOT / "libs" / "geospatial" / "src",
):
    package_path_str = str(package_path)
    if package_path_str not in sys.path:
        sys.path.insert(0, package_path_str)

from dira_common.config import DiraSettings
from dira_common.logging import setup_logging
from dira_geo.road_network import OsmRoadNetworkExtractor, RoadNetworkLoader


logger = structlog.get_logger(__name__)


def _summarize_road_types(edges: Any) -> dict[str, int]:
    if "highway" not in edges.columns:
        return {}
    return {str(key): int(value) for key, value in edges["highway"].value_counts().to_dict().items()}


def bootstrap_road_network(
    settings: DiraSettings | None = None,
    extractor: OsmRoadNetworkExtractor | None = None,
    loader: RoadNetworkLoader | None = None,
    engine_factory: Callable[[str], Any] | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or DiraSettings()
    resolved_extractor = extractor or OsmRoadNetworkExtractor()
    resolved_loader = loader or RoadNetworkLoader()
    resolved_engine_factory = engine_factory or _default_engine_factory

    road_edges = resolved_extractor.extract(tuple(resolved_settings.dsm_bbox))
    total_edges = int(len(road_edges))
    if total_edges < 1000:
        raise RuntimeError(f"expected at least 1000 road edges, got {total_edges}")

    road_type_breakdown = _summarize_road_types(road_edges)
    logger.info(
        "bootstrapped road network",
        total_edges=total_edges,
        road_type_breakdown=road_type_breakdown,
    )

    engine = resolved_engine_factory(resolved_settings.database_url)
    try:
        resolved_loader.load(road_edges, engine)
    finally:
        dispose = getattr(engine, "dispose", None)
        if callable(dispose):
            dispose()

    return {
        "total_edges": total_edges,
        "road_type_breakdown": road_type_breakdown,
    }


def _default_engine_factory(database_url: str) -> Any:
    from sqlalchemy import create_engine

    return create_engine(database_url)


def main() -> int:
    settings = DiraSettings()
    setup_logging(settings.env, service_name="bootstrap-roads")
    bootstrap_road_network(settings=settings)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
