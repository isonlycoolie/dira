from __future__ import annotations

from collections.abc import Sequence
from contextlib import contextmanager
from typing import Any

DEFAULT_USEFUL_TAGS_EDGE: tuple[str, ...] = (
    "osmid",
    "highway",
    "name",
    "length",
    "maxspeed",
    "oneway",
    "lanes",
    "bridge",
    "tunnel",
    "junction",
)


def _load_osmnx() -> Any:
    try:
        import osmnx as ox
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised when dependency is absent
        raise RuntimeError("osmnx is required for road network extraction") from exc
    return ox


def _load_geopandas() -> Any:
    try:
        import geopandas as gpd
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised when dependency is absent
        raise RuntimeError("geopandas is required for road network extraction") from exc
    return gpd


@contextmanager
def _temporary_useful_tags(ox: Any, useful_tags_edge: Sequence[str]):
    settings = getattr(ox, "settings", None)
    if settings is None or not hasattr(settings, "useful_tags_way"):
        yield
        return

    original_tags = list(getattr(settings, "useful_tags_way", []))
    settings.useful_tags_way = list(useful_tags_edge)
    try:
        yield
    finally:
        settings.useful_tags_way = original_tags


class OsmRoadNetworkExtractor:
    def __init__(
        self,
        useful_tags_edge: Sequence[str] | None = None,
        network_type: str = "drive",
    ) -> None:
        self.useful_tags_edge = tuple(useful_tags_edge or DEFAULT_USEFUL_TAGS_EDGE)
        self.network_type = network_type

    def extract(self, bbox: tuple[float, float, float, float]) -> "gpd.GeoDataFrame":
        ox = _load_osmnx()
        gpd = _load_geopandas()
        north, south, east, west = self._normalize_bbox(bbox)

        with _temporary_useful_tags(ox, self.useful_tags_edge):
            graph = self._graph_from_bbox(ox, north, south, east, west)
            _, edges = ox.graph_to_gdfs(graph, nodes=True, edges=True, fill_edge_geometry=True)

        edges = edges.reset_index()
        return gpd.GeoDataFrame(edges)

    def _normalize_bbox(self, bbox: tuple[float, float, float, float]) -> tuple[float, float, float, float]:
        south, west, north, east = bbox
        return north, south, east, west

    def _graph_from_bbox(
        self,
        ox: Any,
        north: float,
        south: float,
        east: float,
        west: float,
    ) -> Any:
        attempts = (
            lambda: ox.graph_from_bbox(
                north=north,
                south=south,
                east=east,
                west=west,
                network_type=self.network_type,
                simplify=True,
                retain_all=False,
                truncate_by_edge=True,
            ),
            lambda: ox.graph_from_bbox(
                north,
                south,
                east,
                west,
                network_type=self.network_type,
                simplify=True,
                retain_all=False,
                truncate_by_edge=True,
            ),
            lambda: ox.graph_from_bbox(
                (north, south, east, west),
                network_type=self.network_type,
                simplify=True,
                retain_all=False,
                truncate_by_edge=True,
            ),
        )

        last_error: Exception | None = None
        for attempt in attempts:
            try:
                return attempt()
            except TypeError as exc:
                last_error = exc

        raise TypeError("unsupported osmnx graph_from_bbox signature") from last_error
