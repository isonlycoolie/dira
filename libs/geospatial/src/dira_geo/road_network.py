from __future__ import annotations

from collections.abc import Sequence
from contextlib import contextmanager
import logging
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

ALLOWED_ROAD_TYPES: tuple[str, ...] = (
    "primary",
    "secondary",
    "tertiary",
    "residential",
)

logger = logging.getLogger(__name__)


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


def _load_shapely_box() -> Any:
    try:
        from shapely.geometry import box
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised when dependency is absent
        raise RuntimeError("shapely is required for road network geometry validation") from exc
    return box


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
        filtered_edges = self._filter_road_types(edges)
        validated_edges = self._validate_geometries(filtered_edges, bbox)
        return gpd.GeoDataFrame(validated_edges)

    def _normalize_bbox(self, bbox: tuple[float, float, float, float]) -> tuple[float, float, float, float]:
        south, west, north, east = bbox
        return north, south, east, west

    def _filter_road_types(self, edges: Any) -> Any:
        if "highway" not in edges.columns:
            return edges

        allowed = {road_type.lower() for road_type in ALLOWED_ROAD_TYPES}

        def normalize_highway(value: Any) -> str | None:
            if isinstance(value, (list, tuple, set)):
                for item in value:
                    normalized = str(item).lower()
                    if normalized in allowed:
                        return normalized
                return None

            if value is None:
                return None

            normalized = str(value).lower()
            return normalized if normalized in allowed else None

        highway_values = edges["highway"].apply(normalize_highway)
        filtered_edges = edges[highway_values.notna()].copy()
        dropped_count = int(len(edges) - len(filtered_edges))
        logger.info(
            "filtered road types dropped=%s kept=%s",
            dropped_count,
            int(len(filtered_edges)),
        )
        filtered_edges["highway"] = highway_values[highway_values.notna()].values
        return filtered_edges

    def _validate_geometries(self, edges: Any, bbox: tuple[float, float, float, float]) -> Any:
        if "geometry" not in edges.columns:
            return edges

        box = _load_shapely_box()
        north, south, east, west = self._normalize_bbox(bbox)
        clip_box = box(west, south, east, north)

        valid_edges = edges[edges["geometry"].notna()].copy()
        null_dropped = int(len(edges) - len(valid_edges))
        if null_dropped:
            logger.warning("dropped null geometries dropped=%s", null_dropped)

        clipped_count = 0
        repaired_count = 0

        def repair_geometry(geometry: Any) -> Any:
            nonlocal clipped_count, repaired_count
            if geometry is None:
                return None

            if getattr(geometry, "is_empty", False):
                return None

            repaired_geometry = geometry
            if not getattr(repaired_geometry, "is_valid", True):
                repaired_geometry = repaired_geometry.buffer(0)
                repaired_count += 1

            try:
                clipped_geometry = repaired_geometry.intersection(clip_box)
            except Exception:  # noqa: BLE001
                clipped_geometry = repaired_geometry
            else:
                if clipped_geometry is not repaired_geometry:
                    clipped_count += 1

            if getattr(clipped_geometry, "is_empty", False):
                return None

            return clipped_geometry

        valid_edges["geometry"] = valid_edges["geometry"].apply(repair_geometry)
        valid_edges = valid_edges[valid_edges["geometry"].notna()].copy()

        if repaired_count:
            logger.warning("repaired invalid geometries repaired=%s", repaired_count)
        if clipped_count:
            logger.warning("clipped geometries to bbox clipped=%s", clipped_count)

        return valid_edges

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


class RoadNetworkLoader:
    def load(self, gdf: Any, engine: Any) -> None:
        gpd = _load_geopandas()
        prepared = self._prepare_road_edges_frame(gdf, gpd)
        prepared.to_postgis("road_edges", engine, if_exists="replace", index=False)
        self._precompute_buffers(engine)
        logger.info("loaded road edges rows=%s", int(len(prepared)))

    def _prepare_road_edges_frame(self, gdf: Any, gpd: Any) -> Any:
        frame = gdf.copy()
        if "geometry" in frame.columns:
            frame = frame.rename(columns={"geometry": "geom"})

        frame["id"] = range(1, len(frame) + 1)
        frame["osm_id"] = frame["osmid"].apply(self._coerce_osm_id) if "osmid" in frame.columns else frame["id"]
        frame["from_node_id"] = frame["u"] if "u" in frame.columns else None
        frame["to_node_id"] = frame["v"] if "v" in frame.columns else None
        frame["name"] = frame["name"] if "name" in frame.columns else None
        frame["road_type"] = frame["highway"].apply(self._coerce_road_type) if "highway" in frame.columns else "residential"
        frame["length_m"] = frame["length"] if "length" in frame.columns else 0.0
        frame["speed_limit_kmh"] = (
            frame["maxspeed"].apply(self._coerce_speed_limit) if "maxspeed" in frame.columns else 50
        )
        frame["buffer_geom"] = None
        frame["h3_index"] = None
        frame["metadata"] = frame.apply(self._build_metadata, axis=1)

        ordered_columns = [
            "id",
            "osm_id",
            "from_node_id",
            "to_node_id",
            "name",
            "road_type",
            "length_m",
            "speed_limit_kmh",
            "geom",
            "buffer_geom",
            "h3_index",
            "metadata",
        ]
        prepared = frame.reindex(columns=ordered_columns)
        return gpd.GeoDataFrame(prepared, geometry="geom", crs=getattr(gdf, "crs", None))

    def _precompute_buffers(self, engine: Any, road_buffer_meters: int = 50) -> None:
        execute = getattr(engine, "execute", None)
        if not callable(execute):
            return

        execute(
            f"UPDATE road_edges SET buffer_geom = ST_Buffer(geom::geography, {road_buffer_meters})::geometry"
        )
        logger.info("precomputed road buffers meters=%s", road_buffer_meters)

    def _coerce_osm_id(self, value: Any) -> int | None:
        if isinstance(value, (list, tuple, set)):
            value = next(iter(value), None)
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _coerce_road_type(self, value: Any) -> str:
        if isinstance(value, (list, tuple, set)):
            for item in value:
                normalized = str(item).lower()
                if normalized in ALLOWED_ROAD_TYPES:
                    return normalized
            return "residential"

        normalized = str(value).lower()
        return normalized if normalized in ALLOWED_ROAD_TYPES else "residential"

    def _coerce_speed_limit(self, value: Any) -> int:
        if value is None:
            return 50
        if isinstance(value, (int, float)):
            return int(value)

        digits = "".join(character for character in str(value) if character.isdigit())
        return int(digits) if digits else 50

    def _build_metadata(self, row: Any) -> dict[str, Any]:
        excluded_columns = {
            "id",
            "osm_id",
            "osmid",
            "from_node_id",
            "to_node_id",
            "u",
            "v",
            "name",
            "road_type",
            "highway",
            "length_m",
            "length",
            "speed_limit_kmh",
            "maxspeed",
            "geom",
            "geometry",
            "buffer_geom",
            "h3_index",
            "metadata",
        }
        metadata = {}
        for key, value in row.items():
            if key in excluded_columns:
                continue
            if value is None:
                continue
            metadata[key] = value
        return metadata
