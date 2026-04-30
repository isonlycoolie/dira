from __future__ import annotations

from typing import Any


def _load_h3() -> Any:
    try:
        import h3 as h3_module
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised when dependency is absent
        raise RuntimeError("h3 is required for H3 utilities") from exc
    return h3_module


def _get_h3_function(h3_module: Any, *names: str) -> Any:
    for name in names:
        function = getattr(h3_module, name, None)
        if callable(function):
            return function
    raise RuntimeError(f"h3 package does not provide any of: {', '.join(names)}")


def _ensure_valid_cell(h3_module: Any, h3_index: str) -> str:
    index = str(h3_index)
    validator = getattr(h3_module, "is_valid_cell", None)
    if not callable(validator):
        validator = getattr(h3_module, "h3_is_valid", None)
    if callable(validator) and not validator(index):
        raise ValueError(f"invalid h3 index: {h3_index}")
    return index


def point_to_h3(lat: float, lon: float, resolution: int = 9) -> str:
    if resolution < 0:
        raise ValueError("resolution must be non-negative")

    h3_module = _load_h3()
    to_cell = _get_h3_function(h3_module, "latlng_to_cell", "geo_to_h3")
    return str(to_cell(lat, lon, resolution))


def h3_to_bbox(h3_index: str) -> tuple[float, float, float, float]:
    h3_module = _load_h3()
    index = _ensure_valid_cell(h3_module, h3_index)
    cell_to_boundary = _get_h3_function(h3_module, "cell_to_boundary", "h3_to_geo_boundary")
    boundary = list(cell_to_boundary(index))
    if not boundary:
        raise ValueError(f"unable to derive bbox from h3 index: {h3_index}")

    latitudes = [float(lat) for lat, _ in boundary]
    longitudes = [float(lon) for _, lon in boundary]
    return (min(latitudes), min(longitudes), max(latitudes), max(longitudes))


def get_neighboring_cells(h3_index: str, k: int = 1) -> list[str]:
    if k < 0:
        raise ValueError("k must be non-negative")

    h3_module = _load_h3()
    index = _ensure_valid_cell(h3_module, h3_index)
    grid_disk = _get_h3_function(h3_module, "grid_disk", "k_ring")
    neighbors = {str(cell) for cell in grid_disk(index, k) if str(cell) != index}
    return sorted(neighbors)


__all__ = ["point_to_h3", "h3_to_bbox", "get_neighboring_cells"]
