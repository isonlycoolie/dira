from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
for package_path in (
    PROJECT_ROOT / "apps" / "ingestion" / "src",
    PROJECT_ROOT / "libs" / "schemas" / "src",
):
    package_path_str = str(package_path)
    if package_path_str not in sys.path:
        sys.path.insert(0, package_path_str)

import simulators.fleet_sim as fleet_sim_module
from simulators.fleet_sim import FleetGPSSimulator


class _FakeRandom:
    def __init__(self, sample_result: list[int], uniform_values: list[float]) -> None:
        self._sample_result = sample_result
        self._uniform_values = iter(uniform_values)
        self.sample_calls: list[tuple[tuple[int, ...], int]] = []
        self.uniform_calls: list[tuple[float, float]] = []

    def sample(self, population, k):
        self.sample_calls.append((tuple(population), k))
        return list(self._sample_result)

    def uniform(self, lower: float, upper: float) -> float:
        self.uniform_calls.append((lower, upper))
        return next(self._uniform_values)


class _FakeOsmnx:
    def __init__(self) -> None:
        self.shortest_path_calls: list[tuple[object, object, object, str]] = []

    def shortest_path(self, graph, origin, destination, weight="length"):
        self.shortest_path_calls.append((graph, origin, destination, weight))
        return [1, 2, 3]


class _FakeGraph:
    nodes = {
        1: {"y": -6.8000, "x": 39.2000},
        2: {"y": -6.8005, "x": 39.2010},
        3: {"y": -6.8010, "x": 39.2020},
    }

    def get_edge_data(self, start_node, end_node):
        if (start_node, end_node) == (1, 2):
            return {"length": 100.0}
        if (start_node, end_node) == (2, 3):
            return {"length": 120.0}
        return {"length": 140.0}


def test_fleet_simulator_generates_shortest_path_trajectories(monkeypatch) -> None:
    fake_osmnx = _FakeOsmnx()
    graph = _FakeGraph()
    fake_rng = _FakeRandom(
        sample_result=[1, 3],
        uniform_values=[45.0, -1.0, 3.0, -2.0, 50.0, -2.0, 1.0, -1.0],
    )
    simulator = FleetGPSSimulator(
        graph=graph,
        rng=fake_rng,
        points_per_trajectory=4,
        provider="fleet-sim",
    )
    monkeypatch.setattr(fleet_sim_module, "_load_osmnx", lambda: fake_osmnx)

    trajectories = simulator.generate_trajectories(2, start_timestamp=datetime(2026, 4, 30, 8, 0, tzinfo=UTC))

    assert len(trajectories) == 2
    assert len(trajectories[0]) == 4
    assert len(trajectories[1]) == 4
    assert fake_osmnx.shortest_path_calls == [
        (graph, 1, 3, "length"),
        (graph, 1, 3, "length"),
    ]
    assert trajectories[0][0]["vehicle_id"] == "vehicle-0001"
    assert trajectories[1][0]["vehicle_id"] == "vehicle-0002"
    assert trajectories[0][0]["provider"] == "fleet-sim"
    assert trajectories[0][0]["lat"] == -6.8
    assert trajectories[0][-1]["lat"] == -6.801
    assert trajectories[0][0]["lon"] == 39.2
    assert trajectories[0][-1]["lon"] == 39.202
    assert all(30.0 <= point["speed_kmh"] <= 60.0 for trajectory in trajectories for point in trajectory)
    assert all(
        datetime.fromisoformat(point["timestamp"]) >= datetime(2026, 4, 30, 8, 0, tzinfo=UTC)
        for trajectory in trajectories
        for point in trajectory
    )


def test_fleet_simulator_flattens_batch_output(monkeypatch) -> None:
    fake_osmnx = _FakeOsmnx()
    fake_rng = _FakeRandom(sample_result=[1, 3], uniform_values=[42.0, 0.0, 0.0, 0.0])
    simulator = FleetGPSSimulator(graph=_FakeGraph(), rng=fake_rng, points_per_trajectory=4)
    monkeypatch.setattr(fleet_sim_module, "_load_osmnx", lambda: fake_osmnx)

    batch = simulator.generate_batch(1, start_timestamp=datetime(2026, 4, 30, 8, 0, tzinfo=UTC))

    assert len(batch) == 4
    assert [point["vehicle_id"] for point in batch] == ["vehicle-0001"] * 4
    assert fake_osmnx.shortest_path_calls[0][1:] == (1, 3, "length")
