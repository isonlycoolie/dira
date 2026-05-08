from __future__ import annotations

import math
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
for package_path in (
    PROJECT_ROOT / "apps" / "pipeline" / "src",
    PROJECT_ROOT / "libs" / "schemas" / "src",
):
    package_path_str = str(package_path)
    if package_path_str not in sys.path:
        sys.path.insert(0, package_path_str)

from dira_schemas.enums import DataSourceType, PipelineStage, WeatherCondition
from dira_schemas.events import UnifiedTrafficEvent
from transforms.feature_engineering import CongestionIndexCalculator, DwellTimeDetector, WeatherImpactFactor


@pytest.mark.parametrize(
    ("avg_speed_kmh", "free_flow_speed_kmh", "vehicle_count", "capacity", "expected_score"),
    [
        (0.0, 50.0, 10, 10, 1.0),
        (50.0, 50.0, 0, 10, 0.0),
    ],
)
def test_congestion_index_calculator_known_inputs(
    avg_speed_kmh: float,
    free_flow_speed_kmh: float,
    vehicle_count: int,
    capacity: int,
    expected_score: float,
) -> None:
    calculator = CongestionIndexCalculator()

    score = calculator.calculate(avg_speed_kmh, free_flow_speed_kmh, vehicle_count, capacity)

    assert math.isclose(score, expected_score, rel_tol=1e-9, abs_tol=1e-9)


@pytest.mark.parametrize(
    ("base_score", "weather", "expected_score"),
    [
        (0.5, WeatherCondition.CLEAR, 0.5),
        (0.5, WeatherCondition.RAIN, 0.55),
        (0.5, WeatherCondition.HEAVY_RAIN, 0.6),
        (0.5, WeatherCondition.FOG, 0.575),
        (0.95, WeatherCondition.HEAVY_RAIN, 1.0),
    ],
)
def test_weather_impact_factor_adjusts_and_caps_score(
    base_score: float,
    weather: WeatherCondition,
    expected_score: float,
) -> None:
    factor = WeatherImpactFactor()

    adjusted_score = factor.adjust_congestion_score(base_score, weather)

    assert math.isclose(adjusted_score, expected_score, rel_tol=1e-9, abs_tol=1e-9)


def test_dwell_time_detector_marks_three_consecutive_slow_windows_as_anchor() -> None:
    detector = DwellTimeDetector()
    segment_id = 101
    base_time = datetime(2026, 5, 8, 12, 0, tzinfo=UTC)
    recent_windows = [
        UnifiedTrafficEvent(
            road_segment_id=segment_id,
            event_time=base_time,
            pipeline_stage=PipelineStage.SILVER,
            source_type=DataSourceType.FUSED,
            avg_speed_kmh=10.0,
            congestion_score=0.2,
        ),
        UnifiedTrafficEvent(
            road_segment_id=segment_id,
            event_time=base_time + timedelta(seconds=30),
            pipeline_stage=PipelineStage.SILVER,
            source_type=DataSourceType.FUSED,
            avg_speed_kmh=4.5,
            congestion_score=0.8,
        ),
        UnifiedTrafficEvent(
            road_segment_id=segment_id,
            event_time=base_time + timedelta(seconds=60),
            pipeline_stage=PipelineStage.SILVER,
            source_type=DataSourceType.FUSED,
            avg_speed_kmh=4.2,
            congestion_score=0.82,
        ),
        UnifiedTrafficEvent(
            road_segment_id=segment_id,
            event_time=base_time + timedelta(seconds=90),
            pipeline_stage=PipelineStage.SILVER,
            source_type=DataSourceType.FUSED,
            avg_speed_kmh=4.8,
            congestion_score=0.85,
        ),
    ]

    is_anchor = detector.is_congestion_anchor(segment_id, recent_windows)

    assert is_anchor is True
    assert recent_windows[0].incident_flag is False
    assert all(window.incident_flag is True for window in recent_windows[1:])


def test_dwell_time_detector_requires_three_consecutive_slow_windows() -> None:
    detector = DwellTimeDetector()
    segment_id = 202
    base_time = datetime(2026, 5, 8, 13, 0, tzinfo=UTC)
    recent_windows = [
        UnifiedTrafficEvent(
            road_segment_id=segment_id,
            event_time=base_time,
            pipeline_stage=PipelineStage.SILVER,
            source_type=DataSourceType.FUSED,
            avg_speed_kmh=4.9,
            congestion_score=0.81,
        ),
        UnifiedTrafficEvent(
            road_segment_id=segment_id,
            event_time=base_time + timedelta(seconds=30),
            pipeline_stage=PipelineStage.SILVER,
            source_type=DataSourceType.FUSED,
            avg_speed_kmh=4.3,
            congestion_score=0.84,
        ),
    ]

    is_anchor = detector.is_congestion_anchor(segment_id, recent_windows)

    assert is_anchor is False
    assert all(window.incident_flag is False for window in recent_windows)
