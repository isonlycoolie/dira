from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from dira_schemas.cv import CVDetection
from dira_schemas.enums import DataSourceType, IncidentType, PipelineStage, WeatherCondition
from dira_schemas.events import UnifiedTrafficEvent
from dira_schemas.fleet import FleetGPSPoint
from dira_schemas.incidents import IncidentReport
from dira_schemas.raw import GeoPoint, RawMessage
from dira_schemas.telecom import DSM_BBOX, TelecomPing


def test_enum_serialization() -> None:
    event = UnifiedTrafficEvent(
        road_segment_id=1,
        event_time=datetime.now(UTC),
        pipeline_stage=PipelineStage.SILVER,
        source_type=DataSourceType.FUSED,
        congestion_score=0.42,
        weather_factor=WeatherCondition.RAIN,
    )

    payload = event.model_dump(mode="json")
    assert payload["pipeline_stage"] == "silver"
    assert payload["source_type"] == "fused"
    assert payload["weather_factor"] == "rain"


def test_boundary_validation_and_clamping() -> None:
    telecom_ping = TelecomPing(
        device_id_hash="abc",
        tower_id="tower-1",
        lat=-10.0,
        lon=50.0,
        timestamp=datetime.now(UTC),
    )
    assert telecom_ping.lat == DSM_BBOX[0]
    assert telecom_ping.lon == DSM_BBOX[3]
    assert json.dumps(telecom_ping.model_dump(mode="json"))


def test_negative_speed_and_severity_validation() -> None:
    try:
        FleetGPSPoint(
            vehicle_id_hash="veh-1",
            provider="provider",
            lat=-6.8,
            lon=39.2,
            speed_kmh=-1.0,
            timestamp=datetime.now(UTC),
        )
    except ValueError:
        pass
    else:
        raise AssertionError("negative fleet speed should fail")

    try:
        CVDetection(
            camera_id="cam-1",
            frame_timestamp=datetime.now(UTC),
            vehicle_count=-1,
            lane_occupancy=0.5,
        )
    except ValueError:
        pass
    else:
        raise AssertionError("negative vehicle_count should fail")

    for severity in (0, 6):
        try:
            IncidentReport(
                id=uuid4(),
                incident_type=IncidentType.ACCIDENT,
                lat=-6.8,
                lon=39.2,
                reported_at=datetime.now(UTC),
                source="web",
                severity=severity,
            )
        except ValueError:
            pass
        else:
            raise AssertionError("severity bounds should fail")


def test_to_kafka_dict_round_trip() -> None:
    event = UnifiedTrafficEvent(
        road_segment_id=7,
        event_time=datetime.now(UTC),
        pipeline_stage=PipelineStage.GOLD,
        source_type=DataSourceType.TELECOM,
        vehicle_count=11,
        avg_speed_kmh=18.5,
        flow_rate=12.0,
        density=2.5,
        congestion_score=0.91,
        raw_attributes={"batch": 3},
    )

    payload = event.to_kafka_dict()
    assert payload["congestion_level"] == "severe"
    assert payload["pipeline_stage"] == "gold"
    assert payload["source_type"] == "telecom"
    round_trip = UnifiedTrafficEvent.model_validate(payload)
    assert round_trip.congestion_score == event.congestion_score
    assert round_trip.raw_attributes == {"batch": 3}


def test_utc_enforcement() -> None:
    message = RawMessage(
        source=DataSourceType.WEATHER,
        timestamp=datetime(2026, 4, 30, 8, 15, 0),
        geo=GeoPoint(lat=-6.8, lon=39.2),
    )

    assert message.timestamp.tzinfo == UTC
