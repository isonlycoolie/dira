from __future__ import annotations

from typing import TypedDict

from dira_schemas.enums import DataSourceType, PipelineStage


class ConsumerGroupConfig(TypedDict):
    group_id: str
    auto_offset_reset: str
    max_poll_records: dict[DataSourceType, int]


CONSUMER_GROUPS: dict[PipelineStage, ConsumerGroupConfig] = {
    PipelineStage.BRONZE: {
        "group_id": "dira-bronze-consumer",
        "auto_offset_reset": "earliest",
        "max_poll_records": {
            DataSourceType.TELECOM: 500,
            DataSourceType.CCTV: 250,
            DataSourceType.FLEET_GPS: 250,
            DataSourceType.INCIDENT: 100,
            DataSourceType.WEATHER: 100,
            DataSourceType.FUSED: 200,
        },
    },
    PipelineStage.SILVER: {
        "group_id": "dira-silver-consumer",
        "auto_offset_reset": "latest",
        "max_poll_records": {
            DataSourceType.TELECOM: 250,
            DataSourceType.CCTV: 150,
            DataSourceType.FLEET_GPS: 150,
            DataSourceType.INCIDENT: 50,
            DataSourceType.WEATHER: 50,
            DataSourceType.FUSED: 150,
        },
    },
    PipelineStage.GOLD: {
        "group_id": "dira-gold-consumer",
        "auto_offset_reset": "latest",
        "max_poll_records": {
            DataSourceType.TELECOM: 100,
            DataSourceType.CCTV: 100,
            DataSourceType.FLEET_GPS: 100,
            DataSourceType.INCIDENT: 25,
            DataSourceType.WEATHER: 25,
            DataSourceType.FUSED: 100,
        },
    },
}
