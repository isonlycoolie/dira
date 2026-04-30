"""partitioned traffic events

Revision ID: 003_traffic_events
Revises: 002_road_edge_buffer
Create Date: 2026-04-30 00:00:00
"""
from __future__ import annotations

from alembic import op


revision = "003_traffic_events"
down_revision = "002_road_edge_buffer"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
    op.execute(
        """
        CREATE TYPE source_type_enum AS ENUM ('telecom', 'cctv', 'fleet_gps', 'incident', 'weather', 'fused')
        """
    )
    op.execute(
        """
        CREATE TYPE pipeline_stage_enum AS ENUM ('bronze', 'silver', 'gold')
        """
    )
    op.execute(
        """
        CREATE TYPE congestion_level_enum AS ENUM ('free_flow', 'light', 'moderate', 'heavy', 'severe')
        """
    )
    op.execute(
        """
        CREATE TYPE weather_condition_enum AS ENUM ('clear', 'rain', 'heavy_rain', 'fog')
        """
    )
    op.execute(
        """
        CREATE TABLE traffic_events (
            id UUID DEFAULT gen_random_uuid(),
            road_segment_id BIGINT NOT NULL REFERENCES road_edges(id),
            source_type source_type_enum NOT NULL,
            pipeline_stage pipeline_stage_enum NOT NULL,
            event_time TIMESTAMPTZ NOT NULL,
            vehicle_count INT,
            avg_speed_kmh DOUBLE PRECISION,
            flow_rate DOUBLE PRECISION,
            density DOUBLE PRECISION,
            congestion_score DOUBLE PRECISION,
            congestion_level congestion_level_enum,
            incident_flag BOOLEAN DEFAULT FALSE,
            weather_factor weather_condition_enum,
            raw_attributes JSONB DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (id, event_time)
        ) PARTITION BY RANGE (event_time)
        """
    )
    op.execute(
        """
        CREATE TABLE traffic_events_default PARTITION OF traffic_events DEFAULT
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS traffic_events_default")
    op.execute("DROP TABLE IF EXISTS traffic_events")
    op.execute("DROP TYPE IF EXISTS weather_condition_enum")
    op.execute("DROP TYPE IF EXISTS congestion_level_enum")
    op.execute("DROP TYPE IF EXISTS pipeline_stage_enum")
    op.execute("DROP TYPE IF EXISTS source_type_enum")
