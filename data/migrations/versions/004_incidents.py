"""incidents table

Revision ID: 004_incidents
Revises: 003_traffic_events
Create Date: 2026-04-30 00:00:00
"""
from __future__ import annotations

from alembic import op


revision = "004_incidents"
down_revision = "003_traffic_events"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TYPE incident_type_enum AS ENUM ('accident', 'roadblock', 'construction', 'flooding', 'other')
        """
    )
    op.execute(
        """
        CREATE TABLE incidents (
            id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
            road_segment_id BIGINT REFERENCES road_edges(id),
            incident_type incident_type_enum NOT NULL,
            severity SMALLINT CHECK (severity BETWEEN 1 AND 5),
            location GEOMETRY(POINT, 4326) NOT NULL,
            reported_at TIMESTAMPTZ NOT NULL,
            resolved_at TIMESTAMPTZ,
            source TEXT NOT NULL,
            description TEXT,
            metadata JSONB DEFAULT '{}'::jsonb
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS incidents")
    op.execute("DROP TYPE IF EXISTS incident_type_enum")
