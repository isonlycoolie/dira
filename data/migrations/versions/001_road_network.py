"""road network tables

Revision ID: 001_road_network
Revises: 
Create Date: 2026-04-30 00:00:00
"""
from __future__ import annotations

from alembic import op


revision = "001_road_network"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TYPE road_type_enum AS ENUM ('primary', 'secondary', 'tertiary', 'residential')
        """
    )
    op.execute(
        """
        CREATE TABLE road_nodes (
            id BIGSERIAL PRIMARY KEY,
            osm_id BIGINT UNIQUE NOT NULL,
            geom GEOMETRY(POINT, 4326) NOT NULL,
            metadata JSONB DEFAULT '{}'::jsonb
        )
        """
    )
    op.execute(
        """
        CREATE TABLE road_edges (
            id BIGSERIAL PRIMARY KEY,
            osm_id BIGINT UNIQUE NOT NULL,
            from_node_id BIGINT REFERENCES road_nodes(id),
            to_node_id BIGINT REFERENCES road_nodes(id),
            name TEXT,
            road_type road_type_enum NOT NULL,
            length_m DOUBLE PRECISION NOT NULL,
            speed_limit_kmh INTEGER DEFAULT 50,
            geom GEOMETRY(LINESTRING, 4326) NOT NULL,
            metadata JSONB DEFAULT '{}'::jsonb
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS road_edges")
    op.execute("DROP TABLE IF EXISTS road_nodes")
    op.execute("DROP TYPE IF EXISTS road_type_enum")
