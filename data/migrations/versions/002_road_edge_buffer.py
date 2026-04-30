"""road edge buffer and corridors

Revision ID: 002_road_edge_buffer
Revises: 001_road_network
Create Date: 2026-04-30 00:00:00
"""
from __future__ import annotations

from alembic import op


revision = "002_road_edge_buffer"
down_revision = "001_road_network"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE road_edges ADD COLUMN buffer_geom GEOMETRY(POLYGON, 4326)")
    op.execute("ALTER TABLE road_edges ADD COLUMN h3_index TEXT")
    op.execute(
        """
        CREATE TABLE road_corridors (
            id BIGSERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            edge_ids BIGINT[] NOT NULL,
            geom GEOMETRY(MULTILINESTRING, 4326)
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS road_corridors")
    op.execute("ALTER TABLE road_edges DROP COLUMN IF EXISTS h3_index")
    op.execute("ALTER TABLE road_edges DROP COLUMN IF EXISTS buffer_geom")
