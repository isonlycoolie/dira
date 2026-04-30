"""spatial and traffic indexes

Revision ID: 007_indexes
Revises: 006_api_keys
Create Date: 2026-04-30 00:00:00
"""
from __future__ import annotations

from alembic import op


revision = "007_indexes"
down_revision = "006_api_keys"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE INDEX idx_road_edges_geom ON road_edges USING GIST(geom)")
    op.execute("CREATE INDEX idx_road_edges_buffer_geom ON road_edges USING GIST(buffer_geom)")
    op.execute("CREATE INDEX idx_incidents_location ON incidents USING GIST(location)")
    op.execute(
        "CREATE INDEX idx_traffic_events_segment_time ON traffic_events (road_segment_id, event_time DESC)"
    )
    op.execute(
        "CREATE INDEX idx_traffic_events_congestion ON traffic_events (congestion_score) WHERE congestion_score > 0.7"
    )
    op.execute(
        "CREATE INDEX idx_predictions_segment_time ON congestion_predictions (road_segment_id, predicted_at DESC)"
    )
    op.execute("CREATE INDEX idx_road_edges_h3 ON road_edges (h3_index)")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_road_edges_h3")
    op.execute("DROP INDEX IF EXISTS idx_predictions_segment_time")
    op.execute("DROP INDEX IF EXISTS idx_traffic_events_congestion")
    op.execute("DROP INDEX IF EXISTS idx_traffic_events_segment_time")
    op.execute("DROP INDEX IF EXISTS idx_incidents_location")
    op.execute("DROP INDEX IF EXISTS idx_road_edges_buffer_geom")
    op.execute("DROP INDEX IF EXISTS idx_road_edges_geom")
