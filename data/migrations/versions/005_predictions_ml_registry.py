"""predictions and ml registry

Revision ID: 005_predictions_ml_registry
Revises: 004_incidents
Create Date: 2026-04-30 00:00:00
"""
from __future__ import annotations

from alembic import op


revision = "005_predictions_ml_registry"
down_revision = "004_incidents"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TYPE model_type_enum AS ENUM ('congestion_predictor', 'queue_propagation', 'route_scorer', 'vehicle_detector')
        """
    )
    op.execute(
        """
        CREATE TABLE congestion_predictions (
            id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
            road_segment_id BIGINT NOT NULL REFERENCES road_edges(id),
            predicted_at TIMESTAMPTZ NOT NULL,
            horizon_minutes INT NOT NULL CHECK (horizon_minutes IN (10, 20, 30)),
            congestion_prob DOUBLE PRECISION NOT NULL,
            predicted_speed_kmh DOUBLE PRECISION,
            model_version TEXT NOT NULL,
            confidence DOUBLE PRECISION,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
        """
    )
    op.execute(
        """
        CREATE TABLE ml_models (
            id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
            model_name TEXT NOT NULL,
            model_type model_type_enum NOT NULL,
            version TEXT NOT NULL,
            trained_at TIMESTAMPTZ NOT NULL,
            metrics JSONB NOT NULL,
            artifact_path TEXT NOT NULL,
            is_active BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (model_name, version)
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS ml_models")
    op.execute("DROP TABLE IF EXISTS congestion_predictions")
    op.execute("DROP TYPE IF EXISTS model_type_enum")
