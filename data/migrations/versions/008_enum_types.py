"""postgres enum types

Revision ID: 008_enum_types
Revises: 007_indexes
Create Date: 2026-04-30 00:00:00
"""
from __future__ import annotations

from alembic import op


revision = "008_enum_types"
down_revision = "007_indexes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'road_type_enum') THEN
                CREATE TYPE road_type_enum AS ENUM ('primary', 'secondary', 'tertiary', 'residential');
            END IF;
        END
        $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'source_type_enum') THEN
                CREATE TYPE source_type_enum AS ENUM ('telecom', 'cctv', 'fleet_gps', 'incident', 'weather', 'fused');
            END IF;
        END
        $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'pipeline_stage_enum') THEN
                CREATE TYPE pipeline_stage_enum AS ENUM ('bronze', 'silver', 'gold');
            END IF;
        END
        $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'incident_type_enum') THEN
                CREATE TYPE incident_type_enum AS ENUM ('accident', 'roadblock', 'construction', 'flooding', 'other');
            END IF;
        END
        $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'congestion_level_enum') THEN
                CREATE TYPE congestion_level_enum AS ENUM ('free_flow', 'light', 'moderate', 'heavy', 'severe');
            END IF;
        END
        $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'weather_condition_enum') THEN
                CREATE TYPE weather_condition_enum AS ENUM ('clear', 'rain', 'heavy_rain', 'fog');
            END IF;
        END
        $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'model_type_enum') THEN
                CREATE TYPE model_type_enum AS ENUM ('congestion_predictor', 'queue_propagation', 'route_scorer', 'vehicle_detector');
            END IF;
        END
        $$;
        """
    )


def downgrade() -> None:
    op.execute("DROP TYPE IF EXISTS model_type_enum")
    op.execute("DROP TYPE IF EXISTS weather_condition_enum")
    op.execute("DROP TYPE IF EXISTS congestion_level_enum")
    op.execute("DROP TYPE IF EXISTS incident_type_enum")
    op.execute("DROP TYPE IF EXISTS pipeline_stage_enum")
    op.execute("DROP TYPE IF EXISTS source_type_enum")
    op.execute("DROP TYPE IF EXISTS road_type_enum")
