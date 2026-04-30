"""api keys table

Revision ID: 006_api_keys
Revises: 005_predictions_ml_registry
Create Date: 2026-04-30 00:00:00
"""
from __future__ import annotations

from alembic import op


revision = "006_api_keys"
down_revision = "005_predictions_ml_registry"
branch_labels = None
depends_on = None

DEV_API_KEY_HASH = "ef7abf0aab9a10c171d92e8f72d7227d299818aa39a4f9a626a4e1c7834c4ffa"


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE api_keys (
            id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
            key_hash TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            permissions JSONB DEFAULT '{"read": true}'::jsonb,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            expires_at TIMESTAMPTZ,
            is_active BOOLEAN DEFAULT TRUE
        )
        """
    )
    op.execute(
        f"""
        INSERT INTO api_keys (key_hash, name, permissions, created_at, expires_at, is_active)
        VALUES ('{DEV_API_KEY_HASH}', 'dev', '{{"read": true}}'::jsonb, NOW(), NULL, TRUE)
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS api_keys")
