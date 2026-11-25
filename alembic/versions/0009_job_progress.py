"""keelflow_jobs progress snapshot

Revision ID: 0009
Revises: 0008
Create Date: 2026-11-25
"""
from alembic import op

revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE keelflow_jobs ADD COLUMN IF NOT EXISTS progress JSONB")
    op.execute("ALTER TABLE keelflow_jobs ADD COLUMN IF NOT EXISTS progress_at TIMESTAMPTZ")
    op.execute("ALTER TABLE keelflow_jobs ADD COLUMN IF NOT EXISTS progress_timeout_seconds DOUBLE PRECISION")


def downgrade() -> None:
    op.execute("ALTER TABLE keelflow_jobs DROP COLUMN IF EXISTS progress_timeout_seconds")
    op.execute("ALTER TABLE keelflow_jobs DROP COLUMN IF EXISTS progress_at")
    op.execute("ALTER TABLE keelflow_jobs DROP COLUMN IF EXISTS progress")
