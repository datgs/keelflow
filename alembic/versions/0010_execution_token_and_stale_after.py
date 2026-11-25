"""keelflow_jobs.execution_token + keelflow_process_health.stale_after_s

Revision ID: 0010
Revises: 0009
Create Date: 2026-11-25
"""
from alembic import op

revision = "0010"
down_revision = "0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE keelflow_jobs"
        " ADD COLUMN IF NOT EXISTS execution_token TEXT"
    )
    op.execute(
        "ALTER TABLE keelflow_process_health"
        " ADD COLUMN IF NOT EXISTS stale_after_s DOUBLE PRECISION"
        " NOT NULL DEFAULT 30"
    )


def downgrade() -> None:
    op.execute(
        "ALTER TABLE keelflow_process_health"
        " DROP COLUMN IF EXISTS stale_after_s"
    )
    op.execute(
        "ALTER TABLE keelflow_jobs"
        " DROP COLUMN IF EXISTS execution_token"
    )
