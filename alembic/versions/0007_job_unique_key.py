"""keelflow_jobs.unique_key — idempotent enqueue dedup

Revision ID: 0007
Revises: 0006
Create Date: 2026-11-20
"""
from alembic import op

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE keelflow_jobs
            ADD COLUMN IF NOT EXISTS unique_key TEXT
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_jobs_unique_key
            ON keelflow_jobs (type, unique_key)
            WHERE unique_key IS NOT NULL
              AND status IN ('pending','claimed','running','blocked')
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ux_jobs_unique_key")
    op.execute("ALTER TABLE keelflow_jobs DROP COLUMN IF EXISTS unique_key")
