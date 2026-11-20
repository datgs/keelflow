"""keelflow_dlq.run_id + step_run_id — workflow link for run-resuming replay

Revision ID: 0005
Revises: 0004
Create Date: 2026-11-20
"""
from alembic import op

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE keelflow_dlq
            ADD COLUMN IF NOT EXISTS run_id UUID,
            ADD COLUMN IF NOT EXISTS step_run_id UUID
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE keelflow_dlq
            DROP COLUMN IF EXISTS run_id,
            DROP COLUMN IF EXISTS step_run_id
        """
    )
