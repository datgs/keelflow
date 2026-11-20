"""result/outputs size columns — queryable size-visibility

Revision ID: 0006
Revises: 0005
Create Date: 2026-11-20
"""
from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE keelflow_jobs
            ADD COLUMN IF NOT EXISTS result_bytes INT
        """
    )
    op.execute(
        """
        ALTER TABLE keelflow_step_runs
            ADD COLUMN IF NOT EXISTS outputs_bytes INT
        """
    )


def downgrade() -> None:
    op.execute("ALTER TABLE keelflow_jobs DROP COLUMN IF EXISTS result_bytes")
    op.execute(
        "ALTER TABLE keelflow_step_runs DROP COLUMN IF EXISTS outputs_bytes"
    )
