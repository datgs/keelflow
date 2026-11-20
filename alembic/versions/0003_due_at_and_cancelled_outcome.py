"""keelflow_jobs.due_at + 'cancelled' terminal outcome

Revision ID: 0003
Revises: 0002
Create Date: 2026-11-20
"""
from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE keelflow_jobs
            ADD COLUMN IF NOT EXISTS due_at TIMESTAMPTZ
        """
    )
    op.execute(
        """
        ALTER TABLE keelflow_workflow_runs
            DROP CONSTRAINT IF EXISTS keelflow_workflow_runs_terminal_outcome_check
        """
    )
    op.execute(
        """
        ALTER TABLE keelflow_workflow_runs
            ADD CONSTRAINT keelflow_workflow_runs_terminal_outcome_check
            CHECK (terminal_outcome IN
                   ('succeeded','rejected','failed','cancelled'))
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE keelflow_workflow_runs
            DROP CONSTRAINT IF EXISTS keelflow_workflow_runs_terminal_outcome_check
        """
    )
    op.execute(
        """
        ALTER TABLE keelflow_workflow_runs
            ADD CONSTRAINT keelflow_workflow_runs_terminal_outcome_check
            CHECK (terminal_outcome IN ('succeeded','rejected','failed'))
        """
    )
    op.execute("ALTER TABLE keelflow_jobs DROP COLUMN IF EXISTS due_at")
