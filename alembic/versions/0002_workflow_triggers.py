"""workflow trigger columns on keelflow_workflow_runs

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-19
"""
from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE keelflow_workflow_runs
            ADD COLUMN IF NOT EXISTS triggered_by_entity_type TEXT,
            ADD COLUMN IF NOT EXISTS triggered_by_entity_id   TEXT,
            ADD COLUMN IF NOT EXISTS trigger_config           JSONB,
            ADD COLUMN IF NOT EXISTS trigger_ctx              JSONB,
            ADD COLUMN IF NOT EXISTS terminal_outcome         TEXT
                CHECK (terminal_outcome IN ('succeeded','rejected','failed')),
            ADD COLUMN IF NOT EXISTS callback_state           TEXT
                NOT NULL DEFAULT 'none'
                CHECK (callback_state IN ('none','pending','done','failed'))
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE keelflow_workflow_runs
            DROP COLUMN IF EXISTS callback_state,
            DROP COLUMN IF EXISTS terminal_outcome,
            DROP COLUMN IF EXISTS trigger_ctx,
            DROP COLUMN IF EXISTS trigger_config,
            DROP COLUMN IF EXISTS triggered_by_entity_id,
            DROP COLUMN IF EXISTS triggered_by_entity_type
        """
    )
