"""keelflow core schema (jobs, workflow/step runs, chunks, row status, outbox, audit, dlq)

Revision ID: 0001
Revises:
Create Date: 2026-11-18
"""
from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    from keelflow.postgres.schema import schema_sql

    op.execute(schema_sql())


def downgrade() -> None:
    op.execute(
        """
        DROP TABLE IF EXISTS
            keelflow_dlq, keelflow_audit, keelflow_outbox_deliveries,
            keelflow_outbox, keelflow_row_status, keelflow_chunks,
            keelflow_step_runs, keelflow_workflow_runs, keelflow_jobs
        CASCADE
        """
    )
