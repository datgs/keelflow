"""hd_flow core schema (jobs, workflow/step runs, chunks, row status, outbox, audit, dlq)

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
    from pyerp.postgres.schema import schema_sql

    op.execute(schema_sql())


def downgrade() -> None:
    op.execute(
        """
        DROP TABLE IF EXISTS
            hd_flow_dlq, hd_flow_audit, hd_flow_outbox,
            hd_flow_row_status, hd_flow_chunks,
            hd_flow_step_runs, hd_flow_workflow_runs, hd_flow_jobs
        CASCADE
        """
    )
