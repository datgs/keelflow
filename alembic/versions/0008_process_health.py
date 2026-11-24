"""keelflow_process_health — worker/scheduler heartbeat rows

Revision ID: 0008
Revises: 0007
Create Date: 2026-11-24
"""
from alembic import op

revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS keelflow_process_health (
            name       TEXT PRIMARY KEY,
            kind       TEXT NOT NULL CHECK (kind IN ('worker','scheduler')),
            last_seen  TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS keelflow_process_health")
