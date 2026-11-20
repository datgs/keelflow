"""keelflow_row_archive — cold store for completed bulk rows

Revision ID: 0004
Revises: 0003
Create Date: 2026-11-20
"""
from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS keelflow_row_archive (
            id          BIGSERIAL PRIMARY KEY,
            job_id      UUID NOT NULL,
            row_id      BIGINT NOT NULL,
            row_key     TEXT NOT NULL,
            chunk_id    UUID,
            data        JSONB,
            archived_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (job_id, row_id)
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_row_archive_job
            ON keelflow_row_archive (job_id, id)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_row_archive_key
            ON keelflow_row_archive (job_id, row_key)
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS keelflow_row_archive")
