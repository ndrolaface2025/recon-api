"""create tbl_upload_scheduler_history

Revision ID: dc4a6bdfd6d2
Revises: f0bcc1715f67
Create Date: 2026-02-05 16:30:01.921917

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "dc4a6bdfd6d2"
down_revision = "f0bcc1715f67"
branch_labels = None
depends_on = "6b0f6540afce"  # Depends on upload_api_v2_and_scheduler_v2 which creates tbl_cfg_upload_schedulers


def upgrade() -> None:
    # First, check if tbl_cfg_upload_schedulers exists before creating foreign key
    op.create_table(
        "tbl_upload_scheduler_history",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column(
            "scheduler_id",
            sa.BigInteger(),
            nullable=False,
        ),
        sa.Column("started_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("finished_at", sa.TIMESTAMP(), nullable=True),
        sa.Column(
            "status",
            sa.SmallInteger(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "total_files",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "failed_files",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("file_names", postgresql.JSONB(), nullable=True),
        sa.Column("error_message", sa.String(length=1000), nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    
    # Add foreign key constraint only if parent table exists
    # This allows the migration to run even if tbl_cfg_upload_schedulers doesn't exist yet
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    if "tbl_cfg_upload_schedulers" in inspector.get_table_names():
        op.create_foreign_key(
            "fk_scheduler_history_scheduler_id",
            "tbl_upload_scheduler_history",
            "tbl_cfg_upload_schedulers",
            ["scheduler_id"],
            ["id"],
            ondelete="CASCADE"
        )

    op.create_index(
        "ix_upload_scheduler_history_scheduler_id",
        "tbl_upload_scheduler_history",
        ["scheduler_id"],
    )
    op.create_index(
        "ix_upload_scheduler_history_status",
        "tbl_upload_scheduler_history",
        ["status"],
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_upload_scheduler_history_status")
    op.execute("DROP INDEX IF EXISTS ix_upload_scheduler_history_scheduler_id")
    op.execute("DROP TABLE IF EXISTS tbl_upload_scheduler_history CASCADE")
