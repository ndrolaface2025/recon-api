"""upload api v2 and scheduler v2

Revision ID: 6b0f6540afce
Revises: 4177d0f7bbce
Create Date: 2026-01-14 01:40:23.495188

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "6b0f6540afce"
down_revision = "4177d0f7bbce"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ---------------------------------------------------------
    # 1. UPLOAD API CONFIG (tbl_cfg_upload_api)
    # ---------------------------------------------------------
    with op.batch_alter_table("tbl_cfg_upload_api") as batch_op:
        batch_op.alter_column("responce_formate", new_column_name="response_format")

        batch_op.alter_column(
            "api_time_out",
            existing_type=sa.String(length=50),
            type_=sa.Integer(),
            postgresql_using="api_time_out::integer",
        )

        batch_op.alter_column(
            "max_try",
            existing_type=sa.String(length=50),
            type_=sa.Integer(),
            postgresql_using="max_try::integer",
        )

        batch_op.add_column(
            sa.Column("is_active", sa.Integer(), server_default="1", nullable=False)
        )

    # ---------------------------------------------------------
    # 2. DROP OLD SCHEDULER TABLE (v1)
    # ---------------------------------------------------------
    op.drop_table("tbl_cfg_upload_schedulars")

    # ---------------------------------------------------------
    # 3. CREATE NEW SCHEDULER TABLE (v2)
    # ---------------------------------------------------------
    op.create_table(
        "tbl_cfg_upload_schedulers",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column(
            "upload_api_id",
            sa.BigInteger(),
            sa.ForeignKey("tbl_cfg_upload_api.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("scheduler_name", sa.String(length=255), nullable=False),
        sa.Column("cron_expression", sa.String(length=100), nullable=False),
        sa.Column("timezone", sa.String(length=50), server_default="UTC"),
        sa.Column("is_active", sa.Integer(), server_default="1", nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "created_by",
            sa.BigInteger(),
            sa.ForeignKey("tbl_cfg_users.id"),
            nullable=True,
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(),
            server_default=sa.func.now(),
            onupdate=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_by",
            sa.BigInteger(),
            sa.ForeignKey("tbl_cfg_users.id"),
            nullable=True,
        ),
        sa.Column("version_number", sa.Integer(), server_default="1"),
    )


def downgrade() -> None:
    # ---------------------------------------------------------
    # 1. DROP NEW SCHEDULER TABLE
    # ---------------------------------------------------------
    op.drop_table("tbl_cfg_upload_schedulers")

    # ---------------------------------------------------------
    # 2. RE-CREATE OLD SCHEDULER TABLE (v1)
    # ---------------------------------------------------------
    op.create_table(
        "tbl_cfg_upload_schedulars",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("channel_id", sa.BigInteger(), nullable=True),
        sa.Column("schedular_name", sa.String(length=255)),
        sa.Column("schedular_time", sa.String(length=50)),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(),
            server_default=sa.func.now(),
        ),
        sa.Column("created_by", sa.BigInteger()),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(),
            server_default=sa.func.now(),
            onupdate=sa.func.now(),
        ),
        sa.Column("updated_by", sa.BigInteger()),
        sa.Column("version_number", sa.Integer()),
    )

    # ---------------------------------------------------------
    # 3. REVERT UPLOAD API TABLE
    # ---------------------------------------------------------
    with op.batch_alter_table("tbl_cfg_upload_api") as batch_op:
        batch_op.drop_column("is_active")

        batch_op.alter_column("response_format", new_column_name="responce_formate")

        batch_op.alter_column(
            "api_time_out",
            existing_type=sa.Integer(),
            type_=sa.String(length=50),
        )

        batch_op.alter_column(
            "max_try",
            existing_type=sa.Integer(),
            type_=sa.String(length=50),
        )
