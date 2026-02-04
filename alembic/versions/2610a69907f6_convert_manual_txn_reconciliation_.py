"""convert manual_txn reconciliation_status to smallint

Revision ID: 2610a69907f6
Revises: 86795926dc84
Create Date: 2026-02-01 20:44:05.453250

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "2610a69907f6"
down_revision = "86795926dc84"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Rename column
    op.alter_column(
        "tbl_txn_manual",
        "reconciled_status",
        new_column_name="reconciliation_status",
    )

    # 2. Change type: BOOLEAN -> SMALLINT with data migration
    op.alter_column(
        "tbl_txn_manual",
        "reconciliation_status",
        existing_type=sa.Boolean(),
        type_=sa.SmallInteger(),
        postgresql_using="""
            CASE
                WHEN reconciliation_status = TRUE THEN 2
                ELSE 0
            END
        """,
        nullable=False,
        server_default="0",
    )


def downgrade() -> None:
    # 1. Convert SMALLINT -> BOOLEAN
    op.alter_column(
        "tbl_txn_manual",
        "reconciliation_status",
        existing_type=sa.SmallInteger(),
        type_=sa.Boolean(),
        postgresql_using="reconciliation_status = 2",
        nullable=True,
    )

    # 2. Rename column back
    op.alter_column(
        "tbl_txn_manual",
        "reconciliation_status",
        new_column_name="reconciled_status",
    )
