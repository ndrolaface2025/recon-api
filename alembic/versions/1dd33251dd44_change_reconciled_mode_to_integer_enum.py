"""change reconciled_mode to integer enum

Revision ID: 1dd33251dd44
Revises: 4fd83833281c
Create Date: 2026-01-23 15:38:54.223757
"""

from alembic import op
import sqlalchemy as sa


revision = "1dd33251dd44"
down_revision = "4fd83833281c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """
    reconciled_mode semantics:
      1 = AUTOMATIC
      2 = MANUAL
    """

    op.execute(
        """
        ALTER TABLE tbl_txn_transactions
        ALTER COLUMN reconciled_mode
        TYPE INTEGER
        USING (
            CASE
                WHEN reconciled_mode = true THEN 1
                WHEN reconciled_mode = false THEN 2
                ELSE NULL
            END
        )
        """
    )

    op.create_check_constraint(
        "ck_txn_reconciled_mode_valid",
        "tbl_txn_transactions",
        "reconciled_mode IN (1, 2)",
    )


def downgrade() -> None:
    """
    Downgrade back to BOOLEAN
      1 -> true
      2 -> false
    """

    op.drop_constraint(
        "ck_txn_reconciled_mode_valid",
        "tbl_txn_transactions",
        type_="check",
    )

    op.execute(
        """
        ALTER TABLE tbl_txn_transactions
        ALTER COLUMN reconciled_mode
        TYPE BOOLEAN
        USING (
            CASE
                WHEN reconciled_mode = 1 THEN true
                WHEN reconciled_mode = 2 THEN false
                ELSE NULL
            END
        )
        """
    )
