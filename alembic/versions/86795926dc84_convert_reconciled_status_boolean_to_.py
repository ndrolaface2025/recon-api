"""convert reconciled_status boolean to reconciliation_status smallint

Revision ID: 86795926dc84
Revises: 5a37d6485b82
Create Date: 2026-01-31 15:31:27.407674

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "86795926dc84"
down_revision = "5a37d6485b82"
branch_labels = None
depends_on = None


def upgrade():
    # Rename column
    op.execute(
        """
        ALTER TABLE tbl_txn_transactions
        RENAME COLUMN reconciled_status TO reconciliation_status;
        """
    )

    # Convert BOOLEAN -> SMALLINT
    op.execute(
        """
        ALTER TABLE tbl_txn_transactions
        ALTER COLUMN reconciliation_status TYPE SMALLINT
        USING CASE
            WHEN reconciliation_status = true THEN 2
            ELSE 0
        END;
        """
    )

    # Default = PENDING
    op.execute(
        """
        ALTER TABLE tbl_txn_transactions
        ALTER COLUMN reconciliation_status SET DEFAULT 0;
        """
    )


def downgrade():
    # Convert SMALLINT -> BOOLEAN
    op.execute(
        """
        ALTER TABLE tbl_txn_transactions
        ALTER COLUMN reconciliation_status TYPE BOOLEAN
        USING CASE
            WHEN reconciliation_status = 2 THEN true
            ELSE false
        END;
        """
    )

    # Rename back
    op.execute(
        """
        ALTER TABLE tbl_txn_transactions
        RENAME COLUMN reconciliation_status TO reconciled_status;
        """
    )
