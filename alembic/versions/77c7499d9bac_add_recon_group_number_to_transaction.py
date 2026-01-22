"""add recon_group_number to transaction

Revision ID: 77c7499d9bac
Revises: 2ab3a5946921
Create Date: 2026-01-22 10:40:46.754803

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '77c7499d9bac'
down_revision = '2ab3a5946921'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add recon_group_number to tbl_txn_transactions"""
    op.add_column(
        "tbl_txn_transactions",
        sa.Column("recon_group_number", sa.String(255), nullable=True)
    )


def downgrade() -> None:
    """Remove recon_group_number from tbl_txn_transactions"""
    op.drop_column("tbl_txn_transactions", "recon_group_number")
