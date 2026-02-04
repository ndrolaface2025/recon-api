"""add new column to transaction table

Revision ID: fb50d2b2a0bb
Revises: 2610a69907f6
Create Date: 2026-02-04 08:32:35.486901

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'fb50d2b2a0bb'
down_revision = '2610a69907f6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'tbl_txn_transactions',
        sa.Column('reconciled_status', sa.Boolean(), nullable=True, default=None)
    )


def downgrade() -> None:
    op.drop_column('tbl_txn_transactions', 'reconciled_status')
