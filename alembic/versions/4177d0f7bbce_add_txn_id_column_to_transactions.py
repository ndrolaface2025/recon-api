"""add_txn_id_column_to_transactions

Revision ID: 4177d0f7bbce
Revises: 249da6abaf07
Create Date: 2026-01-12 13:09:01.049916

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '4177d0f7bbce'
down_revision = '249da6abaf07'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add txn_id column to tbl_txn_transactions
    op.add_column('tbl_txn_transactions', sa.Column('txn_id', sa.String(length=50), nullable=True))
    op.create_index(op.f('ix_tbl_txn_transactions_txn_id'), 'tbl_txn_transactions', ['txn_id'], unique=False)


def downgrade() -> None:
    # Remove txn_id column and index
    op.drop_index(op.f('ix_tbl_txn_transactions_txn_id'), table_name='tbl_txn_transactions')
    op.drop_column('tbl_txn_transactions', 'txn_id')
