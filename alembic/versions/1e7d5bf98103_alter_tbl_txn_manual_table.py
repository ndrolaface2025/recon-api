"""alter tbl_txn_manual table

Revision ID: 1e7d5bf98103
Revises: 4177d0f7bbce
Create Date: 2026-01-13 10:32:24.399556

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1e7d5bf98103'
down_revision = '4177d0f7bbce'
branch_labels = None
depends_on = None


def upgrade() -> None:
     op.alter_column(
        'tbl_txn_manual', 
        'txn_date',
        type_=sa.String()
    )


def downgrade() -> None:
    op.alter_column(
        'tbl_txn_manual',
        'txn_date',
        type_=sa.String()
    )
