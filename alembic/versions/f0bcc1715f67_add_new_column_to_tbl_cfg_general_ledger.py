"""add new column to tbl_cfg_general_ledger

Revision ID: f0bcc1715f67
Revises: fb50d2b2a0bb
Create Date: 2026-02-04 12:14:01.482505

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f0bcc1715f67'
down_revision = 'fb50d2b2a0bb'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'tbl_cfg_general_ledger',
        sa.Column('status', sa.Boolean(), nullable=False, default=False)
    )


def downgrade() -> None:
    op.drop_column('tbl_cfg_general_ledger', 'status')
