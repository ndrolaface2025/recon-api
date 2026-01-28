"""add network_id to matching rule table

Revision ID: e49db6f22db3
Revises: 4fd83833281c
Create Date: 2026-01-23 15:44:05.037755

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e49db6f22db3'
down_revision = '4fd83833281c'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('tbl_cfg_matching_rule',
        sa.Column('network_id', sa.BigInteger(), sa.ForeignKey("tbl_cfg_networks.id"), nullable=True, index=True)
    )


def downgrade() -> None:
    op.drop_column('tbl_cfg_matching_rule', 'network_id')
