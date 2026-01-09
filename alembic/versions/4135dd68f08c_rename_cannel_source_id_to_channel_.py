"""Rename cannel_source_id to channel_source_id

Revision ID: 4135dd68f08c
Revises: ba08c351e35a
Create Date: 2026-01-09 11:24:47.802268

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '4135dd68f08c'
down_revision = 'ba08c351e35a'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        'tbl_cfg_channels',            # The name of the table
        'cannel_source_id', # The old column name
        new_column_name='channel_source_id', # The new column name
        existing_type=sa.BigInteger(), # Specify the existing type (important for MySQL)
        # Add other existing constraints if necessary (nullable, server_default, etc.)
    )


def downgrade() -> None:
    op.alter_column(
        'tbl_cfg_channels',            # The name of the table
        'cannel_source_id', # The old column name
        new_column_name='channel_source_id', # The new column name
        existing_type=sa.BigInteger(), # Specify the existing type (important for MySQL)
        # Add other existing constraints if necessary (nullable, server_default, etc.)
    )
