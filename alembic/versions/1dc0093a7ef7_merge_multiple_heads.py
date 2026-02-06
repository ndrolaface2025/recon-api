"""merge_multiple_heads

Revision ID: 1dc0093a7ef7
Revises: ('29c0d62ef79a', 'dc4a6bdfd6d2')
Create Date: 2026-02-06 16:08:11.677257

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1dc0093a7ef7'
down_revision = ('29c0d62ef79a', 'dc4a6bdfd6d2')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
