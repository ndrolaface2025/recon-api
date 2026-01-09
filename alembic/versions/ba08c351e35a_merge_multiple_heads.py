"""merge multiple heads

Revision ID: ba08c351e35a
Revises: ('19ed1681007d', '59347d06d5b2')
Create Date: 2026-01-09 10:54:07.222402

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ba08c351e35a'
down_revision = ('19ed1681007d', '59347d06d5b2')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
