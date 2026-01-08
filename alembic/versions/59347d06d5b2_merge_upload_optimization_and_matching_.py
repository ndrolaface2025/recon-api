"""merge upload optimization and matching rule branches

Revision ID: 59347d06d5b2
Revises: ('add_progress_tracking', '1b476c0fb4ec')
Create Date: 2026-01-07 08:48:33.702207

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '59347d06d5b2'
down_revision = ('add_progress_tracking', '1b476c0fb4ec')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
