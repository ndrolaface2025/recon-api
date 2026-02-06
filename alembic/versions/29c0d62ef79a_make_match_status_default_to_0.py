"""make match_status default to 0

Revision ID: 29c0d62ef79a
Revises: a8ba416d417c
Create Date: 2026-02-06 13:38:13.124000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '29c0d62ef79a'
down_revision = 'f0bcc1715f67'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "tbl_txn_transactions",
        "match_status",
        existing_type=sa.Integer(),
        server_default=sa.text("0"),
        nullable=True,
    )

def downgrade() -> None:
    # Remove DB-level default (rollback safe)
    op.alter_column(
        "tbl_txn_transactions",
        "match_status",
        existing_type=sa.Integer(),
        server_default=None,
        nullable=True,
    )
