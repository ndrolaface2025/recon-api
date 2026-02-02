"""Alter Journal table

Revision ID: 5a37d6485b82
Revises: 56e5d73f6d69
Create Date: 2026-01-30 13:31:55.897054

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '5a37d6485b82'
down_revision = '56e5d73f6d69'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        'tbl_txn_journal_entries',
        'maker_id',
        existing_type=sa.String(length=50),
        type_=sa.BigInteger(),
        postgresql_using='maker_id::bigint',
        nullable=True
    )

    op.alter_column(
        'tbl_txn_journal_entries',
        'checker_id',
        existing_type=sa.String(length=50),
        type_=sa.BigInteger(),
        postgresql_using='checker_id::bigint',
        nullable=True
    )

    # 2. Add foreign key constraint
    op.create_foreign_key(
        'fk_tbl_txn_journal_entries_maker_id_users',
        'tbl_txn_journal_entries',
        'tbl_cfg_users',
        ['maker_id'],
        ['id']
    )

    op.create_foreign_key(
        'fk_tbl_txn_journal_entries_checker_id_users',
        'tbl_txn_journal_entries',
        'tbl_cfg_users',
        ['checker_id'],
        ['id']
    )


def downgrade() -> None:
    # 1. Drop foreign key constraints
    op.drop_constraint(
        'fk_tbl_txn_journal_entries_maker_id_users',
        'tbl_txn_journal_entries',
        type_='foreignkey'
    )

    op.drop_constraint(
        'fk_tbl_txn_journal_entries_checker_id_users',
        'tbl_txn_journal_entries',
        type_='foreignkey'
    )

    # 2. Revert columns back to String(50)
    op.alter_column(
        'tbl_txn_journal_entries',
        'maker_id',
        existing_type=sa.BigInteger(),
        type_=sa.String(length=50),
        postgresql_using='maker_id::text',
        nullable=True
    )

    op.alter_column(
        'tbl_txn_journal_entries',
        'checker_id',
        existing_type=sa.BigInteger(),
        type_=sa.String(length=50),
        postgresql_using='checker_id::text',
        nullable=True
    )
