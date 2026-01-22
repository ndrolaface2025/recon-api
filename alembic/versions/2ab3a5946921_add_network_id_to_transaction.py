"""add network_id to transaction

Revision ID: 2ab3a5946921
Revises: 72fa8e9b5a6e
Create Date: 2026-01-21 15:08:21.898159

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '2ab3a5946921'
down_revision = '72fa8e9b5a6e'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "tbl_txn_transactions",
        sa.Column("network_id", sa.BigInteger(), nullable=True)
    )

    op.create_foreign_key(
        "fk_transaction_network",
        source_table="tbl_txn_transactions",
        referent_table=
        "tbl_cfg_networks",
        local_cols=["network_id"],
        remote_cols=["id"],
        ondelete="SET NULL"
    )

    op.create_index(
        "ix_transaction_network_id",
        "tbl_txn_transactions",
        ["network_id"]
    )


def downgrade() -> None:
    op.drop_index("ix_transaction_network_id", table_name="tbl_txn_transactions")
    op.drop_constraint(
        "fk_transaction_network",
        "tbl_txn_transactions",
        type_="foreignkey"
    )
    op.drop_column("tbl_txn_transactions", "network_id")
