from alembic import op
import sqlalchemy as sa

revision = 'f5e04aef87f9'
down_revision = '3880782acd21'
branch_labels = None
depends_on = None


def upgrade():
    # DROP FOREIGN KEY FIRST
    # op.drop_constraint(
    #     "tbl_cfg_users_role_fkey",
    #     "tbl_cfg_users",
    #     type_="foreignkey"
    # )

    op.drop_constraint('tbl_cfg_users_role_fkey', 'tbl_cfg_users', type_='foreignkey')

    # gender: Boolean → String
    op.alter_column(
        "tbl_cfg_users",
        "gender",
        existing_type=sa.Boolean(),
        type_=sa.String(length=20),
        nullable=True
    )

    # role: BigInteger → String
    op.alter_column(
        "tbl_cfg_users",
        "role",
        existing_type=sa.BigInteger(),
        type_=sa.String(length=100),
        nullable=True
    )

    # ADD password column
    op.add_column(
        "tbl_cfg_users",
        sa.Column(
            "password",
            sa.String(length=255),
            nullable=False,
            server_default=""  # ⚠ required for existing rows
        )
    )

    # 🔧 Optional: remove default after column creation
    op.alter_column(
        "tbl_cfg_users",
        "password",
        server_default=None
    )


def downgrade():
    # DROP password column
    op.drop_column("tbl_cfg_users", "password")

    # role: String → BigInteger
    op.alter_column(
        "tbl_cfg_users",
        "role",
        existing_type=sa.String(length=100),
        type_=sa.BigInteger(),
        nullable=True
    )

    # gender: String → Boolean
    op.alter_column(
        "tbl_cfg_users",
        "gender",
        existing_type=sa.String(length=20),
        type_=sa.Boolean(),
        nullable=True
    )

    # Recreate FK
    # op.create_foreign_key(
    #     "tbl_cfg_users_role_fkey",
    #     "tbl_cfg_users",
    #     "tbl_cfg_roles",
    #     ["role"],
    #     ["id"]
    # )

     # Recreate the original foreign key
    op.create_foreign_key(
        'tbl_cfg_users_role_fkey',
        'tbl_cfg_users', 'tbl_cfg_roles',  # adjust table name
        ['role'], ['id']
    )
