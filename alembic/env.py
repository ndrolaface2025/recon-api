from logging.config import fileConfig
from sqlalchemy import pool
from sqlalchemy import engine_from_config
from alembic import context
import os

config = context.config

# Get DATABASE_URL from environment
database_url = os.getenv('DATABASE_URL')
if database_url:
    # Convert async URL to sync URL for Alembic (Alembic needs sync connections)
    database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')
    config.set_main_option('sqlalchemy.url', database_url)

fileConfig(config.config_file_name)
target_metadata = None

# Import models to populate metadata
from app.db.base import metadata
# Import the models package so model modules are executed and register with metadata
import app.db.models  # noqa: F401
target_metadata = metadata


def run_migrations_offline():
    url = os.getenv('DATABASE_URL', '')
    if url:
        url = url.replace('postgresql+asyncpg://', 'postgresql://')
    context.configure(url=url, target_metadata=target_metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    configuration = config.get_section(config.config_ini_section)
    db_url = os.getenv('DATABASE_URL', '')
    if db_url:
        # Convert async URL to sync for Alembic
        db_url = db_url.replace('postgresql+asyncpg://', 'postgresql://')
    configuration['sqlalchemy.url'] = db_url or configuration.get('sqlalchemy.url', '')
    connectable = engine_from_config(configuration, prefix='sqlalchemy.', poolclass=pool.NullPool)
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
