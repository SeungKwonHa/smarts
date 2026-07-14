"""Alembic env.py — sync psycopg2 for DDL migrations.

The app uses asyncpg at runtime; Alembic uses psycopg2 for migrations
(standard practice — no async needed for DDL).
"""

from logging.config import fileConfig

from alembic import context
from sqlalchemy import create_engine, pool
from sqlalchemy.engine import Connection

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from relay.core.config import settings

config = context.config

# Convert asyncpg URL to psycopg2 for Alembic
sync_url = (
    settings.database_url
    .replace("postgresql+asyncpg://", "postgresql+psycopg2://")
    .replace("postgresql+asyncpg:", "postgresql:")
)
config.set_main_option("sqlalchemy.url", sync_url)

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = None  # raw SQL DDL migrations


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = create_engine(sync_url, poolclass=pool.NullPool)
    with connectable.connect() as connection:
        do_run_migrations(connection)
    connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
