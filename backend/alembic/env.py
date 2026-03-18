"""Alembic environment configuration with async SQLAlchemy support."""

import os
from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool
from alembic import context

from app.db.base import Base
import app.models  # noqa — ensures all models are imported

config = context.config

# Inject DB URL from environment
config.set_section_option("alembic", "POSTGRES_USER", os.getenv("POSTGRES_USER", "jobharvest"))
config.set_section_option("alembic", "POSTGRES_PASSWORD", os.getenv("POSTGRES_PASSWORD", "jobharvest"))
config.set_section_option("alembic", "POSTGRES_HOST", os.getenv("POSTGRES_HOST", "localhost"))
config.set_section_option("alembic", "POSTGRES_PORT", os.getenv("POSTGRES_PORT", "5432"))
config.set_section_option("alembic", "POSTGRES_DB", os.getenv("POSTGRES_DB", "jobharvest"))

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


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


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
