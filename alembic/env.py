from __future__ import annotations

import asyncio
from logging.config import fileConfig
from typing import Any

from alembic import context
from pydantic import ValidationError
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config
from src.core.config import Settings
from src.models.db_models import Base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name, disable_existing_loggers=False)

target_metadata = Base.metadata

_DEFAULT_OFFLINE_URL = "postgresql+asyncpg://localhost/sro_rankings"


def _normalize_async_url(url: str) -> str:
    cleaned = url.strip()
    if "://" in cleaned:
        scheme, rest = cleaned.split("://", maxsplit=1)
        cleaned = f"{scheme.lower()}://{rest}"
    if cleaned.startswith("postgres://"):
        return "postgresql+asyncpg://" + cleaned.removeprefix("postgres://")
    if cleaned.startswith("postgresql://"):
        return "postgresql+asyncpg://" + cleaned.removeprefix("postgresql://")
    return cleaned


def _resolve_database_url() -> str:
    try:
        url = str(Settings().database_url)
    except ValidationError as exc:
        if any(err.get("type") != "missing" for err in exc.errors()):
            raise
        url = config.get_main_option("sqlalchemy.url") or ""
        if not context.is_offline_mode() and (not url or url == _DEFAULT_OFFLINE_URL):
            raise RuntimeError("DATABASE_URL must be set for online Alembic migrations.") from None
        if not url:
            url = _DEFAULT_OFFLINE_URL

    resolved_url = _normalize_async_url(url)
    config.set_main_option("sqlalchemy.url", resolved_url.replace("%", "%%"))
    return resolved_url


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    _resolve_database_url()
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """Run migrations in 'online' mode using an async engine."""
    _resolve_database_url()
    section: dict[str, Any] = dict(config.get_section(config.config_ini_section) or {})
    connectable = async_engine_from_config(
        section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
