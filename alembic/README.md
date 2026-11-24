# Alembic Migrations (`alembic`)

## Overview
Configures and executes Alembic schema and analytical-view migrations for PostgreSQL 16 using an async SQLAlchemy (`asyncpg`) engine bound to `src.models.db_models.Base.metadata`. Supports both online async execution and offline SQL script generation.

## Interface
- `[env.py:config]` — Module-level Alembic `Config` instance loaded from `context.config`.
- `[env.py:target_metadata]` — SQLAlchemy `MetaData` bound to `src.models.db_models.Base.metadata`.
- `[env.py:run_migrations_offline]` — Resolves the database URL and runs Alembic migrations in offline mode with `literal_binds=True`, `dialect_opts={"paramstyle": "named"}`, and `compare_type=True`.
- `[env.py:do_run_migrations]` — Configures the Alembic migration context on a synchronous `Connection` with `target_metadata` and `compare_type=True`, then runs migrations inside a transaction.
- `[env.py:run_async_migrations]` — Async runner that resolves the database URL, constructs an async engine via `async_engine_from_config` with `pool.NullPool`, executes `do_run_migrations` via `connection.run_sync`, and disposes the engine.
- `[env.py:run_migrations_online]` — Synchronous entrypoint executing `run_async_migrations()` via `asyncio.run`.
- Subdirectories:
  - [versions](./versions/README.md) — Linear PostgreSQL 16 migration chain (`001_initial_schema` -> `002_analytical_views` -> `003_dashboard_indexes`) creating tables, seeds, monthly range partitions, indexes, and analytical SQL views.

## Invariants
- `[env.py:_normalize_async_url]` — Strips whitespace, lowercases the URL scheme, and rewrites `postgres://` and `postgresql://` prefixes to `postgresql+asyncpg://`.
- `[env.py:_resolve_database_url]` — Re-raises `pydantic.ValidationError` from `Settings()` if any error has `type != "missing"`.
- `[env.py:_resolve_database_url]` — In online mode, raises `RuntimeError` if `DATABASE_URL` is missing and `sqlalchemy.url` is empty or equals `_DEFAULT_OFFLINE_URL` (`postgresql+asyncpg://localhost/sro_rankings`).
- `[env.py:_resolve_database_url]` — In offline mode, falls back to `_DEFAULT_OFFLINE_URL` when no URL is configured and escapes `%` as `%%` when writing `sqlalchemy.url` back to Alembic config.
- `[env.py:run_async_migrations]` — Always uses `sqlalchemy.pool.NullPool` and disposes the engine after executing migrations.

## Side Effects
- `[env.py:fileConfig]` — Configures Python logging from `config.config_file_name` at module load with `disable_existing_loggers=False`.
- `[env.py:_resolve_database_url]` — Reads environment configuration via `Settings()` and mutates Alembic `sqlalchemy.url` via `config.set_main_option`.
- `[env.py:context.is_offline_mode]` — Executes `run_migrations_offline()` or `run_migrations_online()` immediately on module execution, connecting to PostgreSQL over `asyncpg` in online mode to apply DDL/DML migrations.

## Verification
- Run: `pytest`
