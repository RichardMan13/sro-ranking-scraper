# Core (`src/core`)

## Overview
Shared foundational infrastructure for the `sro-ranking-scraper` application. This package centralizes environment-backed configuration loading (`config.py`), async SQLAlchemy engine/session construction and PostgreSQL monthly partition management (`database.py`), and structured JSON logging with run-scoped context propagation (`logging.py`). `__init__.py` is an empty package marker.

## Interface
- `[config.py:Settings]`: Pydantic `BaseSettings` model loading `database_url`, `prefect_api_url` (default `"http://localhost:4200/api"`), `log_level` (default `"INFO"`), `quarantine_dir` (default `Path("./quarantine")`), `quarantine_retention_days` (default `30`, `>= 1`), `circuit_breaker_cooldown_hours` (default `6`, `>= 1`), and `electus_cf_clearance` (optional `str | None`).
- `[config.py:Settings.validate_non_empty_database_url]`: After-validator normalizing and validating `database_url` schemes and host/path requirements.
- `[config.py:get_settings]`: Factory returning a fresh `Settings` instance loaded from the current environment and `.env`.
- `[database.py:PARTITIONED_TABLES]`: Constant tuple `("character_snapshots", "guild_snapshots")` naming the range-partitioned snapshot tables.
- `[database.py:create_engine_from_url]`: Creates an `AsyncEngine` after normalizing `postgres://` and `postgresql://` URLs to `postgresql+asyncpg://`; applies `pool_pre_ping=True`, `pool_size=5`, `max_overflow=5` for non-SQLite URLs.
- `[database.py:create_session_factory]`: Returns an `async_sessionmaker[AsyncSession]` bound to an `AsyncEngine` with `expire_on_commit=False`.
- `[database.py:ensure_monthly_partitions]`: Async helper that idempotently creates the current and next UTC monthly `PARTITION OF` tables for every entry in `[database.py:PARTITIONED_TABLES]` and returns the four partition names.
- `[logging.py:configure_logging]`: Configures `structlog` for UTC ISO-timestamped JSON output to stdout at the requested log level.
- `[logging.py:get_logger]`: Returns a `structlog` `FilteringBoundLogger`, optionally bound with `logger=<name>`.
- `[logging.py:bind_run_context]`: Binds non-`None` `server_id` and `run_id` (stringified from `str | UUID`) into `structlog.contextvars`.
- `[logging.py:clear_run_context]`: Clears all `structlog.contextvars` bindings for the current execution context.
- Subdirectories: None.

## Invariants
- `[config.py:Settings]` requires `database_url` to be non-empty, include `://`, match `[config.py:_ALLOWED_DB_SCHEME_RE]` (`postgres`, `postgresql[+<driver>]`, or `sqlite[+<driver>]`), specify a non-root path for SQLite URLs, and specify a hostname for PostgreSQL URLs; extra environment variables are ignored (`extra="ignore"`).
- `[config.py:Settings]` enforces `quarantine_retention_days >= 1` and `circuit_breaker_cooldown_hours >= 1`.
- `[config.py:get_settings]` constructs a new `Settings` instance on every call (no module-level caching).
- `[database.py:_normalize_async_database_url]` always lowercases the URL scheme and rewrites bare `postgres://` and `postgresql://` schemes to `postgresql+asyncpg://`, while preserving explicit driver schemes (e.g., `sqlite+aiosqlite://`).
- `[database.py:create_engine_from_url]` omits connection pool sizing arguments for `sqlite*` URLs and enforces `pool_pre_ping=True`, `pool_size=5`, `max_overflow=5` for PostgreSQL URLs.
- `[database.py:create_session_factory]` sets `expire_on_commit=False` on all created `AsyncSession` instances.
- `[database.py:ensure_monthly_partitions]` interprets naive `reference_time` values as UTC, converts timezone-aware values to UTC, handles December-to-January year rollover, and uses `CREATE TABLE IF NOT EXISTS` for idempotency; it does not commit the `AsyncSession` (callers control transaction boundaries).
- `[logging.py:configure_logging]` maps unrecognized `log_level` strings to `logging.INFO`, sets `cache_logger_on_first_use=False` so reconfiguration takes effect immediately, and merges `structlog.contextvars` on every log event.
- `[logging.py:bind_run_context]` is a no-op when both `server_id` and `run_id` are `None`; callers must invoke `[logging.py:clear_run_context]` when a run/task scope ends to avoid context leakage across async tasks.

## Side Effects
- `[config.py:Settings]` / `[config.py:get_settings]`: Reads process environment variables and the local `.env` file (`env_file=".env"`, UTF-8).
- `[database.py:create_engine_from_url]`: Allocates an async SQLAlchemy engine and (for PostgreSQL) a connection pool (`pool_size=5`, `max_overflow=5`).
- `[database.py:ensure_monthly_partitions]`: Executes PostgreSQL `CREATE TABLE IF NOT EXISTS ... PARTITION OF ...` DDL statements through the provided `AsyncSession`.
- `[logging.py:configure_logging]`: Mutates global `structlog` configuration and directs log emission to stdout via `structlog.PrintLoggerFactory`.
- `[logging.py:bind_run_context]` / `[logging.py:clear_run_context]`: Mutates and clears `structlog.contextvars` state for the active execution context.

## Verification
- Run: `pytest`
