# Core (`src/core`)

## Overview
Shared foundational infrastructure for the `sro-ranking-scraper` application. This package centralizes environment-backed configuration loading (`config.py`), async SQLAlchemy engine/session construction and PostgreSQL monthly partition management (`database.py`), structured JSON logging with run-scoped context propagation (`logging.py`), and best-effort Discord webhook notifications for operational alerts and post-Fortress War summaries (`notifications.py`). `__init__.py` is an empty package marker.

## Interface
- `[config.py:Settings]`: Pydantic `BaseSettings` model (`hide_input_in_errors=True`) loading `database_url`, `prefect_api_url` (default `"http://localhost:4200/api"`), `log_level` (default `"INFO"`), `quarantine_dir` (default `Path("./quarantine")`), `quarantine_retention_days` (default `30`, `>= 1`), `circuit_breaker_cooldown_hours` (default `6`, `>= 1`), `max_pages_per_category` (default `400`, `>= 1`), `electus_cf_clearance` (optional `str | None`), `discord_ops_webhook_url` (optional `SecretStr | None`), and `discord_community_webhook_url` (optional `SecretStr | None`).
- `[config.py:Settings.validate_non_empty_database_url]`: After-validator normalizing and validating `database_url` schemes and host/path requirements.
- `[config.py:Settings.normalize_discord_webhook_url]` / `[config.py:Settings.validate_discord_webhook_url]`: Two-stage validators (`mode="before"` / `mode="after"`) stripping whitespace, mapping empty strings to `None`, and enforcing `https` + official Discord hosts (`_DISCORD_WEBHOOK_HOSTS`) + exact `/api/webhooks/<id>/<token>` path without echoing tokens in `ValidationError`.
- `[config.py:get_settings]`: Factory returning a fresh `Settings` instance loaded from the current environment and `.env`.
- `[database.py:PARTITIONED_TABLES]`: Constant tuple `("character_snapshots", "guild_snapshots")` naming the range-partitioned snapshot tables.
- `[database.py:create_engine_from_url]`: Creates an `AsyncEngine` after normalizing `postgres://` and `postgresql://` URLs to `postgresql+asyncpg://`; applies `pool_pre_ping=True`, `pool_size=5`, `max_overflow=5` for non-SQLite URLs.
- `[database.py:create_session_factory]`: Returns an `async_sessionmaker[AsyncSession]` bound to an `AsyncEngine` with `expire_on_commit=False`.
- `[database.py:ensure_monthly_partitions]`: Async helper that idempotently creates the current and next UTC monthly `PARTITION OF` tables for every entry in `[database.py:PARTITIONED_TABLES]` and returns the four partition names.
- `[logging.py:configure_logging]`: Configures `structlog` for UTC ISO-timestamped JSON output to stdout at the requested log level and raises stdlib `httpx`/`httpcore` loggers to `logging.WARNING` to prevent webhook URL leakage.
- `[logging.py:get_logger]`: Returns a `structlog` `FilteringBoundLogger`, optionally bound with `logger=<name>`.
- `[logging.py:bind_run_context]`: Binds non-`None` `server_id` and `run_id` (stringified from `str | UUID`) into `structlog.contextvars`.
- `[logging.py:clear_run_context]`: Clears all `structlog.contextvars` bindings for the current execution context.
- `[notifications.py:OPS_ALERT_STATUSES]` / `[notifications.py:POST_FW_SUCCESS_STATUSES]`: Canonical `ScrapeStatus` sets gating operational alerts (`SCHEMA_DRIFT`, `LOW_COUNT_QUARANTINE`, `BLOCKED_CHALLENGE`, `HTTP_ERROR`) and post-FW summaries (`SUCCESS`, `PARTIAL_SUCCESS`).
- `[notifications.py:WEBHOOK_TIMEOUT_SECONDS]` (`10.0`), `[notifications.py:WEBHOOK_HTTP_TIMEOUT]` (`httpx.Timeout(connect=5.0, read=10.0, write=5.0, pool=5.0)`), `[notifications.py:POST_FW_BASELINE_WINDOW_HOURS]` (`24`), `[notifications.py:POST_FW_SNAPSHOT_TOLERANCE_MINUTES]` (`5`), `[notifications.py:POST_FW_TOP_GUILDS_LIMIT]` (`5`), `[notifications.py:EMBED_TITLE_MAX_LEN]` (`256`), `[notifications.py:EMBED_DESCRIPTION_MAX_LEN]` (`4096`), `[notifications.py:EMBED_FIELD_VALUE_MAX_LEN]` (`1024`), `[notifications.py:EMBED_FOOTER_MAX_LEN]` (`2048`): Timeout, comparison window, and Discord embed length constants.
- `[notifications.py:GuildRankEntry]` / `[notifications.py:PostFWComparison]`: Frozen dataclasses (`slots=True`) representing Top 5 official guild rows (`total_item_points: Decimal`) and post-FW vs. 24h baseline comparisons.
- `[notifications.py:build_ops_alert_payload]` / `[notifications.py:build_post_fw_payload]`: Pure PT-BR Discord embed builders enforcing `"allowed_mentions": {"parse": []}`, Markdown escaping on untrusted fields, UTC timestamps (`YYYY-MM-DD HH:MM UTC`), PT-BR integer rounding (`ROUND_HALF_UP`), and Discord field length truncation.
- `[notifications.py:send_discord_webhook]`: Async best-effort HTTP `POST` (`asyncio.timeout(10.0)`, `follow_redirects=False`, `0` retries) returning `True` on `2xx` and `False` on no-op or failure without propagating exceptions or logging webhook URLs.
- `[notifications.py:notify_operational_failure]` / `[notifications.py:notify_post_fw_summary]`: Public exception-isolated facades combining eligibility checks, payload building, and webhook dispatch.
- `[notifications.py:load_post_fw_comparison]`: Portable SQLAlchemy 2.0 read-only query (`servers`, `scrape_runs`, `guild_snapshots`, `guilds`) with `±5 min` `created_at` partition pruning, 24h baseline selection (non-`post_fw_extra` primary, any valid run fallback), UTC normalization, and `Decimal` coercion.
- Subdirectories: None.

## Invariants
- `[config.py:Settings]` requires `database_url` to be non-empty, include `://`, match `[config.py:_ALLOWED_DB_SCHEME_RE]` (`postgres`, `postgresql[+<driver>]`, or `sqlite[+<driver>]`), specify a non-root path for SQLite URLs, and specify a hostname for PostgreSQL URLs; extra environment variables are ignored (`extra="ignore"`), and `hide_input_in_errors=True` prevents secret inputs from appearing in `ValidationError`.
- `[config.py:Settings]` enforces `quarantine_retention_days >= 1`, `circuit_breaker_cooldown_hours >= 1`, `max_pages_per_category >= 1`, and official Discord HTTPS webhook paths when webhook URLs are non-empty.
- `[config.py:get_settings]` constructs a new `Settings` instance on every call (no module-level caching).
- `[database.py:_normalize_async_database_url]` always lowercases the URL scheme and rewrites bare `postgres://` and `postgresql://` schemes to `postgresql+asyncpg://`, while preserving explicit driver schemes (e.g., `sqlite+aiosqlite://`).
- `[database.py:create_engine_from_url]` omits connection pool sizing arguments for `sqlite*` URLs and enforces `pool_pre_ping=True`, `pool_size=5`, `max_overflow=5` for PostgreSQL URLs.
- `[database.py:create_session_factory]` sets `expire_on_commit=False` on all created `AsyncSession` instances.
- `[database.py:ensure_monthly_partitions]` interprets naive `reference_time` values as UTC, converts timezone-aware values to UTC, handles December-to-January year rollover, and uses `CREATE TABLE IF NOT EXISTS` for idempotency; it does not commit the `AsyncSession` (callers control transaction boundaries).
- `[logging.py:configure_logging]` maps unrecognized `log_level` strings to `logging.INFO`, sets `cache_logger_on_first_use=False` so reconfiguration takes effect immediately, silences stdlib `httpx`/`httpcore` `INFO` logs to `WARNING`, and merges `structlog.contextvars` on every log event.
- `[logging.py:bind_run_context]` is a no-op when both `server_id` and `run_id` are `None`; callers must invoke `[logging.py:clear_run_context]` when a run/task scope ends to avoid context leakage across async tasks.
- `[notifications.py:send_discord_webhook]`, `[notifications.py:notify_operational_failure]`, and `[notifications.py:notify_post_fw_summary]` never propagate exceptions, never log webhook URLs or raw exception strings (logging only `channel`, `server_id`, stringified `run_id`, `http_status_code`, and `error_type`), and call `get_secret_value()` solely in `client.post(...)`.

## Side Effects
- `[config.py:Settings]` / `[config.py:get_settings]`: Reads process environment variables and the local `.env` file (`env_file=".env"`, UTF-8).
- `[database.py:create_engine_from_url]`: Allocates an async SQLAlchemy engine and (for PostgreSQL) a connection pool (`pool_size=5`, `max_overflow=5`).
- `[database.py:ensure_monthly_partitions]`: Executes PostgreSQL `CREATE TABLE IF NOT EXISTS ... PARTITION OF ...` DDL statements through the provided `AsyncSession`.
- `[logging.py:configure_logging]`: Mutates global `structlog` configuration, adjusts stdlib `httpx`/`httpcore` logger levels to `WARNING`, and directs log emission to stdout via `structlog.PrintLoggerFactory`.
- `[logging.py:bind_run_context]` / `[logging.py:clear_run_context]`: Mutates and clears `structlog.contextvars` state for the active execution context.
- `[notifications.py:send_discord_webhook]`: Performs outbound HTTPS `POST` requests to configured Discord webhook endpoints via `httpx.AsyncClient` and emits structured JSON log events.
- `[notifications.py:load_post_fw_comparison]`: Executes read-only SQL `SELECT` queries against `servers`, `scrape_runs`, `guild_snapshots`, and `guilds` through the provided `AsyncSession`.

## Verification
- Run: `uv run --no-sync pytest`
