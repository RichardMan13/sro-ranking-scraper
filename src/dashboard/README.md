# Analytical Dashboard Package (`src/dashboard`)

## Overview
Read-only analytical Streamlit + Altair dashboard package designed for single-user community analytics on PostgreSQL 16, strictly isolated from the `scraper-worker` process and read-write credentials. `[__init__.py]` is a package marker with no runtime exports.

## Interface
- **Configuration (`config.py`)**
  - `[config.py:DashboardSettings]`: Isolated Pydantic v2 `BaseSettings` model (`hide_input_in_errors=True`, `extra="ignore"`) loading `dashboard_database_url` (`SecretStr`, required), `dashboard_cache_ttl_seconds` (`30..3600`, default `600`), and `log_level` (default `"INFO"`). Never reads or requires `DATABASE_URL`.
  - `[config.py:DashboardSettings.validate_sync_postgres_url]`: After-validator accepting only `[config.py:_ALLOWED_SYNC_SCHEMES]` (`postgres`, `postgresql`, `postgresql+psycopg`), normalizing to `postgresql+psycopg://`, requiring a non-empty hostname, and rejecting malformed ports (port may be omitted).
  - `[config.py:get_dashboard_settings]`: Factory returning a fresh `[config.py:DashboardSettings]` instance from the environment and `.env`.
- **Database Engine & Session Factory (`db.py`)**
  - `[db.py:STATEMENT_TIMEOUT_MS]` (`15000`), `[db.py:LOCK_TIMEOUT_MS]` (`3000`), `[db.py:WORK_MEM_MB]` (`16`), `[db.py:POOL_SIZE]` (`2`), `[db.py:POOL_TIMEOUT_SECONDS]` (`5`), `[db.py:POOL_RECYCLE_SECONDS]` (`300`): Read-only guardrail and connection pool constants.
  - `[db.py:build_postgres_options]`: Builds PostgreSQL startup session options (`-c default_transaction_read_only=on -c statement_timeout=15000 -c lock_timeout=3000 -c work_mem=16MB`).
  - `[db.py:create_dashboard_engine]`: Creates a constrained synchronous SQLAlchemy 2.0 `Engine` over `psycopg` (`pool_size=2`, `max_overflow=0`, `pool_timeout=5`, `pool_recycle=300`, `pool_pre_ping=True`, `connect_args={"options": build_postgres_options()}`).
  - `[db.py:create_dashboard_session_factory]`: Returns a synchronous `sessionmaker[Session]` with `expire_on_commit=False`.
- **DTO Contracts (`schemas.py`)**
  - `[schemas.py:RaceCode]` (`"CH" | "EU" | "UNK"`), `[schemas.py:TopNTier]` (`"Top 10" | "Top 50" | "Top 100"`), `[schemas.py:BehaviorSegment]` (`"SUSPECTED_BOT_24_7" | "SUSPECTED_DONATOR_WHALE" | "HARDCORE_HUMAN" | "CASUAL_OR_BALANCED"`): Literal domain types.
  - `[schemas.py:ServerOption]`, `[schemas.py:ServerFreshness]`, `[schemas.py:TopNAggregatePoint]`, `[schemas.py:TopCharacterTrendPoint]`, `[schemas.py:GuildDominanceRow]`, `[schemas.py:RaceBalanceRow]`, `[schemas.py:WeeklyRetentionRow]`, `[schemas.py:BehaviorClassificationRow]`: Immutable `@dataclass(frozen=True, slots=True)` DTOs with zero UI or `pandas` imports.
- **Read-Only Analytical Queries (`queries.py`)**
  - SQL constants (`TextClause`): `[queries.py:LIST_ACTIVE_SERVERS_SQL]`, `[queries.py:LATEST_VALID_RUN_SQL]`, `[queries.py:TOP_N_ITEM_POINTS_EVOLUTION_SQL]`, `[queries.py:TOP_CHARACTERS_DRILLDOWN_SQL]`, `[queries.py:GUILD_DOMINANCE_SQL]`, `[queries.py:RACE_BALANCE_SQL]`, `[queries.py:WEEKLY_RETENTION_SQL]`, `[queries.py:BOT_VS_DONOR_SQL]`.
  - Query functions: `[queries.py:list_active_servers]`, `[queries.py:get_server_freshness]`, `[queries.py:get_top_n_item_points_evolution]` (`1 <= window_days <= 185`), `[queries.py:get_top_characters_drilldown]` (`1 <= window_days <= 185`, `1 <= top_n <= 10`), `[queries.py:get_guild_dominance]`, `[queries.py:get_race_balance]`, `[queries.py:get_weekly_retention]` (`2 <= window_weeks <= 26`), `[queries.py:get_bot_vs_donor_classification]` (7-day window with 9-day lookback).
- **Streamlit UI Entrypoint (`app.py`)**
  - `[app.py:VIEW_OPTIONS]`, `[app.py:ALL_BEHAVIOR_SEGMENTS]`, `[app.py:RACE_DOMAIN]`, `[app.py:RACE_COLOR_RANGE]`, `[app.py:SEGMENT_DOMAIN]`, `[app.py:SEGMENT_COLOR_RANGE]`: UI navigation and Altair color/domain constants.
  - `[app.py:main]`: Streamlit entrypoint rendering the sidebar server selector, freshness caption, manual cache-clear button, and lazy `st.segmented_control` view dispatcher across all 5 analytical views.
- Subdirectories: None.

## Invariants
- **Two-Layer Read-Only Defense:** Enforced on the PostgreSQL role `sro_readonly` (provisioned via `scripts/provision_readonly_role.sh`) and reinforced on every SQLAlchemy session via `[db.py:build_postgres_options]` (`default_transaction_read_only=on`).
- **Credential Isolation:** `[config.py:DashboardSettings]` only accepts synchronous PostgreSQL `psycopg` URLs via `DASHBOARD_DATABASE_URL` (`SecretStr`, `hide_input_in_errors=True`) and never reads `DATABASE_URL`.
- **Deterministic Partition Pruning & Analytical Alignment:** All `[queries.py]` queries on `character_snapshots` bind explicit temporal bounds on `created_at` (2-step scalar `±1h` windows around the latest valid `scrape_runs` timestamp, or explicit `:lookback_start` / `:window_start` bounds before `LAG()`). Classification thresholds in `[queries.py:BOT_VS_DONOR_SQL]` and activity delta rules in `[queries.py:WEEKLY_RETENTION_SQL]` mirror `alembic/versions/002_analytical_views.py`.
- **Shared Essential Dependencies & Zero `pandas`:** `streamlit` and `altair` are declared in `[project].dependencies` in `pyproject.toml`; `[app.py]` does not import `pandas` (using `[app.py:_altair_data]` with `alt.Data(values=...)`) and has zero import-time side effects.
- **Lazy Execution & Bounded Caching:** Only the active view selected in `st.segmented_control` executes its query (`[app.py:main]`); `[app.py:_get_session_factory]` is cached with `@st.cache_resource` and each data loader in `[app.py:_get_cached_loaders]` is bounded with `@st.cache_data(ttl=ttl_seconds, max_entries=10)`.
- **Input & Domain Validation:** `[queries.py:get_top_n_item_points_evolution]`, `[queries.py:get_top_characters_drilldown]`, and `[queries.py:get_weekly_retention]` raise `ValueError` on out-of-range parameters; `[queries.py:_coerce_tier]` and `[queries.py:_coerce_segment]` raise `ValueError` on unexpected values, while `[queries.py:_coerce_race]` defaults unknown values to `"UNK"`.

## Side Effects
- `[config.py:DashboardSettings]` / `[config.py:get_dashboard_settings]`: Reads process environment variables and the local `.env` file (UTF-8).
- `[db.py:create_dashboard_engine]`: Allocates a synchronous SQLAlchemy connection pool (`pool_size=2`, `max_overflow=0`) to PostgreSQL with read-only session options.
- `[queries.py]` functions: Execute read-only parameterized SQL `SELECT` queries against `servers`, `scrape_runs`, `character_snapshots`, `characters`, and `guilds` via the provided synchronous `Session`.
- `[app.py:main]`: Configures global `structlog` logging (`configure_logging`), caches the DB engine/loaders in Streamlit's resource/data caches, clears `st.cache_data` and triggers `st.rerun()` on manual refresh, logs `dashboard_query_failed` warnings on `SQLAlchemyError`, and renders Streamlit/Altair UI widgets.

## Verification
- Run: `uv run --no-sync pytest`
- Unit & SQL contract tests (offline): `uv run --no-sync pytest tests/test_dashboard_config.py tests/test_dashboard_queries.py`
- Live PostgreSQL 16 integration & partition pruning (`EXPLAIN`): `TEST_POSTGRES_URL="postgresql+psycopg://..." uv run --no-sync pytest -m postgres tests/test_dashboard_queries.py`
