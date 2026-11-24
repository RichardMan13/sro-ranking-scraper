# Analytical Dashboard Package (`src/dashboard`)

## Overview
Read-only analytical Streamlit + Altair dashboard package designed for single-user community analytics on PostgreSQL 16, strictly isolated from the `scraper-worker` process and read-write credentials.

## Modules & Boundaries
- **`config.py` (`DashboardSettings`, `get_dashboard_settings`):** Isolated Pydantic v2 settings (`hide_input_in_errors=True`) requiring only `DASHBOARD_DATABASE_URL` (`SecretStr`, synchronous PostgreSQL `psycopg` schemes only) plus `DASHBOARD_CACHE_TTL_SECONDS` (`30..3600`, default `600`) and `LOG_LEVEL`. Never reads or requires `DATABASE_URL`.
- **`db.py` (`build_postgres_options`, `create_dashboard_engine`, `create_dashboard_session_factory`):** Synchronous SQLAlchemy 2.0 engine over `psycopg` with a constrained pool (`pool_size=2`, `max_overflow=0`, `pool_timeout=5`, `pool_recycle=300`, `pool_pre_ping=True`) and session-level guardrails (`default_transaction_read_only=on`, `statement_timeout=15000`, `lock_timeout=3000`, `work_mem=16MB`).
- **`schemas.py`:** Immutable `@dataclass(frozen=True, slots=True)` DTOs (`ServerOption`, `ServerFreshness`, `TopNAggregatePoint`, `TopCharacterTrendPoint`, `GuildDominanceRow`, `RaceBalanceRow`, `WeeklyRetentionRow`, `BehaviorClassificationRow`) with zero UI or `pandas` imports.
- **`queries.py`:** Pure synchronous parameterized PostgreSQL 16 queries (`sqlalchemy.text`) enforcing deterministic partition pruning on `character_snapshots` (2-step scalar bindings for latest-run views and explicit `:lookback_start` temporal filters prior to `LAG()`). Mirrors the analytical classification thresholds from `alembic/versions/002_analytical_views.py`.
- **`app.py`:** Thin Streamlit + Altair rendering layer (`st.segmented_control` lazy loading, `@st.cache_resource` engine singleton, `@st.cache_data(ttl=..., max_entries=10)` per loader, manual refresh button, controlled `SQLAlchemyError` handling via `structlog`). Does not import `pandas` and has zero import-time side effects.

## Invariants
- **Two-Layer Read-Only Defense:** Enforced on the PostgreSQL role `sro_readonly` (provisioned via `scripts/provision_readonly_role.sh`) and reinforced on every SQLAlchemy session via `connect_args["options"]`.
- **Shared Essential Dependencies:** `streamlit` and `altair` are declared in `[project].dependencies` in `pyproject.toml` as core project dependencies; `src/dashboard/app.py` remains free of `pandas` imports.
- **Lazy Execution:** Only the active view selected in `st.segmented_control` executes its query.

## Verification
- Unit & SQL contract tests (offline): `uv run --no-sync pytest tests/test_dashboard_config.py tests/test_dashboard_queries.py`
- Live PostgreSQL 16 integration & partition pruning (`EXPLAIN`): `TEST_POSTGRES_URL="postgresql+psycopg://..." uv run --no-sync pytest -m postgres tests/test_dashboard_queries.py`
