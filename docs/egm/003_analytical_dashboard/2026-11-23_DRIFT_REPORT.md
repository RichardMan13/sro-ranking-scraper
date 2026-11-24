## 003_analytical_dashboard.md

- **Dependency Isolation & Docker Packaging Drift (`pyproject.toml`, `Dockerfile`, `docker-compose.yml`, `README.md`, `src/dashboard/README.md`, and internal inconsistency in `docs/designs/003_analytical_dashboard.md`):**
  - **Design Doc vs. Code (`pyproject.toml`, `Dockerfile`, `docker-compose.yml`):** While Goal **G5** in `docs/designs/003_analytical_dashboard.md` (line 38) was updated to declare `streamlit` and `altair` in `[project].dependencies`, the rest of the design doc still specifies isolating them under `[project.optional-dependencies]` (`dashboard`) and installing them only for the `dashboard` image (Overview line 15, The Problem line 23, Architecture diagram line 87, Section 5 line 143, Detailed Plan #5 line 299, #14 lines 397–398, #15 line 404, and Verification Plan lines 456–459).
  - In the actual codebase:
    - `pyproject.toml` (lines 11–26) places `altair>=5.0.0,<6.0.0` and `streamlit>=1.40.0,<2.0.0` directly in `[project].dependencies` with no `[project.optional-dependencies]` table.
    - `Dockerfile` (line 23) omits `ARG INSTALL_EXTRAS=""` and runs `RUN pip install --no-cache-dir .` unconditionally.
    - `docker-compose.yml` (lines 95–97) omits `args: { INSTALL_EXTRAS: "[dashboard]" }` under `dashboard.build`.
    - Because both services share the same `Dockerfile` and `[project].dependencies`, `scraper-worker` also receives `streamlit`, `altair`, and transitive `pandas`/`pyarrow`, making the Verification Plan isolation check (lines 456–459) fail against the current image.
  - **Operational Docs Drift:**
    - `README.md` (lines 108–109) still states that `scraper-worker` is `"free of streamlit/pandas/pyarrow"` and that `dashboard` uses `"Python 3.11 + [dashboard] extra"`.
    - `src/dashboard/README.md` (line 4) still states `"strictly isolated from the scraper-worker runtime dependencies"`, contradicting its own line 15 (`"Shared Essential Dependencies: streamlit and altair are declared in [project].dependencies"`).
- **View 2 Bar Labels Computed but Not Rendered (`src/dashboard/app.py`):**
  - `_render_view_2_guild_dominance` (lines 344–347) builds a `"label"` string (`"{players_in_top_100}/{sample_total_players} ({sample_share_pct:.1f}% amostra | {nominal_top_100_pct:.1f}% / 100)"`) for each bar, but never binds it to a `mark_text` layer or tooltip; the horizontal bar chart only renders `mark_bar()` with individual metrics on hover and in the auxiliary table below.
- **View 1 Undocumented Tier Selector (`src/dashboard/app.py`):**
  - `_render_view_1_item_points` (lines 178–186, 197) introduces an extra `st.segmented_control` (`key="view1_tier"`, defaulting to `"Top 100"`) that filters and plots one rank tier (`Top 10`, `Top 50`, or `Top 100`) at a time instead of plotting all three tiers simultaneously as described in Goal **G1 #1** (line 30) and Detailed Plan #13 (line 386).
- **Incomplete `EXPLAIN` Partition Pruning Coverage in PostgreSQL Integration Test (`tests/test_dashboard_queries.py`):**
  - Detailed Plan #12 (line 370) specifies asserting via `EXPLAIN (FORMAT JSON)` that no out-of-window monthly partition appears in query plans across the partitioned queries. `test_postgres_queries_and_partition_pruning` (lines 671–707) asserts `EXPLAIN (FORMAT JSON)` for 4 queries (`TOP_N_ITEM_POINTS_EVOLUTION_SQL`, `GUILD_DOMINANCE_SQL`, `RACE_BALANCE_SQL`, `BOT_VS_DONOR_SQL`), omitting `TOP_CHARACTERS_DRILLDOWN_SQL` and `WEEKLY_RETENTION_SQL`.
- **Document Status & Auxiliary Doc Drift (`docs/designs/003_analytical_dashboard.md`, `alembic/versions/README.md`):**
  - `docs/designs/003_analytical_dashboard.md` (line 4) still lists `Status: Approved`, whereas `docs/designs/SITEMAP.md` (line 11) lists `Implemented`.
  - `alembic/versions/README.md` (lines 26–29) lists `003_dashboard_indexes.py` under `Interface` and `Invariants`, but omits it from `Side Effects`.

### Resolution (2026-11-23)

All divergences above were reconciled in the same session:
- Aligned `docs/designs/003_analytical_dashboard.md` (status `Implemented`, unified `[project].dependencies` packaging across Overview/Problem/Architecture/Section 5/Plan #5/#14/#15/Verification Plan, and View 1 tier selector), `README.md`, `src/dashboard/README.md`, `alembic/versions/README.md`, and `docs/designs/SITEMAP.md`.
- Layered `mark_text` labels onto the View 2 horizontal bar chart in `src/dashboard/app.py`.
- Extended `EXPLAIN (FORMAT JSON)` partition pruning assertions in `tests/test_dashboard_queries.py` to cover all 6 partitioned queries (`TOP_CHARACTERS_DRILLDOWN_SQL` and `WEEKLY_RETENTION_SQL` added).
