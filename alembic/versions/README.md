# Alembic Migration Versions

## Overview
Linear PostgreSQL 16 Alembic migration chain defining the core relational schema, initial server seed records, monthly range partitions, analytical indexes, and SQL time-series views for the SRO ranking scraper.

## Interface
- `[001_initial_schema.py:revision]` (`"001_initial_schema"`), `[001_initial_schema.py:down_revision]` (`None`), `[001_initial_schema.py:branch_labels]` (`None`), `[001_initial_schema.py:depends_on]` (`None`): root migration metadata.
- `[001_initial_schema.py:upgrade]`: enables PostgreSQL extension `pgcrypto`, creates tables `servers`, `scrape_runs`, `characters`, `guilds`, `character_snapshots`, and `guild_snapshots`, seeds `servers` (`chronicle`, `electus`, `play_golden`), creates partitions (`character_snapshots_2026_11`, `character_snapshots_2026_12`, `character_snapshots_default`, `guild_snapshots_2026_11`, `guild_snapshots_2026_12`, `guild_snapshots_default`), and creates indexes (`idx_char_snapshots_lookup`, `idx_char_snapshots_rank`, `idx_char_snapshots_brin_time`, `idx_char_snapshots_extra_metrics`, `idx_guild_snapshots_lookup`, `idx_guild_snapshots_rank`, `idx_guild_snapshots_brin_time`).
- `[001_initial_schema.py:downgrade]`: drops the 7 snapshot indexes, 6 partitions, `guild_snapshots`, `character_snapshots`, `guilds`, `characters`, `scrape_runs`, and `servers` with `IF EXISTS`.
- `[002_analytical_views.py:revision]` (`"002_analytical_views"`), `[002_analytical_views.py:down_revision]` (`"001_initial_schema"`), `[002_analytical_views.py:branch_labels]` (`None`), `[002_analytical_views.py:depends_on]` (`None`): analytical views migration metadata.
- `[002_analytical_views.py:upgrade]`: creates or replaces analytical views `v_character_activity_deltas`, `v_bot_vs_donor_classification`, `v_guild_turnover_dominance`, `v_race_balance_ch_vs_eu`, and `v_server_retention_weekly`.
- `[002_analytical_views.py:downgrade]`: drops `v_server_retention_weekly`, `v_race_balance_ch_vs_eu`, `v_guild_turnover_dominance`, `v_bot_vs_donor_classification`, and `v_character_activity_deltas` with `IF EXISTS`.
- `[003_dashboard_indexes.py:revision]` (`"003_dashboard_indexes"`), `[003_dashboard_indexes.py:down_revision]` (`"002_analytical_views"`), `[003_dashboard_indexes.py:branch_labels]` (`None`), `[003_dashboard_indexes.py:depends_on]` (`None`): dashboard partial index migration metadata.
- `[003_dashboard_indexes.py:upgrade]`: creates partial index `idx_scrape_runs_server_latest` on `scrape_runs (server_id, created_at DESC) WHERE status IN ('SUCCESS', 'PARTIAL_SUCCESS')`.
- `[003_dashboard_indexes.py:downgrade]`: drops `idx_scrape_runs_server_latest` with `IF EXISTS`.
- `[004_seed_rsilkroad.py:revision]` (`"004_seed_rsilkroad"`), `[004_seed_rsilkroad.py:down_revision]` (`"003_dashboard_indexes"`), `[004_seed_rsilkroad.py:branch_labels]` (`None`), `[004_seed_rsilkroad.py:depends_on]` (`None`): RSilkroad server seed migration metadata.
- `[004_seed_rsilkroad.py:upgrade]`: idempotently inserts `rsilkroad` (`display_name='RSilkroad'`, `base_url='https://www.rsilkroad.com'`, `current_season='v1'`, `is_active=TRUE`, `fw_day_of_week=0`, `fw_end_time_utc='20:00:00'`, `min_records_expected=100`) into `servers` with `ON CONFLICT (id) DO NOTHING`.
- `[004_seed_rsilkroad.py:downgrade]`: deletes `rsilkroad` from `servers` (`DELETE FROM servers WHERE id = 'rsilkroad'`).
- Subdirectories: none.

## Invariants
- Linear revision chain: `[004_seed_rsilkroad.py:down_revision]` strictly depends on `[003_dashboard_indexes.py:revision]`, which depends on `[002_analytical_views.py:revision]`, which depends on `[001_initial_schema.py:revision]`, and views in `[002_analytical_views.py:downgrade]` must be dropped before base tables in `[001_initial_schema.py:downgrade]`.
- Partitioned fact primary and unique keys in `[001_initial_schema.py:upgrade]` include the partition key: `character_snapshots` uses `pk_character_snapshots (id, created_at)` and `uq_char_snapshot_run (scrape_run_id, character_id, created_at)`, and `guild_snapshots` uses `pk_guild_snapshots (id, created_at)` and `uq_guild_snapshot_run (scrape_run_id, guild_id, created_at)`, both partitioned by `RANGE (created_at)`.
- Domain check and uniqueness constraints in `[001_initial_schema.py:upgrade]`: `scrape_runs.status` is restricted to `('SUCCESS', 'PARTIAL_SUCCESS', 'SCHEMA_DRIFT', 'LOW_COUNT_QUARANTINE', 'BLOCKED_CHALLENGE', 'TARGET_MAINTENANCE', 'HTTP_ERROR')`; `characters.race` is restricted to `('CH', 'EU', 'UNK')`; `uq_char_per_server (server_id, char_name)` and `uq_guild_per_server (server_id, guild_name)` enforce per-server natural uniqueness.
- Idempotent server seeding in `[001_initial_schema.py:upgrade]` and `[004_seed_rsilkroad.py:upgrade]`: initial rows in `servers` (`chronicle`, `electus`, `play_golden`) and the additional `rsilkroad` seed row use `ON CONFLICT (id) DO NOTHING`.
- View dependency order in `[002_analytical_views.py:upgrade]` and `[002_analytical_views.py:downgrade]`: `v_bot_vs_donor_classification` and `v_server_retention_weekly` query `v_character_activity_deltas`, so `v_character_activity_deltas` must be created first and dropped last.
- Latest-run analytical filtering in `[002_analytical_views.py:upgrade]` and `[003_dashboard_indexes.py:upgrade]`: `v_guild_turnover_dominance`, `v_race_balance_ch_vs_eu`, and partial index `idx_scrape_runs_server_latest` only consider `scrape_runs` with `status IN ('SUCCESS', 'PARTIAL_SUCCESS')`.

## Side Effects
- Executes raw PostgreSQL 16 DDL and DML statements via `alembic.op.execute`.
- `[001_initial_schema.py:upgrade]` installs the `pgcrypto` database extension (not removed by `[001_initial_schema.py:downgrade]`), mutates database schema objects (tables, partitions, constraints, indexes), and inserts seed rows into `servers`.
- `[002_analytical_views.py:upgrade]` creates or replaces 5 SQL views in the target database schema; `[002_analytical_views.py:downgrade]` drops those 5 views.
- `[003_dashboard_indexes.py:upgrade]` creates partial index `idx_scrape_runs_server_latest` on `scrape_runs`; `[003_dashboard_indexes.py:downgrade]` drops that index.
- `[004_seed_rsilkroad.py:upgrade]` inserts the `rsilkroad` seed row into `servers`; `[004_seed_rsilkroad.py:downgrade]` deletes that row from `servers`.

## Verification
- Run: `uv run --no-sync pytest`
