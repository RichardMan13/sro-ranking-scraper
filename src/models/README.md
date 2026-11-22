# Models

## Overview
The `src/models` package defines the data contracts and persistence schema for the SRO Ranking Scraper. It separates external scrape validation from database storage across two modules: `[pydantic_schemas.py]` provides Pydantic v2 ingestion schemas, domain status/trigger constants, and field normalizers for raw scraped ranking records, while `[db_models.py]` defines the SQLAlchemy 2.0 declarative ORM models for PostgreSQL 16 (configuration, audit logs, server-scoped dimensions, and monthly range-partitioned append-only snapshot fact tables). `[__init__.py]` is an empty package marker without re-exports.

## Interface
- **SQLAlchemy ORM Models (`db_models.py`)**
  - `[db_models.py:Base]`: Declarative base (`DeclarativeBase`) shared by all ORM models.
  - `[db_models.py:Server]`: Maps to `servers`; stores target server metadata, Fortress War schedule (`fw_day_of_week`, `fw_end_time_utc`), `min_records_expected`, and persistent `circuit_breaker_until`.
  - `[db_models.py:ScrapeRun]`: Maps to `scrape_runs`; audit log for every execution including `status`, counts, `execution_time_ms`, quarantine file metadata (`quarantine_file_path`, `quarantine_sha256`, `quarantine_size_bytes`), and `error_details` (`JSONB`).
  - `[db_models.py:Character]`: Maps to `characters`; server-scoped character dimension (`server_id`, `char_name`, `race`, `first_seen_at`, `last_seen_at`).
  - `[db_models.py:Guild]`: Maps to `guilds`; server-scoped guild dimension (`server_id`, `guild_name`, `master_char_name`, `first_seen_at`, `last_seen_at`, `is_active`).
  - `[db_models.py:CharacterSnapshot]`: Maps to `character_snapshots`; append-only temporal fact table for player rankings with typed columns and `extra_metrics` (`JSONB`).
  - `[db_models.py:GuildSnapshot]`: Maps to `guild_snapshots`; append-only temporal fact table for guild rankings with typed columns and `extra_metrics` (`JSONB`).
- **Pydantic Schemas & Constants (`pydantic_schemas.py`)**
  - `[pydantic_schemas.py:MAX_ERROR_RATE]` (`0.05`) and `[pydantic_schemas.py:MIN_GUILD_RECORDS]` (`5`): Validation threshold constants.
  - `[pydantic_schemas.py:ScrapeStatus]` / `[pydantic_schemas.py:CANONICAL_STATUSES]`: Canonical `scrape_runs.status` values (`SUCCESS`, `PARTIAL_SUCCESS`, `SCHEMA_DRIFT`, `LOW_COUNT_QUARANTINE`, `BLOCKED_CHALLENGE`, `TARGET_MAINTENANCE`, `HTTP_ERROR`).
  - `[pydantic_schemas.py:TriggerType]` / `[pydantic_schemas.py:CANONICAL_TRIGGER_TYPES]`: Canonical run triggers (`scheduled_12h`, `post_fw_extra`, `manual`).
  - `[pydantic_schemas.py:ErrorDetails]`: Structured error payload serialized into `scrape_runs.error_details`.
  - `[pydantic_schemas.py:RawPlayerRankingItem]`: Validated player ranking row with `mode="before"` normalizers for integers, floats, empty placeholders, race, and job type.
  - `[pydantic_schemas.py:RawJobRankingItem]`: Validated job ranking row used for in-memory enrichment of player snapshots.
  - `[pydantic_schemas.py:RawGuildRankingItem]`: Validated guild ranking row with `mode="before"` normalizers for counts, points, and empty placeholders.
- **Subdirectories**: None.

## Invariants
- **Canonical Status Alignment**: `[db_models.py:ScrapeRun]` enforces `ck_scrape_runs_status` matching `[pydantic_schemas.py:CANONICAL_STATUSES]` and `[pydantic_schemas.py:ScrapeStatus]`; any status change must be kept in sync across both modules and Alembic migrations.
- **Append-Only Partitioned Fact Tables**: `[db_models.py:CharacterSnapshot]` and `[db_models.py:GuildSnapshot]` are partitioned via `postgresql_partition_by="RANGE (created_at)"` and require composite primary keys `(id, created_at)` (`pk_character_snapshots`, `pk_guild_snapshots`) and unique constraints including `created_at` (`uq_char_snapshot_run`, `uq_guild_snapshot_run`).
- **Dimension Uniqueness & Race Constraints**: `[db_models.py:Character]` enforces `uq_char_per_server` on `(server_id, char_name)` and `ck_characters_race` (`'CH'`, `'EU'`, `'UNK'`); `[db_models.py:Guild]` enforces `uq_guild_per_server` on `(server_id, guild_name)`.
- **Strict Required Field Normalization**: In `[pydantic_schemas.py:RawPlayerRankingItem]`, `[pydantic_schemas.py:RawJobRankingItem]`, and `[pydantic_schemas.py:RawGuildRankingItem]`, required integer fields (`rank >= 1`, `level` in `1..140`, `job_level` in `1..140`) use `[pydantic_schemas.py:_clean_required_int]` and `[pydantic_schemas.py:_reject_bool]` to reject booleans, non-integer floats, and empty/non-numeric strings.
- **Placeholder & Locale Normalization**: `[pydantic_schemas.py:_normalize_empty_strings]` maps `[pydantic_schemas.py:_EMPTY_PLACEHOLDERS]` (`""`, `"-"`, `"None"`, `"No Name"`, `"XXXX"`, `"N/A"`) to `None`; `[pydantic_schemas.py:_normalize_race]` normalizes to `"CH"`/`"EU"` or `None` (including when ambiguous); `[pydantic_schemas.py:_normalize_job_type]` maps English/Portuguese labels via `[pydantic_schemas.py:_JOB_TYPE_MAPPING]`; `[pydantic_schemas.py:_clean_int_numbers]` and `[pydantic_schemas.py:_clean_float_numbers]` handle thousand separators, slash suffixes, unit suffixes, and finite-value checks.

## Side Effects
- None at runtime: this package performs no network I/O, disk I/O, or database queries.
- Importing `[db_models.py]` registers all ORM table definitions onto `[db_models.py:Base]` (`Base.metadata`) in memory for SQLAlchemy and Alembic reflection.

## Verification
- Run: `pytest`
