# Flows (`src/flows`)

## Overview
Prefect 3 orchestration layer for the SRO Ranking Scraper. This package defines atomic Prefect tasks for database partition management, per-server scraping, and quarantine retention cleanup (`tasks.py`), along with scheduled, post-Fortress War, and manual Prefect flows, deployment builders, and the CLI entrypoint (`orchestrator.py`).

## Interface
- `[orchestrator.py:VALID_MANUAL_TARGETS]`: Allowed `--target` CLI values (`("all", "chronicle", "electus", "play_golden")`).
- `[orchestrator.py:compute_post_fw_cron]`: Computes a weekly cron expression 30 minutes after a server's `fw_end_time_utc` on `fw_day_of_week` (`0=Sunday .. 6=Saturday`) with day/week rollover.
- `[orchestrator.py:get_active_servers]`: Asynchronously loads all `Server` rows where `is_active` is `True`, ordered deterministically by `Server.id`.
- `[orchestrator.py:scheduled_12h_flow]`: Prefect flow (`scheduled-12h-flow`) that ensures monthly partitions, scrapes all active servers with per-server failure isolation (`trigger_type="scheduled_12h"`), and purges expired quarantine files.
- `[orchestrator.py:post_fw_flow]`: Prefect flow (`post-fw-flow`) that ensures monthly partitions, executes a single-server scrape (`trigger_type="post_fw_extra"`), and—on `SUCCESS`/`PARTIAL_SUCCESS` when `discord_community_webhook_url` is configured—loads a read-only `PostFWComparison` and dispatches a best-effort community Discord summary after engine disposal.
- `[orchestrator.py:manual_scrape_flow]`: Prefect flow (`manual-scrape-flow`) that ensures monthly partitions and runs an immediate scrape (`trigger_type="manual"`) for `"all"` active servers (isolated) or one `server_id`.
- `[orchestrator.py:build_deployments]`: Builds `RunnerDeployment` definitions for `scheduled-12h` (`0 0,12 * * *`) and per-server `post-fw-{server.id}` schedules.
- `[orchestrator.py:serve_deployments]`: Loads active servers from PostgreSQL, constructs deployments, and starts `prefect.serve`.
- `[orchestrator.py:build_parser]` / `[orchestrator.py:main]`: CLI parser and entrypoint (`python -m src.flows.orchestrator`) accepting `[orchestrator.py:--mode]` (`serve` | `manual`) and `[orchestrator.py:--target]` (default `"all"`).
- `[tasks.py:ensure_partitions_task]`: Prefect task (`ensure-partitions`, `retries=0`) that creates current and next UTC monthly partitions for snapshot tables via `ensure_monthly_partitions`.
- `[tasks.py:scrape_target_task]`: Prefect task (`scrape-target`, `retries=0`) that resolves the server scraper via `get_scraper`, executes `scraper.run(session, trigger_type=...)`, disposes the DB engine, and invokes `notify_operational_failure` (best-effort) before returning `ScrapeRunSummary`.
- `[tasks.py:purge_old_quarantine_task]`: Prefect task (`purge-old-quarantine`, `retries=0`) that deletes `*.html.gz` files older than `retention_days` under the quarantine directory and returns the count removed.
- Subdirectories: None (leaf package; `__init__.py` is an empty package marker).

## Invariants
- **Prefect Home Guard Before Import**: Both `[orchestrator.py:_ensure_accessible_prefect_home]` and `[tasks.py:_ensure_accessible_prefect_home]` execute at module import time before importing `prefect`, ensuring `PREFECT_HOME` falls back to `<tempdir>/sro_ranking_scraper_prefect_home` if `~/.prefect/profiles.toml` raises `OSError`.
- **Zero Automatic Task Retries**: All Prefect tasks (`[tasks.py:ensure_partitions_task]`, `[tasks.py:scrape_target_task]`, `[tasks.py:purge_old_quarantine_task]`) are configured with `retries=0`; circuit-breaker and scrape failure policies are handled inside the scraper rather than via Prefect retries.
- **Partitions Before Scraping**: Every flow (`[orchestrator.py:scheduled_12h_flow]`, `[orchestrator.py:post_fw_flow]`, `[orchestrator.py:manual_scrape_flow]`) invokes `[tasks.py:ensure_partitions_task]` before executing any `[tasks.py:scrape_target_task]`.
- **Per-Server Failure Isolation in Multi-Target Runs**: `[orchestrator.py:scheduled_12h_flow]` and `[orchestrator.py:manual_scrape_flow]` (when `target == "all"`) call `[tasks.py:scrape_target_task]` with `return_state=True`, recording `"FAILED:<state.name>"` on failure so one failing target does not abort subsequent servers.
- **Deterministic Ordering & Engine Cleanup**: `[orchestrator.py:get_active_servers]` orders results by `Server.id`, and every async DB helper/task (`[orchestrator.py:get_active_servers]`, `[orchestrator.py:post_fw_flow]` comparison read, `[tasks.py:ensure_partitions_task]`, `[tasks.py:scrape_target_task]`) disposes its SQLAlchemy engine in a `finally` block before any outbound notification call.
- **Post-Persistence Notification Isolation**: `[tasks.py:scrape_target_task]` invokes `notify_operational_failure` only after `engine.dispose()`, and `[orchestrator.py:post_fw_flow]` catches comparison DB read errors and invokes `notify_post_fw_summary` only after `engine.dispose()`, guaranteeing Discord or read failures never alter or abort the returned `ScrapeRunSummary`.
- **Input Validation**: `[orchestrator.py:compute_post_fw_cron]` raises `ValueError` unless `0 <= fw_day_of_week <= 6`; `[tasks.py:purge_old_quarantine_task]` raises `ValueError` if `retention_days < 0`.

## Side Effects
- **Environment & Filesystem Bootstrap**: `[orchestrator.py:_ensure_accessible_prefect_home]` and `[tasks.py:_ensure_accessible_prefect_home]` may create `<tempdir>/sro_ranking_scraper_prefect_home` and mutate `os.environ["PREFECT_HOME"]` at import time.
- **Database Reads & DDL/DML**: `[orchestrator.py:get_active_servers]` queries PostgreSQL `servers`; `[orchestrator.py:post_fw_flow]` opens a short read-only session to run `load_post_fw_comparison` against `servers`, `scrape_runs`, `guild_snapshots`, and `guilds`; `[tasks.py:ensure_partitions_task]` creates monthly table partitions and commits (or rolls back on error); `[tasks.py:scrape_target_task]` delegates network scraping and snapshot/run persistence to the server's scraper implementation.
- **Outbound Discord Webhooks**: `[tasks.py:scrape_target_task]` may dispatch operational failure alerts via `notify_operational_failure`, and `[orchestrator.py:post_fw_flow]` may dispatch post-FW summaries via `notify_post_fw_summary` (both best-effort and no-op when unconfigured).
- **Quarantine Disk Deletion**: `[tasks.py:purge_old_quarantine_task]` recursively unlinks expired `*.html.gz` files under `quarantine_dir`.
- **Process / Prefect Runner**: `[orchestrator.py:serve_deployments]` registers deployments and blocks serving scheduled runs via `prefect.serve`; flows configure global structured logging via `configure_logging`.

## Verification
- Run: `pytest`
