## What this proposes

This document proposes v2.0 of the SRO Ranking Scraper, adding an asynchronous, best-effort Discord webhook notification layer inside the existing `scraper-worker` container with no new containers, Python dependencies, exposed ports, or PostgreSQL schema migrations. It introduces a single module (`src/core/notifications.py`) and two optional `SecretStr`-validated HTTPS webhook settings in `src/core/config.py`: an operational channel (`DISCORD_OPS_WEBHOOK_URL`) and a community channel (`DISCORD_COMMUNITY_WEBHOOK_URL`). After `scrape_target_task` commits and closes its database session, PT-BR operational alerts are dispatched for four critical statuses (`SCHEMA_DRIFT`, `LOW_COUNT_QUARANTINE`, `BLOCKED_CHALLENGE`, `HTTP_ERROR`) across all triggers (`scheduled_12h`, `post_fw_extra`, `manual`). After `post_fw_flow` succeeds (`SUCCESS` or `PARTIAL_SUCCESS`), a read-only query compares the new `guild_snapshots` run against a prior valid baseline from the last 24 hours and posts a PT-BR summary of fortress ownership changes and Top 5 guild rank deltas to the community channel. All webhook calls use a strict <=10s timeout with zero retries, neutralize mentions via `"allowed_mentions": {"parse": []}`, suppress webhook URLs from logs and errors, and isolate failures so notification errors never fail a scraping task or flow.

## How the existing system works, per this document

Per this document, SRO Ranking Scraper v1.0 runs inside a `scraper-worker` container (capped at 512MB RAM) orchestrated by Prefect 3 (`prefect-server`, backed by SQLite) and persists time-series ranking data in PostgreSQL 16. Three Prefect flows (`scheduled_12h_flow` on a 12-hour cadence, `post_fw_flow` running 30 minutes after Fortress War ends, and `manual_scrape_flow` invoked via `--mode manual`) call `scrape_target_task`, which runs `BaseRankingScraper.run()` under an `asyncio.timeout(180.0)` budget and returns a `ScrapeRunSummary`. Execution metadata is audited in `scrape_runs`, guild rankings and fortress ownership are stored in `guild_snapshots` (partitioned by `RANGE (created_at)`) alongside `guilds` and `servers`, and character rankings live in `character_snapshots`. On failures such as HTML layout drift, low record counts, or Cloudflare Turnstile challenges, the scraper quarantines raw HTML locally as `.html.gz` files and, when applicable, sets a 6-hour `circuit_breaker_until` cooldown. Logging uses `structlog` JSON output whose per-run context is cleared in `BaseRankingScraper.run()`'s `finally` block.

---

## Findings

### 1. Undefined `ScrapeRunSummary` contract and internal module locations
* **Severity:** Major
* **Affected Scope:** Technical Plan §§ 1, 3, 4A, 6 (lines 106, 184–189, 203–214, 266–318)
* **Discrepancy Description & Rationale:** The design relies directly on `ScrapeRunSummary` and its nested `error_details` object (`error_details.failed_category`, `error_details.reason`, plus `quarantine_file_path`, `circuit_breaker_until`, `records_extracted`, `validation_errors_count`, `execution_time_ms`, `http_status_code`, `trigger_type`, `status`, `run_id`, `server_id`), as well as existing internal helpers (`get_scraper`, `create_engine_from_url`, `create_session_factory`, `ensure_partitions_task`, `configure_logging`, `get_logger`, `clear_run_context`) and ORM models (`ScrapeRun`, `Server`, `GuildSnapshot`, `Guild`). None of these are defined inline or linked to their defining files/modules, so a fresh reader cannot verify the contract of `ScrapeRunSummary`/`error_details` or locate these dependencies without searching the repository.

### 2. Unexplained `±5 minutes` partition-pruning window assumption
* **Severity:** Major
* **Affected Scope:** Technical Plan § 5, item 3 — "Consulta Unificada em `guild_snapshots` com Partition Pruning" (line 257)
* **Discrepancy Description & Rationale:** The query bounds `GuildSnapshot.created_at` to `[target_run_at - 5 min, target_run_at + 5 min]`, where `target_run_at` is `ScrapeRun.created_at`. The document does not explain when `ScrapeRun.created_at` is stamped relative to `GuildSnapshot.created_at` (e.g., at run start vs. final commit). Because `BaseRankingScraper.run()` allows up to 180 seconds (3 minutes) for fetching (line 69), a naive reader cannot verify why a `±5 minutes` window is guaranteed to encompass every row of a run without silently dropping data on slow runs.

### 3. Undefined project-specific status values, database views, and tables
* **Severity:** Major
* **Affected Scope:** Non-Goals (lines 38–39), Alternatives Considered (lines 335, 350), Technical Plan § 5 (lines 245, 258–259)
* **Discrepancy Description & Rationale:** Several project-specific terms are referenced without definition or link:
  * `TARGET_MAINTENANCE` (line 38) is never defined; `SKIPPED_CIRCUIT_BREAKER` (line 38) is only implied 290 lines later (line 328); and `PARTIAL_SUCCESS` (lines 16, 29, 38) is not explained until line 223 (`<= 5% de erros de validação`).
  * `v_guild_turnover_dominance` (lines 39, 350) and "as 5 views analíticas" (line 335) assume prior institutional knowledge without briefly explaining what they are or linking to their definitions.
  * The `guilds` table (lines 188, 258) and `servers.display_name` (line 245) appear mid-document without a brief schema overview or link to the existing data model.

### 4. Unexplained exception-isolation asymmetry between `scrape_target_task` and `post_fw_flow`
* **Severity:** Major
* **Affected Scope:** Technical Plan § 6 (lines 282, 305–314) & Non-Functional Requirements § 3 (line 391)
* **Discrepancy Description & Rationale:** NFR § 3 guarantees that unexpected payloads or Discord failures never propagate exceptions to `scrape_target_task` or `post_fw_flow`. In `post_fw_flow` (lines 305–314), `notify_post_fw_summary` is wrapped in `try/except Exception`, whereas `scrape_target_task` (line 282) awaits `notify_operational_failure` without a `try/except` block. While `send_discord_webhook` catches `Exception` (lines 186, 327), the document does not state whether `notify_operational_failure` (including `build_ops_alert_payload`) catches all exceptions internally, nor why the outer `try/except` is needed in `post_fw_flow` (presumably for the DB read/engine lifecycle) but omitted in `scrape_target_task`.

### 5. Undefined domain acronyms and internal process jargon
* **Severity:** Nit
* **Affected Scope:** Title & The Problem (lines 1, 20), Payload Formatting (line 236), Alternatives Considered (line 362), Footer (line 478)
* **Discrepancy Description & Rationale:**
  * `SRO` (lines 20, 203, 215) is never expanded or briefly defined for a reader unfamiliar with the domain (nor is "Fortress War" briefly contextualized beyond being a scheduled event).
  * `IP` in `{total_item_points:,.0f} IP` (line 236) is not explicitly defined as an abbreviation for Item Points.
  * `egm-zoom-out` (line 362) and `EGM` (line 478) are internal methodology terms used without definition or link.

### 6. Unlinked project references and unexplained ADR numbering
* **Severity:** Nit
* **Affected Scope:** Technical Plan § 3 (line 131), Decision Log (lines 368–374), Verification Plan (line 457)
* **Discrepancy Description & Rationale:**
  * `AGENTS.md` is cited three times (lines 131, 374, 457) without a link or brief explanation of its role in the repository.
  * The Decision Log starts at `ADR-007` (line 370) without mentioning or linking to where `ADR-001` through `ADR-006` live.

### 7. Undocumented dictionary key/value orientation in `PostFWComparison`
* **Severity:** Nit
* **Affected Scope:** Technical Plan § 3 — "Estruturas de Dados Tipadas" (lines 175–176)
* **Discrepancy Description & Rationale:** `current_fortresses: dict[str, str]` and `baseline_fortresses: dict[str, str]` do not state whether the mapping is `{fortress_name: guild_name}` or `{guild_name: fortress_name}`. A reader only learns the orientation 85 lines later from `setdefault(fortress_held, guild_name)` (line 260). Adding inline comments or type aliases would make the dataclass self-contained.
