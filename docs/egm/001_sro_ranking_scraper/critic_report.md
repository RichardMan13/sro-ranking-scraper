# Critic Review Report: SRO Ranking Scraper Pipeline (v1.0)

**Target:** `docs/designs/001_sro_ranking_scraper_pipeline.md` (`Lines 1-358`)

## Reviewer Status

| Reviewer | Status |
| --- | --- |
| `egm-design` | completed |
| `egm-structural` | completed |
| `egm-style` | completed |

---

## Prioritized Findings (Blockers -> Major -> Nits)

### 1. PostgreSQL Declarative Partitioning Primary Key Violation - Severity: Blocker
* **Reviewer Lens:** `egm-design`
* **Affected Scope:** Lines 264–277 (`character_snapshots` & `guild_snapshots`), Line 355
* **Discrepancy Description**: The design specifies that `character_snapshots` and `guild_snapshots` are partitioned by range on `scraped_at` (`RANGE (scraped_at)`), while defining `id (BIGINT IDENTITY)` as the primary identifier. In PostgreSQL 11+, any unique constraint or primary key on a partitioned table **must** include all partitioning columns. Executing `PRIMARY KEY (id)` on a table partitioned by `scraped_at` triggers `ERROR: unique constraint on partitioned table must include all partitioning columns`.
* **Rationale & Recommendation**: Explicitly define composite primary keys that include the partition key: `PRIMARY KEY (id, scraped_at)` or `PRIMARY KEY (server_id, character_id, scraped_at)`. Update the DDL specification in Section 5 and migration plan 001 accordingly.

---

### 2. Omission of `xp` in Pydantic Schema vs. DB Schema & Downstream Views - Severity: Blocker
* **Reviewer Lens:** `egm-design`
* **Affected Scope:** Lines 137–163 (`RawPlayerRankingItem`), Line 267 (`character_snapshots.xp`), Lines 286–290 (`v_character_activity_deltas`, `v_bot_vs_donor_classification`)
* **Discrepancy Description**: `RawPlayerRankingItem` completely omits the `xp` / `exp` field. However, `character_snapshots` includes `xp (BIGINT)`, `v_character_activity_deltas` calculates `xp_rate_per_hour = delta_xp / hours_diff`, and `v_bot_vs_donor_classification` computes bot heuristics based on XP rate and XP rate variance. Without `xp` in `RawPlayerRankingItem`, raw character XP cannot be validated or extracted from payloads, breaking the downstream analytical views.
* **Rationale & Recommendation**: Add `xp: Optional[int] = Field(default=None, ge=0)` to `RawPlayerRankingItem` and include it in `clean_int_numbers` sanitization. Also document that servers displaying only level (without raw XP) will yield NULL XP deltas, requiring fallback metrics (e.g. level progression or item points).

---

### 3. `race` Field Omitted from Database Model - Severity: Blocker
* **Reviewer Lens:** `egm-design`
* **Affected Scope:** Line 147 (`RawPlayerRankingItem.race`), Lines 265–271 (`character_snapshots`), Line 292 (`v_race_balance_ch_vs_eu`), Line 26 (Goal G1)
* **Discrepancy Description**: `RawPlayerRankingItem` extracts and validates `race: Optional[Literal["CH", "EU"]]`, and Goal G1 along with analytical view `v_race_balance_ch_vs_eu` explicitly requires tracking Chinese vs. European race balance. However, `race` is entirely omitted from `character_snapshots` (and is not specified in the `characters` dimension table either). As specified, race data is discarded upon persistence, making `v_race_balance_ch_vs_eu` impossible to populate.
* **Rationale & Recommendation**: Add `race VARCHAR(2)` to `character_snapshots` (or persist `race` in the `characters` dimension table if race is immutable per character) so that `v_race_balance_ch_vs_eu` has access to the column.

---

### 4. Ephemeral Circuit Breaker State Loss Across Flow Runs - Severity: Major
* **Reviewer Lens:** `egm-design`
* **Affected Scope:** Lines 122–126 (Section 3: Circuit Breaker Logic), Line 407 (`src/core/config.py`)
* **Discrepancy Description**: The Circuit Breaker trips to `OPEN` with a 6-hour cooldown upon encountering Cloudflare Turnstile (`BLOCKED_CHALLENGE`). However, the document does not specify where Circuit Breaker state is persisted. Because `scraper-worker` runs discrete, scheduled Prefect flows (every 12h or post-FW) that terminate between runs or execute in isolated container processes, an in-memory circuit breaker state will be lost on container restart or flow completion.
* **Rationale & Recommendation**: Clarify that the Circuit Breaker state is persisted in PostgreSQL—either via a `circuit_breaker_until TIMESTAMPTZ` column on the `servers` table or by inspecting the latest `scrape_runs` status for that `server_id` before initiating an HTTP session.

---

### 5. Undefined Dimension Upsert Mechanism & Unverifiable Name Change Resolution - Severity: Major
* **Reviewer Lens:** `egm-design`
* **Affected Scope:** Lines 86, 101 (Mermaid Diagram), Lines 256–260 (Section 5: Dimensions), Concepts Brief Line 19
* **Discrepancy Description**: 
  1. The document specifies that raw parsed items flow into `DIM & FACT`, but does not define the dimension upsert resolution strategy (e.g. `INSERT INTO characters ... ON CONFLICT (server_id, char_name) DO UPDATE SET last_seen_at = EXCLUDED.last_seen_at RETURNING id`) needed to obtain UUIDs prior to bulk-inserting snapshots.
  2. The concepts brief claimed to resolve Name Change Scrolls via surrogate UUIDs. However, public SRO rankings only display character names without account IDs or master player IDs. If `characters` is keyed on `(server_id, char_name)`, any name change inevitably generates a new UUID.
* **Rationale & Recommendation**: 
  1. Specify the exact SQL/ORM upsert pattern for dimensions `characters` and `guilds` before snapshot insertion.
  2. Clarify that automatic tracking across Name Change Scrolls is an upstream limitation of public web rankings unless the server provides a stable profile URL/ID, and document this constraint explicitly under Assumptions/Limitations.

---

### 6. Missing Pydantic Schema Specification for `RawGuildRankingItem` - Severity: Major
* **Reviewer Lens:** `egm-structural`
* **Affected Scope:** Lines 132–243 (Section 4), Line 272 (`guild_snapshots`), Line 423 (Implementation Plan)
* **Discrepancy Description**: Section 4 provides full Python implementations for `RawPlayerRankingItem` and `RawJobRankingItem`, but omits `RawGuildRankingItem`. Given that guild scraping and `guild_snapshots` are core requirements (G1, CUJ 1, `v_guild_turnover_dominance`), the contract for raw guild records must be explicitly defined.
* **Rationale & Recommendation**: Provide the Pydantic v2 code specification for `RawGuildRankingItem` in Section 4, including fields: `rank: int`, `guild_name: str`, `master_name: Optional[str]`, `members_count: Optional[int]`, `total_item_points: Optional[float]`, `fortress_held: Optional[str]`, and `extra_metrics: dict[str, Any]`.

---

### 7. Overly Strict `race` Literal and Missing Number Sanitizer on `item_points` - Severity: Major
* **Reviewer Lens:** `egm-design`
* **Affected Scope:** Lines 147–148 (`RawPlayerRankingItem`), Lines 190–206 (`clean_int_numbers`)
* **Discrepancy Description**: 
  1. `race: Optional[Literal["CH", "EU"]]` lacks a validator. If target HTMLs contain strings like `"Chinese"`, `"European"`, `"CH"`, `"EU"`, or image alt tags, strict validation will fail.
  2. `item_points: Optional[float]` is not processed by `clean_int_numbers` or a float sanitizer. If rankings format numbers with commas or dots (e.g., `"1,450.50"` or `"1.450,50"`), Pydantic's default float coercion will raise a `ValidationError`.
  3. If these errors exceed the 5% threshold, entire batches will unnecessarily be quarantined.
* **Rationale & Recommendation**:
  1. Add a `@field_validator("race", mode="before")` that normalizes `"CHINESE" -> "CH"`, `"EUROPEAN" -> "EU"`, etc.
  2. Add a `@field_validator("item_points", mode="before")` that strips commas/formatting before float parsing.

---

### 8. Unspecified Trigger Mechanism for Extraordinary Post-FW Runs - Severity: Major
* **Reviewer Lens:** `egm-structural`
* **Affected Scope:** Lines 26 (G1), 41–42 (Q2), 463–465 (`post_fw_extraordinary_flow`)
* **Discrepancy Description**: The document highlights an extraordinary scrape 30 minutes after Fortress War (Sunday evening) and Open Question Q2 notes that FW timing varies by server. However, the architecture does not specify how Prefect triggers this flow (e.g. an independent weekly Cron schedule per server, an external webhook, or an event-driven automation in Prefect 3).
* **Rationale & Recommendation**: Specify the concrete triggering mechanism in Section 1 or Section 3: e.g., a secondary Prefect Cron schedule configured with server-specific cron expressions derived from the `servers` table metadata or environment variables.

---

### 9. Database Async Driver Discrepancy (`asyncpg` vs. `psycopg`) - Severity: Nit
* **Reviewer Lens:** `egm-structural`
* **Affected Scope:** Line 347 (Section NFR: Throughput de Banco) vs. Line 383 (`pyproject.toml`)
* **Discrepancy Description**: Line 347 states that PostgreSQL inserts occur via `SQLAlchemy 2.0 Core / asyncpg`, but Line 383 lists `psycopg[binary]>=3.1.0` in `pyproject.toml` without `asyncpg`.
* **Rationale & Recommendation**: Standardize on one async driver: either replace `asyncpg` with `psycopg3` in line 347 (`postgresql+psycopg://`), or add `asyncpg>=0.29.0` to `pyproject.toml` dependencies if using `asyncpg`.

---

### 10. Inconsistent Quarantine Storage Path Notation - Severity: Nit
* **Reviewer Lens:** `egm-structural`
* **Affected Scope:** Line 53, Line 81, Line 248, Line 330
* **Discrepancy Description**: The quarantine file path is cited with differing directory structures across the document:
  - Line 53: `/quarantine/{target}/{run_id}.html.gz`
  - Line 81: `/quarantine/{run_id}.html.gz`
  - Line 248: `./quarantine/{server_id}/{run_id}.html.gz`
  - Line 330: `/quarantine/{target}/{run_id}.html.gz`
* **Rationale & Recommendation**: Standardize to a single convention across text and diagrams, preferably `./quarantine/{server_id}/{run_id}.html.gz` matching the volume mount defined in Docker Compose.

---

### 11. Undefined Status Enum for `scrape_runs.status` - Severity: Nit
* **Reviewer Lens:** `egm-structural`
* **Affected Scope:** Lines 53, 125, 262, 504
* **Discrepancy Description**: Various statuses are mentioned ad-hoc (`SUCCESS`, `SCHEMA_DRIFT`, `BLOCKED_CHALLENGE`), but the complete set of valid enum values for `scrape_runs.status` is never formally defined in Section 5.
* **Rationale & Recommendation**: Add a table or list of all allowed status enum values (e.g., `SUCCESS`, `PARTIAL_SUCCESS`, `SCHEMA_DRIFT`, `BLOCKED_CHALLENGE`, `HTTP_TIMEOUT`, `SYSTEM_ERROR`) in the data modeling section.

---

### 12. Inconsistent Return Types and Behavior in `normalize_job_type` - Severity: Nit
* **Reviewer Lens:** `egm-style`
* **Affected Scope:** Lines 174–188 vs. Lines 220–232
* **Discrepancy Description**: In `RawPlayerRankingItem`, `normalize_job_type` returns `Optional[str]` and returns `None` for unmapped values. In `RawJobRankingItem`, `normalize_job_type` returns `str` and returns `mapping.get(val, val)`.
* **Rationale & Recommendation**: Harmonize the helper logic: if an unmapped job string is encountered in `RawJobRankingItem`, define whether it should fall back to a default, return `None`, or fail validation explicitly.

---

### 13. Mixed Language in Section Headers vs. Body Prose - Severity: Nit
* **Reviewer Lens:** `egm-style`
* **Affected Scope:** Document-wide (Lines 1–358)
* **Discrepancy Description**: Major section headers are in English (`## The Problem`, `## Goals`, `## Non-Goals`, `## Critical User Journeys (CUJs)`, `## Technical Plan (Architecture)`, `## Alternatives Considered`, `## Non-Functional Requirements`), while body text and subheadings (`### 1. High-Level Architecture & Data Flow`) are predominantly in Portuguese.
* **Rationale & Recommendation**: Ensure consistent header style across the document; maintain English headers according to standard conventions while keeping bilingual terminology clean and uniform.
