# Goldfish Annotated Review Report

**Status:** resolved
**Target:** `docs/designs/001_sro_ranking_scraper_pipeline.md` (`Lines 1-358`)

## Prioritized Findings & AI Suggestions

### 1. PostgreSQL Declarative Partitioning Primary Key Violation [`egm-design`] - Severity: Blocker
*   **Affected Scope:** [Lines 264-277](docs/designs/001_sro_ranking_scraper_pipeline.md#L264-L277) (Section 5: Modelagem de Dados Relacional & Temporal)
*   **Discrepancy Description:** Declarative partitioning by range on `scraped_at` with `id (BIGINT IDENTITY)` as sole primary key fails in PostgreSQL 11+. Any primary key or unique constraint must include the partition key `scraped_at`.
*   **Rationale:** PostgreSQL triggers `ERROR: unique constraint on partitioned table must include all partitioning columns` during DDL migration.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:** Essential database schema fix required for valid Alembic migrations and table partition creation in PostgreSQL 16.
*   ***Proposed Edit:***
    *   Location: Section 5 (`character_snapshots` & `guild_snapshots` DDL)
    *   Change: Define composite primary keys: `PRIMARY KEY (id, scraped_at)` and unique constraint `CONSTRAINT uq_char_snapshot_run UNIQUE (scrape_run_id, character_id, scraped_at)`.

---

### 2. Omission of `xp` in Pydantic Schema vs. DB Schema & Downstream Views [`egm-design`] - Severity: Blocker
*   **Affected Scope:** [Lines 137-163](docs/designs/001_sro_ranking_scraper_pipeline.md#L137-L163) (Section 4: `RawPlayerRankingItem`)
*   **Discrepancy Description:** `RawPlayerRankingItem` omits `xp`, while `character_snapshots` and downstream views (`v_character_activity_deltas`, `v_bot_vs_donor_classification`) require `xp` to calculate XP deltas and bot persistence ratios.
*   **Rationale:** Without `xp` in the extraction model, raw character XP cannot be validated or ingested into PostgreSQL.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:** `xp` is a core metric for monitoring level progression and activity deltas.
*   ***Proposed Edit:***
    *   Location: Section 4 (`RawPlayerRankingItem`)
    *   Change: Add `xp: Optional[int] = Field(default=None, ge=0)` and include `xp` in the `clean_int_numbers` validator.

---

### 3. `race` Field Omitted from Database Model [`egm-design`] - Severity: Blocker
*   **Affected Scope:** [Lines 265-271](docs/designs/001_sro_ranking_scraper_pipeline.md#L265-L271) (Section 5: `character_snapshots` & `characters`)
*   **Discrepancy Description:** `RawPlayerRankingItem` extracts `race`, and `v_race_balance_ch_vs_eu` aggregates by race, but `race` is not persisted in `character_snapshots` or `characters`.
*   **Rationale:** The Chinese vs. European race balance view cannot be populated if `race` is dropped during persistence.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:** Race is immutable per character and critical for meta-balance analytics.
*   ***Proposed Edit:</a>
    *   Location: Section 5 (`characters` dimension and `character_snapshots` fact table)
    *   Change: Add `race VARCHAR(4) CHECK (race IN ('CH', 'EU', 'UNK'))` to both `characters` and `character_snapshots`.

---

### 4. Ephemeral Circuit Breaker State Loss Across Flow Runs [`egm-design`] - Severity: Major
*   **Affected Scope:** [Lines 122-126](docs/designs/001_sro_ranking_scraper_pipeline.md#L122-L126) (Section 3: Lógica do Circuit Breaker)
*   **Discrepancy Description:** Scheduled container flows terminate between executions. An in-memory circuit breaker loses cooldown state on exit, leading to immediate retry on the next run.
*   **Rationale:** To prevent burning host IP addresses, the circuit breaker cooldown must persist in the database.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:** Persistent state ensures cooldown enforcement across independent Prefect worker runs.
*   ***Proposed Edit:***
    *   Location: Section 3 and Section 5 (`servers` table DDL)
    *   Change: Add `circuit_breaker_until TIMESTAMPTZ DEFAULT NULL` to `servers`. Before each run, the scraper queries `servers.circuit_breaker_until`; if `NOW() < circuit_breaker_until`, the target is skipped immediately.

---

### 5. Undefined Dimension Upsert Mechanism & Unverifiable Name Change Resolution [`egm-design`] - Severity: Major
*   **Affected Scope:** [Lines 256-260](docs/designs/001_sro_ranking_scraper_pipeline.md#L256-L260) (Section 5: Dimensões)
*   **Discrepancy Description:** Raw items contain names (`char_name`, `guild_name`), but snapshots require UUID foreign keys. The resolution strategy is not specified. Additionally, public SRO rankings lack upstream player IDs, making rename tracking across "Name Change Scrolls" impossible without heuristics.
*   **Rationale:** Engineers need the exact SQL/ORM pattern to resolve foreign keys before bulk-inserting snapshots. Scope must be grounded in what public rankings actually expose.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:** Defining explicit dimension upserts and scoping identity to `(server_id, char_name)` resolves the ambiguity.
*   ***Proposed Edit:***
    *   Location: Section 5 (Modelagem)
    *   Change: Add explicit upsert specification: `INSERT INTO characters (server_id, char_name, race, first_seen_at, last_seen_at) ... ON CONFLICT (server_id, char_name) DO UPDATE SET last_seen_at = EXCLUDED.last_seen_at RETURNING id`. Clarify under limitations that v1.0 scopes character identity to `(server_id, char_name)`.

---

### 6. Missing Pydantic Schema Specification for `RawGuildRankingItem` [`egm-structural`] - Severity: Major
*   **Affected Scope:** [Lines 132-243](docs/designs/001_sro_ranking_scraper_pipeline.md#L132-L243) (Section 4: Schemas Pydantic)
*   **Discrepancy Description:** Section 4 omits the code definition for `RawGuildRankingItem`, despite guild scraping being a primary goal.
*   **Rationale:** The contract for guild rankings must be fully articulated alongside player and job rankings.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:** Completes the contract specification for all ranking types.
*   ***Proposed Edit:***
    *   Location: Section 4
    *   Change: Add complete `RawGuildRankingItem` definition with fields `rank`, `guild_name`, `master_name`, `members_count`, `total_item_points`, `fortress_held`, and `extra_metrics`.

---

### 7. Overly Strict `race` Literal and Missing Number Sanitizer on `item_points` [`egm-design`] - Severity: Major
*   **Affected Scope:** [Lines 147-152](docs/designs/001_sro_ranking_scraper_pipeline.md#L147-L152) (Section 4)
*   **Discrepancy Description:** `race` lacks a pre-validator to handle variations like `"Chinese"` or `"CH"`. `item_points` lacks pre-sanitization for formatted numbers (e.g. `"1,250.00"`).
*   **Rationale:** Unsanitized string formats will trigger validation errors and cause unnecessary quarantine triggers.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:** Robust input normalization prevents false schema drift rejections.
*   ***Proposed Edit:***
    *   Location: Section 4 (`RawPlayerRankingItem`)
    *   Change: Add `@field_validator("race", mode="before")` normalizing case/words and `@field_validator("item_points", mode="before")` stripping thousands separators.

---

### 8. Unspecified Trigger Mechanism for Extraordinary Post-FW Runs [`egm-structural`] - Severity: Major
*   **Affected Scope:** [Lines 26, 41-42, 463-465](docs/designs/001_sro_ranking_scraper_pipeline.md#L26)
*   **Discrepancy Description:** Fortress War schedules vary by server, but the triggering mechanism was not concretely specified.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:** Prefect 3 supports independent deployments per server.
*   ***Proposed Edit:***
    *   Location: Section 1 and Section 3
    *   Change: Clarify that post-FW runs are individual Prefect deployments scheduled per server using the `fw_day_of_week` and `fw_end_time_utc` stored in the `servers` table (firing 30 minutes after FW end).

---

### 9. Database Driver Harmonization (`asyncpg` vs. `psycopg`) [`egm-structural`] - Severity: Nit
*   **Affected Scope:** [Line 347](docs/designs/001_sro_ranking_scraper_pipeline.md#L347) vs [Line 383](docs/designs/001_sro_ranking_scraper_pipeline.md#L383)
*   **Discrepancy Description:** Text references `asyncpg` while `pyproject.toml` references `psycopg[binary]`.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:** Avoid driver confusion during installation.
*   ***Proposed Edit:***
    *   Location: Section NFR and `pyproject.toml`
    *   Change: Add `asyncpg>=0.29.0` to dependencies and standardize async engine URL to `postgresql+asyncpg://`.

---

### 10. Standardization of Quarantine Storage Path Notation [`egm-structural`] - Severity: Nit
*   **Affected Scope:** Lines 53, 81, 248, 330
*   **Discrepancy Description:** Path is formatted inconsistently across diagrams and text.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:** Clarity and consistency.
*   ***Proposed Edit:***
    *   Location: Document-wide
    *   Change: Standardize to `./quarantine/{server_id}/{run_id}.html.gz` (mapped to `/app/quarantine/{server_id}/{run_id}.html.gz` in container).

---

### 11. Complete Status Enum Definition for `scrape_runs.status` [`egm-structural`] - Severity: Nit
*   **Affected Scope:** Section 5 (Tabela de Auditoria)
*   **Discrepancy Description:** Valid status enum values are mentioned ad-hoc.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:** Strict contract definition for audit logging.
*   ***Proposed Edit:***
    *   Location: Section 5
    *   Change: List valid statuses: `'SUCCESS'`, `'PARTIAL_SUCCESS'`, `'SCHEMA_DRIFT'`, `'LOW_COUNT_QUARANTINE'`, `'BLOCKED_CHALLENGE'`, `'TARGET_MAINTENANCE'`, `'HTTP_ERROR'`.

---

### 12. Level Cap Awareness in Bot Classification View [`egm-inventive-catalyst`] - Severity: Major
*   **Affected Scope:** Section 6 (`v_bot_vs_donor_classification`)
*   **Discrepancy Description:** Max-level characters earn zero XP (`delta_xp = 0`), which causes endgame active players to be misclassified as inactive.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:** High-ranking players on mature servers are almost all at the level cap.
*   ***Proposed Edit:</a>
    *   Location: Section 6
    *   Change: In `v_bot_vs_donor_classification`, when character level equals maximum server level (or `level >= 130`), evaluate active grind intervals using `(delta_xp > 0 OR delta_job_exp > 0 OR delta_job_points > 0 OR delta_item_points > 0)`.
