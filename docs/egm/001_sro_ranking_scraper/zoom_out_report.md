# Goldfish Review Report: Zoom Out & Inventive Catalyst

**Target:** `docs/designs/001_sro_ranking_scraper_pipeline.md` (`Lines 1-508`)

## Reviewer Status

| Reviewer | Status |
| --- | --- |
| `egm-zoom-out` | completed |
| `egm-inventive-catalyst` | completed |

## Summary of Findings

* 🔴 **Blocker:** 2
* 🟡 **Major:** 4
* 🟢 **Nit:** 3

---

## Prioritized Findings (Blockers -> Major -> Nits)

### 1. Entity Identity vs. Name Change Scrolls Tracking Paradox - Severity: Blocker
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `docs/designs/001_sro_ranking_scraper_pipeline.md#L255-L277` (Section 5: Modelagem Relacional & Temporal) and `concepts_brief.md#L19`
* **Discrepancy Description**: The document asserts that the pipeline maintains persistent character identity across "Name Change Scrolls" by separating an immutable surrogate UUID in `characters` from the displayed name. However, public Silkroad Online ranking tables only expose `char_name`, `rank`, `level`, `item_points`, and `guild_name`—never an internal player ID (`CharID`) or account ID. When a player uses a Name Change Scroll, the ingestion engine receives an unfamiliar `char_name` with no primary key or deterministic identity signal to link it to the existing UUID, inevitably spawning a new character UUID and corrupting time-series continuity (`LAG()` history).
* **Rationale & Recommendation**: Public web scraping cannot deterministically resolve character renames without an external persistent key. The design must either:
  1. Explicitly scope character identity in v1.0 to `(server_id, char_name)` with `first_seen_at` and `last_seen_at`, acknowledging that name changes are recorded as character churn; OR
  2. Specify a concrete heuristic reconciliation engine (e.g. matching sudden disappearance of Name A with appearance of Name B having identical XP, Item Points, and Guild trajectory) if persistent tracking across renames is an unnegotiable requirement.

---

### 2. Max-Level XP Delta Stagnation in Bot vs Donor Classification - Severity: Blocker
* **Reviewer Lens:** `egm-inventive-catalyst`
* **Affected Scope:** `docs/designs/001_sro_ranking_scraper_pipeline.md#L287-L293` (Section 6: Camada Analítica SQL) and `docs/designs/001_sro_ranking_scraper_pipeline.md#L402` (Section 6)
* **Discrepancy Description**: `v_bot_vs_donor_classification` classifies characters as `SUSPECTED_BOT_24_7` only if their average XP rate is `> 1000/h` and XP rate standard deviation is `< 35%`. In Silkroad Online private servers, active ranking players routinely reach the server Level Cap (e.g., Level 80, 100, 110, or 140). Once capped, character XP gains freeze completely (`delta_xp = 0`). Consequently, virtually all active endgame players in the Top 100 will fail the bot criteria and either trigger zero-division errors in coefficient of variation calculations or be misclassified as inactive.
* **Rationale & Recommendation**: Incorporate level-cap awareness into the analytical view:
  1. When `level == max_server_level`, base grind activity on `delta_job_exp`, `delta_job_points`, or `delta_item_points` rather than raw character XP.
  2. Replace hardcoded absolute thresholds (`> 1000/h`, which vary drastically across different server level caps) with relative percentiles or non-zero progression activity.

---

### 3. Analytical Query Scalability & LAG() Window Overhead on Partitioned Tables - Severity: Major
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `docs/designs/001_sro_ranking_scraper_pipeline.md#L279-L294` (Sections 5 & 6)
* **Discrepancy Description**: `character_snapshots` is partitioned monthly and will accumulate millions of rows over time. Querying `v_character_activity_deltas` executes `LAG(...) OVER (PARTITION BY server_id, character_id ORDER BY scraped_at)` without any mandatory partition pruning or date boundary in the view definition. Running unconstrained window functions across multiple partitions on a PostgreSQL 16 container with `work_mem = 64MB` will trigger full table scans, heavy disk spills, and slow response times when queried by Power BI or Streamlit.
* **Rationale & Recommendation**: 
  1. Introduce incrementally refreshed Materialized Views post-run or table-valued functions requiring date filters (e.g., `WHERE scraped_at >= CURRENT_DATE - INTERVAL '30 days'`).
  2. Ensure downstream BI queries leverage partition pruning and utilize the composite B-Tree index `(server_id, character_id, scraped_at DESC)` effectively.

---

### 4. Unbounded Storage Growth and Missing Retention Lifecycle for Quarantine Volume - Severity: Major
* **Reviewer Lens:** `egm-inventive-catalyst`
* **Affected Scope:** `docs/designs/001_sro_ranking_scraper_pipeline.md#L81` (Section 1), `#L247-L250` (Section 4), `#L370` (Section 1)
* **Discrepancy Description**: The dead-letter quarantine mechanism dumps compressed `.html.gz` payloads to `./quarantine/{server_id}/{run_id}.html.gz` upon validation failure or challenge detection. If a server experiences sustained schema drift or persistent Cloudflare challenge enforcement, runs every 12 hours (plus retries) will indefinitely generate gz files without any retention TTL or volume storage quota, creating a disk exhaustion risk on the host container.
* **Rationale & Recommendation**: Define an explicit data lifecycle policy for the `./quarantine` directory in Section 4. Implement an automated cleanup routine (e.g., purge payloads older than 14 or 30 days, or cap storage at 1GB per server) either within `BaseRankingScraper` or as a dedicated Prefect maintenance task.

---

### 5. Pagination Throughput vs 60-Second Execution Budget Contradiction - Severity: Major
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `docs/designs/001_sro_ranking_scraper_pipeline.md#L344-L346` (Section Non-Functional Requirements), `#L116-L122` (Section 3)
* **Discrepancy Description**: The performance requirement dictates that each target scraping execution must not exceed 60 seconds total. However, Silkroad rankings often span 10 to 50 pages (e.g. 1,000 players at 20-50 per page). Given HTTPX sequential page fetching with necessary anti-ban jitter delays (500ms to 1.5s per request), scraping 20-50 pages will predictably exceed 60 seconds, causing premature task cancellations or timeouts.
* **Rationale & Recommendation**: Reconcile the execution budget: either increase the per-target timeout limit to 180–300 seconds, or specify asynchronous bounded concurrency (`asyncio.gather` bounded by a semaphore of 3–5 workers) for multi-page fetches, ensuring polite crawling without exceeding the timeout window.

---

### 6. Cloudflare Turnstile Permanent Stalemate on Electus Online - Severity: Major
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `docs/designs/001_sro_ranking_scraper_pipeline.md#L123-L126` (Section 3), `#L35-L36` (Section Non-Goals NG3), `#L40` (Section Open Questions Q1)
* **Discrepancy Description**: The Circuit Breaker transitions to `OPEN` for 6 hours whenever Cloudflare Turnstile is detected. Electus Online historically enforces permanent Cloudflare Turnstile / Managed Challenges on its ranking web pages. Under Non-Goal NG3 ("sem bypasses pagos ou infraestrutura pesada"), the scraper will remain permanently trapped in a cooldown/failure loop, resulting in zero data ingested for Electus.
* **Rationale & Recommendation**: Define an operational fallback strategy for persistent Turnstile targets: evaluate whether Electus exposes an unprotected internal API route (`/api/...`), support manual session token/cookie injection via `.env` (`cf_clearance`), or explicitly designate Electus as "Experimental / Best-Effort" in v1.0 goals to manage expectations.

---

### 7. False-Positive Schema Drift on Server Maintenance (HTTP 502/503/Empty Tables) - Severity: Nit
* **Reviewer Lens:** `egm-inventive-catalyst`
* **Affected Scope:** `docs/designs/001_sro_ranking_scraper_pipeline.md#L244-L246` (Section 4)
* **Discrepancy Description**: Silkroad private servers have frequent unannounced weekly maintenances during which websites return HTTP 502/503 or serve an empty HTML table (`0` rows). The current contract logic classifies any row count below `min_records_threshold` as `SCHEMA_DRIFT`, polluting the quarantine directory with maintenance pages and skewing scraper health metrics.
* **Rationale & Recommendation**: Differentiate between `TARGET_MAINTENANCE` (HTTP 502/503/521 or explicit maintenance banners) and structural `SCHEMA_DRIFT`, recording a non-erroneous maintenance status in `scrape_runs` without triggering dead-letter quarantine dumps.

---

### 8. Top-Rank Integrity Guard for Distinct Podium Styling - Severity: Nit
* **Reviewer Lens:** `egm-inventive-catalyst`
* **Affected Scope:** `docs/designs/001_sro_ranking_scraper_pipeline.md#L244-L246` (Section 4)
* **Discrepancy Description**: The drift detection rule permits up to 5% validation errors (`validation_errors / total_records <= 0.05`). In Silkroad rankings, Ranks 1 to 3 frequently have unique markup (gold/silver/bronze icons, distinct CSS classes) that differs from the rest of the table. A parser could fail on Ranks 1-3 (3% error rate), accepting the batch while dropping the most critical competitive data.
* **Rationale & Recommendation**: Implement a "Top-Rank Integrity Guard" ensuring that if Ranks 1 to 3 fail validation or parsing, the batch is flagged immediately regardless of the 5% aggregate threshold.

---

### 9. Fortress War Heterogeneity & Orchestration Scheduling - Severity: Nit
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `docs/designs/001_sro_ranking_scraper_pipeline.md#L41` (Open Questions Q2), `#L464` (Section 17)
* **Discrepancy Description**: `post_fw_extraordinary_flow` is designed as a single monolithic Sunday flow. However, Fortress War schedules often differ by server (e.g. Saturday vs Sunday, 18:00 vs 20:30 UTC).
* **Rationale & Recommendation**: Decouple the post-FW flow into per-server dynamic schedules configured via the `servers` table or distinct Prefect cron deployments per target.
