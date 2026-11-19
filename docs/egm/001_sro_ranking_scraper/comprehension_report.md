## What this proposes

This document proposes a containerized data pipeline (the SRO Ranking Scraper) built with Python 3.11, Prefect 3, and PostgreSQL 16 to extract, validate, and persist public ranking data from three private Silkroad Online servers (Chronicle SRO, Electus Online, and Play Golden). Operating on a 12-hour cadence alongside an extraordinary post-Fortress War run, the pipeline extracts HTML and JSON ranking tables using HTTPX, enforces data contracts via Pydantic v2 and minimum record count thresholds, and diverts anomalous or blocked responses to a local gzip quarantine. Validated snapshots are stored in append-only partitioned relational tables (`character_snapshots` and `guild_snapshots`), which are then transformed through native PostgreSQL window function views (`LAG()`) into hourly normalized time series to measure real player activity, retention, bot vs. donor profiles, and guild dominance.

## How the existing system works, per this document

According to the document, the existing landscape relies on public websites and API endpoints operated by private Silkroad Online servers that publish only their transient, current-state rankings without historical archives. Daily player progression, gear upgrades (Item Points), and guild roster turnovers are continuously overwritten, making long-term retention and activity analysis impossible. Furthermore, existing crawling approaches are described as fragile scripts that break silently or ingest corrupted data whenever server administrators alter HTML layouts or introduce Cloudflare Turnstile anti-bot challenges.

---

### Findings

#### 1. Missing `RawGuildRankingItem` Schema and Ambiguous Job Ranking Storage
- **Severity**: Blocker
- **Target Scope**: Section 4 (Lines 127–243), Section 5 (Lines 265–276), Section Detailed Plan Item 10 (Line 423)
- **Description**: Section 4 provides full Pydantic v2 code definitions for `RawPlayerRankingItem` and `RawJobRankingItem`, but completely omits `RawGuildRankingItem` (only mentioning it in a single line under the implementation file list). Additionally, the document does not clarify how Job rankings and Player rankings relate during ingestion: `character_snapshots` contains columns for both character level/XP and job level/exp, but `RawJobRankingItem` does not contain character level/XP, and `RawPlayerRankingItem` has optional job fields. It is undefined whether player and job rankings are scraped from distinct endpoints, whether they are joined in-memory prior to persistence, or whether they produce separate snapshot rows with NULL fields.
- **Recommendation**: Provide the complete Pydantic model for `RawGuildRankingItem` including required fields, validation rules, and threshold checks. Explicitly specify the URL/endpoint structure for each ranking type per server and define the ingestion mapping logic that reconciles player ranking data and job ranking data into `character_snapshots`.

#### 2. Unstated Identity Resolution and Dimension Foreign Key Population
- **Severity**: Major
- **Target Scope**: Section 4 (Lines 137–163), Section 5 (Lines 253–276), Section Non-Functional Requirements (Line 347)
- **Description**: Raw scraped payloads contain string identifiers (`char_name`, `guild_name`), whereas fact tables (`character_snapshots`, `guild_snapshots`) require UUID foreign keys (`character_id`, `guild_id`) linking to the dimension tables (`characters`, `guilds`). The document never describes how or when string names are resolved into UUIDs. Without specifying an upsert step (`INSERT ... ON CONFLICT DO UPDATE/NOTHING RETURNING id`) prior to snapshot insertion, an engineer cannot implement the flow tasks, and doing this naively could violate the sub-500ms bulk insert requirement (`COPY` / `bulk_insert_mappings`).
- **Recommendation**: Detail the dimension resolution workflow in Section 5 and task descriptions: before inserting fact snapshots, the pipeline must upsert unique names against `characters` and `guilds` partitioned by `server_id` to generate an in-memory lookup cache of `char_name -> character_id` and `guild_name -> guild_id`.

#### 3. Undefined Persistence for Circuit Breaker State Across Runs
- **Severity**: Major
- **Target Scope**: Section 3 (Lines 122–126), CUJ 2 (Lines 50–54), Section Detailed Plan Item 7 (Line 407)
- **Description**: The document specifies that upon encountering Cloudflare Turnstile or HTTP 403/429, the Circuit Breaker transitions to `OPEN` and suspends scraping for that specific server for a 6-hour cooldown. However, Prefect tasks run as scheduled processes, and no distributed in-memory state store (such as Redis) exists in the Docker Compose topology. The document does not state where the Circuit Breaker records the `OPEN` state and cooldown expiration timestamp so that subsequent task runs know to skip the target.
- **Recommendation**: Specify the backing storage for Circuit Breaker state. Document whether the scraper inspects the latest status and timestamp in `scrape_runs` upon startup or maintains a persistent `circuit_breaker_until` timestamp in the `servers` table.

#### 4. Undefined Domain Term "Fortress War" and Ambiguous Trigger Scheduling
- **Severity**: Major
- **Target Scope**: Overview (Lines 13–14), Open Questions Q2 (Lines 41–42), Section Detailed Plan Item 17 (Lines 464–465)
- **Description**: "Fortress War (FW)" is introduced as a critical domain event and extraordinary execution trigger, but is never defined for readers unfamiliar with Silkroad Online. Furthermore, while Q2 notes that FW schedules vary across servers and should be configured in the `servers` table, Section Detailed Plan describes `post_fw_extraordinary_flow` as a single static flow executed on Sundays. It is left unexplained how Prefect coordinates server-specific FW schedules or schedules this flow.
- **Recommendation**: Provide a brief definition of Fortress War (weekly in-game guild siege event that reallocates fortress ownership and causes major metric shifts). Clarify whether `post_fw_extraordinary_flow` is triggered via server-specific Prefect Cron schedules or dynamically scheduled by querying the `servers` table.

#### 5. Conflicting Database Driver Specifications (`asyncpg` vs `psycopg`)
- **Severity**: Major
- **Target Scope**: Non-Functional Requirements (Line 347) vs Detailed Plan Item 3 (Line 383) & Item 8 (Line 412)
- **Description**: Non-Functional Requirements state that append-only insertions should use SQLAlchemy 2.0 Core / `asyncpg` to achieve bulk insertion via PostgreSQL `COPY` in under 500ms. In contrast, `pyproject.toml` (Line 383) lists `psycopg[binary]>=3.1.0` and omits `asyncpg` entirely. If the application engine is configured for `asyncpg`, missing the driver will cause runtime import failures.
- **Recommendation**: Align driver references across all sections. Either replace `psycopg[binary]` with `asyncpg` in `pyproject.toml` and connection setup, or update Line 347 to specify `psycopg` async.

#### 6. Undefined Domain Jargon ("vSRO", "Uniques", "Item Points")
- **Severity**: Nit
- **Target Scope**: The Problem (Line 18), Non-Goals (Line 34), Section 3 (Line 121)
- **Description**: The document uses terms specific to the Silkroad private server community without explanation: "vSRO" (the underlying server file base and standard web templates), "Uniques" (rare in-game world bosses), and "Item Points" (gear score calculation).
- **Recommendation**: Provide concise inline explanations for these terms upon first mention (e.g., explaining that vSRO refers to standard private server web schemas and Item Points denotes gear score).

#### 7. Container Networking vs `localhost` in Prefect API Configuration
- **Severity**: Nit
- **Target Scope**: Section 2 (Lines 110–113), Section Detailed Plan Item 7 (Line 407)
- **Description**: Section 2 notes that `prefect-server` is exposed at `localhost:4200`, and `src/core/config.py` loads `PREFECT_API_URL`. When running inside Docker Compose, configuring `localhost:4200` in the `scraper-worker` container will fail because `localhost` points to the container itself rather than the Prefect server container.
- **Recommendation**: Explicitly note in Section 2 and Item 7 that `PREFECT_API_URL` should default to `http://prefect-server:4200/api` for worker container networking.

#### 8. Inconsistent Quarantine Path References
- **Severity**: Nit
- **Target Scope**: CUJ 2 (Line 53), Architecture Diagram (Line 81), Section 4 (Line 248), Alternative C (Line 330)
- **Description**: The quarantine file path is inconsistently named across the document, appearing as `/quarantine/{target}/{run_id}.html.gz`, `./quarantine/{server_id}/{run_id}.html.gz`, and `/quarantine/{run_id}.html.gz`.
- **Recommendation**: Standardize the quarantine path specification across all diagrams and text to `/quarantine/{server_id}/{run_id}.html.gz` (mapped locally from `./quarantine` to container path `/app/quarantine`).
