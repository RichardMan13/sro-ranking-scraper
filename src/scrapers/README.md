# Scrapers (`src/scrapers`)

## Overview
This package implements the HTTP extraction, HTML/JSON parsing, integrity validation, dead-letter quarantine, and PostgreSQL persistence layer for Silkroad Online private-server rankings. `[base.py:BaseRankingScraper]` defines the shared execution pipeline (`run()`), while server-specific subclasses (`[chronicle.py:ChronicleScraper]`, `[electus.py:ElectusScraper]`, `[golden.py:PlayGoldenScraper]`) implement category-specific fetching and row mapping. All scrapers operate purely over `httpx` HTTP/2 async requests with `lxml` / `orjson` parsing—without headless browsers.

## Interface
- Factory & Registry:
  - `[__init__.py:get_scraper]` — instantiates the registered `[base.py:BaseRankingScraper]` subclass for a configured `server_id` (`"chronicle"`, `"electus"`, `"play_golden"` in `[__init__.py:_SCRAPER_REGISTRY]`); raises `ValueError` for unknown IDs.
- Base Scraper & Pipeline Types:
  - `[base.py:BaseRankingScraper]` — abstract base class requiring `server_id` and abstract methods `[base.py:BaseRankingScraper.fetch_player_pages]`, `[base.py:BaseRankingScraper.fetch_job_pages]`, and `[base.py:BaseRankingScraper.fetch_guild_pages]`; exposes `[base.py:BaseRankingScraper.run]`.
  - `[base.py:PagePayload]`, `[base.py:CategoryFetchResult]`, `[base.py:ScrapeRunSummary]` — immutable/slotted dataclasses for raw HTTP payloads, category extraction results, and run outcomes.
  - `[base.py:ScraperFetchError]`, `[base.py:BlockedChallengeError]`, `[base.py:TargetMaintenanceError]`, `[base.py:TransportFetchError]` — fetch failure hierarchy distinguishing anti-bot blocks, maintenance windows, and transport/HTTP errors.
- Concrete Scrapers:
  - `[chronicle.py:ChronicleScraper]` (`server_id="chronicle"`) — paginated HTML scraper for `/ranking/player` (up to 10 pages), `/ranking/job` (up to 5 pages), and `/ranking/guild` (up to 2 pages) targeting `<table class="... ranking-table ...">`.
  - `[electus.py:ElectusScraper]` (`server_id="electus"`) — JSON-first scraper for `/api/ranking/{category}?page={page}&limit={limit}` (player: 10×100, job: 5×100, guild: 2×50) with automatic page-1 fallback to HTML `/ranking/{category}?page={page}` (`<table id="ranking-table">`) on HTTP 404 or non-JSON responses.
  - `[golden.py:PlayGoldenScraper]` (`server_id="play_golden"`) — paginated HTML scraper for `/ranking/characters` (up to 10 pages), `/ranking/jobs` (up to 5 pages), and `/ranking/guilds` (up to 2 pages) targeting `<table class="... table-ranking ...">`.
- Subdirectories: None.

## Invariants
- **Subclass & Trigger Validation:** Every `[base.py:BaseRankingScraper]` subclass must define a non-empty `server_id` and positive `timeout_budget_seconds` (`[base.py:BaseRankingScraper.__init__]`). `[base.py:BaseRankingScraper.run]` rejects any `trigger_type` outside `CANONICAL_TRIGGER_TYPES` and raises `LookupError` if `self.server_id` does not exist in `servers`.
- **Circuit Breaker Gate:** `[base.py:BaseRankingScraper.run]` checks `Server.circuit_breaker_until` (normalized to UTC) before opening any HTTP connection. If active, it rolls back the session and returns `status="SKIPPED_CIRCUIT_BREAKER"` with `skipped=True` and zero HTTP requests.
- **Zero-Retry on Anti-Bot & Maintenance:** `[base.py:BaseRankingScraper._fetch_url]` raises `[base.py:BlockedChallengeError]` immediately (no retry) on HTTP `403`/`429` (`[base.py:CHALLENGE_HTTP_STATUSES]`), header `cf-mitigated: challenge`, or `[base.py:CHALLENGE_HTML_MARKERS]`. It raises `[base.py:TargetMaintenanceError]` immediately on `[base.py:MAINTENANCE_HTML_MARKERS]` or HTTP `503` containing `"maintenance"` without triggering the circuit breaker cooldown. Only transient transport/timeout exceptions and HTTP `502`/`503`/`504` (`[base.py:RETRYABLE_HTTP_STATUSES]`) retry using `[base.py:DEFAULT_RETRY_DELAYS]` (`(2.0, 5.0)`).
- **Concurrency & Timeout Budgets:** HTTP concurrency per run is capped at `[base.py:DEFAULT_MAX_CONCURRENT_REQUESTS]` (`3`) via `asyncio.Semaphore`, and total category fetching is bounded by `asyncio.timeout` using `[base.py:DEFAULT_TIMEOUT_BUDGET_SECONDS]` (`180.0` seconds).
- **Strict Integrity Guards & Deduplication:** `[base.py:BaseRankingScraper._validate_and_persist]` validates all rows with Pydantic v2, deduplicates by entity name keeping the lowest `rank` (`[base.py:_deduplicate_players]`, `[base.py:_deduplicate_jobs]`, `[base.py:_deduplicate_guilds]`), and enforces rejection guards in strict order:
  1. Zero extracted records → `SCHEMA_DRIFT`.
  2. Player Top-Rank Integrity Guard (ranks `1`, `2`, `3` must all pass validation and be present) → `SCHEMA_DRIFT`.
  3. Guild Top-Rank Integrity Guard (when ≥ 3 raw guild records exist, ranks `1`, `2`, `3` must pass and be present) → `SCHEMA_DRIFT`.
  4. Total error rate `> MAX_ERROR_RATE` (5%) → `SCHEMA_DRIFT`.
  5. Valid deduplicated players `< server.min_records_expected` → `LOW_COUNT_QUARANTINE`.
  6. Valid deduplicated guilds `< MIN_GUILD_RECORDS` → `LOW_COUNT_QUARANTINE`.
- **Job Reconciliation:** Validated job rankings are left-joined onto deduplicated players by case-sensitive `char_name` (`[base.py:_reconcile_jobs_into_players]`), merging `job_type`, `job_level`, `job_exp`, fallback `guild_name`, and `extra_metrics` (adding `"job_rank"`).
- **Pagination Termination & Fallback Rules:** HTML scrapers (`[chronicle.py:ChronicleScraper._fetch_paginated_html]`, `[golden.py:PlayGoldenScraper._fetch_paginated_html]`, `[electus.py:ElectusScraper._fetch_html_category]`) skip single-cell `colspan` empty-notice rows and terminate pagination on the first page yielding zero data rows. `[electus.py:ElectusScraper._fetch_category]` falls back to HTML only on page 1; non-JSON on page ≥ 2 raises `[base.py:TransportFetchError]`.
- **No Raw HTML in Database:** Raw response bytes are never stored in PostgreSQL; rejected or challenge payloads are written exclusively as gzip files on disk with SHA-256 metadata stored in `scrape_runs`.

## Side Effects
- **External Network I/O:** Performs HTTP/2 `GET` requests via `httpx.AsyncClient` (`[base.py:BaseRankingScraper._build_http_client]`) rotating `[base.py:DEFAULT_HEADER_PROFILES]` and, for Electus, attaching `cf_clearance` from `settings.electus_cf_clearance` (`[electus.py:ElectusScraper._cf_cookies]`).
- **Disk I/O (Dead-Letter Quarantine):** `[base.py:BaseRankingScraper._write_quarantine_file]` creates `<quarantine_dir>/<server_id>/` and writes deterministic gzip archives (`mtime=0`) to `<quarantine_dir>/<server_id>/<run_id>.html.gz` on `BLOCKED_CHALLENGE`, `SCHEMA_DRIFT`, or `LOW_COUNT_QUARANTINE`.
- **PostgreSQL Mutations:**
  - Always inserts an audit row in `scrape_runs` for non-skipped runs (`SUCCESS`, `PARTIAL_SUCCESS`, `BLOCKED_CHALLENGE`, `TARGET_MAINTENANCE`, `HTTP_ERROR`, `SCHEMA_DRIFT`, `LOW_COUNT_QUARANTINE`).
  - On `BLOCKED_CHALLENGE`, updates `servers.circuit_breaker_until` to `now + circuit_breaker_cooldown_hours`.
  - On accepted runs (`SUCCESS` / `PARTIAL_SUCCESS`), bulk-upserts `guilds` and `characters` (preserving existing non-null `master_char_name` and known `CH`/`EU` race) and bulk-inserts append-only `character_snapshots` and `guild_snapshots` in chunks of `[base.py:SNAPSHOT_INSERT_CHUNK_SIZE]` (`1000`) within a single committed transaction (rolling back on error).
- **Structured Logging Context:** `[base.py:BaseRankingScraper.run]` binds `server_id` and `run_id` into `structlog` contextvars via `bind_run_context` and clears them in a `finally` block via `clear_run_context`.

## Verification
- Run: `pytest`
