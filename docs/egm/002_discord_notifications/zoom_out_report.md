# Stage 5: Initial Zoom Out Review Report

* **Target Document:** [design_draft.md](design_draft.md)
* **Preset:** Feature spec only (`FeatureRange = 1..End`, `DetailedRange = N/A`)
* **GoldfishContext:** [concepts_brief.md](concepts_brief.md)
* **Reviewers:** `egm-zoom-out`, `egm-inventive-catalyst`

---

## 1. Zoom Out Review (`egm-zoom-out`)

**Overall Assessment:** The macro-architecture is well-scoped, minimalist, and directly aligned with the root problems. It avoids unnecessary infrastructure (no new containers, no new dependencies, no Alembic migrations, no stateful alert tables, and no dedicated Prefect `@task` wrappers). Two **Major** architectural/altitude issues and two **Nit** YAGNI items were identified.

### Finding 1 (Major) — DB Session & Engine Held Open During Discord HTTP I/O in `post_fw_flow`
* **Affected Scope:** Technical Plan — Section 3 (`notify_post_fw_summary`, line 189), Section 6 (`src/flows/orchestrator.py`, lines 288–319), and Detailed Implementation Plan — Item 4 (lines 421–426).
* **Simplicity Rationale:** Section 1 (line 69) and Section 6 (line 285) establish an architectural invariant: close the DB session and dispose the `AsyncEngine` *before* performing outbound HTTP I/O to Discord. In `post_fw_flow`, however, `notify_post_fw_summary(session, summary, settings)` runs **inside** `async with session_factory() as session:`, holding both a PostgreSQL connection and the engine open for up to 10 seconds while awaiting the Discord webhook. Additionally, the trigger guards (`summary.status in POST_FW_SUCCESS_STATUSES` and `settings.discord_community_webhook_url is not None`) are checked redundantly in both `post_fw_flow` and `notify_post_fw_summary`.
* **Recommendation:** Decouple the read-only DB query from the HTTP send in `post_fw_flow`: run `load_post_fw_comparison(session, summary)` inside the short `async with` + `engine.dispose()` block, and invoke `notify_post_fw_summary(comparison, settings)` **after** the DB resources are released.

### Finding 2 (Major) — Implementation Weeds Inside Feature Spec Altitude
* **Affected Scope:** Technical Plan — Section 2 (lines 116–121), Section 3 (lines 133–179), Section 4 (lines 196, 260), Section 6 (lines 264–319), and Detailed Implementation Plan (lines 410–412).
* **Simplicity Rationale:** Under the Feature Spec Only preset (`DetailedRange = N/A`), the document drops into implementation weeds rather than staying at architectural/contract altitude:
  * Full verbatim Python implementations of `scrape_target_task` (lines 265–284) and `post_fw_flow` (lines 289–319) are pasted into the Architecture section.
  * Complete `@dataclass(frozen=True, slots=True)` class definitions (lines 156–179) and 10 `Final[...]` constant declarations (lines 134–153) are hardcoded in the spec.
  * Procedural micro-details are prescribed in prose: step-by-step validator branching (`isinstance(value, SecretStr)`, `.strip()`, `urllib.parse.urlsplit`), private helper names (`_truncate`, `_format_utc_timestamp`), and dictionary method choices (`setdefault(fortress_held, guild_name)`).
* **Recommendation:** Replace verbatim function bodies and private helper/micro-implementation details with concise architectural contracts (component boundaries, lifecycle order, data invariants, and error-isolation guarantees).

### Finding 3 (Nit) — Unused Constant & Redundant `client` Injection
* **Affected Scope:** Technical Plan — Section 3 (lines 148, 186–189).
* **Simplicity Rationale:**
  * `DISCORD_MAX_CONTENT_LEN: Final[int] = 2000` (line 148) is unused because both channels send embed-only payloads without `"content"`.
  * Threading `client: httpx.AsyncClient | None = None` across three public functions solely for testability is redundant when `respx` is already in `pyproject.toml` and intercepts `httpx.AsyncClient` transparently.
* **Recommendation:** Remove `DISCORD_MAX_CONTENT_LEN` and drop the `client` parameter from the public notification wrappers.

### Finding 4 (Nit) — Test-Clock Hook & Raw `str` Status Sets
* **Affected Scope:** Technical Plan — Section 3 (`build_ops_alert_payload`, line 184).
* **Simplicity Rationale:** Exposing `now: datetime | None = None` in `build_ops_alert_payload` leaks a test-clock hook into the public contract. Additionally, `OPS_ALERT_STATUSES` and `POST_FW_SUCCESS_STATUSES` can reference `ScrapeStatus` from `src/models/pydantic_schemas.py` rather than raw `str`.
* **Recommendation:** Keep clock injection out of the architectural spec and reuse `ScrapeStatus` for status sets.

---

## 2. Inventive Catalyst Review (`egm-inventive-catalyst`)

The foundational design resolves two major contradictions effectively (stateless anti-spam via 12h cadence + 6h Circuit Breaker, and single-table `guild_snapshots` querying with partition pruning + SQLite portability). Two unexamined architectural compromises remain:

### Finding 1 (Major) — Infrastructure-Failure Blindness Coupled to `ScrapeRunSummary`
* **Affected Scope:** Section 1 (line 106) and Section 6 (`src/flows/tasks.py`, lines 264–284).
* **False Compromise:** Accepting silence on the operational Discord channel whenever a failure occurs before `ScrapeRunSummary` is returned (e.g., PostgreSQL outage `OperationalError`, misconfigured `server_id` `LookupError`, or an unhandled exception inside `scraper.run()`) forces the operator back to manually checking Prefect UI or container logs—even though outbound Discord notification (`httpx` + `Settings`) has zero runtime dependency on PostgreSQL.
* **Proposed Resolution:** Add a lightweight catch–notify–reraise boundary around `get_scraper()` / DB session / `scraper.run()` in `scrape_target_task`. On an unhandled `Exception as exc`, dispatch a minimal, DB-independent degraded ops alert containing only `server_id`, `trigger_type`, `error_type=type(exc).__name__`, and UTC timestamp (strictly omitting `str(exc)` to prevent leaking `DATABASE_URL` or driver credentials), then re-raise `exc` so Prefect still records the task as `Failed`.
* **Secondary Liabilities:** Introduces a minimal fallback alert payload format; adds up to `10s` of delay before an infrastructure exception propagates to Prefect; repeated manual invocations during a DB outage will each emit a degraded alert.

### Finding 2 (Major) — DB Connection & Pool Held Open During External Discord HTTP I/O in `post_fw_flow`
* **Affected Scope:** Section 3 (`notify_post_fw_summary`, line 189) and Section 6 (`src/flows/orchestrator.py`, lines 299–316).
* **False Compromise:** Unlike `scrape_target_task`, `post_fw_flow` executes `notify_post_fw_summary(session, summary, settings=settings)` inside `async with session_factory() as session:`, holding an active `asyncpg` connection and pool open during up to 10s of external HTTPS I/O.
* **Proposed Resolution:** Separate the DB read phase (`load_post_fw_comparison`) inside `async with` + `engine.dispose()` from the HTTP dispatch phase (`notify_post_fw_summary(comparison, settings=settings)`) outside the database lifecycle.
* **Secondary Liabilities:** Adds explicit two-step orchestration in `post_fw_flow` and updates `notify_post_fw_summary` to accept `PostFWComparison | None`.
