# Stage 8: Intermediate Zoom Out Review Report

* **Target Document:** [design_draft.md](design_draft.md)
* **Preset:** Feature spec only (`FeatureRange = 1..End`, `DetailedRange = N/A`)
* **Prior Reports Evaluated:** [zoom_out_report.md](zoom_out_report.md) (Stage 5), [comprehension_report.md](comprehension_report.md) (Stage 6), [critic_report.md](critic_report.md) (Stage 7)

---

## 1. High-Level Architecture Minimalism Assessment

The macro-architecture of v2.0 remains minimal and free of structural bloat:
* Single new module (`src/core/notifications.py`), 2 optional `SecretStr` settings, zero new containers, zero exposed ports, zero Alembic migrations, zero stateful alert tables, and zero dedicated Prefect `@task` wrappers.
* Stateless anti-spam reuses the existing 12h cadence + 6h Circuit Breaker.
* Post-FW comparison collapses to a single partitioned table (`guild_snapshots`).

**Remaining Altitude Risk:** The draft drops into implementation details (verbatim Python bodies for `scrape_target_task`/`post_fw_flow`, full `@dataclass` and 10 `Final[...]` blocks, private helper names). Applying Stage 5–7 feedback carelessly could compound this bloat unless scoped strictly to architectural invariants.

---

## 2. Triage of Stage 5, 6, and 7 Proposals (Scope Creep & YAGNI Filter)

### A. REJECT / DEFER (YAGNI, Scope Creep, or Over-Engineering)

1. **[Major] Reject Stage 5 Inventive Catalyst Finding 1 — Degraded Ops Alert for Unhandled Infrastructure Exceptions**
   * **Affected Scope:** Technical Plan § 1 (line 106), § 6 (`scrape_target_task`, lines 264–284).
   * **Rationale:** `concepts_brief.md` explicitly accepted that unhandled pre-`ScrapeRunSummary` failures (`OperationalError`, `LookupError`) remain in Prefect UI (`Failed`) and container logs. Adding a catch–notify–reraise boundary with a degraded embed schema adds payload/test surface, delays Prefect failure propagation by up to 10s, and breaks stateless anti-spam during a DB outage when `circuit_breaker_until` is unreachable. Keep the explicit boundary in Section 1.

2. **[Major] Constrain Stage 7 Critic Major 8 — Reject 6,000-Char Aggregate Truncation Engine**
   * **Affected Scope:** Technical Plan § 3 (lines 148–153), § 4 (lines 193–196, 227–237).
   * **Rationale:** Adopt Markdown/masked-link escaping (`[text](url)`) and newline collapsing for untrusted scraped strings (`guild_name`, `fortress_held`). Reject building a 6,000-char aggregate truncation engine: with per-field caps (`1024`), ops embeds max out under ~1,800 chars and post-FW embeds under ~2,500 chars, well below Discord's 6,000-char ceiling.

3. **[Major] Constrain Stage 7 Critic Blocker 2 — Avoid Dialect-Translation Scaffolding in Spec**
   * **Affected Scope:** Goals (line 31), Technical Plan § 5 (line 242), Implementation Plan Item 6 (line 439).
   * **Rationale:** Do not introduce SQLAlchemy `@compiles` overrides or dialect-translation frameworks. Clarify G4 as *zero new runtime dependencies* (declaring `aiosqlite` under `[dependency-groups] dev` if needed), specify a minimal 4-table SQLite DDL test fixture in `tests/test_notifications.py`, and normalize timestamps to UTC on read.

4. **[Nit] Reject Stage 7 Critic Nit 10 (Second Half) — Manual PostgreSQL 16 `EXPLAIN` Step in Verification Plan**
   * **Affected Scope:** Verification Plan (lines 469–475).
   * **Rationale:** Partition pruning on `RANGE (created_at)` is already a v1.0 schema invariant; a manual live `EXPLAIN` step adds ceremony without proportional risk reduction.

---

### B. ADOPT (Essential Security, Correctness, or Simplification Fixes)

1. **[Major — Unanimous across Stages 5 & 7] Close DB Session & Dispose Engine Before Discord HTTP I/O in `post_fw_flow`:** Split `load_post_fw_comparison(session, summary)` inside `async with` + `engine.dispose()` from `notify_post_fw_summary(comparison, settings)` outside the DB lifecycle.
2. **[Blocker — Stage 7 Critic Blocker #1] Fix `SecretStr` Token Leak in Pydantic v2 `ValidationError`:** Keep `mode="before"` strictly for normalizing `None`/empty/whitespace to `None`; run HTTPS/host/path validation in `mode="after"` on `SecretStr | None` and enable `hide_input_in_errors=True` in `SettingsConfigDict`.
3. **[Major — Stage 7 Critic Major #3] Silence Stdlib `httpx` / `httpcore` `INFO` Loggers in `src/core/logging.py`:** Set both loggers to `WARNING` in `configure_logging()` so webhook URLs are never logged on successful requests; add `src/core/logging.py` to the implementation plan.
4. **[Major — Stage 7 Critic Major #5] Enforce Wall-Clock `asyncio.timeout(10.0)` and `follow_redirects=False`:** Prevent per-phase timeout stacking (> 10s) and token forwarding on `307`/`308` redirects.
5. **[Major — Stage 7 Critic Major #6 & Stage 6 Comprehension #4] Uniform Top-Level `try/except Exception` Isolation in Both Public Facades:** Wrap payload build + send in `try/except Exception` inside both `notify_operational_failure` and `notify_post_fw_summary`.
6. **[Major — Stage 7 Critic Major #7] Pin Official Discord Webhook Hostnames and `/api/webhooks/` Path:** Replace `bool(parsed.netloc)` with `parsed.hostname` check against official Discord webhook hosts (`discord.com`, `discordapp.com`, `ptb.discord.com`, `canary.discord.com`) and `/api/webhooks/` path segments.
7. **[Major / Nit — Stage 5 Zoom Out #2, #3, #4] Remove Implementation Weeds & YAGNI API Parameters:** Replace verbatim function bodies, full `@dataclass`/constant blocks, and private helper names with architectural contracts; drop unused `DISCORD_MAX_CONTENT_LEN`, drop `client` and `now` from the public spec contract, and reuse `ScrapeStatus` from `src/models/pydantic_schemas.py`.

---

### C. ADOPT LIGHTLY (1–2 Line Clarifications Only)

* **Stage 6 Comprehension #1–#3, #5–#7:** Add inline links (`ScrapeRunSummary`, `db_models.py`, `AGENTS.md`, `001_sro_ranking_scraper_pipeline.md`), 1 sentence explaining `ScrapeRun.created_at` / `GuildSnapshot.created_at` persistence-time stamping (`±5 min` window), and brief parenthetical glosses (`SRO`, `IP`, `{fortress_name: guild_name}`, `TARGET_MAINTENANCE`, `SKIPPED_CIRCUIT_BREAKER`, `PARTIAL_SUCCESS`).
* **Stage 7 Critic Nit #9 & Nit #10 (first half):** Add 1 concise bullet for query edge guards (`server_id` filter, `ScrapeRun.id.desc()` tie-breaker, empty `IN` guard, stripping whitespace-only `fortress_held`) and PT-BR thousands formatting (`12.345 IP`).
* **Stage 7 Structural Nits #11–#14 & Style Majors #9–#11 / Nits #15–#18:** Calibrate `docker compose config`, `< 50ms`, and 512MB claims; remove `egm-zoom-out` jargon, promotional adjectives, and backticks around prose; standardize sibling heading language; split bundled H4s in Implementation Plan.

---

## 3. Summary Verdict

**PASS WITH TARGETED REVISIONS.** Applying Section 2B (Adopt) + Section 2C (Adopt Lightly) while rejecting Section 2A (YAGNI/Scope Creep) resolves both Blockers and all valid Majors across Stages 5–7 while making the design doc shorter, higher-altitude, and safer.
