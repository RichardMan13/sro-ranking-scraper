# Goldfish Annotated Review Report

**Status:** resolved
**Target:** `docs/egm/002_discord_notifications/design_draft.md` (`full file`)

## Prioritized Findings & AI Suggestions

### [Issue 1] [`egm-design`] - Severity: Blocker
*   **Affected Scope:** [Lines 116-121](design_draft.md#L116-L121) (Technical Plan § 2 — Configuration & Secret Management)
*   **Discrepancy Description:** Running HTTPS URL validation inside `@field_validator(..., mode="before")` executes before Pydantic v2 wraps the string in `SecretStr`, causing `ValidationError.__str__` to print `input_value='<raw_secret_url>'` verbatim in startup logs/tracebacks.
*   **Rationale:** Directly violates G3 and Section 7's secret-masking invariant whenever an operator misconfigures a webhook URL (e.g., `http://...`).
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:*** Confirmed against Pydantic v2 behavior. Splitting `mode="before"` (normalize `None`/empty/whitespace to `None`) from `mode="after"` (validate `SecretStr | None`) and enabling `hide_input_in_errors=True` in `SettingsConfigDict` closes the leak completely.
*   ***Proposed Edit:***
    *   Location: Technical Plan § 2, § 7, and Detailed Implementation Plan (`src/core/config.py`, `tests/test_models_db.py`).
    *   Change: Specify two-stage validation (`mode="before"` for empty -> `None`, `mode="after"` on `SecretStr | None` for HTTPS/Discord URL checks) + `hide_input_in_errors=True` in `SettingsConfigDict`, and add a unit test asserting the raw token does not appear in `str(exc_info.value)`.

### [Issue 2] [`egm-design`] - Severity: Blocker
*   **Affected Scope:** [Lines 31, 242, 392, 439](design_draft.md#L242) (Goals, Technical Plan § 5, NFR § 3, Detailed Implementation Plan)
*   **Discrepancy Description:** `src/models/db_models.py` uses PostgreSQL-only DDL (`UUID`, `JSONB`, `gen_random_uuid()`, `'{}'::jsonb`, `BRIN`/`GIN`, `postgresql_partition_by`), so `Base.metadata.create_all()` fails on SQLite; SQLite also strips timezone info from `TIMESTAMP(timezone=True)`, and `aiosqlite` is not explicitly declared in `pyproject.toml`.
*   **Rationale:** Blocks implementing the SQLite unit test suite as written.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:*** Verified in `src/models/db_models.py` and `pyproject.toml`. Per Stage 8 triage, avoid `@compiles` dialect-translation frameworks: use a minimal 4-table SQLite DDL fixture in `tests/test_notifications.py`, normalize timestamps to UTC on read, and clarify G4 as zero new *runtime* dependencies (declaring `aiosqlite` in `[dependency-groups] dev`).
*   ***Proposed Edit:***
    *   Location: Goals (G4), Technical Plan § 5, NFR § 3, Detailed Implementation Plan (`pyproject.toml`, `tests/test_notifications.py`).
    *   Change: Clarify G4 as zero new runtime dependencies (add `aiosqlite` to `[dependency-groups] dev`), document UTC normalization on read, and specify a minimal 4-table SQLite DDL test fixture (`servers`, `scrape_runs`, `guilds`, `guild_snapshots`).

### [Issue 3] [`egm-design`] - Severity: Major
*   **Affected Scope:** [Lines 186, 323-326](design_draft.md#L323-L326) (Technical Plan § 7 — Security, Observability & Failure Isolation)
*   **Discrepancy Description:** `src/core/logging.py` does not silence stdlib `httpx` / `httpcore` loggers, which emit `INFO: HTTP Request: POST https://discord.com/api/webhooks/{id}/{token} ...` under Prefect 3.
*   **Rationale:** Leaks the full webhook secret on every successful notification.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:*** Verified in `src/core/logging.py`. Setting stdlib `httpx` and `httpcore` loggers to `WARNING` in `configure_logging()` is a 2-line essential security fix.
*   ***Proposed Edit:***
    *   Location: Technical Plan § 7 and Detailed Implementation Plan (add `src/core/logging.py` item + `caplog` `INFO` test in `tests/test_notifications.py`).
    *   Change: Require `logging.getLogger("httpx").setLevel(logging.WARNING)` and `logging.getLogger("httpcore").setLevel(logging.WARNING)` in `configure_logging()` and verify zero URL leakage in both `structlog` and stdlib `caplog`.

### [Issue 4] [`egm-zoom-out` / `egm-inventive-catalyst` / `egm-design`] - Severity: Major
*   **Affected Scope:** [Lines 189, 288-319](design_draft.md#L288-L319) (Technical Plan § 3 & § 6 — `post_fw_flow`)
*   **Discrepancy Description:** `post_fw_flow` runs `notify_post_fw_summary(session, summary, settings)` inside `async with session_factory() as session:`, holding a PostgreSQL connection `IDLE IN TRANSACTION` during up to 10s of external Discord HTTP I/O, and duplicates status/URL guard checks.
*   **Rationale:** Violates the DB-isolation invariant established in Section 1 and `scrape_target_task`.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:*** Unanimous finding across Stages 5, 7, and 8. Decoupling DB read from HTTP send aligns `post_fw_flow` with `scrape_target_task` and simplifies the contract.
*   ***Proposed Edit:***
    *   Location: Technical Plan § 1 (Mermaid + prose), § 3, § 6 (`post_fw_flow`), and Detailed Implementation Plan (`src/core/notifications.py`, `src/flows/orchestrator.py`).
    *   Change: Run `load_post_fw_comparison(session, summary)` inside the short `async with` + `engine.dispose()` block, then invoke `notify_post_fw_summary(comparison, settings)` after DB resources are released.

### [Issue 5] [`egm-design`] - Severity: Major
*   **Affected Scope:** [Lines 30, 186, 327, 381](design_draft.md#L186) (Goals, Technical Plan § 3 & § 7, NFR § 1)
*   **Discrepancy Description:** `httpx.Timeout(connect=5.0, read=10.0, write=5.0, pool=5.0)` is per-phase (can stack up to 20s wall-clock), and `follow_redirects=False` is not explicitly pinned.
*   **Rationale:** Can breach the `<= 10s` SLA and risks forwarding webhook tokens on `307`/`308` redirects.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:*** Wrapping the POST in `asyncio.timeout(10.0)` and pinning `follow_redirects=False` enforces both guarantees with zero architectural complexity.
*   ***Proposed Edit:***
    *   Location: Technical Plan § 3, § 7, and NFR § 1.
    *   Change: Require wall-clock `asyncio.timeout(10.0)` in addition to `httpx.Timeout` and explicit `follow_redirects=False`.

### [Issue 6] [`egm-design` / `egm-comprehension`] - Severity: Major
*   **Affected Scope:** [Lines 186-187, 282, 305-314, 391](design_draft.md#L282) (Technical Plan § 3, § 6, NFR § 3)
*   **Discrepancy Description:** Only `send_discord_webhook` is specified to catch `Exception`, leaving `build_ops_alert_payload` unprotected inside `scrape_target_task` and creating an unexplained `try/except` asymmetry with `post_fw_flow`.
*   **Rationale:** A formatting exception after DB commit would fail `scrape_target_task`, violating G3 and NFR § 3.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:*** Wrapping payload construction + HTTP dispatch inside `try/except Exception` in both public facades (`notify_operational_failure` and `notify_post_fw_summary`) guarantees uniform best-effort isolation; `post_fw_flow`'s outer `try/except` remains strictly for the read-only DB query.
*   ***Proposed Edit:***
    *   Location: Technical Plan § 3, § 6, and NFR § 3.
    *   Change: State explicitly that both public notification facades catch `Exception` across payload formatting and HTTP dispatch, returning `False` with sanitized logs, and that `post_fw_flow` guards only the read-only DB query with `try/except Exception`.

### [Issue 7] [`egm-design`] - Severity: Major
*   **Affected Scope:** [Lines 120, 405](design_draft.md#L120) (Technical Plan § 2, Detailed Implementation Plan)
*   **Discrepancy Description:** Checking `bool(parsed.netloc)` accepts malformed URLs (`https://:80` where `parsed.hostname is None`) and arbitrary HTTPS hosts (SSRF or misconfiguration).
*   **Rationale:** NG2 restricts v2.0 strictly to Discord webhooks; `config.py` already uses `parsed.hostname` for `database_url`.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:*** Pinning `parsed.hostname` to official Discord webhook hosts and requiring `/api/webhooks/{id}/{token}` closes SSRF and catches paste errors immediately.
*   ***Proposed Edit:***
    *   Location: Technical Plan § 2 and Detailed Implementation Plan (`src/core/config.py`).
    *   Change: Validate `parsed.hostname in {"discord.com", "discordapp.com", "ptb.discord.com", "canary.discord.com"}` and `parsed.path.startswith("/api/webhooks/")` with non-empty webhook ID and token segments.

### [Issue 8] [`egm-design`] - Severity: Major
*   **Affected Scope:** [Lines 148-153, 193-196, 227-237](design_draft.md#L193-L196) (Technical Plan § 3 & § 4)
*   **Discrepancy Description:** `"allowed_mentions": {"parse": []}` does not prevent Discord Embeds from rendering masked phishing links (`[text](https://...)`) injected via untrusted scraped `guild_name` or `fortress_held` strings into the public community channel; also notes Discord's 6,000-char aggregate embed ceiling.
*   **Rationale:** Scraped third-party game strings are untrusted input displayed in a public Discord channel.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:*** Per Stage 8 triage: adopt Markdown escaping (`\`, `*`, `_`, `` ` ``, `~`, `[`, `]`, `(`, `)`, `|`, `>`) and newline collapsing for untrusted scraped strings, and document that per-field caps (`1024`) keep total embed length well under 6,000 chars without building an aggregate truncation engine.
*   ***Proposed Edit:***
    *   Location: Technical Plan § 4 and § 7.
    *   Change: Require escaping Markdown control characters and collapsing newlines on untrusted dynamic fields (`guild_name`, `fortress_held`, `quarantine_file_path`, `error_details.reason`) and note that per-field truncation keeps total embed length below Discord's 6,000-character aggregate cap.

### [Issue 9] [`egm-inventive-catalyst`] - Severity: Major
*   **Affected Scope:** [Lines 106, 264-284](design_draft.md#L106) (Technical Plan § 1 & § 6)
*   **Discrepancy Description:** Unhandled infrastructure exceptions occurring before `ScrapeRunSummary` is returned (e.g., PostgreSQL `OperationalError`, `LookupError`) do not trigger a Discord ops alert.
*   **Rationale:** Reviewer proposes adding a catch–notify–reraise boundary with a minimal DB-independent degraded ops alert.
*   ***AI Suggestion:*** CONTEST
*   ***Justification:*** Checkable evidence in [concepts_brief.md](concepts_brief.md#L22): *"Se o PostgreSQL cair (`OperationalError`) ou ocorrer `LookupError` antes do retorno de `ScrapeRunSummary`, o alerta ops não dispara porque o erro ocorre fora do contrato de status canônico. Esse limite ficará explícito no design doc `002`."* Confirmed by Stage 8 Zoom Out triage: adding a degraded alert path introduces a third payload schema, delays Prefect `Failed` propagation by up to 10s, and breaks stateless anti-spam during a DB outage when `servers.circuit_breaker_until` is unreachable.
*   ***Proposed Edit:***
    *   Location: None
    *   Change: Retain the explicit scope boundary in Technical Plan § 1.

### [Issue 10] [`egm-zoom-out`] - Severity: Major
*   **Affected Scope:** [Lines 116-121, 133-179, 264-319](design_draft.md#L133-L179) (Technical Plan § 2, § 3, § 6)
*   **Discrepancy Description:** Under the Feature Spec altitude, the Architecture section includes verbatim Python bodies of `scrape_target_task` and `post_fw_flow`, full `@dataclass` and 10 `Final[...]` blocks, and private helper names (`_truncate`, `_format_utc_timestamp`, `setdefault`).
*   **Rationale:** Mixes implementation weeds into the architectural specification.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:*** Replacing verbatim function bodies and private helper names with concise architectural contracts (component boundaries, lifecycle sequence, and public signatures) makes the design doc shorter, cleaner, and less brittle to refactorings.
*   ***Proposed Edit:***
    *   Location: Technical Plan § 2, § 3, § 4, § 5, § 6.
    *   Change: Replace verbatim code blocks for `scrape_target_task`/`post_fw_flow`, raw constant/dataclass listings, and private helper names with concise architectural contracts and lifecycle steps.

### [Issue 11] [`egm-comprehension`] - Severity: Major
*   **Affected Scope:** [Lines 106, 184-189, 266-318](design_draft.md#L184-L189) (Technical Plan §§ 1, 3, 4A, 6)
*   **Discrepancy Description:** `ScrapeRunSummary`, `ErrorDetails`, ORM models (`Server`, `ScrapeRun`, `Guild`, `GuildSnapshot`), and internal helpers are referenced without linking to their defining modules.
*   **Rationale:** A fresh reader cannot locate these existing contracts without searching the repo.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:*** Adding concise inline links to `src/scrapers/base.py`, `src/models/pydantic_schemas.py`, `src/models/db_models.py`, and `src/core/database.py` resolves the gap without duplicating v1.0 code.
*   ***Proposed Edit:***
    *   Location: Technical Plan §§ 1, 3, 5.
    *   Change: Link existing symbols to their defining files at first mention.

### [Issue 12] [`egm-comprehension`] - Severity: Major
*   **Affected Scope:** [Line 257](design_draft.md#L257) (Technical Plan § 5 — Partition Pruning)
*   **Discrepancy Description:** The `±5 minutes` window around `ScrapeRun.created_at` is unexplained relative to the 180s fetch timeout.
*   **Rationale:** A reader cannot tell whether `ScrapeRun.created_at` is stamped at run start or persistence time.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:*** Verified in `src/scrapers/base.py` (lines 1054–1204): both `ScrapeRun.created_at` and `GuildSnapshot.created_at` are stamped with the same `snapshot_ts` at persistence time (after the 180s fetch). One sentence makes the `±5 min` window self-explanatory.
*   ***Proposed Edit:***
    *   Location: Technical Plan § 5, item 3.
    *   Change: Add 1 sentence explaining that `BaseRankingScraper.run()` stamps `ScrapeRun.created_at` and `GuildSnapshot.created_at` with the same UTC timestamp at persistence time (after fetch completion), so `±5 min` safely covers minor transaction/fixture skew while preserving partition pruning.

### [Issue 13] [`egm-comprehension`] - Severity: Major
*   **Affected Scope:** [Lines 38-39, 245, 335, 350](design_draft.md#L38-L39) (Non-Goals, Technical Plan § 5, Alternatives Considered)
*   **Discrepancy Description:** Project-specific statuses (`TARGET_MAINTENANCE`, `SKIPPED_CIRCUIT_BREAKER`, `PARTIAL_SUCCESS`) and v1.0 artifacts (`v_guild_turnover_dominance`, 5 views, `guilds`, `servers.display_name`) appear without brief glosses or links to Design 001.
*   **Rationale:** Fresh readers lack context for v1.0 terminology.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:*** Brief parenthetical glosses at first mention and a link to Design 001 make the document self-contained without adding bloat.
*   ***Proposed Edit:***
    *   Location: Non-Goals, Technical Plan § 5, Alternatives Considered.
    *   Change: Add terse parenthetical explanations for `TARGET_MAINTENANCE`, `SKIPPED_CIRCUIT_BREAKER`, and `PARTIAL_SUCCESS` (`<= 5%` validation errors) at first mention, and link `docs/designs/001_sro_ranking_scraper_pipeline.md`.

### [Issue 14] [`egm-style`] - Severity: Major
*   **Affected Scope:** [Line 467](design_draft.md#L467) (Verification Plan § 2)
*   **Discrepancy Description:** Claims `docker compose config` confirms that `scraper-worker` starts without `Settings` validation errors.
*   **Rationale:** `docker compose config` only validates YAML interpolation and port declarations; it does not execute Python/Pydantic.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:*** Accurate calibration of verification scope.
*   ***Proposed Edit:***
    *   Location: Verification Plan § 2.
    *   Change: State that `docker compose config` validates variable interpolation and port declarations, while `Settings` empty-string normalization is verified by unit tests (`tests/test_models_db.py`) and container/CLI startup.

### [Issue 15] [`egm-style`] - Severity: Major
*   **Affected Scope:** [Lines 372, 382-383](design_draft.md#L382-L383) (Decision Log ADR-009, NFR § 1)
*   **Discrepancy Description:** Uncalibrated resource/performance claims: *"mantém folga dentro do teto rígido de 512MB"*, *"garantindo ... execução em `< 50ms`"*, and *"preserva alta performance"* (ADR-009).
*   **Rationale:** States targets/invariants as benchmarked guarantees or subjective claims.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:*** Calibrating to factual invariants and target budgets improves technical precision.
*   ***Proposed Edit:***
    *   Location: Decision Log ADR-009 and NFR § 1.
    *   Change: State zero new runtime dependencies/processes under the 512MB limit, frame `< 50ms` as target latency (`meta de execução < 50ms`), and replace *"preserva alta performance"* with *"preserva partition pruning"* in ADR-009.

### [Issue 16] [`egm-style`] - Severity: Major
*   **Affected Scope:** [Line 362](design_draft.md#L362) (Alternatives Considered — Alternative F)
*   **Discrepancy Description:** Cites internal review lens identifier `egm-zoom-out` in the rejection rationale.
*   **Rationale:** Opaque internal process jargon for engineering readers of the approved design doc.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:*** Technical rationale stands on its own without internal methodology labels.
*   ***Proposed Edit:***
    *   Location: Alternatives Considered — Alternative F.
    *   Change: Remove `(egm-zoom-out)` and retain the technical rationale (avoids extra SQLite state overhead in `prefect-server` for a `retries=0` best-effort call).

### [Issue 17] [`egm-zoom-out`] - Severity: Nit
*   **Affected Scope:** [Lines 148, 184-189](design_draft.md#L184-L189) (Technical Plan § 3)
*   **Discrepancy Description:** Includes unused `DISCORD_MAX_CONTENT_LEN`, redundant `client: httpx.AsyncClient | None = None` injection (given `respx`), `now: datetime | None = None` test-clock hook, and raw `str` status sets instead of `ScrapeStatus`.
*   **Rationale:** Minor YAGNI inflation of public API surface.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:*** Removing unused parameters/constants and reusing `ScrapeStatus` tightens the module contract.
*   ***Proposed Edit:***
    *   Location: Technical Plan § 3.
    *   Change: Remove `DISCORD_MAX_CONTENT_LEN`, `client`, and `now` from the public spec contract; type status sets with `ScrapeStatus`.

### [Issue 18] [`egm-design`] - Severity: Nit
*   **Affected Scope:** [Lines 248-260](design_draft.md#L248-L260) (Technical Plan § 5)
*   **Discrepancy Description:** Missing secondary tie-breaker (`ScrapeRun.id.desc()`), defense-in-depth `GuildSnapshot.server_id == summary.server_id` filter, empty `current_top_guild_ids` guard before `.in_(...)`, and whitespace-only `fortress_held` stripping.
*   **Rationale:** Prevents subtle edge-case bugs in baseline comparison queries.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:*** Worth fixing in 1 concise bullet in Section 5.
*   ***Proposed Edit:***
    *   Location: Technical Plan § 5.
    *   Change: Add 1 concise bullet specifying `ScrapeRun.id.desc()` tie-breaker, `server_id` filter on both snapshot queries, empty-list guard before `IN`, and stripping whitespace-only `fortress_held` values.

### [Issue 19] [`egm-design`] - Severity: Nit
*   **Affected Scope:** [Lines 236, 469-475](design_draft.md#L236) (Technical Plan § 4B, Verification Plan § 3)
*   **Discrepancy Description:** `{total_item_points:,.0f}` renders English commas in a PT-BR embed; Verification Plan lacks a manual PostgreSQL 16 `EXPLAIN` step for `post_fw_flow`.
*   **Rationale:** Formatting consistency in PT-BR; optional live DB check.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:*** Adopting PT-BR thousands formatting (`12.345 IP`) is a 1-line improvement; per Stage 8 triage, skipping a manual `EXPLAIN` step avoids unnecessary verification ceremony.
*   ***Proposed Edit:***
    *   Location: Technical Plan § 4B.
    *   Change: Specify PT-BR thousands formatting (e.g., `12.345 IP`).

### [Issue 20] [`egm-comprehension`] - Severity: Nit
*   **Affected Scope:** [Lines 1, 131, 175-176, 236, 368-374](design_draft.md#L175-L176) (Title, Technical Plan § 3 & § 4B, Decision Log)
*   **Discrepancy Description:** `SRO` and `IP` are not expanded at first use; `AGENTS.md` and `ADR-001..006` are unlinked; `current_fortresses`/`baseline_fortresses` key/value orientation is implicit.
*   **Rationale:** Minor self-containment polish.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:*** Quick inline glosses and links eliminate ambiguity with minimal text.
*   ***Proposed Edit:***
    *   Location: Overview, Technical Plan § 3 & § 4B, Decision Log.
    *   Change: Expand `Silkroad Online (SRO)` and `Item Points (IP)` at first use, clarify `{fortress_name: guild_name}` mapping, and link `AGENTS.md` and Design 001 (`ADR-001`–`ADR-006`).

### [Issue 21] [`egm-structural` / `egm-style`] - Severity: Nit
*   **Affected Scope:** [Lines 12, 24, 133-238, 257, 335-362, 380-391, 427-445](design_draft.md#L335-L362) (Overview, Problem, Technical Plan, Alternatives, NFR, Implementation Plan)
*   **Discrepancy Description:** Asymmetric English/Portuguese sibling headings, bundled H4s for two files (`docker-compose.yml` & `.env.example`; `test_models_db.py` & `test_flows.py`), backtick-wrapped rejection prose, promotional adjectives/intensifiers, and *"na virada exata do mês"*.
*   **Rationale:** Structural symmetry and plain-language precision.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:*** Low-effort copy/structure polish that improves readability.
*   ***Proposed Edit:***
    *   Location: Across affected headings and paragraphs.
    *   Change: Standardize sibling headings to Portuguese, split bundled target files into 1 H4 per file, render `Reason for Rejection` as normal prose, remove promotional adjectives/intensifiers and *"servidor quebrado"*, and change *"na virada exata do mês"* to *"próximo à virada do mês"*.

### [Issue 22] [`egm-structural`] - Severity: Nit
*   **Affected Scope:** [Lines 6, 44](design_draft.md#L6) (Metadata & Open Questions)
*   **Discrepancy Description:** Relative link `[concepts_brief.md](concepts_brief.md)` will break when promoted to `docs/designs/002_discord_notifications.md`.
*   **Rationale:** Valid in current draft directory; only changes upon promotion.
*   ***AI Suggestion:*** DECLINE
*   ***Justification:*** The relative link is valid where `design_draft.md` currently resides (`docs/egm/002_discord_notifications/`). Updating it now would break the draft link; it should be adjusted during promotion to `docs/designs/002_discord_notifications.md`.
*   ***Proposed Edit:***
    *   Location: None
    *   Change: No change in draft; update relative path upon promotion to `docs/designs/`.
