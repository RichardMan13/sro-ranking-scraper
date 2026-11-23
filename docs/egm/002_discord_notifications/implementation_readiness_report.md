# Stage 9: Implementation Readiness Report

* **Target Document:** [002_discord_notifications.md](../../designs/002_discord_notifications.md) (`DetailedRange = lines 295..396`)
* **Reviewers:** `egm-implementation-readiness`, `egm-codebase-details`, `egm-style`
* **Summary:** **0 Blockers**, **6 Majors**, **10 Nits** — Ready for implementation after resolving the 6 concrete Major details below.

---

## 1. Codebase Details Audit (`egm-codebase-details`)

### Major 1 — `structlog.processors.JSONRenderer()` Fails on Raw `uuid.UUID` (`run_id`)
* **Affected Scope:** [002_discord_notifications.md](../../designs/002_discord_notifications.md#L316), [L327](../../designs/002_discord_notifications.md#L327); `src/core/logging.py`.
* **Finding:** `configure_logging()` uses `structlog.processors.JSONRenderer()` with stdlib `json.dumps` (no `default=str`). Passing `run_id=run_id` (`uuid.UUID`) directly to `logger.warning/error/info(...)` raises `TypeError: Object of type UUID is not JSON serializable` — which inside an `except Exception:` block would escape fault isolation and fail the task/flow.
* **Recommendation:** Always pass `run_id=str(run_id)` in every `structlog` call in `src/core/notifications.py` and `src/flows/orchestrator.py`.

### Major 2 — SQLite DDL Fixture & PostgreSQL-Dialect ORM Pitfalls in `tests/test_notifications.py`
* **Affected Scope:** [002_discord_notifications.md](../../designs/002_discord_notifications.md#L348), [L352](../../designs/002_discord_notifications.md#L352); `src/models/db_models.py`.
* **Finding:**
  1. `postgresql.UUID(as_uuid=True)` binds `uuid.UUID` parameters as **32-character hex strings without hyphens** (`u.hex`) on SQLite. Seeding raw SQL rows with hyphenated `str(u)` causes ORM `WHERE scrape_runs.id == summary.run_id` to return zero rows.
  2. `GuildSnapshot` uses composite PK `(id, created_at)` with `Identity(always=True)` and `extra_metrics: JSONB` (`'{}'::jsonb`). SQLite DDL must omit `::jsonb` (`TEXT NOT NULL DEFAULT '{}'`) and use explicit `id` or single-column `INTEGER PRIMARY KEY`.
  3. `GuildSnapshot.total_item_points` is `Mapped[Decimal]`; `GuildRankEntry.total_item_points` and the PT-BR formatter must accept `Decimal` cleanly under `mypy --strict`.
* **Recommendation:** Seed SQLite test rows using bound `uuid.UUID` parameters (or `u.hex`) and document the SQLite DDL conventions in item 9.

### Major 3 — Reuse of Windows-Sandbox Async Test Runner (`_run_async`) in `tests/test_notifications.py`
* **Affected Scope:** [002_discord_notifications.md](../../designs/002_discord_notifications.md#L345-L353); `tests/test_validation.py`, `tests/test_flows.py`.
* **Finding:** Existing async tests use synchronous `def test_...()` with `_run_async(...)` (imported from `tests.test_validation`) because restricted Windows sandboxes block `socket.socketpair()` needed by `pytest-asyncio`'s default loop. Also reuse `create_engine_from_url("sqlite+aiosqlite:///:memory:")` and `capsys` for `structlog` (`PrintLoggerFactory`) capture.
* **Recommendation:** Specify reusing `_run_async`, `create_engine_from_url`, and `capsys` in item 9.

### Major 4 — Environment Variable Leakage Risk in `tests/test_flows.py::_make_test_settings()`
* **Affected Scope:** [002_discord_notifications.md](../../designs/002_discord_notifications.md#L359-L362); `tests/test_flows.py`.
* **Finding:** `_make_test_settings()` calls `Settings(_env_file=None, ...)` without explicitly setting `discord_ops_webhook_url=None` and `discord_community_webhook_url=None`. If `DISCORD_COMMUNITY_WEBHOOK_URL` is set in the shell environment, existing `test_post_fw_flow_uses_post_fw_extra_trigger` will attempt a real PostgreSQL connection.
* **Recommendation:** Explicitly default both webhook fields to `None` in `_make_test_settings()` (with optional keyword overrides for notification flow tests).

### Nit 1 — `mypy --strict`, Ruff `E402`, and Minor Codebase Alignment Details
* **Affected Scope:** [002_discord_notifications.md](../../designs/002_discord_notifications.md#L304), [L314-L328](../../designs/002_discord_notifications.md#L314-L328).
* **Notes:**
  * Add `# noqa: E402` to new imports from `src.core.notifications` in `src/flows/tasks.py` and `src/flows/orchestrator.py` (below `_ensure_accessible_prefect_home()`).
  * Reuse existing `urlsplit` import in `src/core/config.py`; handle `SecretStr` wrapping empty/whitespace strings in `mode="before"`.
  * Initialize `engine: AsyncEngine | None = None` before `try:` in `post_fw_flow` and instantiate `logger = get_logger("orchestrator")`.
  * In `tests/test_notifications.py`, assert `httpx` suppression using `caplog.at_level(logging.INFO)` on the root logger (not `logger="httpx"`, which overrides `WARNING`).

---

## 2. Implementation Readiness Audit (`egm-implementation-readiness`)

### Major 5 — Missing Field Types on `GuildRankEntry`/`PostFWComparison` & Ambiguous PT-BR Decimal Formatting
* **Affected Scope:** [002_discord_notifications.md](../../designs/002_discord_notifications.md#L136-L138), [L155](../../designs/002_discord_notifications.md#L155), [L183](../../designs/002_discord_notifications.md#L183), [L314-L315](../../designs/002_discord_notifications.md#L314-L315).
* **Finding:**
  1. `GuildRankEntry` and `PostFWComparison` omit explicit Python types for several fields (`total_item_points: Decimal` coerced on read from SQLite affinity; `current_status: str` matching `ScrapeRunSummary.status: str` in `src/scrapers/base.py` to avoid `mypy --strict` friction).
  2. `12.345 IP` does not specify whether fractional `NUMERIC(12, 2)` values (e.g., `Decimal("12345.67")`) are rounded to the nearest integer (`12.346 IP`), truncated (`12.345 IP`), or displayed with decimals.
* **Recommendation:** Specify exact field types (`total_item_points: Decimal`, `current_status: str`, `baseline_run_id: uuid.UUID | None`, `baseline_run_at: datetime | None`) and define rounding to nearest integer (`ROUND_HALF_UP` / `int(round(val))` formatted with `.` thousands separator, e.g., `12.346 IP`).

### Nit 2 — `config.py` Whitespace Trimming & Exact Webhook Path Segments
* **Affected Scope:** [002_discord_notifications.md](../../designs/002_discord_notifications.md#L117-L122), [L304](../../designs/002_discord_notifications.md#L304).
* **Recommendation:** Clarify that `mode="before"` `.strip()`s surrounding whitespace on non-empty URLs, and that `/api/webhooks/{id}/{token}` validates exact 4 non-empty segments (`["api", "webhooks", webhook_id, webhook_token]` after stripping trailing slash).

### Nit 3 — `build_ops_alert_payload` Footer Timestamp Source
* **Affected Scope:** [002_discord_notifications.md](../../designs/002_discord_notifications.md#L143), [L167](../../designs/002_discord_notifications.md#L167).
* **Recommendation:** Clarify that `build_ops_alert_payload` uses `datetime.now(UTC)` internally (since `ScrapeRunSummary` has no `created_at` field).

### Nit 4 — `build_post_fw_payload` Line Templates & Union Query Filtering
* **Affected Scope:** [002_discord_notifications.md](../../designs/002_discord_notifications.md#L176-L184), [L199](../../designs/002_discord_notifications.md#L199).
* **Recommendation:** Confirm all 4 fortress diff cases share `• **{fortress}:** ...`; confirm no-baseline Top 5 omits `[delta]` and extra space; confirm `delta = baseline_rank - current_rank`; confirm `current_top_guilds` filters union rows to `rank_position <= 5`.

---

## 3. Style & Copy Edit Review (`egm-style`)

### Major 6 — Uncalibrated Manual Smoke Test Acceptance Criterion
* **Affected Scope:** Verification Plan § 3 ([002_discord_notifications.md](../../designs/002_discord_notifications.md#L387-L392)).
* **Finding:** Running `python -m src.flows.orchestrator --mode manual --target electus` on a healthy target yields `SUCCESS` (which does not trigger an ops alert), making *"Caso ocorra falha crítica simulada ou real..."* non-reproducible as written.
* **Recommendation:** Calibrate Verification Plan § 3 to state what is verified on a normal run (no ops alert, clean exit) versus a fault-injected test (e.g., temporary unreachable `base_url` or simulated `HTTP_ERROR` triggering the PT-BR ops embed with `trigger_type=manual`).

### Nits 5–10 — Copy Precision in Detailed Implementation Plan & Verification Plan
* Replace *"guardas de borda"* (L315) with concrete guards; replace *"try/except Exception uniforme"* (L316) with *"captura de `Exception` sem propagação nas fachadas públicas"*; drop redundant *"Garantir conformidade com `mypy --strict`"* (L316).
* Split L352 (`tests/test_notifications.py`) into two bullets and replace *"whitespace"* with *"espaços em branco"*.
* Split L362 (`tests/test_flows.py`) into two bullets and fix *"sem falhar se o webhook falhar"*.
* Refine L304 (*"validador `mode="after"` verificando"*; *"`None`, string vazia ou apenas espaços"*), drop *"explicitamente"* / *"explícita"* (L343, L357), and change *"todos os testes unitários"* to *"toda a suíte de testes (`tests/`)"* (L381).
