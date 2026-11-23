# Goldfish Annotated Review Report (Stage 9 — Implementation Readiness)

**Status:** resolved
**Target:** `docs/designs/002_discord_notifications.md` (`lines 295..396`, plus contract clarifications in `lines 117..199`)

## Prioritized Findings & AI Suggestions

### [Issue 1] [`egm-codebase-details`] - Severity: Major
*   **Affected Scope:** [Lines 316, 327](../../designs/002_discord_notifications.md#L316) (Detailed Implementation Plan — `src/core/notifications.py`, `src/flows/orchestrator.py`)
*   **Discrepancy Description:** `structlog.processors.JSONRenderer()` in `src/core/logging.py` uses stdlib `json.dumps` without `default=str`, so passing `run_id: uuid.UUID` directly to `logger.*(...)` raises `TypeError` and can escape `except Exception:` fault isolation.
*   **Rationale:** Verified in `src/core/logging.py:23-36` and `logging.py:56` (`str(run_id)`).
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:*** Essential runtime bug prevention before coding.
*   ***Proposed Edit:***
    *   Location: Detailed Implementation Plan items 3 (`src/core/notifications.py`) and 5 (`src/flows/orchestrator.py`).
    *   Change: Explicitly require `run_id=str(run_id)` in every `structlog` call.

### [Issue 2] [`egm-codebase-details`] - Severity: Major
*   **Affected Scope:** [Lines 348, 352](../../designs/002_discord_notifications.md#L348) (Detailed Implementation Plan — `tests/test_notifications.py`)
*   **Discrepancy Description:** In SQLite, `postgresql.UUID(as_uuid=True)` binds as 32-char hex without hyphens (`u.hex`), so seeding raw SQL rows with hyphenated `str(u)` causes ORM queries to return zero rows; SQLite DDL must also omit `::jsonb` and `Identity(always=True)` composite PK.
*   **Rationale:** Verified in `src/models/db_models.py`.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:*** Eliminates guaranteed test fixture failure during implementation.
*   ***Proposed Edit:***
    *   Location: Detailed Implementation Plan item 9 (`tests/test_notifications.py`).
    *   Change: Specify seeding SQLite rows via bound `uuid.UUID` parameters (or `u.hex`) and DDL using `TEXT NOT NULL DEFAULT '{}'` (without `::jsonb`) and `INTEGER PRIMARY KEY` on `guild_snapshots`.

### [Issue 3] [`egm-codebase-details`] - Severity: Major
*   **Affected Scope:** [Lines 345-353](../../designs/002_discord_notifications.md#L345-L353) (Detailed Implementation Plan — `tests/test_notifications.py`)
*   **Discrepancy Description:** Existing async tests use synchronous `def test_...()` with `_run_async(...)` (imported from `tests.test_validation`) to avoid Windows sandbox `socket.socketpair()` blocks, plus `create_engine_from_url("sqlite+aiosqlite:///:memory:")` and `capsys` for `structlog` capture.
*   **Rationale:** Verified in `tests/test_validation.py` and `tests/test_flows.py:28`.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:*** Ensures `pytest` runs cleanly in Windows sandboxes and reuses existing test helpers.
*   ***Proposed Edit:***
    *   Location: Detailed Implementation Plan item 9 (`tests/test_notifications.py`).
    *   Change: Specify reusing `_run_async` from `tests.test_validation`, `create_engine_from_url("sqlite+aiosqlite:///:memory:")`, `capsys` for `structlog`, and `caplog.at_level(logging.INFO)` on the root logger for stdlib `httpx` suppression.

### [Issue 4] [`egm-codebase-details`] - Severity: Major
*   **Affected Scope:** [Lines 359-362](../../designs/002_discord_notifications.md#L359-L362) (Detailed Implementation Plan — `tests/test_flows.py`)
*   **Discrepancy Description:** `_make_test_settings()` in `tests/test_flows.py` does not pass `discord_ops_webhook_url=None` and `discord_community_webhook_url=None`, allowing shell env vars to leak into existing `test_post_fw_flow_uses_post_fw_extra_trigger`.
*   **Rationale:** Verified in `tests/test_flows.py:31-41`.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:*** Prevents flaky failures whenever webhook env vars are set in the shell.
*   ***Proposed Edit:***
    *   Location: Detailed Implementation Plan item 11 (`tests/test_flows.py`).
    *   Change: Require `_make_test_settings()` to default both webhook fields to `None` with optional keyword overrides.

### [Issue 5] [`egm-implementation-readiness`] - Severity: Major
*   **Affected Scope:** [Lines 136-138, 155, 183, 314-315](../../designs/002_discord_notifications.md#L136-L138) (Technical Plan § 3, § 4B, Detailed Implementation Plan item 3)
*   **Discrepancy Description:** `GuildRankEntry` and `PostFWComparison` omit explicit Python types for several fields (`total_item_points: Decimal`, `current_status: str`, `baseline_run_id: uuid.UUID | None`, `baseline_run_at: datetime | None`), and `12.345 IP` does not specify rounding vs truncation for fractional `NUMERIC(12, 2)` values.
*   **Rationale:** Avoids `mypy --strict` friction and ambiguous test expectations.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:*** Makes `GuildRankEntry`, `PostFWComparison`, and PT-BR numeric formatting unambiguous.
*   ***Proposed Edit:***
    *   Location: Technical Plan § 3, § 4B, and Detailed Implementation Plan item 3.
    *   Change: Specify exact field types (`total_item_points: Decimal` coerced via `Decimal(str(val))` on SQLite read, `current_status: str` matching `ScrapeRunSummary.status`) and define rounding to nearest integer (`ROUND_HALF_UP`) with `.` thousands separator (e.g., `12345.67` -> `12.346 IP`).

### [Issue 6] [`egm-style`] - Severity: Major
*   **Affected Scope:** [Lines 387-392](../../designs/002_discord_notifications.md#L387-L392) (Verification Plan § 3)
*   **Discrepancy Description:** Smoke test command `--mode manual --target electus` runs a normal scrape (`SUCCESS`, no ops alert), leaving *"Caso ocorra falha crítica simulada ou real"* non-reproducible.
*   **Rationale:** Acceptance criteria must be deterministic and reproducible.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:*** Clarifying normal run vs. fault-injected run makes Verification Plan § 3 actionable.
*   ***Proposed Edit:***
    *   Location: Verification Plan § 3.
    *   Change: Distinguish expected behavior on a normal run (no ops alert on `SUCCESS`) versus a fault-injected run (e.g., temporary unreachable `base_url` triggering `HTTP_ERROR` and delivering the PT-BR ops embed with `trigger_type=manual`).

### [Issue 7] [`egm-codebase-details` / `egm-implementation-readiness` / `egm-style`] - Severity: Nit
*   **Affected Scope:** [Lines 117-122, 143, 176-184, 199, 304, 315-316, 327, 343, 352, 357, 362, 381](../../designs/002_discord_notifications.md#L304) (Contract & Implementation Plan Nits)
*   **Discrepancy Description:** Minor implementation and copy clarifications: `# noqa: E402` on new imports in `tasks.py`/`orchestrator.py`; `engine: AsyncEngine | None = None` + `logger = get_logger("orchestrator")` in `post_fw_flow`; `.strip()` on non-empty URLs and exact 4 path segments `["api", "webhooks", id, token]` (plus `SecretStr` wrapping empty string) in `config.py`; `datetime.now(UTC)` in `build_ops_alert_payload`; consistent `• **{fortress}:**` prefix, no-baseline `[delta]` omission, `delta = baseline_rank - current_rank`, and `rank_position <= 5` filter on `current_top_guilds`; copy tightening in items 1, 3, 8, 9, 10, 11 and Verification Plan § 1.
*   **Rationale:** Eliminates all remaining micro-ambiguities before writing code.
*   ***AI Suggestion:*** PROPOSE FIX
*   ***Justification:*** Quick, surgical edits that ensure zero friction during implementation.
*   ***Proposed Edit:***
    *   Location: Technical Plan § 2, § 4, § 5 and Detailed Implementation Plan items 1, 3, 4, 5, 8, 9, 10, 11 + Verification Plan § 1.
    *   Change: Apply all minor technical and copy refinements listed above.
