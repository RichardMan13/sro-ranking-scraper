# Stage 7: Critic Review Report

* **Target Document:** [design_draft.md](design_draft.md) (`FeatureRange = 1..End`)
* **GoldfishContext:** [concepts_brief.md](concepts_brief.md)
* **Reviewers:** `egm-design`, `egm-structural`, `egm-style`
* **Summary:** **2 Blockers**, **9 Majors**, **10 Nits**

---

## 1. Hostile Design Critique (`egm-design`)

### Blocker 1 — Secret Token Leak via Pydantic v2 `ValidationError` (`input_value`) in `mode="before"` Validator
* **Affected Scope:** Technical Plan § 2 (lines 116–121), § 7 (lines 323–326), Detailed Implementation Plan Item 1 (line 405).
* **Finding:** Running URL validation in `@field_validator(..., mode="before")` executes *before* Pydantic wraps the value in `SecretStr`. On `ValidationError` (e.g., operator configures `http://discord.com/api/webhooks/123/SECRET_TOKEN`), Pydantic v2 formats `input_value='<raw_secret_url>'` verbatim into the exception string and startup traceback—violating G3 and Section 7.
* **Recommendation:** Keep `mode="before"` strictly for normalizing `None` / `""` / whitespace-only strings to `None`; run HTTPS/host/path validation in a `mode="after"` validator on `SecretStr | None` (where Pydantic renders `SecretStr('**********')`), and enable `hide_input_in_errors=True` in `SettingsConfigDict`. Add a unit test asserting the raw secret token does not appear in `str(exc_info.value)`.

### Blocker 2 — SQLite Unit Test Strategy Incompatible with PostgreSQL-Dialect ORM Models & Undeclared `aiosqlite` Dependency
* **Affected Scope:** Goals (line 31), Technical Plan § 5 (line 242), NFR § 3 (line 392), Detailed Implementation Plan Item 6 (line 439).
* **Finding:**
  1. `src/models/db_models.py` uses PostgreSQL-only constructs (`UUID`, `JSONB`, `gen_random_uuid()`, `'{}'::jsonb`, `BRIN`/`GIN` indexes, `postgresql_partition_by`), so `Base.metadata.create_all()` cannot compile directly on SQLite.
  2. SQLite strips timezone info from `TIMESTAMP(timezone=True)`, returning naive `datetime` objects that break UTC-aware comparisons/formatting unless normalized.
  3. `aiosqlite` is not declared in `pyproject.toml` (only transitive via `prefect`).
* **Recommendation:** Specify the SQLite test schema provisioning strategy (dedicated SQLite DDL fixture or `@compiles` overrides for the 4 tables `servers`, `scrape_runs`, `guilds`, `guild_snapshots`), normalize datetimes to UTC in `load_post_fw_comparison` / timestamp formatting, and either declare `aiosqlite` under `[dependency-groups] dev` or explicitly document the existing test harness pattern.

### Major 3 — Webhook Secret Exposure via Unsilenced Stdlib `httpx` / `httpcore` `INFO` Logs
* **Affected Scope:** Technical Plan § 3 (line 186), § 7 (lines 323–326).
* **Finding:** `src/core/logging.py` does not silence stdlib `httpx` / `httpcore` loggers. Under Prefect 3 (`INFO` level), `httpx` logs `HTTP Request: POST https://discord.com/api/webhooks/{id}/{token} "HTTP/1.1 204 No Content"`, leaking the webhook secret on every successful send.
* **Recommendation:** Set `logging.getLogger("httpx").setLevel(logging.WARNING)` and `logging.getLogger("httpcore").setLevel(logging.WARNING)` in `configure_logging()`, document this in Section 7, and add a `caplog` test at `INFO` level verifying zero leakage across both `structlog` and stdlib logs.

### Major 4 — Open PostgreSQL Session Held `IDLE IN TRANSACTION` During Outbound Discord HTTP I/O in `post_fw_flow`
* **Affected Scope:** Technical Plan § 1 (line 69), § 3 (line 189), § 6 (lines 285, 303–316).
* **Finding:** `post_fw_flow` executes `notify_post_fw_summary(session, summary, settings=settings)` inside `async with session_factory() as session:`, holding a PostgreSQL connection in `IDLE IN TRANSACTION` for up to 10–20s during external HTTP I/O.
* **Recommendation:** Run only `load_post_fw_comparison(session, summary)` inside `async with` + `engine.dispose()`, and invoke `notify_post_fw_summary(comparison, settings=settings)` after releasing DB resources.

### Major 5 — Per-Phase `httpx.Timeout` Exceeds 10s Wall-Clock Budget & Redirects Not Pinned
* **Affected Scope:** Goals (line 30), Technical Plan § 3 (line 186), § 7 (line 327), NFR § 1 (line 381).
* **Finding:** `httpx.Timeout(connect=5.0, read=10.0, write=5.0, pool=5.0)` is per-phase and can block up to 20s wall-clock. Additionally, `follow_redirects=False` is not explicitly pinned, risking forwarding the webhook secret on `307`/`308` redirects.
* **Recommendation:** Wrap the POST in `async with asyncio.timeout(DISCORD_WEBHOOK_TIMEOUT_SECONDS):` and explicitly set `follow_redirects=False`.

### Major 6 — Missing Top-Level `try/except` Isolation Boundary in `notify_operational_failure` / `scrape_target_task`
* **Affected Scope:** Technical Plan § 3 (lines 186–187), § 6 (line 282), NFR § 3 (line 391).
* **Finding:** Only `send_discord_webhook` is specified to catch `Exception`. If `build_ops_alert_payload` raises an exception, `scrape_target_task` would fail after committing to PostgreSQL.
* **Recommendation:** Require both public facades (`notify_operational_failure` and `notify_post_fw_summary`) to wrap payload construction and HTTP dispatch in `try/except Exception`, logging only sanitized metadata and returning `False`.

### Major 7 — Overly Permissive Webhook URL Validation (`parsed.netloc` Accepts `https://:80` and Arbitrary SSRF Hosts)
* **Affected Scope:** Technical Plan § 2 (line 120), Detailed Implementation Plan Item 1 (line 405).
* **Finding:** Checking `bool(parsed.netloc)` accepts malformed URLs like `https://:80` (`parsed.hostname is None`) and arbitrary HTTPS hosts (SSRF or misconfiguration).
* **Recommendation:** Validate `parsed.hostname` against official Discord webhook hosts (`{"discord.com", "discordapp.com", "ptb.discord.com", "canary.discord.com"}`) and require `parsed.path.startswith("/api/webhooks/")` with non-empty ID/token segments.

### Major 8 — Markdown / Masked-Link Injection in Public Community Embeds & Missing 6,000-Char Aggregate Limit
* **Affected Scope:** Technical Plan § 3 (lines 148–153), § 4 (lines 193–196, 211–212, 227–237).
* **Finding:** `"allowed_mentions": {"parse": []}` does not prevent Discord Embeds from rendering masked links (`[text](https://phishing.site)`) injected via untrusted scraped `guild_name`/`fortress_held` strings into the public community channel. Discord also enforces a 6,000-character aggregate embed cap.
* **Recommendation:** Escape Markdown special characters (`\`, `*`, `_`, `` ` ``, `~`, `[`, `]`, `(`, `)`, `|`, `>`) and collapse newlines on untrusted scraped strings before formatting, and enforce the 6,000-character aggregate embed limit.

### Nit 9 — Baseline Query Determinism, Empty `IN ()` / Whitespace `fortress_held` Guards, and Defense-in-Depth `server_id` Filter
* **Affected Scope:** Technical Plan § 5 (lines 248–260).
* **Recommendation:** Add `ScrapeRun.id.desc()` tie-breaker to baseline ordering; filter `GuildSnapshot.server_id == summary.server_id` in both snapshot queries; guard empty `current_top_guild_ids` before `.in_(...)`; strip and ignore whitespace-only `fortress_held` values.

### Nit 10 — PT-BR Number Formatting Consistency & PostgreSQL 16 Verification for `post_fw_flow`
* **Affected Scope:** Technical Plan § 4B (line 236), Verification Plan (lines 469–475).
* **Recommendation:** Clarify or localize `{total_item_points:,.0f}` to PT-BR (`12.345 IP`), and add an optional PostgreSQL 16 verification step for `post_fw_flow` (including `EXPLAIN` partition pruning).

---

## 2. Structural Audit (`egm-structural`)

**Verdict:** PASS (0 Blockers, 0 Majors, 4 Nits). All mandatory sections, heading hierarchy (`H1 -> H4`), fenced blocks, Mermaid diagram, tables, and traceability to `concepts_brief.md` are valid.

* **Nit 11 — Asymmetric Sibling Heading Languages (Lines 133–238, 380–390):** `### 1. Performance & Latency` is in English while siblings `### 2. Segurança & Privacidade` and `### 3. Confiabilidade & Portabilidade` are in Portuguese (similarly H3 vs H4 in Technical Plan). Standardize sibling heading language.
* **Nit 12 — Relative Link Breakage on Promotion (Lines 6, 44):** `[concepts_brief.md](concepts_brief.md)` will break when promoted to `docs/designs/002_discord_notifications.md`; update to `../egm/002_discord_notifications/concepts_brief.md` upon promotion.
* **Nit 13 — Bundled Files in Implementation Plan (Lines 427, 441):** Items 5 (`docker-compose.yml` & `.env.example`) and 7 (`tests/test_models_db.py` & `tests/test_flows.py`) bundle two files under one H4. Split to one file per H4 for checklist symmetry.
* **Nit 14 — Backtick-Wrapped Prose in Alternatives Considered (Lines 337, 342, 347, 352, 357, 362):** Render `Reason for Rejection` as normal prose and reserve backticks for code/CLI identifiers.

---

## 3. Style & Copy Edit Review (`egm-style`)

**Verdict:** 0 Blockers, 3 Majors, 4 Nits.

* **Major 9 — Uncalibrated `docker compose config` Verification Claim (Line 467):** `docker compose config` validates YAML interpolation and declared ports; it does not start the container or execute Python/Pydantic `Settings` validation. Calibrate to state that `Settings` empty-string handling is verified by unit tests (or an explicit startup/CLI check).
* **Major 10 — Uncalibrated Resource & Performance Claims (Lines 372, 382–383):**
  * Line 382: Replace *"mantém folga dentro do teto rígido de 512MB de RAM"* with the factual invariant (zero new dependencies or resident processes added under the 512MB cap).
  * Line 383: Frame `< 50ms` as a target/expected bound rather than a guaranteed benchmarked outcome.
  * Line 372 (ADR-009): Replace *"preserva alta performance no PostgreSQL 16"* with *"preserva partition pruning no PostgreSQL 16"*.
* **Major 11 — Internal Process Jargon Leak (Line 362):** Remove `egm-zoom-out` from Alternative F's rejection rationale and retain only the technical rationale.
* **Nit 15 — Promotional Adjectives & Filler (Lines 12, 24, 335):** Remove *"leve"*, *"diretamente"*, *"coeso"*, *"estrito"*, *"rigoroso"*, *"rica"*.
* **Nit 16 — Decorative Adverbs, Dramatic Intensifiers & Colloquialism (Lines 30–31, 69, 107, 196, 285, 327–328, 371, 381, 388, 391, 437):** Replace *"estritamente"*, *"naturalmente"*, *"rígidos"*, *"jamais"*, *"100% de compatibilidade"*, *"rastreabilidade completa"*, *"eliminando qualquer risco"*, *"Isolamento Total"*, *"ausência absoluta"*, and *"servidor quebrado"* (use *"servidor em estado de falha"*).
* **Nit 17 — Imprecise Modifier on Month Boundary (Line 257):** Replace *"janela estrita"* and *"na virada exata do mês"* with *"janela de `±5 min`"* and *"próximo à virada do mês"*.
* **Nit 18 — Backtick-Wrapped Prose in Alternatives Considered (Lines 337–362):** Same as Nit 14 above.
