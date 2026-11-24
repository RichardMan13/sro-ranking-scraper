# EGM Bootstrap Drift Report (2026-11-24)

> **Status: RESOLVED (Zero Drift — 2026-11-24).** Todos os 6 desvios originais abaixo e os desvios subsequentes introduzidos por `FEAT-001` e `FEAT-002` (`README.md`, `alembic/README.md`, `alembic/versions/README.md`, `src/README.md`, `src/core/README.md`, `src/flows/README.md`, `src/scrapers/README.md`) foram corrigidos e validados como **ZERO DRIFT (12/12 arquivos limpos)** por auditoria independente `egm-codebase-details`.

## alembic/versions/README.md

No drift detected.

## alembic/README.md

- **Inaccurate invariant scope (`alembic/README.md:L20` vs `alembic/env.py:L49-L51`):** README states under `[env.py:_resolve_database_url]` that *"In offline mode, falls back to `_DEFAULT_OFFLINE_URL` when no URL is configured and escapes `%` as `%%` when writing `sqlalchemy.url` back to Alembic config."* In actual code (`alembic/env.py:L50`), `config.set_main_option("sqlalchemy.url", resolved_url.replace("%", "%%"))` executes unconditionally for both online and offline modes, not only in offline mode.

## scripts/README.md

No drift detected.

## src/core/README.md

- **Missing public constants (`src/core/notifications.py:L39-L53`):** The following 9 public module-level constants are exported in `src/core/notifications.py` but omitted from `src/core/README.md` (some numeric values are mentioned inline without naming the symbols):
  - `WEBHOOK_TIMEOUT_SECONDS` (`10.0`, `L39`)
  - `WEBHOOK_HTTP_TIMEOUT` (`httpx.Timeout(connect=5.0, read=10.0, write=5.0, pool=5.0)`, `L40-L45`)
  - `POST_FW_BASELINE_WINDOW_HOURS` (`24`, `L46`)
  - `POST_FW_SNAPSHOT_TOLERANCE_MINUTES` (`5`, `L47`)
  - `POST_FW_TOP_GUILDS_LIMIT` (`5`, `L48`)
  - `EMBED_TITLE_MAX_LEN` (`256`, `L50`)
  - `EMBED_DESCRIPTION_MAX_LEN` (`4096`, `L51`)
  - `EMBED_FIELD_VALUE_MAX_LEN` (`1024`, `L52`)
  - `EMBED_FOOTER_MAX_LEN` (`2048`, `L53`)

## src/models/README.md

- **Missing ORM index coverage (`src/models/db_models.py`):** 8 SQLAlchemy `Index` definitions declared in `__table_args__` are not documented in `src/models/README.md`:
  - `ScrapeRun` (`src/models/db_models.py:L82-L87`): partial index `idx_scrape_runs_server_latest` on `(server_id, created_at DESC) WHERE status IN ('SUCCESS', 'PARTIAL_SUCCESS')`.
  - `CharacterSnapshot` (`src/models/db_models.py:L170-L191`): `idx_char_snapshots_lookup`, `idx_char_snapshots_rank`, `idx_char_snapshots_brin_time` (`USING brin`), and `idx_char_snapshots_extra_metrics` (`USING gin`).
  - `GuildSnapshot` (`src/models/db_models.py:L246-L262`): `idx_guild_snapshots_lookup`, `idx_guild_snapshots_rank`, and `idx_guild_snapshots_brin_time` (`USING brin`).

## src/dashboard/README.md

- **Doc-to-code divergence (`src/dashboard/README.md:L9` vs `src/dashboard/config.py:L60-L70`):** README states `[config.py:DashboardSettings.validate_sync_postgres_url]` requires *"a valid hostname and port"*. In `config.py:L61-L70`, `parsed.hostname` is required (`if not parsed.hostname: raise ValueError(...)`), whereas a port is **optional**—evaluating `_ = parsed.port` only rejects malformed ports and permits omitted ports (`None`).

## src/flows/README.md

No drift detected.

## src/scrapers/README.md

- **Missing public constant (`src/scrapers/base.py:L47`):** `MAX_SAMPLE_ERRORS: Final[int] = 25` (which caps the number of serialized validation error entries collected into `ErrorDetails.sample_errors` at `base.py:L898,L906,L917`) is not documented in `src/scrapers/README.md`.

## src/README.md

No drift detected.

## README.md

- **Doc-to-code divergence (`README.md:L191` vs `docker-compose.yml:L64-L67`):** `README.md:L191` states *"The `scraper-worker` container runs `python -m src.flows.orchestrator --mode serve` by default."* While that matches `Dockerfile:L35`, `docker-compose.yml:L64-L67` overrides the `scraper-worker` service `command` to `sh -c "alembic upgrade head && exec python -m src.flows.orchestrator --mode serve"`.
