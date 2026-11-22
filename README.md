# SRO Ranking Scraper

A containerized, HTTP-first data pipeline that extracts public player, guild, and in-game job (Trader, Thief, Hunter) rankings from Silkroad Online private servers, validates them with Pydantic v2, persists append-only monthly-partitioned snapshots into PostgreSQL 16, and exposes analytical SQL views for BI clients.

## Architecture Overview

```text
[Chronicle / Electus / Play Golden]
        │
        ▼ (HTTPX HTTP/2 + Semaphore(3) + lxml / orjson)
[BaseRankingScraper + Persistent Circuit Breaker]
        │
        ├── Blocked / Drift / Low Count ──▶ ./quarantine/{server_id}/{run_id}.html.gz + scrape_runs
        │
        ▼ Validated & Job->Player Reconciled
[PostgreSQL 16 (dimensions + RANGE-partitioned snapshots)]
        │
        ▼
[5 Analytical SQL Views] ──▶ Power BI / Streamlit / psql
```

### Target Servers
- **Chronicle SRO** (`chronicle`): `https://play-chronicle.com` (HTML via `lxml`)
- **Electus Online** (`electus`): `https://electus.online` (JSON API via `orjson`, HTML fallback, optional `ELECTUS_CF_CLEARANCE`)
- **Play Golden** (`play_golden`): `https://play-golden.online` (HTML via `lxml`)

### Core Safeguards
- **Append-Only Partitioned Storage:** `character_snapshots` and `guild_snapshots` are partitioned by `RANGE (created_at)` with automated monthly partition provisioning (`ensure_monthly_partitions`).
- **Persistent Circuit Breaker:** HTTP `403`/`429` or Cloudflare/Turnstile challenges trip a configurable cooldown (`servers.circuit_breaker_until`, default 6 hours) and record `BLOCKED_CHALLENGE` in `scrape_runs`. Maintenance pages (`503` / maintenance banners) record `TARGET_MAINTENANCE` without tripping the 6-hour cooldown.
- **Top-Rank Integrity Guard & Quarantine:** Rejects batches if podium ranks `1..3` fail validation (`SCHEMA_DRIFT`), if error rate exceeds `5%`, or if counts fall below `min_records_expected` / `5` guilds (`LOW_COUNT_QUARANTINE`). Rejected/blocked payloads are gzipped to `./quarantine/{server_id}/{run_id}.html.gz` with SHA-256 checksums and purged after 30 days by `mtime`.

---

## Subdirectories & System Boundaries
- [alembic](./alembic/README.md) — Async Alembic migration runner (`env.py`) and linear PostgreSQL 16 migration chain ([alembic/versions](./alembic/versions/README.md)) defining tables, server seeds, monthly range partitions, indexes, and 5 analytical SQL views.
- [src](./src/README.md) — Core Python application package organized into:
  - [src/core](./src/core/README.md) — Pydantic environment settings, async SQLAlchemy engine/session factories, monthly partition DDL provisioning, and JSON `structlog` configuration.
  - [src/models](./src/models/README.md) — SQLAlchemy 2.0 PostgreSQL ORM models and Pydantic v2 raw ranking validation/normalization schemas.
  - [src/scrapers](./src/scrapers/README.md) — HTTP/2 scraper engine (`BaseRankingScraper`), target server parsers (`ChronicleScraper`, `ElectusScraper`, `PlayGoldenScraper`), circuit breaker, integrity guards, gzip quarantine, and bulk persistence.
  - [src/flows](./src/flows/README.md) — Prefect 3 tasks, scheduled/post-FW/manual flows, dynamic cron deployment builder, and CLI entrypoint.

## Global Invariants
- **Append-Only Partitioned Fact Tables:** `character_snapshots` and `guild_snapshots` use PostgreSQL `RANGE (created_at)` partitioning with composite primary keys `(id, created_at)` and unique constraints including `created_at`; every Prefect flow invokes `ensure_partitions_task` before scraping.
- **Canonical Status & Trigger Alignment:** `scrape_runs.status` (`ck_scrape_runs_status`) stays aligned with `CANONICAL_STATUSES` (`SUCCESS`, `PARTIAL_SUCCESS`, `SCHEMA_DRIFT`, `LOW_COUNT_QUARANTINE`, `BLOCKED_CHALLENGE`, `TARGET_MAINTENANCE`, `HTTP_ERROR`), and `BaseRankingScraper.run` validates `trigger_type` against `CANONICAL_TRIGGER_TYPES`.
- **Circuit Breaker & Retry Isolation:** `BaseRankingScraper.run` checks `servers.circuit_breaker_until` (UTC) before any network call; anti-bot challenges (`403`/`429`, `cf-mitigated: challenge`, Cloudflare/Turnstile markers) and maintenance responses abort with zero retries, while Prefect tasks execute with `retries=0` and isolate multi-server failures via `return_state=True`.
- **No Raw HTML in PostgreSQL:** Rejected or challenge response payloads are quarantined solely as deterministic `.html.gz` files on local disk (`<quarantine_dir>/<server_id>/<run_id>.html.gz`), never stored in PostgreSQL columns.
- **Integrity Guards & Reconciliation:** Validation enforces entity deduplication (lowest rank wins), podium rank `1..3` checks for players and guilds, `MAX_ERROR_RATE = 0.05`, minimum player (`servers.min_records_expected`) and guild (`MIN_GUILD_RECORDS = 5`) thresholds, and case-sensitive job-to-player left-join reconciliation.

## Global Side Effects
- **External Network I/O:** Issues async HTTP/2 `GET` requests via `httpx.AsyncClient` to Chronicle SRO, Electus Online, and Play Golden with rotating browser header profiles and optional `cf_clearance` cookies.
- **PostgreSQL 16 Mutations:** Runs Alembic DDL/DML migrations, executes `CREATE TABLE IF NOT EXISTS ... PARTITION OF` monthly partition DDL, updates `servers.circuit_breaker_until`, records `scrape_runs` audit rows, bulk-upserts `guilds` and `characters`, and bulk-inserts `character_snapshots` and `guild_snapshots` in 1,000-row chunks.
- **Local Filesystem & Process State:** Writes gzipped dead-letter archives to `<quarantine_dir>/<server_id>/<run_id>.html.gz`, recursively unlinks expired `*.html.gz` files in `purge_old_quarantine_task`, configures global `structlog` JSON stdout logging and contextvars, and registers/serves Prefect 3 cron deployments.

## System Verification
- Run: `pytest`

---

## Environment Configuration

Copy `.env.example` to `.env` before starting production or custom deployments and replace placeholder credentials:

```bash
cp .env.example .env
```

| Variable | Description | Default / Example |
|---|---|---|
| `POSTGRES_USER` | PostgreSQL role used inside `sro_network` | `sro_user` |
| `POSTGRES_PASSWORD` | PostgreSQL password (set a strong value in `.env`) | `CHANGE_ME_STRONG_PASSWORD` |
| `POSTGRES_DB` | PostgreSQL database name | `sro_rankings` |
| `DATABASE_URL` | Async SQLAlchemy DSN (`postgresql+asyncpg://...`) | `postgresql+asyncpg://sro_user:...@postgres:5432/sro_rankings` |
| `PREFECT_API_URL` | Prefect 3 API URL (`prefect-server` in Compose; `localhost` on host) | `http://prefect-server:4200/api` |
| `LOG_LEVEL` | `structlog` JSON log threshold | `INFO` |
| `QUARANTINE_DIR` | Local dead-letter directory for `.html.gz` artifacts | `./quarantine` (container: `/app/quarantine`) |
| `QUARANTINE_RETENTION_DAYS` | Days before old `.html.gz` quarantine files are purged | `30` |
| `CIRCUIT_BREAKER_COOLDOWN_HOURS` | Cooldown hours after anti-bot challenge | `6` |
| `ELECTUS_CF_CLEARANCE` | Optional `cf_clearance` cookie for Electus Online | *(empty)* |

---

## Quick Start (Docker Compose)

The Compose stack defines three services on the internal `sro_network`:
1. **`postgres`** (`postgres:16-alpine`, `mem_limit: 1g`, not exposed on host port `5432`)
2. **`prefect-server`** (`prefecthq/prefect:3-python3.11`, UI/API on `http://localhost:4200`)
3. **`scraper-worker`** (Python 3.11 non-root `appuser`, `mem_limit: 512m`, mounts `./quarantine:/app/quarantine`)

```bash
# 1. Build the scraper-worker image
docker compose build

# 2. Start PostgreSQL 16, Prefect 3 server, and scraper-worker
docker compose up -d

# 3. Verify / apply database migrations, monthly partitions, and analytical SQL views
docker compose exec scraper-worker alembic upgrade head

# 4. Check container status and logs
docker compose ps
docker compose logs -f scraper-worker
```

For local (non-containerized) development against a reachable PostgreSQL instance, run:

```bash
alembic upgrade head
```

---

## Orchestration, CLI & Prefect UI

### 1. Scheduled Mode (`--mode serve`)
The `scraper-worker` container runs `python -m src.flows.orchestrator --mode serve` by default. At startup, it queries active rows in `servers` and registers four Prefect 3 deployments:
- `scheduled-12h` (`cron="0 0,12 * * *"`) — runs `ensure_partitions_task`, scrapes every active server in isolation (`return_state=True`), and runs `purge_old_quarantine_task`.
- `post-fw-chronicle` (`cron="30 22 * * 0"`) — runs 30 minutes after Chronicle Fortress War (Sunday 22:00 UTC).
- `post-fw-electus` (`cron="30 21 * * 0"`) — runs 30 minutes after Electus Fortress War (Sunday 21:00 UTC).
- `post-fw-play_golden` (`cron="30 20 * * 6"`) — runs 30 minutes after Play Golden Fortress War (Saturday 20:00 UTC).

To launch the deployment runner manually:

```bash
python -m src.flows.orchestrator --mode serve
```

### 2. Manual Scrape Mode (`--mode manual`)
Trigger an immediate extraction run for a single server (`chronicle`, `electus`, `play_golden`) or `all` active servers with `trigger_type='manual'`:

```bash
# Inside Docker Compose
docker compose exec scraper-worker python -m src.flows.orchestrator --mode manual --target chronicle

# Or run all active servers locally
python -m src.flows.orchestrator --mode manual --target all
```

### 3. Verifying in the Prefect 3 UI
Open `http://localhost:4200` in a browser to verify:
- **Deployments:** Confirm `scheduled-12h`, `post-fw-chronicle`, `post-fw-electus`, and `post-fw-play_golden` appear as `Ready`.
- **Flow Runs:** Inspect completed/failed task states (`ensure-partitions`, `scrape-target`, `purge-old-quarantine`) and trigger ad-hoc runs.

---

## Analytical SQL Views & Operational Queries

Migration `002_analytical_views` creates five PostgreSQL views. Connect via `psql` inside the `postgres` container:

```bash
docker compose exec postgres psql -U sro_user -d sro_rankings
```

### 1. `v_character_activity_deltas`
Tracks progression deltas between consecutive snapshots per character (requires at least 2 snapshots):

```sql
SELECT
    server_id,
    char_name,
    race,
    rank_position,
    rank_gain,
    level,
    delta_level,
    delta_item_points,
    hours_since_last_snapshot,
    item_points_rate_per_hour,
    created_at
FROM v_character_activity_deltas
WHERE server_id = 'chronicle'
ORDER BY item_points_rate_per_hour DESC
LIMIT 20;
```

### 2. `v_bot_vs_donor_classification`
Classifies characters with >= 6 activity intervals over the last 7 days into `SUSPECTED_BOT_24_7`, `SUSPECTED_DONATOR_WHALE`, `HARDCORE_HUMAN`, or `CASUAL_OR_BALANCED`:

```sql
SELECT
    server_id,
    char_name,
    race,
    evaluated_intervals,
    total_item_points_gain,
    max_single_jump_item_points,
    grind_persistence_ratio,
    player_behavior_segment
FROM v_bot_vs_donor_classification
WHERE server_id = 'chronicle'
ORDER BY grind_persistence_ratio DESC, total_item_points_gain DESC
LIMIT 25;
```

### 3. `v_guild_turnover_dominance`
Summarizes Top-100 and Top-50 dominance per guild on the latest valid run (players without a guild appear as `[SEM GUILDA]`):

```sql
SELECT
    server_id,
    guild_name,
    players_in_top_100,
    players_in_top_50,
    total_top_item_points,
    top_100_market_share_percent
FROM v_guild_turnover_dominance
WHERE server_id = 'chronicle';
```

### 4. `v_race_balance_ch_vs_eu`
Compares Chinese (`CH`) vs. European (`EU`) representation, Top-100 counts, average level, and item points on the latest valid run:

```sql
SELECT
    server_id,
    race,
    total_ranked_characters,
    top_100_count,
    avg_level,
    avg_item_points,
    max_item_points
FROM v_race_balance_ch_vs_eu
ORDER BY server_id, race;
```

### 5. `v_server_retention_weekly`
Computes weekly character activity retention rates across tracked servers:

```sql
SELECT
    server_id,
    week_start,
    total_tracked_characters,
    active_characters,
    activity_retention_rate_pct
FROM v_server_retention_weekly
ORDER BY server_id, week_start DESC;
```

### Operational Audit Queries (`scrape_runs` & `servers`)

```sql
SELECT
    id,
    server_id,
    trigger_type,
    status,
    records_extracted,
    records_persisted,
    validation_errors_count,
    execution_time_ms,
    quarantine_file_path,
    created_at
FROM scrape_runs
ORDER BY created_at DESC
LIMIT 20;

SELECT id, is_active, circuit_breaker_until
FROM servers
ORDER BY id;
```

---

## Quarantine Inspection & Development Quality Gates

### Inspecting Quarantined Payloads
```bash
ls -lh ./quarantine/chronicle/
gzip -dc ./quarantine/chronicle/<run_id>.html.gz | head -n 40
```

### Verification Checklist
```bash
ruff check .
ruff format --check .
mypy src tests
pytest --cov=src tests/
```
