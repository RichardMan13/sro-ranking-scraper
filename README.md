# SRO Ranking Scraper

> **Status:** Design / Pre-implementation — architecture and schema details are specified in [001_sro_ranking_scraper_pipeline.md](docs/designs/001_sro_ranking_scraper_pipeline.md) (in Portuguese); implementation files (`src/`, `docker-compose.yml`, Alembic migrations) are not yet checked in.

A containerized pipeline that extracts public player, guild, and in-game job (Trader, Thief, Hunter) rankings from private Silkroad Online (SRO) servers, stores them as temporal snapshots in PostgreSQL, and exposes analytical SQL views for Power BI, Streamlit, or SQL clients. Dashboards are out of scope for v1.0.

## Overview
```text
[Ranking Sites] -> HTTPX Scraper -> Pydantic Validation -> PostgreSQL Snapshots -> Analytical SQL Views
```
- **Sources**: Chronicle SRO, Electus Online, Play Golden.
- **Planned Features**:
  - Append-only partitioned snapshot tables (`character_snapshots`, `guild_snapshots`) with `created_at`/`updated_at` audit timestamps.
  - Bulk upsert of dimension tables (`characters`, `guilds`) tracked with `first_seen_at`/`last_seen_at`.
  - Per-server circuit breaker persisted in the `servers` table (opens on HTTP 403/429 or anti-bot challenges with a configurable cooldown).
  - Quarantine of anomalous or blocked payloads as compressed files for offline inspection.
  - Prefect 3 orchestration on a 12-hour cadence (`scheduled_12h_flow`) plus post-Fortress War runs (weekly in-game guild siege event, e.g., `post-fw-chronicle`).

## Planned Usage (Post-Implementation)
```bash
# 1. Create a .env file with configuration variables:
#    DATABASE_URL, PREFECT_API_URL, LOG_LEVEL, QUARANTINE_DIR, CIRCUIT_BREAKER_COOLDOWN_HOURS

# 2. Build and start containers (PostgreSQL 16, Prefect 3 server, and scraper-worker)
docker compose up -d --build

# 3. Apply database schema, partitions, and analytical SQL views
docker compose exec scraper-worker alembic upgrade head

# 4. Run a manual scrape for a server (chronicle | electus | play_golden)
docker compose exec scraper-worker python -m src.flows.orchestrator --mode manual --target chronicle
```

