# SRO Ranking Scraper

A minimal pipeline that extracts public player and guild rankings from private Silkroad Online servers, stores them as temporal snapshots in PostgreSQL, and exposes analytical SQL views for Streamlit.

## Quick Overview
```
[Ranking Sites] → HTTPX Scraper → Pydantic validation → PostgreSQL snapshots
```
- **Sources**: Chronicle SRO, Electus Online, Play Golden.
- **Key Features**:
  - Append‑only, auditable `created_at`/`updated_at` columns.
  - Per‑server circuit‑breaker persisted in `servers` table.
  - Bulk upsert of dimension tables (`characters`, `guilds`).
  - Automated quarantine of malformed payloads.
  - Prefect 3 orchestration with 12‑hour cadence and post‑Fortress‑War runs.

## Getting Started
```bash
# Build containers
docker compose up -d
# Run the scraper (Prefect flow)
prefect deployment run "sro-scraper" -p server=chronicle
```

*(See `docs/designs/001_sro_ranking_scraper_pipeline.md` for full architecture and schema details.)*