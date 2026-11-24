# Source Package (`src`)

## Overview
Top-level Python package grouping the SRO Ranking Scraper application into five decoupled subsystems: core infrastructure (`core`), data models and validation schemas (`models`), HTTP-first server scrapers and persistence (`scrapers`), Prefect 3 orchestration (`flows`), and the read-only analytical Streamlit dashboard (`dashboard`).

## Subdirectories & Boundaries
- [core](./core/README.md) — Environment settings (`Settings`), async SQLAlchemy engine/session factories, monthly PostgreSQL partition DDL management (`ensure_monthly_partitions`), JSON `structlog` logging, and best-effort Discord webhook notifications (`notifications.py`).
- [models](./models/README.md) — SQLAlchemy 2.0 PostgreSQL ORM models (`Server`, `ScrapeRun`, `Character`, `Guild`, `CharacterSnapshot`, `GuildSnapshot`) and Pydantic v2 validation/normalization schemas (`RawPlayerRankingItem`, `RawJobRankingItem`, `RawGuildRankingItem`, `ErrorDetails`).
- [scrapers](./scrapers/README.md) — HTTP/2 extraction engine (`BaseRankingScraper`), target server parsers (`ChronicleScraper`, `ElectusScraper`, `PlayGoldenScraper`, `RSilkroadScraper`), persistent circuit breaker, Top-Rank Integrity Guards, gzip dead-letter quarantine, and bulk PostgreSQL persistence.
- [flows](./flows/README.md) — Prefect 3 tasks (`ensure_partitions_task`, `scrape_target_task`, `purge_old_quarantine_task`), flows (`scheduled_12h_flow`, `post_fw_flow`, `manual_scrape_flow`), cron deployment builder, and CLI entrypoint.
- [dashboard](./dashboard/README.md) — Isolated read-only Streamlit + Altair analytical dashboard (`DashboardSettings`, synchronous `psycopg` engine with read-only guardrails, immutable DTOs, partition-pruned PostgreSQL 16 queries, and lazy single-page UI).
