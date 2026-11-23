# Designs Sitemap

> **Memória do Elefante (EGM):** Este arquivo é o índice mestre de arquitetura do repositório. Nenhum código deve ser escrito sem consultar este mapa e o design doc correspondente em `docs/designs/`.

## Design Documents

| Document | Date | Author | Components | Description | Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| [001_sro_ranking_scraper_pipeline.md](001_sro_ranking_scraper_pipeline.md) | 2026-11-18 | richardman13 | Scrapers, Prefect 3, PostgreSQL 16, Pydantic v2, Alembic | Pipeline containerizado append-only de extração de rankings de Silkroad Online com views analíticas de séries temporais | Implemented |
| [002_discord_notifications.md](002_discord_notifications.md) | 2026-11-22 | richardman13 | Notifications, Discord Webhooks, Prefect 3, Pydantic v2, SQLAlchemy 2.0 | Alertas operacionais em falhas críticas e resumo automático pós-Fortress War via webhooks do Discord | Implemented |
| [003_analytical_dashboard.md](003_analytical_dashboard.md) | 2026-11-22 | richardman13 | Dashboard, Streamlit, Altair, PostgreSQL 16, Alembic, Docker Compose | Dashboard analítico read-only em Streamlit para 1 usuário na VPS Hostinger KVM 2 atrás de Traefik/EasyPanel | Approved |

## Status Legend

- **Draft**: Proposta inicial em elaboração.
- **In Review**: Sob revisão crítica (ciclo Goldfish / EGM).
- **Approved**: Aprovado para implementação; fonte de verdade arquitetural.
- **Implemented**: Totalmente implementado e verificado em código.
- **Deprecated**: Substituído por um design mais recente.

## EGM Review History

Os relatórios de revisão crítica gerados durante o refinamento de cada design são preservados em `docs/egm/` para rastreabilidade histórica:

- **Design 001**: [docs/egm/001_sro_ranking_scraper/](../egm/001_sro_ranking_scraper/) (`design_draft.md`, `critic_report.md`, `comprehension_report.md`, `zoom_out_report.md`, `goldfish_review_report.md`, `2026-11-22_DRIFT_REPORT.md`)
- **Design 002**: [docs/egm/002_discord_notifications/](../egm/002_discord_notifications/) (`concepts_brief.md`, `design_draft.md`, `zoom_out_report.md`, `comprehension_report.md`, `critic_report.md`, `intermediate_zoom_out_report.md`, `goldfish_review_report.md`, `implementation_readiness_report.md`, `goldfish_review_report_stage9.md`, `2026-11-22_DRIFT_REPORT.md`)
- **Design 003**: [docs/egm/003_analytical_dashboard/](../egm/003_analytical_dashboard/) (`concepts_brief.md`, `design_draft.md`, `zoom_out_report.md`, `comprehension_report.md`, `critic_report.md`, `goldfish_review_report.md`, `intermediate_zoom_out_report.md`, `implementation_readiness_report.md`, `goldfish_review_report_stage9.md`)
