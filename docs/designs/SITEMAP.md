# Designs Sitemap

> **Memória do Elefante (EGM):** Este arquivo é o índice mestre de arquitetura do repositório. Nenhum código deve ser escrito sem consultar este mapa e o design doc correspondente em `docs/designs/`.

## Design Documents

| Document | Date | Author | Components | Description | Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| [001_sro_ranking_scraper_pipeline.md](001_sro_ranking_scraper_pipeline.md) | 2026-11-18 | richardman13 | Scrapers, Prefect 3, PostgreSQL 16, Pydantic v2, Alembic | Pipeline containerizado append-only de extração de rankings de Silkroad Online com views analíticas de séries temporais | Approved |

## Status Legend

- **Draft**: Proposta inicial em elaboração.
- **In Review**: Sob revisão crítica (ciclo Goldfish / EGM).
- **Approved**: Aprovado para implementação; fonte de verdade arquitetural.
- **Implemented**: Totalmente implementado e verificado em código.
- **Deprecated**: Substituído por um design mais recente.

## EGM Review History

Os relatórios de revisão crítica gerados durante o refinamento de cada design são preservados em `docs/egm/` para rastreabilidade histórica:

- **Design 001**: [docs/egm/001_sro_ranking_scraper/](../egm/001_sro_ranking_scraper/) (`design_draft.md`, `critic_report.md`, `comprehension_report.md`, `zoom_out_report.md`, `goldfish_review_report.md`)
