# FIX-003: Atualizar rotas e parser HTML do `ChronicleScraper` para o layout real

- **Status:** Done
- **Prioridade:** Alta
- **Componente:** scraper
- **Design Relacionado:** [001_sro_ranking_scraper_pipeline.md](../../designs/001_sro_ranking_scraper_pipeline.md)
- **Criado em:** 2026-11-23

## Sintoma
Todas as execuções do servidor `chronicle` (`https://play-chronicle.com`) falham com status `HTTP_ERROR` e HTTP `404` ao acessar `https://play-chronicle.com/ranking/player?page=1`.

## Causa Raiz
A sondagem ao vivo em `https://play-chronicle.com/ranking/index.html` revelou que o site atual serve os rankings paginados via fragmentos HTML nas rotas:
- Players: `/ranking/top-player-{page}.html` (`25` linhas/página, `<table class="table table-striped table-hover">` sem `<tbody>`, colunas: `#`, `Character`, `Guild`, `Level`, `ItemPoints`)
- Guilds: `/ranking/top-guild-{page}.html` (`<table class="table table-striped table-hover">` sem `<tbody>`, colunas: `#`, `Guild`, `Level`, `Members`, `ItemPoints`)
- Jobs: `/ranking/top-job-{page}.html` (`<table class="table table-striped table-hover">` sem `<tbody>`, colunas: `#`, `Character`, `Guild`, `Job K/D Ratio`, `Job Points` — atualmente vazio)

`ChronicleScraper` estava consultando `/ranking/player?page={page}`, `/ranking/job?page={page}` e `/ranking/guild?page={page}` e buscando `<table class="ranking-table">` com um mapeamento de colunas diferente.

## Plano de Correção
- [x] Atualizar as rotas paginadas de `ChronicleScraper` (`src/scrapers/chronicle.py`) para `/ranking/top-player-{page}.html`, `/ranking/top-job-{page}.html` e `/ranking/top-guild-{page}.html`, encerrando paginação em HTTP `404` para `page > 1` e mantendo falha em `page == 1`.
- [x] Atualizar a extração da tabela (`table table-striped table-hover`) e o mapeamento de 5 colunas para `player`, `guild` (`extra_metrics["guild_level"]`) e `job` (`job_points` + `extra_metrics["job_kd_ratio"]`), tornando `job_type` e `job_level` opcionais em `RawJobRankingItem` com proteção contra clobbering em `_reconcile_jobs_into_players`.
- [x] Atualizar fixtures e testes unitários de regressão em `tests/` (incluindo `tests/fixtures/chronicle_real_player_page1.html` e `tests/fixtures/chronicle_real_guild_page1.html`).

## Critério de Aceite
- `ChronicleScraper` extrai e valida corretamente rankings de `player`, `guild` e `job` no formato real de `play-chronicle.com`.
- Gates do `AGENTS.md` passando (`ruff`, `mypy`, `pytest`).
