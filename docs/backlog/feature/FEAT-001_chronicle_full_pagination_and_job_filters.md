# FEAT-001: Extrair todas as páginas e filtros de job (`trader`, `hunter`, `thieves`) no `ChronicleScraper`

- **Status:** Pending
- **Prioridade:** Alta
- **Componente:** scraper
- **Design Relacionado:** [001_sro_ranking_scraper_pipeline.md](../../designs/001_sro_ranking_scraper_pipeline.md)
- **Criado em:** 2026-11-23

## Objetivo
Extrair **todas as páginas** disponíveis de `https://play-chronicle.com/ranking/index.html` aplicando os filtros reais do menu `#menuRanking`: **Top Player**, **Top Guild**, **Top Traders**, **Top Hunters** e **Top Thieves**.

## Achados da Investigação
- **Rotas reais dos filtros (`#menuRanking`):**
  - Top Player: `/ranking/top-player-{page}.html` — colunas `['#', 'Character', 'Guild', 'Level', 'ItemPoints']` (**86 páginas** atualmente, 25 linhas/página, ~2.140 players).
  - Top Guild: `/ranking/top-guild-{page}.html` — colunas `['#', 'Guild', 'Level', 'Members', 'ItemPoints']` (**3 páginas** atualmente, 25 + 25 + 14 = 64 guilds).
  - Top Traders: `/ranking/job/top-trader-{page}.html` — colunas `['#', 'Character', 'Guild', 'Level', 'Exp']` (**9 páginas** atualmente, `job_type="TRADER"`).
  - Top Hunters: `/ranking/job/top-hunter-{page}.html` — colunas `['#', 'Character', 'Guild', 'Level', 'Exp']` (1 página, 8 registros, `job_type="HUNTER"`).
  - Top Thieves: `/ranking/job/top-thieves-{page}.html` — colunas `['#', 'Character', 'Guild', 'Level', 'Exp']` (1 página, 18 registros, `job_type="THIEF"`).
- **Armadilha de paginação identificada (clamping):**
  - Solicitar `page > last_page` **não** retorna tabela vazia nem HTTP `404` — o servidor repete a última página (ex.: `top-guild-5.html` devolve a página 3 com 14 linhas; `top-player-100.html` devolve a página 86 com `rank=2126`).
  - O HTML de cada página inclui chamadas `paginatorAjax('#result','/ranking/...-{next}.html')` com o botão `>>` apontando para a próxima página (ou ausente na última página).

## Escopo Proposto
- Atualizar `ChronicleScraper.fetch_job_pages` (`src/scrapers/chronicle.py`) para consultar os 3 filtros reais (`/ranking/job/top-trader-{page}.html`, `/ranking/job/top-hunter-{page}.html`, `/ranking/job/top-thieves-{page}.html`), atribuindo `job_type` (`TRADER` / `HUNTER` / `THIEF`), `job_level` (col 3) e `job_exp` (col 4).
- Remover os limites rígidos (`_PLAYER_MAX_PAGES=10`, `_JOB_MAX_PAGES=5`, `_GUILD_MAX_PAGES=2`) e implementar paginação completa orientada pelo paginador (`paginatorAjax` / link `>>`) + guarda anti-loop contra clamping (parar se não houver próxima página no paginador ou se os registros da página repetirem a página anterior, com teto de segurança configurável).
- Avaliar paralelização das páginas respeitando `asyncio.Semaphore(3)` para manter a coleta completa (~100 páginas) dentro do budget de 180s.
- Adicionar fixtures estáticas e testes unitários de regressão em `tests/` (incluindo cenário de clamping na última página e os 3 filtros de job).

## Critérios de Aceite
- `ChronicleScraper` coleta todas as páginas de `top-player`, `top-guild`, `top-trader`, `top-hunter` e `top-thieves` sem entrar em loop infinito quando o site clampa `page > last_page`.
- Reconciliação de `job_type`, `job_level` e `job_exp` nos snapshots de personagens validada por testes unitários.
- Gates do `AGENTS.md` passando (`ruff`, `mypy`, `pytest`).
