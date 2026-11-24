# FEAT-001: Extrair todas as páginas e filtros de job (`trader`, `hunter`, `thieves`) no `ChronicleScraper`

- **Status:** Done
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

## Escopo Implementado
- Atualizado `ChronicleScraper.fetch_job_pages` ([src/scrapers/chronicle.py](../../../src/scrapers/chronicle.py)) para consultar em paralelo (via `asyncio.TaskGroup`) os 3 filtros reais (`/ranking/job/top-trader-{page}.html`, `/ranking/job/top-hunter-{page}.html`, `/ranking/job/top-thieves-{page}.html`), atribuindo `job_type` (`TRADER` / `HUNTER` / `THIEF`), `job_level` (col 3) e `job_exp` (col 4).
- Removidos os limites rígidos (`_PLAYER_MAX_PAGES=10`, `_JOB_MAX_PAGES=5`, `_GUILD_MAX_PAGES=2`) e implementada paginação completa em `_fetch_paginated_html()`:
  - **Orientada pelo paginador (`_extract_paginator_info`):** extrai todos os sufixos `-{N}.html` de chamadas `paginatorAjax(...)` no HTML e avança em lotes concorrentes (até `DEFAULT_MAX_CONCURRENT_REQUESTS = 3`, sob `asyncio.Semaphore(3)`) enquanto `max(page_numbers) > current_page`.
  - **Guardas anti-loop e fallback:** interrompe se uma página repetir o fingerprint da página anterior (clamping), retornar tabela vazia ou `HTTP 404` em `page > 1`; quando o HTML não contém chamadas `paginatorAjax`, executa fallback sequencial com as mesmas guardas.
  - **Teto de segurança:** configurável via `MAX_PAGES_PER_CATEGORY` em [src/core/config.py](../../../src/core/config.py) (`Settings.max_pages_per_category`, padrão `400`) e sobrescrevível via `ChronicleScraper.__init__(max_pages_per_category=...)`.
  - **Helpers HTML compartilhados:** extraídos para [src/scrapers/_html_utils.py](../../../src/scrapers/_html_utils.py).
- Adicionadas fixtures estáticas e testes unitários de regressão em [tests/test_parsers.py](../../../tests/test_parsers.py) (incluindo janela deslizante, clamping na última página e reconciliação dos 3 filtros de job):
  - [tests/fixtures/chronicle_trader_page1.html](../../../tests/fixtures/chronicle_trader_page1.html)
  - [tests/fixtures/chronicle_trader_page2.html](../../../tests/fixtures/chronicle_trader_page2.html)
  - [tests/fixtures/chronicle_hunter_page1.html](../../../tests/fixtures/chronicle_hunter_page1.html)
  - [tests/fixtures/chronicle_thieves_page1.html](../../../tests/fixtures/chronicle_thieves_page1.html)

## Critérios de Aceite
- `ChronicleScraper` coleta todas as páginas de `top-player`, `top-guild`, `top-trader`, `top-hunter` e `top-thieves` sem entrar em loop infinito quando o site clampa `page > last_page`.
- Reconciliação de `job_type`, `job_level` e `job_exp` nos snapshots de personagens validada por testes unitários.
- Gates do `AGENTS.md` passando (`ruff`, `mypy`, `pytest`).
