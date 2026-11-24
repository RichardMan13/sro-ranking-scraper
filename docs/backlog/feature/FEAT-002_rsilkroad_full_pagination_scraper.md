# FEAT-002: Adicionar extração completa (todas as páginas) de `rsilkroad.com` (`Player` e `Guild`)

- **Status:** Done
- **Prioridade:** Alta
- **Componente:** scraper
- **Design Relacionado:** [001_sro_ranking_scraper_pipeline.md](../../designs/001_sro_ranking_scraper_pipeline.md)
- **Criado em:** 2026-11-23

## Objetivo
Extrair **todas as páginas** dos rankings de `https://www.rsilkroad.com/Rank/Player` e `https://www.rsilkroad.com/Rank/Guild`.

> **Decisão de Escopo (`BACKLOG.md`):** Mantido como item incremental (`Done`) por reaproveitar integralmente o contrato `BaseRankingScraper` sem alterações de arquitetura ou novas tabelas, com a inclusão do alvo refletida em [001_sro_ranking_scraper_pipeline.md](../../designs/001_sro_ranking_scraper_pipeline.md).

## Achados da Investigação
- **Status HTTP & Proteção:** Sondas pontuais retornaram HTTP `200` limpo em `/Rank/Player` e `/Rank/Guild` (ASP.NET MVC + PagedList atrás de Cloudflare, sem challenge JS). Em execução real com paginação em rajada (`2026-11-24 12:04:53 UTC`), o Cloudflare aplicou rate limiting com `HTTP 429` (`BLOCKED_CHALLENGE`, acionando quarentena, alerta no Discord e circuit breaker de 6h), motivando a inclusão de pausa com jitter entre lotes (`DEFAULT_INTER_BATCH_DELAY_RANGE = (0.35, 0.75)`).
- **Otimização de paginação (`pageSize=100`):**
  - Com `pageSize=25` (padrão), `/Rank/Player` tem **277 páginas** (~6.925 players).
  - O servidor aceita `pageSize=100` (`/Rank/Player?page={page}&pageSize=100`), reduzindo para **70 páginas** (4× menos requisições, viável dentro do budget de 180s com `Semaphore(3)`).
  - Páginas além da última (ex.: `page=278&pageSize=25`) retornam **HTTP 200 com 0 linhas** (não há clamping).
  - O paginador `<ul class="pagination">` expõe `<li class="PagedList-skipToLast"><a href="...?page=N&amp;pageSize=100">»»</a></li>` e `<li class="PagedList-skipToNext"><a rel="next">»</a></li>`.
- **Estrutura HTML das tabelas:**
  - Ambas as páginas têm **2 tabelas**: a principal `<table class="table">` (25 ou 100 linhas) e uma tabela lateral sem `class` (Top 5) que deve ser ignorada.
  - **Player (`/Rank/Player?page={p}&pageSize=100`):** colunas `['#', 'İsim', 'Guild', 'Level', 'Item Puanı', 'Bilgi']`. Ícone de raça em `td[1]//img/@src` (`european.png` / `chinese.png`).
  - **Guild (`/Rank/Guild?page={p}&pageSize=100`):** colunas `['#', 'Guild', 'Level', 'Üye Sayısı', 'Item Puanı', 'Bilgi']` (`guild_level` em `extra_metrics`, `members_count` na col 3, `total_item_points` na col 4; `master_name=None`, `fortress_held=None`).
  - **Job:** não há rota de Job ranking em `/Rank/*` (`fetch_job_pages` retorna `CategoryFetchResult()` vazio).

## Escopo Implementado
- Criado `RSilkroadScraper` ([src/scrapers/rsilkroad.py](../../../src/scrapers/rsilkroad.py), `server_id = "rsilkroad"`) e registrado em [src/scrapers/__init__.py](../../../src/scrapers/__init__.py).
- Adicionado `"rsilkroad"` a `VALID_MANUAL_TARGETS` no CLI do orquestrador ([src/flows/orchestrator.py](../../../src/flows/orchestrator.py)) com cobertura em [tests/test_flows.py](../../../tests/test_flows.py).
- Implementada paginação completa em `/Rank/Player?page={p}&pageSize=100` e `/Rank/Guild?page={p}&pageSize=100`:
  - **Detecção de páginas (`_extract_pagedlist_info`):** coleta o query param `page` de todos os `<a href>` em `<ul class="pagination">` cujos `<li>` não possuam classe `disabled`, avançando em lotes concorrentes (até `DEFAULT_MAX_CONCURRENT_REQUESTS = 3`, sob `asyncio.Semaphore(3)`) enquanto `max(page_numbers) > current_page`.
  - **Guardas, pacing e parâmetros:** interrompe em tabela vazia ou `HTTP 404` para `page > 1`; inclui fallback sequencial quando `<ul class="pagination">` estiver ausente; aplica pausa com jitter entre lotes (`DEFAULT_INTER_BATCH_DELAY_RANGE = (0.35, 0.75)`, configurável via `inter_batch_delay_range`) para mitigar rajadas `HTTP 429`; lê `MAX_PAGES_PER_CATEGORY` de [src/core/config.py](../../../src/core/config.py) (`Settings.max_pages_per_category`, padrão `400`, sobrescrevível via `max_pages_per_category`) e `page_size = 100`; utiliza helpers HTML compartilhados de [src/scrapers/_html_utils.py](../../../src/scrapers/_html_utils.py).
- Adicionada migração Alembic [alembic/versions/004_seed_rsilkroad.py](../../../alembic/versions/004_seed_rsilkroad.py) inserindo `rsilkroad` (`https://www.rsilkroad.com`, `current_season='v1'`, `fw_day_of_week=0`, `fw_end_time_utc='20:00:00'`, `min_records_expected=100`) na tabela `servers`.
- Adicionadas fixtures estáticas e testes unitários completos em [tests/test_parsers.py](../../../tests/test_parsers.py):
  - [tests/fixtures/rsilkroad_player_page1.html](../../../tests/fixtures/rsilkroad_player_page1.html)
  - [tests/fixtures/rsilkroad_player_page2.html](../../../tests/fixtures/rsilkroad_player_page2.html)
  - [tests/fixtures/rsilkroad_guild_page1.html](../../../tests/fixtures/rsilkroad_guild_page1.html)

## Critérios de Aceite
- `RSilkroadScraper` extrai todas as páginas de `Player` e `Guild` usando `pageSize=100`, ignorando a tabela lateral Top 5 e extraindo `race` de `european.png`/`chinese.png`.
- Migração Alembic aplicada com sucesso (`alembic upgrade head`).
- Gates do `AGENTS.md` passando (`ruff`, `mypy`, `pytest`).
