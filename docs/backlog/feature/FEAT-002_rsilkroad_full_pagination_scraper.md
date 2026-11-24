# FEAT-002: Adicionar extração completa (todas as páginas) de `rsilkroad.com` (`Player` e `Guild`)

- **Status:** Pending
- **Prioridade:** Alta
- **Componente:** scraper
- **Design Relacionado:** [001_sro_ranking_scraper_pipeline.md](../../designs/001_sro_ranking_scraper_pipeline.md)
- **Criado em:** 2026-11-23

## Objetivo
Extrair **todas as páginas** dos rankings de `https://www.rsilkroad.com/Rank/Player` e `https://www.rsilkroad.com/Rank/Guild`.

> **Nota de Escopo (`BACKLOG.md`):** Por envolver um novo servidor alvo (`src/scrapers/rsilkroad.py`, registro em `src/scrapers/__init__.py` e seed na tabela `servers` via Alembic), avaliar se permanece como item incremental ou se deve ser promovido para `Moved to Design` antes da implementação.

## Achados da Investigação
- **Status HTTP & Proteção:** HTTP `200` limpo em `/Rank/Player` e `/Rank/Guild` (ASP.NET MVC + PagedList atrás de Cloudflare, sem challenge anti-bot).
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

## Escopo Proposto
- Criar `RSilkroadScraper` (`src/scrapers/rsilkroad.py`, `server_id = "rsilkroad"`) e registrá-lo em `src/scrapers/__init__.py`.
- Implementar paginação completa em `/Rank/Player?page={p}&pageSize=100` e `/Rank/Guild?page={p}&pageSize=100` (parando ao encontrar tabela vazia ou fim do paginador `skipToLast`/`skipToNext`, respeitando `Semaphore(3)` e o teto de 512MB / 180s).
- Adicionar migração Alembic para inserir o servidor `rsilkroad` (`https://www.rsilkroad.com`) na tabela `servers`.
- Adicionar fixtures estáticas em `tests/fixtures/` e testes unitários completos em `tests/test_parsers.py`.

## Critérios de Aceite
- `RSilkroadScraper` extrai todas as páginas de `Player` e `Guild` usando `pageSize=100`, ignorando a tabela lateral Top 5 e extraindo `race` de `european.png`/`chinese.png`.
- Migração Alembic aplicada com sucesso (`alembic upgrade head`).
- Gates do `AGENTS.md` passando (`ruff`, `mypy`, `pytest`).
