# Goldfish Annotated Review Report (Stage 9)

**Status:** resolved
**Target:** `docs/designs/003_analytical_dashboard.md` (`DetailedRange`, lines 257–418, + ajustes pontuais em L111, L123, L185)

## Prioritized Findings & AI Suggestions

### [Issue 1] [`egm-codebase-details`] - Severity: Blocker
*   **Affected Scope:** [Lines 111, 185, 382](../../designs/003_analytical_dashboard.md#L382) (`Technical Plan §2`, `Alternatives G`, `Detailed Implementation Plan #15`)
*   **Discrepancy Description:** O plano afirma que o Alembic usa `psycopg`, mas `alembic/env.py` executa via `async_engine_from_config` (`postgresql+asyncpg://`).
*   **Rationale:** Embora `psycopg[binary]>=3.1.0` já esteja em `pyproject.toml`, apenas `src/dashboard/` utilizará `psycopg` síncrono.
*   **AI Suggestion:** PROPOSE FIX
*   **Justification:** Verificado diretamente em `alembic/env.py` e `alembic.ini`.
*   **Proposed Edit:**
    *   Location: Linhas 111, 185 e 382
    *   Change: Corrigir para indicar que `psycopg[binary]` já está em `pyproject.toml` e que o driver síncrono `psycopg` (`postgresql+psycopg://`) é permitido especificamente para `src/dashboard/`, mantendo `asyncpg` no `scraper-worker` e em `alembic/env.py`.

### [Issue 2] [`egm-implementation-readiness`, `egm-codebase-details`, `egm-style`] - Severity: Blocker
*   **Affected Scope:** [Lines 300–307](../../designs/003_analytical_dashboard.md#L300-L307) (`Detailed Implementation Plan #6`)
*   **Discrepancy Description:** `scripts/provision_readonly_role.sh` omite `-U`/`-d` no `psql`, não especifica defaults (`sro_user`/`sro_rankings`) nem leitura não-destrutiva de `.env`, não especifica mecanismo válido para `psql -v` (`\gexec` ou `set_config` + `DO $$`), e agrupa 4 parâmetros em uma única cláusula `ALTER ROLE ... SET`.
*   **Rationale:** Sem `-U`/`-d` o `psql` falha em `postgres:16-alpine` (`role "root" does not exist`), `psql -v` não interpola dentro de `DO $$`, e `ALTER ROLE ... SET` aceita apenas 1 parâmetro por instrução.
*   **AI Suggestion:** PROPOSE FIX
*   **Justification:** Todos os 4 pontos causariam falha imediata em runtime na VPS.
*   **Proposed Edit:**
    *   Location: Item `scripts/provision_readonly_role.sh`
    *   Change: Especificar leitura não-destrutiva de `.env` (linha a linha / `set -a` apenas para variáveis não definidas no ambiente), defaults `POSTGRES_USER="${POSTGRES_USER:-sro_user}"` e `POSTGRES_DB="${POSTGRES_DB:-sro_rankings}"`, comando `docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -v readonly_pass="$DASHBOARD_DB_PASSWORD" -v db_name="$POSTGRES_DB" -v owner_role="$POSTGRES_USER"`, execução via `SELECT format(...) \gexec` (evitando `psql -v` em `DO $$`) e 4 instruções `ALTER ROLE sro_readonly SET <param> = <valor>` independentes.

### [Issue 3] [`egm-implementation-readiness`, `egm-style`] - Severity: Blocker
*   **Affected Scope:** [Lines 343–355](../../designs/003_analytical_dashboard.md#L343-L355) (`Detailed Implementation Plan #11`)
*   **Discrepancy Description:** Faltam o fluxo SQL completo de `get_top_characters_drilldown`, clarificação de que `get_race_balance` não herda `rank_position <= 100`, detalhes de truncagem UTC (`DATE_TRUNC(..., 'UTC')`), `PERCENTILE_CONT(0.5 / 0.9)`, definição de `rank1_item_points`, regra `window_weeks - 1` na Visão 4, `ORDER BY` em todas as queries e `ValueError` para validação de parâmetros.
*   **Rationale:** Elimina decisões abertas para o implementador (*Zero-Decision Rule*).
*   **AI Suggestion:** PROPOSE FIX
*   **Justification:** Especificar exatamente cada função em `src/dashboard/queries.py` remove toda ambiguidade de implementação e teste.
*   **Proposed Edit:**
    *   Location: Item `src/dashboard/queries.py`
    *   Change: Documentar explicitamente: (a) `get_top_characters_drilldown` em 2 passos (passo 1 obtém `(run_id, created_at)` via `idx_scrape_runs_server_latest`, retornando `[]` se ausente; passo 2 seleciona `character_id` com `rank_position <= :top_n` na janela `±1h` do run e busca a série diária deduplicada em UTC com `created_at >= :window_start`, ordenada por `snapshot_day ASC, rank_position ASC`); (b) `get_race_balance` usando apenas `:run_id`, `:win_start`, `:win_end` sem filtro de Top 100, ordenado por `total_ranked_characters DESC`; (c) `get_top_n_item_points_evolution` usando `DATE_TRUNC('day', created_at, 'UTC')::date`, `PERCENTILE_CONT(0.5)` / `PERCENTILE_CONT(0.9)`, `MAX(item_points) FILTER (WHERE rank_position = 1)` (fallback `MAX(item_points)`), ordenado por `snapshot_day ASC, tier ASC`; (d) `get_weekly_retention` com `window_start = current_week_start - timedelta(weeks=window_weeks - 1)` (incluindo a semana corrente), `DATE_TRUNC('week', created_at, 'UTC')::date` e `ORDER BY week_start ASC`; (e) `ValueError` para parâmetros fora de faixa; (f) comentário espelhando `alembic/versions/002_analytical_views.py` nos limiares de classificação.

### [Issue 4] [`egm-implementation-readiness`, `egm-codebase-details`] - Severity: Major
*   **Affected Scope:** [Lines 257–415](../../designs/003_analytical_dashboard.md#L257-L415) (`Detailed Implementation Plan`)
*   **Discrepancy Description:** Lista plana de arquivos sem fases incrementais nem checkpoints de verificação intermediários.
*   **Rationale:** Exigido pelo `implementation-readiness-standard.md` (*Phasing & Checkpoints*).
*   **AI Suggestion:** PROPOSE FIX
*   **Justification:** Estruturar em 4 fases incrementais torna a execução e a validação progressiva determinísticas.
*   **Proposed Edit:**
    *   Location: `Detailed Implementation Plan`
    *   Change: Organizar os itens em **Fase 1** (Banco, ORM `src/models/db_models.py`, Migração `003`, `scripts/provision_readonly_role.sh`, `.env.example` + checkpoint `alembic`/`ruff`/`mypy`), **Fase 2** (`pyproject.toml`, `src/dashboard/{__init__,config,db,schemas,queries}.py`, `tests/test_dashboard_{config,queries}.py` + checkpoint `ruff`/`mypy`/`pytest`), **Fase 3** (`src/dashboard/app.py`, `Dockerfile`, `docker-compose.yml` + checkpoint build/isolamento/healthcheck) e **Fase 4** (Documentação + verificação de integração PG16/VPS).

### [Issue 5] [`egm-codebase-details`, `egm-implementation-readiness`, `egm-style`] - Severity: Major
*   **Affected Scope:** [Lines 265, 357–364](../../designs/003_analytical_dashboard.md#L357-L364) (`pyproject.toml` e `src/dashboard/app.py`)
*   **Discrepancy Description:** Decoradores `@st.cache_resource`/`@st.cache_data` falham em `mypy --strict` (`disallow_untyped_decorators`) sem override em `src.dashboard.app`; falta definir política para `pandas` (evitar `pandas` usando `alt.Data(values=[...])` / listas de dicts), carregamento *lazy* de settings em `main()` (evitando side-effect no `import`), tratamento de `st.segmented_control` retornando `None`, `list_active_servers() == []`, gráficos/eixos/cores fixas (`CH`/`EU`/`UNK`), inversão de eixo Y em `rank_position` e estados vazios concretos.
*   **Rationale:** Previne falhas no gate `mypy --strict` e elimina decisões abertas de UI.
*   **AI Suggestion:** PROPOSE FIX
*   **Justification:** Verificado em `pyproject.toml` (`strict = true`) e na API do Streamlit 1.40+.
*   **Proposed Edit:**
    *   Location: Itens `pyproject.toml` e `src/dashboard/app.py`
    *   Change: Adicionar `[[tool.mypy.overrides]]` para `module = ["src.dashboard.app"]` com `disallow_untyped_decorators = false`; definir que `app.py` não importa `pandas` (passando `alt.Data(values=[asdict(r) for r in rows])` ao Altair e listas de dicts ao `st.dataframe`); inicializar settings e cache *lazily* dentro de `main()` (zero side-effects no `import`); tratar retorno `None` de `st.segmented_control` mantendo a visão padrão (Visão 1) e `list_active_servers() == []` exibindo `st.info`; especificar os 5 gráficos Altair (incluindo escala de cores fixa `CH`/`EU`/`UNK`, eixo Y invertido em `rank_position` no drill-down, destaque visual em `is_current_week` e `st.multiselect` por segmento na Visão 5) e os 4 estados vazios concretos (`st.caption` para frescor e `st.warning` genérico em erros SQL).

### [Issue 6] [`egm-codebase-details`] - Severity: Major
*   **Affected Scope:** [Lines 293–298](../../designs/003_analytical_dashboard.md#L293-L298) (`alembic/versions/003_dashboard_indexes.py`)
*   **Discrepancy Description:** `idx_scrape_runs_server_latest` não está espelhado em `src/models/db_models.py` (`ScrapeRun.__table_args__`), causando drift com `Base.metadata`.
*   **Rationale:** Todos os índices existentes estão declarados em `src/models/db_models.py`.
*   **AI Suggestion:** PROPOSE FIX
*   **Justification:** Verificado em `src/models/db_models.py` e `alembic/env.py` (`target_metadata = Base.metadata`).
*   **Proposed Edit:**
    *   Location: Fase 1 do `Detailed Implementation Plan`
    *   Change: Adicionar arquivo-alvo `src/models/db_models.py` (`Modify`) declarando `Index("idx_scrape_runs_server_latest", "server_id", text("created_at DESC"), postgresql_where=text("status IN ('SUCCESS', 'PARTIAL_SUCCESS')"))` em `ScrapeRun.__table_args__`.

### [Issue 7] [`egm-implementation-readiness`] - Severity: Major
*   **Affected Scope:** [Line 282](../../designs/003_analytical_dashboard.md#L282) (`docker-compose.yml`)
*   **Discrepancy Description:** Falta definir a sintaxe de interpolação das variáveis de ambiente do serviço `dashboard` no `docker-compose.yml`.
*   **Rationale:** Usar `${DASHBOARD_DATABASE_URL:?}` impediria `docker compose exec postgres` durante o rollout antes de configurar `DASHBOARD_DATABASE_URL`.
*   **AI Suggestion:** PROPOSE FIX
*   **Justification:** Correção crítica para não quebrar o fluxo de provisionamento da VPS.
*   **Proposed Edit:**
    *   Location: Item `docker-compose.yml`
    *   Change: Especificar `DASHBOARD_DATABASE_URL: ${DASHBOARD_DATABASE_URL:-}`, `DASHBOARD_CACHE_TTL_SECONDS: ${DASHBOARD_CACHE_TTL_SECONDS:-600}`, `LOG_LEVEL: ${LOG_LEVEL:-INFO}` e `MALLOC_ARENA_MAX: "2"`.

### [Issue 8] [`egm-implementation-readiness`, `egm-codebase-details`, Stage 8] - Severity: Major
*   **Affected Scope:** [Lines 372–377, 397–404](../../designs/003_analytical_dashboard.md#L372-L404) (`tests/test_dashboard_queries.py` e `Verification Plan`)
*   **Discrepancy Description:** Falta especificar variável de ambiente de conexão, auto-skip (`pytest.skip`), estratégia de seed/cleanup e critério de `EXPLAIN (FORMAT JSON)` para `@pytest.mark.postgres`, garantindo que o gate canônico `uv run --no-sync pytest --cov=src tests/` (`AGENTS.md`) passe offline.
*   **Rationale:** Mantém aderência estrita ao `AGENTS.md` sem quebrar execuções locais sem PostgreSQL.
*   **AI Suggestion:** PROPOSE FIX
*   **Justification:** Resolve simultaneamente Stage 8 Major #1 e Stage 9 M5.
*   **Proposed Edit:**
    *   Location: Item `tests/test_dashboard_queries.py` e `Verification Plan`
    *   Change: Definir fixture `@pytest.mark.postgres` lendo `TEST_POSTGRES_URL` (executando `pytest.skip("TEST_POSTGRES_URL not set")` automaticamente se ausente), populando dados multi-mês/multi-semana em transação isolada com rollback ao final, e asserindo no JSON de `EXPLAIN (FORMAT JSON)` que nenhuma partição mensal fora da janela consultada aparece nos nós do plano; manter `uv run --no-sync pytest --cov=src tests/` como comando padrão de testes unitários no `Verification Plan`.

### [Issue 9] [`egm-style`] - Severity: Major
*   **Affected Scope:** [Line 312](../../designs/003_analytical_dashboard.md#L312) (`src/dashboard/__init__.py`)
*   **Discrepancy Description:** "Exportar metadados públicos mínimos" é vago.
*   **Rationale:** Deve especificar conteúdo exato.
*   **AI Suggestion:** PROPOSE FIX
*   **Justification:** Elimina ambiguidade trivial.
*   **Proposed Edit:**
    *   Location: Item `src/dashboard/__init__.py`
    *   Change: Definir como módulo marcador contendo apenas docstring (`"""Read-only analytical dashboard package."""`) e sem imports de UI.

### [Issue 10] [`egm-style`, `egm-implementation-readiness`, `egm-codebase-details`] - Severity: Major
*   **Affected Scope:** [Lines 401–414](../../designs/003_analytical_dashboard.md#L401-L414) (`Verification Plan`)
*   **Discrepancy Description:** (1) `alembic downgrade` sem `uv run --no-sync`; (2) `assert` de isolamento checa apenas `streamlit` em vez de `('streamlit', 'pandas', 'pyarrow')`; (3) healthcheck não explicita execução dentro do container `dashboard`; (4) falta `SHOW lock_timeout;`; (5) critérios de WebSocket (`HTTP 101` sem reconexões ao alternar entre as 5 visões) e `< 5s` por visão precisam ser explícitos.
*   **Rationale:** Garante aderência ao `AGENTS.md` e verificabilidade objetiva.
*   **AI Suggestion:** PROPOSE FIX
*   **Justification:** Todos os 5 pontos são correções objetivas.
*   **Proposed Edit:**
    *   Location: `Verification Plan`
    *   Change: Atualizar os 5 itens com os comandos exatos e critérios mensuráveis descritos acima.

### [Issue 11] [`egm-codebase-details`, `egm-implementation-readiness`, `egm-style`] - Severity: Nit
*   **Affected Scope:** [Lines 123, 264, 272, 283, 303, 320, 341, 344, 376, 379–384, 401](../../designs/003_analytical_dashboard.md#L264) (`DetailedRange` + L123)
*   **Discrepancy Description:** Conjunto de nits de precisão: `dependencies` na tabela `[project]` (L264); marcar `(Modify)` vs `(Create)` por arquivo no item de docs + incluir `alembic/README.md` (`Modify`) e diretório `scripts/` na árvore do `AGENTS.md`; explicitar que `docker-compose.vps.yml` é apenas exemplo no `README.md` (não versionado); explicitar `get_dashboard_settings()` sem `@lru_cache`; tipar `race: Literal["CH", "EU", "UNK"]` em `BehaviorClassificationRow`; alinhar `rank1_item_points` na L123; e ajustes editoriais menores (`st.caption`, `código de saída diferente de zero`, sub-itens de testes no item 14, padronização de idioma na L401).
*   **Rationale:** Elimina pequenas inconsistências de nomenclatura e fronteira de arquivos.
*   **AI Suggestion:** PROPOSE FIX
*   **Justification:** Ajustes pontuais de baixo risco que completam a prontidão para implementação.
*   **Proposed Edit:**
    *   Location: Linhas correspondentes em `003_analytical_dashboard.md`
    *   Change: Aplicar todos os ajustes pontuais em bloco único.
