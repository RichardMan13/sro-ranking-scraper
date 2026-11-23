# Stage 9 — Implementation Readiness Report (`DetailedRange`, linhas 257–418)

*   **Target Document:** [003_analytical_dashboard.md](../../designs/003_analytical_dashboard.md) (`DetailedRange`, linhas 257–418)
*   **Date:** 2026-11-23
*   **Reviewers:** `egm-implementation-readiness`, `egm-codebase-details`, `egm-style`
*   **Standard Applied:** `implementation-readiness-standard.md`
*   **Summary:** `3` 🔴 Blocker | `7` 🟡 Major | `8` 🟢 Nit (consolidados e deduplicados)

---

## 🔴 Blockers

### B1. Contradição com o Codebase: Alembic Usa `asyncpg`, Não `psycopg` (`egm-codebase-details`)
* **Escopo afetado:** Linha 382 (e menções nas linhas 111 e 185) vs. `alembic/env.py`, `alembic.ini` e `alembic/README.md`.
* **Achado:** O plano instrui registrar em `AGENTS.md` que `psycopg` é permitido "para Alembic e `src/dashboard/`". No repositório atual, `alembic/env.py` executa via `async_engine_from_config` (`postgresql+asyncpg://`); `psycopg[binary]` consta em `pyproject.toml`, mas não é usado pelo Alembic.
* **Correção requerida:** Corrigir as referências (L111, L185, L382) para registrar que `psycopg[binary]` já está em `pyproject.toml` e é permitido especificamente para `src/dashboard/`, mantendo `asyncpg` no `scraper-worker` e em `alembic/env.py`.

### B2. `scripts/provision_readonly_role.sh`: Flags `-U`/`-d` Ausentes, Armadilha de `psql -v` em `DO $$`, Defaults e Sintaxe `ALTER ROLE SET` (`egm-implementation-readiness`, `egm-codebase-details`, `egm-style`)
* **Escopo afetado:** Item 6, linhas 300–307.
* **Achado:** (1) `psql` sem `-U "${POSTGRES_USER:-sro_user}" -d "${POSTGRES_DB:-sro_rankings}"` falha em `postgres:16-alpine` (`FATAL: role "root" does not exist`); (2) variáveis `-v` do `psql` não são interpoladas dentro de blocos `DO $$ ... $$` (exigindo `\gexec` ou `set_config` + `current_setting`); (3) falta especificar defaults e leitura não-destrutiva de `.env`; (4) linha 307 sugere um único `ALTER ROLE ... SET` separado por vírgula (sintaxe inválida no PostgreSQL — são 4 instruções independentes).
* **Correção requerida:** Especificar invocação exata de `psql -U -d`, defaults (`sro_user`/`sro_rankings`), carga não-destrutiva de `.env`, execução via `SELECT format(...) \gexec` (ou `set_config` + `DO $$`) e 4 comandos `ALTER ROLE sro_readonly SET ...` independentes.

### B3. Lacunas de Contrato SQL em `src/dashboard/queries.py` (`egm-implementation-readiness`, `egm-style`)
* **Escopo afetado:** Item 11, linhas 343–355.
* **Achado:** (1) `get_top_characters_drilldown` não possui especificação SQL (passos, *bind parameters*, deduplicação diária, retorno `[]` sem run válido e `ORDER BY`); (2) `get_race_balance` usa abreviação ambígua `(mesma estratégia em 2 passos)` que pode sugerir herdar `rank_position <= 100` (incorreto para `total_ranked_characters`); (3) `get_top_n_item_points_evolution` omite truncagem UTC (`DATE_TRUNC('day', created_at, 'UTC')::date`), `PERCENTILE_CONT`, definição de `rank1_item_points` e `ORDER BY`; (4) `get_weekly_retention` omite se `window_weeks = 12` inclui a semana corrente (`window_weeks - 1`), `DATE_TRUNC('week', created_at, 'UTC')` e `ORDER BY`; (5) falta declarar `ValueError` para limites inválidos.
* **Correção requerida:** Documentar fluxo SQL, *bind parameters*, truncagem UTC, percentis, borda `window_weeks - 1`, `ORDER BY` e `ValueError` em `src/dashboard/queries.py`.

---

## 🟡 Majors

### M1. Ausência de Fases Incrementais e Checkpoints (`egm-implementation-readiness`, `egm-codebase-details`)
* **Escopo afetado:** Linhas 257–415.
* **Achado:** Lista plana de 15 arquivos sem divisão em fases incrementais com checkpoints de validação intermediários, exigida pelo `implementation-readiness-standard.md`.
* **Correção requerida:** Agrupar as entregas em 4 fases incrementais (Fase 1 Banco/Migração/Script, Fase 2 Core de Dados + Testes Unitários, Fase 3 App Streamlit + Docker/Compose, Fase 4 Documentação + Integração PG16/VPS) com comando de checkpoint ao fim de cada fase.

### M2. Armadilhas de `mypy --strict` e Especificação de UI em `src/dashboard/app.py` (`egm-codebase-details`, `egm-implementation-readiness`, `egm-style`)
* **Escopo afetado:** Item 1 (L265) e Item 12 (L357–364).
* **Achado:** (1) `@st.cache_resource`/`@st.cache_data` falham em `mypy --strict` (`disallow_untyped_decorators`) quando `[dashboard]` não está instalado — exige `[[tool.mypy.overrides]]` com `disallow_untyped_decorators = false` para `src.dashboard.app`; (2) `app.py` deve evitar `pandas` (usando `alt.Data(values=[...])` / listas de dicts) ou incluí-lo no override do `mypy`; (3) `@st.cache_data(ttl=settings...)` no nível de módulo causa side-effect no `import` — carregar settings de forma *lazy* em `main()`; (4) especificar tratamento de `st.segmented_control` retornando `None`, `list_active_servers() == []`, gráficos/eixos/cores fixas (`CH`/`EU`/`UNK`), inversão do eixo Y em `rank_position` no drill-down e estados vazios concretos.
* **Correção requerida:** Atualizar itens 1 e 12 com essas especificações determinísticas.

### M3. Drift de Metadados ORM: `idx_scrape_runs_server_latest` Ausente de `src/models/db_models.py` (`egm-codebase-details`)
* **Escopo afetado:** Item 5 (L293–298) vs. `src/models/db_models.py` (L69–83).
* **Achado:** Todos os índices de `001_initial_schema.py` estão espelhados em `src/models/db_models.py`; omitir `idx_scrape_runs_server_latest` de `ScrapeRun.__table_args__` causa drift com `Base.metadata`.
* **Correção requerida:** Adicionar `src/models/db_models.py` (`Modify`) declarando `Index("idx_scrape_runs_server_latest", "server_id", text("created_at DESC"), postgresql_where=text("status IN ('SUCCESS', 'PARTIAL_SUCCESS')"))` em `ScrapeRun.__table_args__`.

### M4. Interpolação de Variáveis de Ambiente em `docker-compose.yml` (`egm-implementation-readiness`)
* **Escopo afetado:** Item 3, linha 282.
* **Achado:** Não especifica `${VAR:-default}` vs `${VAR:?required}`; usar `${DASHBOARD_DATABASE_URL:?}` quebraria `docker compose exec postgres` durante o rollout antes da role ser provisionada.
* **Correção requerida:** Especificar `DASHBOARD_DATABASE_URL: ${DASHBOARD_DATABASE_URL:-}`, `DASHBOARD_CACHE_TTL_SECONDS: ${DASHBOARD_CACHE_TTL_SECONDS:-600}`, `LOG_LEVEL: ${LOG_LEVEL:-INFO}`, `MALLOC_ARENA_MAX: "2"`.

### M5. Contrato de Conexão, Seed, `EXPLAIN (FORMAT JSON)` e Auto-Skip em `@pytest.mark.postgres` (`egm-implementation-readiness`, `egm-codebase-details`, Stage 8 Major #1)
* **Escopo afetado:** Item 14 (L372–377) e `Verification Plan` (L397–404).
* **Achado:** Sem `pytest.skip(...)` automático quando a DSN de teste não estiver configurada, o gate canônico `uv run --no-sync pytest --cov=src tests/` (`AGENTS.md`) quebra offline. Também faltam a variável de ambiente de teste (`TEST_POSTGRES_URL` ou `DATABASE_URL` read-write em transação com rollback / cleanup isolado) e a asserção exata no JSON de `EXPLAIN (FORMAT JSON)`.
* **Correção requerida:** Especificar `TEST_POSTGRES_URL` (auto-skip se ausente), seeding em transação com rollback (ou `server_id` efêmero com cleanup), asserção de ausência de partições fora da janela no plano JSON, e manter `uv run --no-sync pytest --cov=src tests/` no `Verification Plan`.

### M6. Especificação Vaga de `src/dashboard/__init__.py` (`egm-style`)
* **Escopo afetado:** Item 7, linha 312.
* **Achado:** "Exportar metadados públicos mínimos" não define símbolos.
* **Correção requerida:** Definir explicitamente como módulo marcador com docstring (`"""Read-only analytical dashboard package."""`) e sem imports de UI.

### M7. Precisão dos Comandos e Critérios no `Verification Plan` (`egm-style`, `egm-implementation-readiness`, `egm-codebase-details`)
* **Escopo afetado:** Linhas 401–414.
* **Achado:** (1) L409 omite `uv run --no-sync` no `alembic downgrade`; (2) L411 afirma checar `streamlit`, `pandas` e `pyarrow`, mas o comando só testa `streamlit`; (3) L412 não explicita que o `curl` do healthcheck roda dentro do container `dashboard`; (4) L413 omite `SHOW lock_timeout;`; (5) L414 precisa definir critério observável de WebSocket (`HTTP 101` sem reconexões ao alternar as 5 visões) e `< 5s` **por visão**.
* **Correção requerida:** Alinhar comandos e critérios objetivos no `Verification Plan`.

---

## 🟢 Nits

1. **`pyproject.toml` chave `dependencies` (L264):** Corrigir `[project.dependencies]` para `dependencies` na tabela `[project]`.
2. **Ações explícitas por arquivo no Item 15 + `alembic/README.md` + `scripts/` no `AGENTS.md` (L379–384):** Marcar individualmente `(Modify)` vs `(Create)`, adicionar `alembic/README.md` (`Modify`) e incluir `scripts/` na árvore do `AGENTS.md`.
3. **`docker-compose.vps.yml` não versionado (L283, L383):** Explicitar que é apenas exemplo documentado no `README.md`.
4. **`get_dashboard_settings()` sem `@lru_cache` (L320):** Explicitar paridade com `src/core/config.py`.
5. **Tipagem consistente de `race` (L341 vs L339) e nome `rank1_item_points` (L123 vs L336):** Usar `Literal["CH", "EU", "UNK"]` em `BehaviorClassificationRow` e alinhar `rank1_item_points` na L123.
6. **Referência espelhada em `queries.py` (Stage 8 Nit #2):** Comentar referência à `002_analytical_views.py` nos limiares de classificação.
7. **Higiene editorial menor (`egm-style` N1–N3, N5–N7, N9):** Remover pleonasmos/subjetividades (`subtítulo discreto` -> `st.caption`, `amigável` -> `genérica`, `erro não-zero` -> `código de saída diferente de zero`, `otimizadas` -> `com partition pruning`), quebrar casos de borda do item 14 em sub-itens e padronizar idioma do rótulo L401.
