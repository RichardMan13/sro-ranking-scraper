# Stage 5 — Initial Zoom Out & Inventive Catalyst Review Report

*   **Target Document:** [003_analytical_dashboard.md](../../designs/003_analytical_dashboard.md) (lines 1–446)
*   **Date:** 2026-11-23
*   **Reviewers:** `egm-zoom-out`, `egm-inventive-catalyst`
*   **Verdict:** 0 🔴 Blocker | 8 🟡 Major (consolidados de 5 + 5, com 2 sobreposições convergentes) | 5 🟢 Nit

---

## Part 1: Zoom Out Review (`egm-zoom-out`)

**Veredito Geral:** **Aprovado com ressalvas (0 Blocker, 5 Majors, 4 Nits).**
O contexto, o problema raiz e os limites de escopo (Streamlit single-page, sem JVM/Metabase, sem Plotly, sem Materialized Views, sem auth customizada, 1 Dockerfile compartilhado, reuso de `psycopg`, `altair` e `structlog`) estão sólidos e bem alinhados ao `AGENTS.md`. Os pontos abaixo concentram oportunidades concretas de reduzir duplicação arquitetural, cortar YAGNI e restaurar a altitude correta na seção de arquitetura.

### 🔴 Blocker

Nenhum *Blocker* encontrado.

### 🟡 Major

#### 1. Workaround em vez de correção estrutural: duplicação das regras SQL das views existentes em Python
* **Escopo afetado:** `The Problem` (linha 21), `Technical Plan §3` (linhas 149–171) e `Detailed Implementation Plan #5 / #11` (linhas 322–327, 368–381).
* **Racional de simplicidade:** O design diagnostica corretamente que as views de `002_analytical_views.py` não fazem *partition pruning*, mas decide deixá-las intactas (lentas) e reescrever/duplicar a lógica SQL inteira dentro de `src/dashboard/queries.py`. Para 3 das 5 views — `v_guild_turnover_dominance`, `v_race_balance_ch_vs_eu` e `v_bot_vs_donor_classification` (que já é fixa em 7 dias) — corrigir o *partition pruning* diretamente via `CREATE OR REPLACE VIEW` na migração `003` **não quebra nenhum contrato de saída** e resolve o problema na raiz tanto para consultas SQL manuais quanto para o dashboard. Duplicar os limiares de classificação comportamental (`>= 0.90`, `>= 150.0`, `HAVING COUNT(*) >= 6`) em strings SQL no Python cria **duas fontes de verdade** sujeitas a drift.
* **Recomendação:** Na migração `003`, aplicar `CREATE OR REPLACE VIEW` nessas 3 views sem parâmetros (adicionando o `JOIN` com janela temporal em `v_guild_turnover_dominance`/`v_race_balance_ch_vs_eu` e a CTE de *lookback* de 9 dias em `v_bot_vs_donor_classification`) para que `queries.py` apenas consulte as views com `WHERE server_id = :server_id`. Reservar CTEs parametrizadas em `queries.py` apenas para as visões que realmente precisam de janela dinâmica (`window_days`: Visão 1 e, se parametrizada, Visão 4).

#### 2. Queda de altitude (*Weeds*) e duplicação literal entre `Technical Plan` e `Detailed Implementation Plan`
* **Escopo afetado:** `Technical Plan §2` (linhas 104–148) e `Technical Plan §5` (linhas 189–198), repetidos em `Detailed Implementation Plan` (linhas 291–390).
* **Racional de simplicidade:** A seção de arquitetura de alto nível (`FeatureRange 1..286`) desce ao nível de código: lista limites `Field(default=..., ge=..., le=...)` (linhas 113–116), f-string exata de `connect_args` (linha 123), todos os campos e `Literal[...]` de 8 DTOs (linhas 128–135), assinaturas de decoradores Streamlit e rótulos de botão (linhas 142–147), flags exatas de CLI do Streamlit, comando `curl` de healthcheck e JSON de rotação de logs (linhas 192–198). Em seguida, repete exatamente os mesmos detalhes no `Detailed Implementation Plan`. Isso viola o padrão *Anti-Weeds* e dificulta a leitura e manutenção do documento.
* **Recomendação:** Manter `Technical Plan` focado em fronteiras de módulos, fluxos de dados, contratos de segurança e invariantes arquiteturais; concentrar detalhes de implementação (campos Pydantic, flags CLI, strings `connect_args`, opções de `healthcheck`/`logging`) exclusivamente no `Detailed Implementation Plan`.

#### 3. Fronteira de rede ambígua entre `dashboard` e `Traefik/EasyPanel`
* **Escopo afetado:** Diagrama Mermaid (linha 91), `Technical Plan §5` (linhas 195, 202) e `Detailed Implementation Plan #3` (linha 312).
* **Racional de simplicidade:** O texto afirma que o proxy aponta para `dashboard:8501` na rede interna Docker **ou** `127.0.0.1:8501`, mas a especificação do `docker-compose.yml` (linha 312) coloca `dashboard` apenas na rede privada `sro_network` e **omite `ports:`**. Se o Traefik/EasyPanel na VPS não estiver anexado à rede `sro_network`, o container ficará inacessível sem uma rede externa declarada ou sem bind `127.0.0.1:8501:8501`.
* **Recomendação:** Definir explicitamente um único contrato de conectividade na topologia (ex.: publicar `127.0.0.1:8501:8501` — análogo ao hardening de `127.0.0.1:4200:4200` do `prefect-server` — **ou** anexar explicitamente a rede externa do Traefik/EasyPanel ao serviço `dashboard`), eliminando a ambiguidade "ou".

#### 4. YAGNI em variáveis de ambiente para guardrails fixos do PostgreSQL
* **Escopo afetado:** `Technical Plan §2` (linhas 113–116), `Detailed Implementation Plan #3, #4, #8` (linhas 311, 320, 344).
* **Racional de simplicidade:** Para um dashboard de **1 usuário** rodando contra um PostgreSQL fixo de `1g` na mesma VPS, expor `DASHBOARD_STATEMENT_TIMEOUT_MS`, `DASHBOARD_LOCK_TIMEOUT_MS` e `DASHBOARD_WORK_MEM_MB` como 3 variáveis de ambiente independentes (com validação `ge`/`le`, passagem no `docker-compose.yml`, documentação no `.env.example` e testes dedicados) adiciona superfície de configuração desnecessária para guardrails que não variam por ambiente.
* **Recomendação:** Manter configuráveis apenas `DASHBOARD_DATABASE_URL`, `LOG_LEVEL` e `DASHBOARD_CACHE_TTL_SECONDS` (explicitamente pedido no brief); definir `statement_timeout=15000`, `lock_timeout=3000` e `work_mem=16MB` como constantes de guardrail diretamente em `src/dashboard/db.py`.

#### 5. Camada de 8 modelos Pydantic v2 (`schemas.py`) para dados internos confiáveis (inconsistente com Design 002)
* **Escopo afetado:** `Technical Plan §2` (linhas 126–136) e `Detailed Implementation Plan #10` (linhas 355–366).
* **Racional de simplicidade:** O fluxo proposto valida centenas/milhares de linhas vindas do próprio PostgreSQL interno através de 8 classes `pydantic.BaseModel` apenas para, na etapa seguinte em `app.py`, convertê-las de volta em dicionários/DataFrames para o `altair`. No Design 002 (`docs/designs/002_discord_notifications.md`, linha 136), o padrão estabelecido para resultados de queries internas já é `@dataclass(frozen=True, slots=True)` da stdlib — mais leve em memória (relevante no teto de `512m`), sem overhead de validação por linha e 100% compatível com `mypy --strict` e `pytest`.
* **Recomendação:** Substituir `pydantic.BaseModel` por `@dataclass(frozen=True, slots=True)` em `src/dashboard/schemas.py`, mantendo consistência com `src/core/notifications.py`.

### 🟢 Nit

1. **Suporte a `sqlite://` em código de produção para queries exclusivas de PostgreSQL (linhas 112, 124, 345, 352, 402):** Como as queries das Visões 1–5 usam SQL exclusivo de PostgreSQL e serão testadas com `Session` mockada, manter ramificações de produção para `sqlite://` em `config.py` e `db.py` adiciona código morto. Uniformizar testes unitários com `Session` mockada simplifica `config.py` e `db.py`.
2. **Fallback morto para `st.radio` com `streamlit>=1.40.0` já fixado (linhas 145, 192, 294, 388):** `st.segmented_control` faz parte da API estável do Streamlit desde `1.40.0`. O fallback para `st.radio` é código morto (YAGNI).
3. **Consulta de frescor (`MAX(created_at)`) em `character_snapshots` vs. índice parcial em `scrape_runs` (linha 166 vs. 168–169):** Reutilizar `idx_scrape_runs_server_latest` (`SELECT created_at FROM scrape_runs WHERE server_id = :server_id AND status IN ('SUCCESS', 'PARTIAL_SUCCESS') ORDER BY created_at DESC LIMIT 1`) evita varrer o topo de índice de todas as partições mensais de `character_snapshots`.
4. **Duplicação de validação de URL de banco entre `src/core/config.py` e `src/dashboard/config.py` (linhas 112, 345):** Isolar `DashboardSettings` é correto, mas a lógica de parse/validação de esquema e host pode ser compartilhada via utilitário pequeno para evitar duplicação.

---

## Part 2: Inventive Catalyst (TRIZ) (`egm-inventive-catalyst`)

**Resumo Executivo:** 0 🔴 Blocker, 5 🟡 Major, 1 🟢 Nit.

### 🟡 Major

#### 1. Duplicação de Regras SQL entre Views `002` e `src/dashboard/queries.py` *(convergente com Zoom Out #1)*
* **Falso Compromisso:** Manter as views de `002` lentas (sem pruning) e duplicar todas as regras de negócio (`0.90`, `150.0`, `HAVING COUNT(*) >= 6`, `COALESCE(...)`) em strings SQL no Python cria drift permanente.
* **Resolução Proposta:** Otimizar *in-place* via `CREATE OR REPLACE VIEW` na migração `003` as views de janela fixa (`v_bot_vs_donor_classification`, `v_race_balance_ch_vs_eu`, `v_guild_turnover_dominance`) e manter CTEs parametrizadas em Python apenas para visões com `window_days` dinâmico (Visões 1 e 4).

#### 2. Cache Cego (`TTL=600s` + Reset Global) vs. Cadência Real de Snapshots (12h)
* **Escopo Afetado:** CUJ 2 (linhas 58–61) e Seção 2.5 (linhas 141–145).
* **Falso Compromisso:** `TTL=600s` reexecuta queries pesadas a cada 10 min mesmo sem novos dados (cadência de 12h), e ainda exibe dados defasados por até 10 min após um novo scrape até reset global.
* **Resolução Proposta (*Watermark-Keyed Cache*):** Consultar `last_snapshot_at` com TTL curto (`60s`) e passar `last_snapshot_at` como parâmetro de chave nas funções pesadas decoradas com `@st.cache_data(ttl=43200, max_entries=30)`. Invalidação automática e por servidor na chegada de cada snapshot.

#### 3. Acoplamento de Imagem Docker: `dashboard` Embarca Prefect 3, Scrapers e `gcc`
* **Escopo Afetado:** Seção 5 (linhas 191–194), `Dockerfile` e `pyproject.toml`.
* **Falso Compromisso:** `pip install ".${INSTALL_EXTRAS}"` isola `scraper-worker` do Streamlit, mas não isola `dashboard` do `prefect`, `httpx`, `lxml`, `beautifulsoup4`, `asyncpg`, `alembic` nem de `gcc`/`libpq-dev`.
* **Resolução Proposta:** Transformar o `Dockerfile` em multi-stage (`worker` e `dashboard`) ou segmentar extras no `pyproject.toml` (core mínimo + extras `worker` e `dashboard`), omitindo `gcc`/`libpq-dev` no target `dashboard`.

#### 4. Ambiguidade de Topologia entre Traefik/EasyPanel e `sro_network` *(convergente com Zoom Out #3)*
* **Escopo Afetado:** Seção 1 (linhas 76–95), Seção 5 (linhas 194–202) e `docker-compose.yml`.
* **Falso Compromisso:** `sro_network` é bridge local do Compose; omitir `ports:` sem anexar rede externa do Traefik/EasyPanel torna o container inacessível ao proxy.
* **Resolução Proposta:** Definir um contrato determinístico: **Dual-Network** (`dashboard` em `sro_network` + rede externa do proxy, mantendo `postgres` isolado) **ou** loopback bind explícito (`127.0.0.1:8501:8501`).

#### 5. *Partition Pruning* Não-Determinístico em CTE `JOIN` (Visões 2 e 3) e Cegueira de Testes ao Dialeto PostgreSQL
* **Escopo Afetado:** Seção 3 (linhas 168–169) e Seção 14 (linhas 398–408).
* **Falso Compromisso:** `JOIN` com CTE depende de *run-time partition pruning* (só ocorre com *Nested Loop*); e testar SQL específico de PostgreSQL apenas com `Session` mockada oculta erros de sintaxe/alias/cast até o deploy.
* **Resolução Proposta:** Buscar `(run_id, created_at)` em passo 1 (~1 ms via `idx_scrape_runs_server_latest`) e passar `:window_start`/`:window_end` como escalares bindados (garantindo *plan-time pruning*); adicionar suíte opt-in `@pytest.mark.postgres` contra o `postgres:16-alpine` do Compose no gate pré-deploy.

### 🟢 Nit

1. **`pool_timeout` padrão (30s) desalinhado do SLA de `< 5s` (linhas 121–124):** Definir `pool_timeout=5` em `create_dashboard_engine()` para falhar rápido sob saturação do pool (`pool_size=2, max_overflow=0`).
