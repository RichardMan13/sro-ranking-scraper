# Dashboard Analítico Read-Only em Streamlit (v3.0)

*   **Author:** richardman13
*   **Status:** Draft
*   **Date:** 2026-11-22
*   **Source Brief:** [concepts_brief.md](concepts_brief.md)

--------------------------------------------------------------------------------

## Overview

A versão **v3.0** adiciona uma camada de visualização analítica somente leitura (*read-only*) ao **SRO Ranking Scraper** usando **Streamlit**, voltada para análise de comunidade por um único usuário. O serviço executa como container dedicado e sempre ativo na **VPS Hostinger KVM 2** (Debian 13, 8 GB RAM, 2 vCPUs, ~100 GB de disco), atrás de domínio HTTPS gerenciado por **Traefik/EasyPanel** (terminação TLS, autenticação na borda e suporte a WebSockets, sem exposição direta da porta `8501`).

O dashboard expõe 5 visões interativas por servidor (com suporte previsto para até **10 servidores** e **6 meses** de histórico médio), isolando suas dependências Python da imagem do `scraper-worker` e conectando-se ao PostgreSQL 16 exclusivamente por meio de uma role de leitura dedicada (`sro_readonly`) com guardrails de sessão (`default_transaction_read_only=on`, `statement_timeout=15000`, `lock_timeout=3000` e `work_mem=16MB`). Telemetria operacional (`scrape_runs`, Circuit Breaker e quarentena em `./quarantine/`) permanece fora do escopo deste dashboard, sob responsabilidade da UI do Prefect e de consultas manuais via SQL/CLI.

## The Problem

A v1.0 do **SRO Ranking Scraper** ([Design 001](../../designs/001_sro_ranking_scraper_pipeline.md)) criou 5 views analíticas no PostgreSQL 16 (`v_character_activity_deltas`, `v_bot_vs_donor_classification`, `v_guild_turnover_dominance`, `v_race_balance_ch_vs_eu` e `v_server_retention_weekly`), e a v2.0 ([Design 002](../../designs/002_discord_notifications.md)) automatizou alertas de falha e resumos pós-Fortress War no Discord. No entanto, inspecionar tendências históricas da comunidade ainda apresenta três atritos práticos:

1. **Consumo manual via SQL:** Consultar evolução de `item_points`, dominância de guildas, balanço CH vs EU, retenção semanal e classificação comportamental (Bot vs Whale) exige conexão direta ao banco e queries manuais repetitivas.
2. **Risco de degradação sobre o banco em consultas analíticas:** As views atuais aplicam `LAG()` sem corte temporal prévio (`v_character_activity_deltas`) e fazem `JOIN` em `character_snapshots` sem filtrar `created_at` (`v_guild_turnover_dominance`, `v_race_balance_ch_vs_eu`), impedindo *partition pruning* e expondo o PostgreSQL de `1g` de RAM (`work_mem=64MB`) a leituras de todas as partições mensais e risco de OOM durante coletas do `scraper-worker`.
3. **Ausência de publicação segura na VPS:** Expor uma interface web diretamente por porta Docker (`8501:8501`) ou acoplar bibliotecas de UI (`streamlit`, `pandas`, `pyarrow`) ao `scraper-worker` violaria tanto o isolamento de rede da VPS quanto o teto estrito de `512MB` de RAM do worker.

A solução entrega uma interface web single-page em Streamlit em container separado, alimentada por queries read-only com *partition pruning* e cache em memória, publicada exclusivamente atrás do proxy reverso Traefik/EasyPanel.

## Goals

*   **G1 (5 Visões Analíticas da Comunidade):** Disponibilizar, por servidor selecionado e sem SQL manual, cinco visões interativas com carregamento *lazy* (apenas a visão ativa executa consulta):
    1. Evolução agregada de `item_points`/rank do Top N (curvas diárias de média, mediana e máximo para faixas como Top 10/50/100 em janela padrão de 30 dias expansível até 6 meses, com drill-down opcional para Top 5–10 personagens individuais).
    2. Dominância de guildas no Top 100 (exibindo contagem absoluta de jogadores ranqueados junto ao percentual e rotulando `[SEM GUILDA]` com clareza).
    3. Balanço de raças CH vs EU (tratando servidores 100% CH e registros `UNK` sem quebra de gráfico ou legenda).
    4. Retenção semanal (sinalizando visualmente a semana corrente incompleta e tratando servidores com menos de 2 snapshots).
    5. Radar comportamental Bot vs Whale (gráfico de dispersão `grind_persistence_ratio` × `max_single_jump_item_points` + tabela filtrável, com estado vazio informativo quando houver menos de 7 snapshots em 7 dias).
*   **G2 (Latência e Orçamento de Recursos na VPS):** Carregar qualquer uma das 5 visões para qualquer servidor (até 10 servidores e 6 meses de histórico) em **menos de 5 segundos** via domínio HTTPS, mantendo o container `dashboard` dentro de `mem_limit: 512m` e `cpus: "1.0"`, sem impactar `scraper-worker` (`512m`) nem `postgres` (`1g`).
*   **G3 (Defesa Read-Only em Duas Camadas):** Garantir que o dashboard utilize exclusivamente a role dedicada `sro_readonly` via `DASHBOARD_DATABASE_URL` (com `GRANT SELECT` nas tabelas/views) somada a guardrails de sessão (`default_transaction_read_only=on`, `statement_timeout=15000`, `lock_timeout=3000`, `work_mem=16MB`) e pool mínimo (`pool_size=2, max_overflow=0`).
*   **G4 (Publicação Segura e Hardening na VPS):** Expor o dashboard somente atrás do proxy reverso Traefik/EasyPanel com HTTPS (Let's Encrypt), autenticação na borda para 1 usuário e suporte a WebSockets (`/_stcore/stream`), sem publicar a porta `8501` em `0.0.0.0`, sem expor a porta `5432` no host e restringindo a porta `4200` do `prefect-server` (hoje aberta em `0.0.0.0`).
*   **G5 (Isolamento de Dependências e Qualidade de Código):** Manter o `scraper-worker` livre de dependências de UI (`streamlit`, `altair`, `pandas`, `pyarrow`) via `[project.optional-dependencies]`, manter `mypy --strict`, `ruff` e cobertura de testes unitários (`pytest`) para a camada pura de queries/schemas, e exibir frescor discreto (`Último snapshot: YYYY-MM-DD HH:MM UTC`) via `MAX(created_at)` em `character_snapshots`.

## Non-Goals

*   **NG1 (Painel Operacional):** Monitoramento de `scrape_runs`, estado do Circuit Breaker ou leitura/download de arquivos em `./quarantine/*.html.gz` permanece na UI do Prefect e via SQL/CLI.
*   **NG2 (Autenticação Customizada ou Multi-Usuário no App):** Não será implementada tela de login, `streamlit-authenticator`, RBAC ou acesso público anônimo no código Python; a autenticação de usuário único fica delegada ao Traefik/EasyPanel.
*   **NG3 (Operações de Escrita):** O dashboard não executa `INSERT`, `UPDATE`, `DELETE`, reset de Circuit Breaker, edição de cadastro de servidores ou anotações manuais.
*   **NG4 (Materialized Views com Agendamento no Prefect):** Pré-agregação materializada e jobs periódicos de refresh ficam fora desta versão (adiados para quando o volume ultrapassar 10 servidores ou 6 meses).
*   **NG5 (Bibliotecas Extras de Gráficos):** Não será adicionado `plotly` ou equivalentes; toda visualização utilizará `altair`, já embutido no Streamlit.

## Open Questions

Todas as questões de escopo, arquitetura e infraestrutura levantadas no brainstorm ([concepts_brief.md](concepts_brief.md)) foram resolvidas. Não há questões em aberto.

## Critical User Journeys (CUJs)

### CUJ 1: Inspeção de Tendências de Servidor via Domínio HTTPS
*   **Given:** O container `dashboard` está ativo na VPS Hostinger KVM 2 atrás do Traefik/EasyPanel (HTTPS + autenticação na borda), conectado ao PostgreSQL 16 via `DASHBOARD_DATABASE_URL` (`sro_readonly`), com dados históricos de pelo menos um servidor em `character_snapshots`.
*   **When:** O usuário acessa `https://<domínio-dashboard>`, autentica-se na borda, seleciona um servidor no seletor principal e alterna entre as 5 visões no `st.segmented_control` (ajustando a janela temporal de 30 dias até 6 meses na Visão 1 quando desejado).
*   **Then:** Apenas a consulta correspondente à visão ativa é executada (ou servida do `st.cache_data` dentro do TTL), o gráfico em `altair` e as tabelas/métricas renderizam em menos de 5 segundos com o subtítulo de frescor `Último snapshot: YYYY-MM-DD HH:MM UTC`, e nenhuma query de escrita é permitida pelo banco.

### CUJ 2: Atualização Manual Após Nova Coleta
*   **Given:** O usuário já consultou uma visão dentro da janela de TTL do cache (`600s`) e o `scraper-worker` concluiu um novo snapshot no PostgreSQL.
*   **When:** O usuário aciona o botão manual de atualização na interface.
*   **Then:** O cache de dados da aplicação é invalidado, a visão ativa consulta novamente o PostgreSQL respeitando `statement_timeout=15000` e atualiza os gráficos e o timestamp de `Último snapshot`.

### CUJ 3: Comportamento em Casos de Borda de Dados e Proteção contra Consultas Lentas
*   **Given:** Um servidor recém-adicionado (< 2 snapshots), em cooldown (< 7 snapshots nos últimos 7 dias), com menos de 100 personagens ranqueados (ex.: `play_golden`) ou 100% da raça `CH` — **ou** uma contenção momentânea no PostgreSQL que exceda `statement_timeout=15000` / `lock_timeout=3000`.
*   **When:** O usuário abre qualquer uma das 5 visões para esse servidor.
*   **Then:** Nos casos de dados escassos ou assimétricos, o dashboard exibe estados vazios informativos ou rótulos explícitos (`[SEM GUILDA]`, contagem absoluta ao lado do percentual, sinalização de semana corrente incompleta) sem exceções de runtime. Em caso de timeout ou erro de leitura no banco, a interface exibe mensagem controlada de erro temporário, registra log JSON estruturado sem vazar credenciais e preserva o pool de conexões e o processo do PostgreSQL intactos.

--------------------------------------------------------------------------------

## Technical Plan (Architecture)

### 1. Arquitetura de Alto Nível e Topologia na VPS Hostinger KVM 2

O dashboard opera como um processo Python/Streamlit independente em container próprio (`dashboard`) na rede interna `sro_network`, sem exposição de porta pública direta e sem acoplamento de código ou dependências com o `scraper-worker`.

```mermaid
flowchart LR
    USER["Navegador (1 Usuário)"]
    PROXY["Traefik / EasyPanel\n(HTTPS Let's Encrypt + Basic Auth\n+ WebSocket /_stcore/stream)"]

    subgraph VPS["Docker Compose — VPS Hostinger KVM 2 (8 GB RAM / 2 vCPUs)"]
        subgraph NET["Rede Interna: sro_network"]
            DASH["dashboard (Streamlit)\nmem_limit: 512m | cpus: 1.0\nporta interna 8501 (sem bind 0.0.0.0)"]
            WORKER["scraper-worker (Prefect 3)\nmem_limit: 512m\nsem streamlit/pandas/pyarrow"]
            PREFECT["prefect-server\n127.0.0.1:4200:4200"]
            PG["postgres (PostgreSQL 16)\nmem_limit: 1g | shared_buffers=512MB\nsem porta 5432 no host"]
        end
    end

    USER -- "HTTPS / WSS" --> PROXY
    PROXY -- "HTTP + Upgrade/Connection\n(rede interna Docker ou 127.0.0.1:8501)" --> DASH
    DASH -- "psycopg síncrono\nrole sro_readonly | pool_size=2\nread_only=on | work_mem=16MB" --> PG
    WORKER -- "asyncpg (read-write)" --> PG
    WORKER --> PREFECT
```

**Orçamento consolidado de recursos na VPS (8 GB RAM / 2 vCPUs / ~100 GB disco):**
*   `postgres`: `mem_limit: 1g`
*   `scraper-worker`: `mem_limit: 512m`
*   `prefect-server`: ~300–600 MB
*   `dashboard`: `mem_limit: 512m`, `cpus: "1.0"` (evita que reruns ou serialização de gráficos monopolizem as 2 vCPUs durante janelas de coleta)
*   **Total da stack:** ~2,5–2,8 GB de RAM, mantendo folga operacional junto ao EasyPanel/Traefik e ao sistema operacional Debian 13.

### 2. Separação de Módulos e Contrato de Tipagem (`mypy --strict`)

Para manter 100% de compatibilidade com `mypy --strict` e testes unitários em `pytest` sem carregar runtime do Streamlit nos testes de dados, o pacote `src/dashboard/` é dividido em quatro responsabilidades isoladas:

1. **`src/dashboard/config.py` (`DashboardSettings`):**
   *   Classe `BaseSettings` independente de `src/core/config.py` com `hide_input_in_errors=True`.
   *   **Motivo arquitetural:** `src/core/config.py` exige `DATABASE_URL` (credencial read-write do `scraper-worker`). O container `dashboard` **não** recebe `DATABASE_URL` em seu ambiente; recebe apenas `DASHBOARD_DATABASE_URL` (role `sro_readonly`), reforçando o isolamento de credenciais por container.
   *   Campos:
       *   `dashboard_database_url: SecretStr` (obrigatório; normaliza `postgres://` e `postgresql://` para `postgresql+psycopg://`, aceita `postgresql+psycopg://` e `sqlite://` para testes, rejeita esquemas assíncronos como `postgresql+asyncpg://`).
       *   `dashboard_cache_ttl_seconds: int = Field(default=600, ge=30, le=3600)`
       *   `dashboard_statement_timeout_ms: int = Field(default=15000, ge=1000, le=60000)`
       *   `dashboard_lock_timeout_ms: int = Field(default=3000, ge=500, le=15000)`
       *   `dashboard_work_mem_mb: int = Field(default=16, ge=4, le=32)`
       *   `log_level: str = "INFO"`

2. **`src/dashboard/db.py` (Engine Síncrona Read-Only):**
   *   Constrói `sqlalchemy.Engine` síncrona com driver `psycopg` (já presente em `pyproject.toml`), eliminando conflitos de *event loop* do `asyncpg` nos reruns síncronos do Streamlit.
   *   Parâmetros obrigatórios em PostgreSQL:
       *   `pool_size=2`, `max_overflow=0`, `pool_recycle=300`, `pool_pre_ping=True`.
       *   `connect_args={"options": f"-c default_transaction_read_only=on -c statement_timeout={timeout_ms} -c lock_timeout={lock_ms} -c work_mem={work_mem_mb}MB"}`.
   *   Em `sqlite://` (exclusivo para testes unitários), cria engine síncrona sem `connect_args` específicos de PostgreSQL.

3. **`src/dashboard/schemas.py` (DTOs Imutáveis — sem import de Streamlit/Pandas):**
   *   Modelos Pydantic v2 (`ConfigDict(frozen=True)`) representando os contratos de saída das consultas:
       *   `ServerOption` (`id`, `display_name`, `min_records_expected`)
       *   `ServerFreshness` (`server_id`, `last_snapshot_at: datetime | None`)
       *   `TopNAggregatePoint` (`snapshot_day: date`, `tier: Literal["Top 10", "Top 50", "Top 100"]`, `avg_item_points`, `median_item_points`, `max_item_points`, `sample_size`)
       *   `TopCharacterTrendPoint` (`snapshot_day: date`, `char_name`, `rank_position`, `item_points`)
       *   `GuildDominanceRow` (`guild_name`, `players_in_top_100`, `players_in_top_50`, `total_top_item_points`, `nominal_top_100_pct`, `sample_share_pct`, `sample_total_players`)
       *   `RaceBalanceRow` (`race: Literal["CH", "EU", "UNK"]`, `total_ranked_characters`, `top_100_count`, `avg_level`, `avg_item_points`, `max_item_points`)
       *   `WeeklyRetentionRow` (`week_start: date`, `total_tracked_characters`, `active_characters`, `activity_retention_rate_pct`, `is_current_week: bool`)
       *   `BehaviorClassificationRow` (`char_name`, `race`, `evaluated_intervals`, `total_item_points_gain`, `max_single_jump_item_points`, `grind_persistence_ratio`, `player_behavior_segment`)

4. **`src/dashboard/queries.py` (Camada Pura de Consultas — sem import de Streamlit/Pandas):**
   *   Recebe `Session` síncrona do SQLAlchemy e parâmetros tipados (`server_id`, `window_days`, `drilldown_top_n`), executa SQL parametrizado via `sqlalchemy.text()` e retorna listas de DTOs validados.
   *   Totalmente coberto por `mypy --strict` e `pytest`.

5. **`src/dashboard/app.py` (Camada Fina de Renderização Streamlit + Altair):**
   *   Mantém engine singleton via `@st.cache_resource` e wrappers de consulta via `@st.cache_data(ttl=settings.dashboard_cache_ttl_seconds, max_entries=30)` indexados por `(server_id, ...)`.
   *   Botão manual **"Atualizar dados"** chama `st.cache_data.clear()` e dispara `st.rerun()`.
   *   Seletor de servidor na barra lateral (ou cabeçalho) + subtítulo discreto de frescor (`Último snapshot: YYYY-MM-DD HH:MM UTC` ou `Sem snapshots registrados`).
   *   Seletor de visão via `st.segmented_control` (com fallback para `st.radio(..., horizontal=True)`), executando **exclusivamente** a query da visão selecionada (*lazy loading*).
   *   Todos os gráficos renderizados com `altair` (nativo do Streamlit).
   *   Captura `SQLAlchemyError` / `OperationalError` (incluindo `QueryCanceled` por `statement_timeout` e `LockNotAvailable` por `lock_timeout`), registra evento em `structlog` JSON (`server_id`, `view_name`, `error_type`, sem credenciais) e exibe `st.warning` controlado na UI.

### 3. Estratégia SQL, *Partition Pruning* e Migração Alembic `003`

As 5 views criadas em `alembic/versions/002_analytical_views.py` permanecem intactas para compatibilidade com consultas manuais externas. O dashboard executa queries parametrizadas otimizadas em `src/dashboard/queries.py` que restauram 100% do *partition pruning* em `character_snapshots`:

#### Migração `alembic/versions/003_dashboard_indexes.py` (`down_revision = "002_analytical_views"`)
*   Cria índice parcial em `scrape_runs` para resolver o último snapshot válido de um servidor em *Index Only Scan* / *Index Scan* sem varredura sequencial:
    ```sql
    CREATE INDEX IF NOT EXISTS idx_scrape_runs_server_latest
        ON scrape_runs (server_id, created_at DESC)
        WHERE status IN ('SUCCESS', 'PARTIAL_SUCCESS');
    ```
*   `downgrade()` executa `DROP INDEX IF EXISTS idx_scrape_runs_server_latest`.

#### Contrato de Consulta das 5 Visões e Casos de Borda

| Visão | Estratégia de *Partition Pruning* e Índice | Tratamento de Casos de Borda |
| :--- | :--- | :--- |
| **Frescor (Cabeçalho)** | `SELECT MAX(created_at) FROM character_snapshots WHERE server_id = :server_id` (usa `idx_char_snapshots_rank`). | Se `NULL`, exibe `"Sem snapshots registrados"` sem consultar `scrape_runs`. |
| **1. Evolução Agregada do Top N** | Filtra `server_id = :server_id AND created_at >= :window_start AND rank_position <= 100` (usa `idx_char_snapshots_rank` + pruning mensal). Agrega por `DATE_TRUNC('day', created_at)` para faixas Top 10, Top 50 e Top 100 (média, mediana via `PERCENTILE_CONT(0.5)` e máximo). Drill-down opcional busca histórico na mesma janela apenas para os `character_id` do Top 5–10 do último snapshot. | Janela padrão `30 dias` (opções `30d`, `90d`, `180d`). Servidores sem dados na janela exibem estado vazio informativo. |
| **2. Dominância de Guildas (Top 100)** | CTE `latest_run` consulta `scrape_runs` usando `idx_scrape_runs_server_latest` (`LIMIT 1`). O `JOIN` com `character_snapshots` inclui `cs.server_id = lr.server_id AND cs.scrape_run_id = lr.id AND cs.created_at >= lr.created_at - INTERVAL '1 hour' AND cs.created_at <= lr.created_at + INTERVAL '1 hour'`, restringindo a leitura à partição mensal exata do run. | Servidores com `< 100` registros (ex.: `play_golden`, `min_records_expected = 50`) exibem contagem absoluta (`players_in_top_100` / `sample_total_players`), percentual sobre a amostra e percentual nominal `/ 100`. Jogadores sem guilda são rotulados explicitamente como `[SEM GUILDA]`. |
| **3. Balanço de Raças (CH vs EU)** | Mesma CTE `latest_run` + janela de `created_at` de ±1h no `JOIN` com `character_snapshots` e `characters`. Agrupa por `COALESCE(cs.race, c.race, 'UNK')`. | Trata servidores 100% `CH` (0% `EU`) e registros `UNK` com paleta de cores fixa (`CH`, `EU`, `UNK`) sem erro de divisão por zero ou quebra de legenda. |
| **4. Retenção Semanal** | CTE `windowed_snapshots` filtra `cs.server_id = :server_id AND cs.created_at >= :window_start - INTERVAL '2 days'` **antes** do `LAG(...) OVER (PARTITION BY cs.server_id, cs.character_id ORDER BY cs.created_at)`. A CTE seguinte descarta `prev_created_at IS NULL` e recorta `created_at >= :window_start`, preservando o delta exato na borda da janela sem ler partições antigas. | Calcula `is_current_week` comparando `week_start` com `DATE_TRUNC('week', CURRENT_TIMESTAMP AT TIME ZONE 'UTC')` para destacar visualmente a semana corrente incompleta (barra tracejada/anotação). Servidores com `< 2` snapshots exibem estado vazio explicativo. |
| **5. Radar Bot vs Whale** | CTE `windowed_snapshots` filtra `cs.server_id = :server_id AND cs.created_at >= CURRENT_TIMESTAMP - INTERVAL '9 days'` (7 dias + *lookback buffer* de 2 dias) **antes** do `LAG(...)`. Recorta `created_at >= CURRENT_TIMESTAMP - INTERVAL '7 days'` e aplica `HAVING COUNT(*) >= 6` e as mesmas regras de segmentação de `v_bot_vs_donor_classification` (`SUSPECTED_BOT_24_7`, `SUSPECTED_DONATOR_WHALE`, `HARDCORE_HUMAN`, `CASUAL_OR_BALANCED`). | Servidores em cooldown ou com menos de 7 snapshots em 7 dias retornam 0 linhas; a UI exibe estado vazio explicando o requisito mínimo de 7 snapshots. Exibe gráfico de dispersão (`grind_persistence_ratio` × `max_single_jump_item_points`) + tabela filtrável por segmento. |

### 4. Defesa Read-Only em Duas Camadas e Provisionamento de `sro_readonly`

A proteção contra escrita acidental ou injeção lógica opera em duas camadas obrigatórias na VPS:

1. **Camada 1 — Role Dedicada de Menor Privilégio (`sro_readonly`):**
   *   Provisionada via script idempotente `scripts/provision_readonly_role.sh` (executado manualmente na VPS durante o setup ou rotação de senha, lendo `POSTGRES_USER`, `POSTGRES_DB` e `DASHBOARD_DB_PASSWORD` do `.env`, sem senhas hardcoded em código ou migrações Alembic).
   *   Comandos aplicados na transação:
       *   Cria ou atualiza a role `sro_readonly` com `NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT LOGIN` e senha parametrizada.
       *   `GRANT CONNECT ON DATABASE <db> TO sro_readonly;`
       *   `GRANT USAGE ON SCHEMA public TO sro_readonly;`
       *   `GRANT SELECT ON ALL TABLES IN SCHEMA public TO sro_readonly;` (no PostgreSQL 16, `GRANT SELECT` na tabela pai particionada `character_snapshots` cobre automaticamente todas as partições mensais futuras criadas por `ensure_monthly_partitions`).
       *   `ALTER DEFAULT PRIVILEGES FOR ROLE <owner> IN SCHEMA public GRANT SELECT ON TABLES TO sro_readonly;`
       *   `REVOKE CREATE ON SCHEMA public FROM sro_readonly;`
2. **Camada 2 — Guardrails de Sessão SQLAlchemy (`connect_args`):**
   *   Toda conexão aberta pelo dashboard força `default_transaction_read_only=on`, `statement_timeout=15000`, `lock_timeout=3000` e `work_mem=16MB`.

### 5. Empacotamento Docker, Publicação Traefik/EasyPanel e Hardening Colateral

*   **Isolamento de dependências (`pyproject.toml` + `Dockerfile`):**
    *   `pyproject.toml` declara `[project.optional-dependencies] dashboard = ["streamlit>=1.40.0,<2.0.0", "altair>=5.0.0,<6.0.0"]`.
    *   `Dockerfile` aceita `ARG INSTALL_EXTRAS=""` e executa `RUN pip install --no-cache-dir ".${INSTALL_EXTRAS}"`. O serviço `scraper-worker` mantém o build padrão (`INSTALL_EXTRAS=""`, sem `streamlit`/`pandas`/`pyarrow`); o serviço `dashboard` passa `args: { INSTALL_EXTRAS: "[dashboard]" }`.
*   **Serviço `dashboard` em `docker-compose.yml`:**
    *   Sempre ativo (`restart: unless-stopped`), `mem_limit: 512m`, `cpus: "1.0"`, sem `ports:` abertos em `0.0.0.0`.
    *   Comando: `streamlit run src/dashboard/app.py --server.address=0.0.0.0 --server.port=8501 --server.headless=true --browser.gatherUsageStats=false`.
    *   `healthcheck`: `curl -fsS http://127.0.0.1:8501/_stcore/health || exit 1` (intervalo `30s`, timeout `5s`, `retries: 3`, `start_period: 20s`).
    *   Rotação de logs: `logging: { driver: "json-file", options: { max-size: "10m", max-file: "3" } }`.
*   **Roteamento no Traefik / EasyPanel (VPS Hostinger KVM 2):**
    *   Domínio HTTPS dedicado com certificado Let's Encrypt automático.
    *   Middleware de autenticação na borda (Basic Auth para 1 usuário) gerenciado pelo Traefik/EasyPanel.
    *   Suporte explícito a WebSockets (`Upgrade` / `Connection` + `X-Forwarded-Proto: https`) para o endpoint `/_stcore/stream`, com upstream apontando para o container `dashboard:8501` na rede interna Docker (ou `127.0.0.1:8501` caso o proxy opere fora da rede do Compose).
*   **Hardening Colateral (`prefect-server`):**
    *   Alterar `ports: - "4200:4200"` no `docker-compose.yml` para `"127.0.0.1:4200:4200"`, impedindo exposição pública da UI/API do Prefect em `0.0.0.0` na VPS (acesso via túnel SSH ou publicação opcional atrás do mesmo Traefik/EasyPanel com autenticação).

--------------------------------------------------------------------------------

## Alternatives Considered

### Alternativa A: Metabase ou Ferramentas de BI Baseadas em JVM
*   **Pros:** Interface SQL pronta, construtor visual de dashboards sem código Python e gerenciamento embutido de perguntas salvas.
*   **Cons:** Exige JVM e consumo base de RAM significativamente maior (frequentemente > 1 GB), requer banco de metadados adicional e destoa da stack Python enxuta do repositório.
*   **Reason for Rejection:** Rejeitado pelo autor: o consumo de memória e a introdução de runtime JVM comprometem a folga da VPS Hostinger KVM 2 (8 GB RAM) e aumentam o custo operacional em comparação a um serviço Python/Streamlit com teto de `512MB`.

### Alternativa B: Painel Operacional Integrado no Mesmo Dashboard
*   **Pros:** Centralizaria saúde de coletas (`scrape_runs`), estado do Circuit Breaker e download de `./quarantine/*.html.gz` junto às visões de comunidade.
*   **Cons:** Exigiria montar o volume `./quarantine` no container exposto via HTTPS, ampliaria superfície de acesso sobre payloads brutos e duplicaria funcionalidades já presentes na UI do Prefect.
*   **Reason for Rejection:** Rejeitado pelo autor: telemetria operacional permanece na UI do Prefect e em consultas SQL/CLI, mantendo o dashboard estritamente focado em análise de comunidade e desacoplado da quarentena em disco.

### Alternativa C: Container Sob Demanda via Docker Compose `profiles`
*   **Pros:** Zero consumo de RAM quando o dashboard não estiver em uso.
*   **Cons:** Exige acesso SSH para subir e derrubar o container antes e depois de cada consulta, quebrando o acesso direto pelo domínio HTTPS.
*   **Reason for Rejection:** Rejeitado pelo autor: preferência por container sempre ativo junto com `postgres` e `scraper-worker`, com impacto controlado via `mem_limit: 512m`, `cpus: "1.0"`, `pool_size=2` e `max_entries=30` no cache.

### Alternativa D: Autenticação no App (`streamlit-authenticator`) ou Acesso Exclusivo via SSH Tunnel/VPN
*   **Pros:** `streamlit-authenticator` evitaria configuração de middleware no proxy; SSH Tunnel eliminaria qualquer exposição HTTP/HTTPS.
*   **Cons:** `streamlit-authenticator` adiciona dependência Python, gestão de hashes/cookies de sessão na aplicação e superfície de bugs de autenticação; SSH Tunnel/VPN impede acesso direto pelo navegador sem cliente/túnel prévio.
*   **Reason for Rejection:** Rejeitado pelo autor: a VPS Hostinger KVM 2 já opera com **Traefik/EasyPanel**, permitindo aplicar TLS (Let's Encrypt) e Basic Auth para 1 usuário na borda sem adicionar código ou dependências de autenticação ao Python.

### Alternativa E: Materialized Views com Refresh Agendado no Prefect
*   **Pros:** Leituras instantâneas sobre agregações pré-computadas mesmo com milhões de linhas.
*   **Cons:** Introduz nova task/flow de refresh no Prefect, maior uso de disco no PostgreSQL e defasagem entre a conclusão do scrape e o refresh da view.
*   **Reason for Rejection:** Rejeitado pelo autor (YAGNI): para o volume-alvo de até 10 servidores, 6 meses de histórico e 1 usuário, queries com *partition pruning* (CTEs com *lookback* de 2 dias + índice parcial em `scrape_runs`) somadas a `@st.cache_data` atendem o SLA de `< 5s` sem nova orquestração.

### Alternativa F: Biblioteca `plotly` para Visualização
*   **Pros:** Ampla variedade de tipos de gráficos interativos e controles prontos de zoom/exportação.
*   **Cons:** Adiciona dependência pesada adicional na imagem Docker e maior sobrecarga de serialização JSON/memória para gráficos de séries temporais, barras e dispersão.
*   **Reason for Rejection:** Rejeitado pelo autor (YAGNI): `altair` já vem embutido no Streamlit e cobre todas as 5 visões (séries temporais, barras empilhadas/agrupadas e gráfico de dispersão) sem aumentar o tamanho da imagem nem o uso de RAM.

### Alternativa G: Reusar Engine Assíncrona (`asyncpg`) de `src/core/database.py`
*   **Pros:** Reutilização direta da factory de conexão do `scraper-worker`.
*   **Cons:** O ciclo de execução do Streamlit é síncrono por rerun; encapsular `asyncpg` via `asyncio.run()` causa erros recorrentes de *event loop* fechado e pool preso entre reruns. Além disso, `src/core/config.py` acoplaria o dashboard à variável read-write `DATABASE_URL`.
*   **Reason for Rejection:** Rejeitado pelo autor: o driver síncrono `psycopg` já está instalado no projeto (usado pelo Alembic) e funciona nativamente com SQLAlchemy 2.0 síncrono e `@st.cache_resource` no Streamlit.

--------------------------------------------------------------------------------

## Non-Functional Requirements

### Security & Privacy
*   **Menor Privilégio no Banco:** O container `dashboard` recebe exclusivamente `DASHBOARD_DATABASE_URL` apontando para a role `sro_readonly` (`GRANT SELECT`), nunca a `DATABASE_URL` read-write.
*   **Defesa em Profundidade na Sessão:** Toda conexão PostgreSQL aplica `default_transaction_read_only=on`, `statement_timeout=15000`, `lock_timeout=3000` e `work_mem=16MB`.
*   **Proteção de Credenciais:** `DASHBOARD_DATABASE_URL` é tipada como `SecretStr` com `hide_input_in_errors=True` em `DashboardSettings`; exceções e logs JSON nunca registram a URL ou senha.
*   **Isolamento de Rede na VPS:** Porta `8501` não é publicada em `0.0.0.0`, porta `5432` permanece sem exposição no host, e porta `4200` do `prefect-server` é restrita a `127.0.0.1:4200:4200`. TLS e Basic Auth operam na borda (Traefik/EasyPanel).

### Performance & Latency
*   **SLA de Carregamento:** Qualquer uma das 5 visões carrega em `< 5s` para até 10 servidores e 6 meses de histórico.
*   **Carregamento *Lazy*:** `st.segmented_control` executa apenas a query da visão ativa (evitando o disparo simultâneo de 5 queries típico de `st.tabs`).
*   **Limites de Recursos:** Container `dashboard` limitado a `mem_limit: 512m` e `cpus: "1.0"`; pool SQLAlchemy limitado a `pool_size=2, max_overflow=0, pool_recycle=300`; cache em memória limitado a `max_entries=30` com `ttl=600s` (configurável).
*   **Rotação de Logs:** Driver `json-file` com `max-size: "10m"` e `max-file: "3"` para proteger os ~100 GB de disco da VPS em operação 24/7.

### Rollout & Rollback
*   **Rollout:**
    1. Aplicar migração Alembic `003` (`alembic upgrade head`) — criação de índice parcial não bloqueia consultas de leitura e não altera tabelas/views existentes.
    2. Executar `scripts/provision_readonly_role.sh` na VPS com `DASHBOARD_DB_PASSWORD` definido no `.env`.
    3. Configurar `DASHBOARD_DATABASE_URL` no `.env`, subir o serviço `dashboard` (`docker compose up -d --build dashboard`) e configurar a rota HTTPS + Basic Auth + WebSockets no Traefik/EasyPanel.
*   **Rollback:**
    1. Derrubar o container `dashboard` (`docker compose stop dashboard && docker compose rm -f dashboard`) e remover a rota no Traefik/EasyPanel.
    2. Opcionalmente reverter o índice via `alembic downgrade 002_analytical_views` e revogar a role `sro_readonly`. Nenhuma alteração impacta o `scraper-worker` ou os dados históricos.

--------------------------------------------------------------------------------

### Additional Resources

*   [SITEMAP.md](../../designs/SITEMAP.md) — Índice mestre de designs aprovados.
*   [001_sro_ranking_scraper_pipeline.md](../../designs/001_sro_ranking_scraper_pipeline.md) — Schema particionado e views analíticas originais.
*   [002_discord_notifications.md](../../designs/002_discord_notifications.md) — Padrões de `SecretStr`, `hide_input_in_errors` e consultas com *partition pruning*.
*   [concepts_brief.md](concepts_brief.md) — Brief de brainstorm original deste design.

### Future Improvements

*   **Materialized Views com refresh no Prefect:** Caso o escopo ultrapasse 10 servidores ou 6 meses de histórico e a latência se aproxime de `5s`, introduzir views materializadas atualizadas ao final de `scheduled_12h_flow`.
*   **Comparação Multi-Servidor Lado a Lado:** Permitir selecionar 2–3 servidores simultaneamente para comparar retenção semanal e curva média de `item_points`.
*   **Exportação CSV da Tabela Filtrada:** Botão `st.download_button` sobre os DataFrames já em memória das visões 2 e 5.

--------------------------------------------------------------------------------

## Detailed Implementation Plan

### Target Files to Create/Modify

#### 1. `pyproject.toml` (Modify)
*   **Type / Scope:** Empacotamento Python, dependências opcionais e configuração `mypy`.
*   **Proposed Changes:**
    *   Adicionar grupo opcional `[project.optional-dependencies]` com `dashboard = ["streamlit>=1.40.0,<2.0.0", "altair>=5.0.0,<6.0.0"]`, mantendo `[project.dependencies]` inalterado para que o `scraper-worker` não instale `streamlit`, `pandas` ou `pyarrow`.
    *   Adicionar bloco `[[tool.mypy.overrides]]` para `module = ["streamlit", "streamlit.*", "altair", "altair.*"]` com `ignore_missing_imports = true`, preservando `mypy --strict` em todo `src/` e `tests/` mesmo quando o extra opcional `[dashboard]` não estiver instalado no `.venv` local.

#### 2. `Dockerfile` (Modify)
*   **Type / Scope:** Imagem Docker compartilhada entre `scraper-worker` e `dashboard`.
*   **Proposed Changes:**
    *   Declarar `ARG INSTALL_EXTRAS=""` antes da instalação do pacote e atualizar o passo `pip install` para instalar `".${INSTALL_EXTRAS}"`.
    *   Manter o `CMD` padrão do `scraper-worker` inalterado; o serviço `dashboard` sobrescreve o `command` no `docker-compose.yml` e passa `INSTALL_EXTRAS: "[dashboard]"` no `build.args`.

#### 3. `docker-compose.yml` (Modify)
*   **Type / Scope:** Orquestração de containers e hardening de rede na VPS Hostinger KVM 2.
*   **Proposed Changes:**
    *   Adicionar serviço `dashboard`:
        *   `build`: `context: .`, `dockerfile: Dockerfile`, `args: { INSTALL_EXTRAS: "[dashboard]" }`.
        *   Limites: `mem_limit: 512m`, `cpus: "1.0"`, `restart: unless-stopped`.
        *   `depends_on`: `postgres` (`condition: service_healthy`).
        *   `command`: `["streamlit", "run", "src/dashboard/app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true", "--browser.gatherUsageStats=false"]`.
        *   `environment`: apenas `DASHBOARD_DATABASE_URL`, `DASHBOARD_CACHE_TTL_SECONDS`, `DASHBOARD_STATEMENT_TIMEOUT_MS`, `DASHBOARD_LOCK_TIMEOUT_MS`, `DASHBOARD_WORK_MEM_MB` e `LOG_LEVEL` (sem `DATABASE_URL`).
        *   `networks`: `[sro_network]`, sem `ports:` abertos em `0.0.0.0` (roteado via Traefik/EasyPanel).
        *   `healthcheck`: `["CMD-SHELL", "curl -fsS http://127.0.0.1:8501/_stcore/health || exit 1"]` (`interval: 30s`, `timeout: 5s`, `retries: 3`, `start_period: 20s`).
        *   `logging`: `driver: json-file`, `options: { max-size: "10m", max-file: "3" }`.
    *   Hardening colateral em `prefect-server`: alterar `ports` de `"4200:4200"` para `"127.0.0.1:4200:4200"`.

#### 4. `.env.example` (Modify)
*   **Type / Scope:** Modelo de variáveis de ambiente.
*   **Proposed Changes:**
    *   Documentar `DASHBOARD_DB_PASSWORD`, `DASHBOARD_DATABASE_URL` (`postgresql+psycopg://sro_readonly:CHANGE_ME_READONLY_PASSWORD@postgres:5432/sro_rankings`), `DASHBOARD_CACHE_TTL_SECONDS=600`, `DASHBOARD_STATEMENT_TIMEOUT_MS=15000`, `DASHBOARD_LOCK_TIMEOUT_MS=3000` e `DASHBOARD_WORK_MEM_MB=16`.

#### 5. `alembic/versions/003_dashboard_indexes.py` (Create)
*   **Type / Scope:** Migração Alembic de índice parcial no PostgreSQL 16.
*   **Proposed Changes:**
    *   Definir `revision = "003_dashboard_indexes"` e `down_revision = "002_analytical_views"`.
    *   Em `upgrade()`: executar `CREATE INDEX IF NOT EXISTS idx_scrape_runs_server_latest ON scrape_runs (server_id, created_at DESC) WHERE status IN ('SUCCESS', 'PARTIAL_SUCCESS')`.
    *   Em `downgrade()`: executar `DROP INDEX IF EXISTS idx_scrape_runs_server_latest`.

#### 6. `scripts/provision_readonly_role.sh` (Create)
*   **Type / Scope:** Script operacional idempotente para provisionamento da role `sro_readonly` na VPS.
*   **Proposed Changes:**
    *   Ler `POSTGRES_USER`, `POSTGRES_DB` e `DASHBOARD_DB_PASSWORD` do ambiente (falhar imediatamente com código de saída diferente de zero se `DASHBOARD_DB_PASSWORD` estiver vazia ou ausente; nunca imprimir a senha no stdout/stderr).
    *   Executar bloco SQL idempotente via `docker compose exec -T postgres psql -v ON_ERROR_STOP=1` com escape seguro de literal para criar ou atualizar `sro_readonly` (`NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT LOGIN`), conceder `CONNECT`, `USAGE ON SCHEMA public`, `SELECT ON ALL TABLES IN SCHEMA public`, configurar `ALTER DEFAULT PRIVILEGES FOR ROLE <owner> IN SCHEMA public GRANT SELECT ON TABLES TO sro_readonly` e executar `REVOKE CREATE ON SCHEMA public FROM sro_readonly`.

#### 7. `src/dashboard/__init__.py` (Create)
*   **Type / Scope:** Inicialização do pacote `src.dashboard`.
*   **Proposed Changes:**
    *   Exportar metadados públicos mínimos do pacote sem importar `streamlit` ou `altair` no nível de módulo (garantindo que testes da camada de dados não exijam o extra `[dashboard]`).

#### 8. `src/dashboard/config.py` (Create)
*   **Type / Scope:** Configuração Pydantic v2 isolada para o dashboard.
*   **Proposed Changes:**
    *   Definir `DashboardSettings(BaseSettings)` com `SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore", hide_input_in_errors=True)`.
    *   Campos: `dashboard_database_url: SecretStr`, `dashboard_cache_ttl_seconds: int = Field(default=600, ge=30, le=3600)`, `dashboard_statement_timeout_ms: int = Field(default=15000, ge=1000, le=60000)`, `dashboard_lock_timeout_ms: int = Field(default=3000, ge=500, le=15000)`, `dashboard_work_mem_mb: int = Field(default=16, ge=4, le=32)` e `log_level: str = "INFO"`.
    *   Validador de `dashboard_database_url`: rejeitar string vazia/espaços, normalizar `postgres://` e `postgresql://` para `postgresql+psycopg://`, aceitar `postgresql+psycopg://` e `sqlite://` (testes), rejeitar esquemas assíncronos (`postgresql+asyncpg://`, `sqlite+aiosqlite://`) e validar presença de host (PostgreSQL) ou path (SQLite).
    *   Função `get_dashboard_settings() -> DashboardSettings`.

#### 9. `src/dashboard/db.py` (Create)
*   **Type / Scope:** Factory síncrona SQLAlchemy 2.0 com guardrails read-only.
*   **Proposed Changes:**
    *   `build_postgres_options(settings: DashboardSettings) -> str`: montar string `-c default_transaction_read_only=on -c statement_timeout=<ms> -c lock_timeout=<ms> -c work_mem=<mb>MB`.
    *   `create_dashboard_engine(settings: DashboardSettings) -> Engine`: extrair URL via `get_secret_value()`, criar engine síncrona com `pool_size=2`, `max_overflow=0`, `pool_recycle=300`, `pool_pre_ping=True` e `connect_args={"options": build_postgres_options(settings)}` para PostgreSQL (e engine síncrona simples para `sqlite://` em testes).
    *   `create_dashboard_session_factory(engine: Engine) -> sessionmaker[Session]`: retornar factory síncrona com `expire_on_commit=False`.

#### 10. `src/dashboard/schemas.py` (Create)
*   **Type / Scope:** Contratos de dados (DTOs Pydantic v2 imutáveis, sem dependência de UI).
*   **Proposed Changes:**
    *   Definir modelos `ConfigDict(frozen=True)` com tipagem estrita (`Decimal` / `float` / `int` / `date` / `datetime` / `Literal`):
        *   `ServerOption(id: str, display_name: str, min_records_expected: int)`
        *   `ServerFreshness(server_id: str, last_snapshot_at: datetime | None)`
        *   `TopNAggregatePoint(snapshot_day: date, tier: Literal["Top 10", "Top 50", "Top 100"], avg_item_points: float, median_item_points: float, max_item_points: float, sample_size: int)`
        *   `TopCharacterTrendPoint(snapshot_day: date, char_name: str, rank_position: int, item_points: float)`
        *   `GuildDominanceRow(guild_name: str, players_in_top_100: int, players_in_top_50: int, total_top_item_points: float, nominal_top_100_pct: float, sample_share_pct: float, sample_total_players: int)`
        *   `RaceBalanceRow(race: Literal["CH", "EU", "UNK"], total_ranked_characters: int, top_100_count: int, avg_level: float, avg_item_points: float, max_item_points: float)`
        *   `WeeklyRetentionRow(week_start: date, total_tracked_characters: int, active_characters: int, activity_retention_rate_pct: float, is_current_week: bool)`
        *   `BehaviorClassificationRow(char_name: str, race: str, evaluated_intervals: int, total_item_points_gain: float, max_single_jump_item_points: float, grind_persistence_ratio: float, player_behavior_segment: Literal["SUSPECTED_BOT_24_7", "SUSPECTED_DONATOR_WHALE", "HARDCORE_HUMAN", "CASUAL_OR_BALANCED"])`

#### 11. `src/dashboard/queries.py` (Create)
*   **Type / Scope:** Camada pura de consultas SQL otimizadas com *partition pruning* (sem dependência de UI).
*   **Proposed Changes:**
    *   Exportar constantes SQL parametrizadas (`sqlalchemy.text`) e 8 funções síncronas tipadas:
        *   `list_active_servers(session: Session) -> list[ServerOption]`
        *   `get_server_freshness(session: Session, server_id: str) -> ServerFreshness`
        *   `get_top_n_item_points_evolution(session: Session, server_id: str, window_days: int = 30, reference_time: datetime | None = None) -> list[TopNAggregatePoint]`
        *   `get_top_characters_drilldown(session: Session, server_id: str, window_days: int = 30, top_n: int = 5, reference_time: datetime | None = None) -> list[TopCharacterTrendPoint]`
        *   `get_guild_dominance(session: Session, server_id: str) -> list[GuildDominanceRow]`
        *   `get_race_balance(session: Session, server_id: str) -> list[RaceBalanceRow]`
        *   `get_weekly_retention(session: Session, server_id: str, window_days: int = 180, reference_time: datetime | None = None) -> list[WeeklyRetentionRow]`
        *   `get_bot_vs_donor_classification(session: Session, server_id: str, reference_time: datetime | None = None) -> list[BehaviorClassificationRow]`
    *   Validar `window_days` (`1 <= window_days <= 185`) e `top_n` (`1 <= top_n <= 10`) antes de executar a query.
    *   Garantir *partition pruning* em todas as queries temporais: filtro `created_at >= :window_start` na Visão 1, `JOIN` com janela `±1 hour` do `latest_run` nas Visões 2 e 3, e CTE com `created_at >= :lookback_start` (`window_start - 2 days` / `reference_time - 9 days`) **antes** do `LAG()` nas Visões 4 e 5.

#### 12. `src/dashboard/app.py` (Create)
*   **Type / Scope:** Aplicação Streamlit single-page + renderização `altair`.
*   **Proposed Changes:**
    *   Configurar `structlog` via `configure_logging(settings.log_level)` e `st.set_page_config(page_title="SRO Ranking Analytics", layout="wide")`.
    *   Instanciar `Engine` e `sessionmaker` via `@st.cache_resource` e encapsular cada uma das 5 visões + lista de servidores + frescor com `@st.cache_data(ttl=settings.dashboard_cache_ttl_seconds, max_entries=30)`.
    *   Renderizar seletor de servidor, subtítulo discreto `Último snapshot: YYYY-MM-DD HH:MM UTC` (ou estado vazio), botão **"Atualizar dados"** (`st.cache_data.clear()` + `st.rerun()`) e seletor de visão `st.segmented_control` (com fallback para `st.radio(..., horizontal=True)`), disparando apenas a query da visão ativa.
    *   Implementar os 5 renderizadores `altair` + tabelas auxiliares e estados vazios informativos para todos os casos de borda (`< 100` jogadores + `[SEM GUILDA]`, 100% `CH` + `UNK`, semana corrente incompleta + `< 2` snapshots, `< 7` snapshots em 7 dias).
    *   Capturar `SQLAlchemyError` em cada visão, registrar evento JSON em `structlog` (`server_id`, `view_name`, `error_type`, sem credenciais) e exibir `st.warning` amigável.

#### 13. `tests/test_dashboard_config.py` (Create)
*   **Type / Scope:** Testes unitários de configuração e engine read-only.
*   **Proposed Changes:**
    *   Validar normalização de esquema (`postgres://`, `postgresql://`, `postgresql+psycopg://`, `sqlite://`), rejeição de `postgresql+asyncpg://` e strings vazias, mascaramento `SecretStr` + ausência de credenciais em `ValidationError`.
    *   Validar `build_postgres_options` e criação da engine PostgreSQL com `pool_size=2`, `max_overflow=0`, `pool_recycle=300` e `connect_args["options"]` contendo `default_transaction_read_only=on`, `statement_timeout`, `lock_timeout` e `work_mem`.

#### 14. `tests/test_dashboard_queries.py` (Create)
*   **Type / Scope:** Testes unitários da camada de queries, DTOs e contratos de *partition pruning*.
*   **Proposed Changes:**
    *   Validar contratos SQL de *partition pruning* nas constantes de `src/dashboard/queries.py` (presença de filtro `created_at >= :lookback_start` antes de `LAG()`, janela `±1 hour` no `JOIN` do `latest_run` e uso exclusivo de parâmetros bindados sem interpolação).
    *   Validar mapeamento de DTOs e casos de borda com `Session` mockada (e SQLite em memória para queries neutras em dialeto como `list_active_servers` e `get_server_freshness`):
        *   Visão 1: agregação por faixas Top 10/50/100, validação de `window_days` e `top_n`, e retorno vazio.
        *   Visão 2: servidor com `< 100` registros (ex.: `50` jogadores no `play_golden`) exibindo contagem absoluta + percentual da amostra + `[SEM GUILDA]`.
        *   Visão 3: servidor 100% `CH` (0% `EU`) e registros `UNK`.
        *   Visão 4: sinalização `is_current_week=True` na semana corrente UTC e retorno vazio para `< 2` snapshots.
        *   Visão 5: retorno vazio (`0` linhas) quando há `< 7` snapshots em 7 dias e mapeamento completo dos 4 segmentos comportamentais.

#### 15. `README.md`, `src/README.md`, `src/dashboard/README.md`, `alembic/versions/README.md` (Modify / Create)
*   **Type / Scope:** Documentação de operação e arquitetura do repositório.
*   **Proposed Changes:**
    *   `README.md`: documentar novas variáveis `DASHBOARD_*`, execução de `scripts/provision_readonly_role.sh`, configuração do Traefik/EasyPanel na VPS (HTTPS, Basic Auth e WebSockets `/_stcore/stream`) e restrição da porta `4200` do `prefect-server` para `127.0.0.1:4200:4200`.
    *   `src/README.md`: registrar o subpacote `./dashboard/README.md`.
    *   `src/dashboard/README.md`: documentar responsabilidade, invariantes read-only e fronteiras de `config.py`, `db.py`, `schemas.py`, `queries.py` e `app.py`.
    *   `alembic/versions/README.md`: registrar a migração `003_dashboard_indexes.py`.

### Verification Plan

*   **Static Analysis & Linting (Ruff):**
    ```bash
    uv run --no-sync ruff check .
    uv run --no-sync ruff format --check .
    ```
*   **Type Checking (Mypy Strict):**
    ```bash
    uv run --no-sync mypy src tests
    ```
*   **Unit Tests & Coverage (Pytest):**
    ```bash
    uv run --no-sync pytest --cov=src tests/
    ```
    *   Cobertura obrigatória sobre `src/dashboard/config.py`, `src/dashboard/db.py`, `src/dashboard/schemas.py` e `src/dashboard/queries.py` sem chamadas de rede reais e sem dependência de instância PostgreSQL ativa nos testes unitários.
*   **Database Migration Verification (Alembic):**
    ```bash
    uv run --no-sync alembic upgrade head
    ```
    *   Confirmar existência do índice `idx_scrape_runs_server_latest` em `scrape_runs` e reversibilidade via `alembic downgrade 002_analytical_views && alembic upgrade head` em ambiente de validação.
*   **Container Build, Isolation & Integration Verification (VPS / Docker Compose):**
    *   Confirmar que a imagem de `scraper-worker` não possui `streamlit`, `pandas` ou `pyarrow` instalados (`docker compose run --rm --no-deps scraper-worker python -c "import importlib.util; assert importlib.util.find_spec('streamlit') is None"`).
    *   Confirmar healthcheck `200 OK` em `http://127.0.0.1:8501/_stcore/health` dentro do container `dashboard` e ausência de publicação da porta `8501` em `0.0.0.0` (`docker compose ps`).
    *   Confirmar que `sro_readonly` falha com erro de permissão ao tentar `INSERT`/`UPDATE`/`DELETE` e que `EXPLAIN (ANALYZE, BUFFERS)` nas queries das Visões 1–5 confirma *partition pruning* no PostgreSQL 16.
    *   Validar acesso via domínio HTTPS atrás do Traefik/EasyPanel (Basic Auth + conexão WebSocket `/_stcore/stream` ativa + carregamento de todas as 5 visões em `< 5s`).

--------------------------------------------------------------------------------

*Drafted with EGM*
