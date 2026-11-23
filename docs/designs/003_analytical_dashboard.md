# Dashboard Analítico Read-Only em Streamlit (v3.0)

*   **Author:** richardman13
*   **Status:** Approved
*   **Date:** 2026-11-22
*   **Approved Date:** 2026-11-23
*   **Source Brief:** [concepts_brief.md](../egm/003_analytical_dashboard/concepts_brief.md)

--------------------------------------------------------------------------------

## Overview

A versão **v3.0** adiciona uma camada de visualização analítica somente leitura (*read-only*) ao **SRO Ranking Scraper** (*Silkroad Online* — **SRO**) usando **Streamlit**, voltada para análise de comunidade por um único usuário. O serviço executa como container dedicado e sempre ativo na **VPS Hostinger KVM 2** (Debian 13, 8 GB RAM, 2 vCPUs, ~100 GB de disco), atrás de domínio HTTPS gerenciado por **Traefik/EasyPanel** (terminação TLS, autenticação na borda e suporte a WebSockets, sem exposição direta da porta `8501`).

O dashboard expõe 5 visões interativas por servidor (dimensionado para até **10 servidores** e **6 meses** de histórico), isolando suas dependências Python da imagem do `scraper-worker` e conectando-se ao PostgreSQL 16 exclusivamente por meio de uma role de leitura dedicada (`sro_readonly`) protegida por guardrails tanto no banco quanto na sessão SQLAlchemy. Telemetria operacional (`scrape_runs`, estado do Circuit Breaker e quarentena em `./quarantine/`) permanece fora do escopo deste dashboard, sob responsabilidade da UI do Prefect e de consultas manuais via SQL/CLI.

## The Problem

A v1.0 do **SRO Ranking Scraper** ([Design 001](001_sro_ranking_scraper_pipeline.md)) criou 5 views analíticas no PostgreSQL 16 (`v_character_activity_deltas`, `v_bot_vs_donor_classification`, `v_guild_turnover_dominance`, `v_race_balance_ch_vs_eu` e `v_server_retention_weekly`), e a v2.0 ([Design 002](002_discord_notifications.md)) automatizou alertas de falha e resumos pós-Fortress War (evento semanal de disputa de fortalezas) no Discord. No entanto, inspecionar tendências históricas da comunidade ainda apresenta três atritos práticos:

1. **Consumo manual via SQL:** Consultar evolução de `item_points` (pontuação de equipamento do personagem), dominância de guildas, balanço de raças (`CH` = *Chinese*, `EU` = *European*, `UNK` = *Unknown*), retenção semanal e classificação comportamental (Bot vs Whale) exige conexão direta ao banco e queries manuais repetitivas.
2. **Risco de degradação sobre o banco em consultas analíticas:** As views atuais aplicam `LAG()` sem corte temporal prévio (`v_character_activity_deltas`) e fazem `JOIN` em `character_snapshots` sem filtrar `created_at` (`v_guild_turnover_dominance`, `v_race_balance_ch_vs_eu`), impedindo *partition pruning* e expondo o PostgreSQL de 1 GB de RAM (`work_mem=64MB`) a leituras de todas as partições mensais e risco de OOM durante coletas do `scraper-worker`.
3. **Ausência de publicação segura na VPS:** Expor uma interface web diretamente por porta Docker (`8501:8501`) ou acoplar bibliotecas de UI (`streamlit`, `pandas`, `pyarrow`) ao `scraper-worker` violaria tanto o isolamento de rede da VPS quanto o teto estrito de 512 MB de RAM do worker.

A solução entrega uma interface web single-page em Streamlit em container separado, alimentada por queries read-only com *partition pruning* determinístico e cache em memória, publicada exclusivamente atrás do proxy reverso Traefik/EasyPanel.

## Goals

*   **G1 (5 Visões Analíticas da Comunidade):** Disponibilizar, por servidor ativo selecionado e sem SQL manual, cinco visões interativas com carregamento *lazy* (apenas a visão ativa executa consulta):
    1. Evolução diária de `item_points` por faixa de rank (curvas agregadas de média, mediana P50 e P90 para Top 10, Top 50 e Top 100 + curva única do teto Rank #1 em janela padrão de 30 dias expansível até 180 dias, com drill-down opcional de `item_points` e `rank_position` para Top 5–10 personagens individuais).
    2. Dominância de guildas no Top 100 (exibindo contagem absoluta de jogadores ranqueados junto ao percentual sobre a amostra e nominal sobre 100 vagas, rotulando `[SEM GUILDA]` com clareza).
    3. Balanço de raças `CH` vs `EU` (tratando servidores 100% `CH` e registros `UNK` sem quebra de gráfico ou legenda).
    4. Retenção semanal (janela padrão de 12 semanas alinhada à segunda-feira UTC, expansível até 26 semanas, sinalizando visualmente a semana corrente incompleta e tratando servidores com menos de 2 snapshots).
    5. Radar comportamental Bot vs Whale (gráfico de dispersão `grind_persistence_ratio` × `max_single_jump_item_points` + tabela filtrável pelos 4 segmentos, com estado vazio informativo quando houver menos de 7 snapshots em 7 dias).
*   **G2 (Latência e Orçamento de Recursos na VPS):** Carregar qualquer uma das 5 visões para qualquer servidor (até 10 servidores e 6 meses de histórico) em **menos de 5 segundos** via domínio HTTPS, mantendo o container `dashboard` dentro de `mem_limit: 768m` (`MALLOC_ARENA_MAX=2`) e `cpus: "1.0"`, sem impactar `scraper-worker` (512 MB) nem `postgres` (1 GB).
*   **G3 (Defesa Read-Only em Duas Camadas):** Garantir que o dashboard utilize exclusivamente a role dedicada `sro_readonly` via `DASHBOARD_DATABASE_URL` (`GRANT SELECT` + limites aplicados na própria role via `ALTER ROLE sro_readonly`) somada a guardrails de sessão na conexão SQLAlchemy e pool mínimo (`pool_size=2, max_overflow=0, pool_timeout=5`).
*   **G4 (Publicação Segura e Hardening na VPS):** Expor o dashboard somente atrás do proxy reverso Traefik/EasyPanel em topologia *dual-network* com HTTPS (Let's Encrypt), autenticação na borda (Basic Auth + rate-limiting) para 1 usuário e suporte a WebSockets (`/_stcore/stream`), sem publicar a porta `8501` em `0.0.0.0`, sem expor a porta `5432` no host e restringindo a porta `4200` do `prefect-server` para `127.0.0.1:4200:4200`.
*   **G5 (Isolamento de Dependências e Qualidade de Código):** Manter o `scraper-worker` livre de dependências de UI (`streamlit`, `altair`, `pandas`, `pyarrow`) via `[project.optional-dependencies]`, manter conformidade com `mypy --strict`, `ruff` e testes unitários (`pytest`) para a camada pura de queries/schemas, e exibir frescor discreto (`Último snapshot: YYYY-MM-DD HH:MM UTC`) no cabeçalho.

## Non-Goals

*   **NG1 (Painel Operacional):** Monitoramento de `scrape_runs`, estado do Circuit Breaker ou leitura/download de arquivos em `./quarantine/*.html.gz` permanece na UI do Prefect e via SQL/CLI.
*   **NG2 (Autenticação Customizada ou Multi-Usuário no App):** Não será implementada tela de login, `streamlit-authenticator`, RBAC ou acesso público anônimo no código Python; a autenticação de usuário único fica delegada ao Traefik/EasyPanel.
*   **NG3 (Operações de Escrita):** O dashboard não executa `INSERT`, `UPDATE`, `DELETE`, reset de Circuit Breaker, edição de cadastro de servidores ou anotações manuais.
*   **NG4 (Materialized Views com Agendamento no Prefect):** Pré-agregação materializada e jobs periódicos de refresh ficam fora desta versão (adiados para quando o volume ultrapassar 10 servidores ou 6 meses).
*   **NG5 (Bibliotecas Extras de Gráficos):** Não será adicionado `plotly` ou equivalentes; toda visualização utilizará `altair`, já embutido no Streamlit.

## Open Questions

Todas as questões de escopo, arquitetura e infraestrutura levantadas no brainstorm ([concepts_brief.md](../egm/003_analytical_dashboard/concepts_brief.md)) foram resolvidas. Não há questões em aberto.

## Critical User Journeys (CUJs)

### CUJ 1: Inspeção de Tendências de Servidor via Domínio HTTPS
*   **Given:** O container `dashboard` está ativo na VPS Hostinger KVM 2 atrás do Traefik/EasyPanel (HTTPS + Basic Auth + rate-limit na borda), conectado ao PostgreSQL 16 via `DASHBOARD_DATABASE_URL` (`sro_readonly`), com dados históricos de pelo menos um servidor ativo em `character_snapshots`.
*   **When:** O usuário acessa `https://<domínio-dashboard>`, autentica-se na borda, seleciona um servidor na barra lateral e alterna entre as 5 visões no `st.segmented_control` (ajustando a janela temporal na Visão 1 ou Visão 4 quando desejado).
*   **Then:** Apenas a consulta correspondente à visão ativa é executada (ou servida do `st.cache_data` dentro do TTL), o gráfico em `altair` e as tabelas/métricas renderizam em menos de 5 segundos com o subtítulo de frescor `Último snapshot: YYYY-MM-DD HH:MM UTC`, e nenhuma query de escrita é permitida pelo banco.

### CUJ 2: Atualização Manual Após Nova Coleta
*   **Given:** O usuário já consultou uma visão dentro da janela de TTL do cache (`600s`) e o `scraper-worker` concluiu um novo snapshot no PostgreSQL.
*   **When:** O usuário aciona o botão manual de atualização na interface.
*   **Then:** O cache de dados da aplicação é invalidado, a visão ativa consulta novamente o PostgreSQL respeitando `statement_timeout=15000` e atualiza os gráficos e o timestamp de `Último snapshot`.

### CUJ 3: Comportamento em Casos de Borda de Dados e Proteção contra Consultas Lentas
*   **Given:** Um servidor recém-adicionado (< 2 snapshots), em cooldown (< 7 snapshots nos últimos 7 dias), com menos de 100 personagens ranqueados (ex.: `play_golden`) ou 100% da raça `CH` — **ou** uma contenção momentânea no PostgreSQL que exceda `statement_timeout=15000`, `lock_timeout=3000` ou `pool_timeout=5`.
*   **When:** O usuário abre qualquer uma das 5 visões para esse servidor.
*   **Then:** Nos casos de dados escassos ou assimétricos, o dashboard exibe estados vazios informativos ou rótulos explícitos (`[SEM GUILDA]`, contagem absoluta ao lado do percentual, sinalização de semana corrente incompleta) sem exceções de runtime. Em caso de timeout ou erro de leitura no banco, a interface exibe mensagem controlada de erro temporário, registra log JSON estruturado sem vazar credenciais e preserva o pool de conexões e o processo do PostgreSQL intactos.

--------------------------------------------------------------------------------

## Technical Plan (Architecture)

### 1. Arquitetura de Alto Nível e Topologia na VPS Hostinger KVM 2

O dashboard opera como um processo Python/Streamlit independente em container próprio (`dashboard`), conectado em topologia **dual-network**: pertence à rede interna `sro_network` (para falar com `postgres`) **e** à rede externa do proxy Traefik/EasyPanel (via override de produção), mantendo `postgres`, `scraper-worker` e `prefect-server` isolados exclusivamente em `sro_network` e sem porta `8501` publicada em `0.0.0.0`.

```mermaid
flowchart LR
    USER["Navegador (1 Usuario)"]
    PROXY["Traefik / EasyPanel - HTTPS + Basic Auth + Rate Limit + WSS"]

    subgraph VPS["Docker Compose - VPS Hostinger KVM 2 (8 GB RAM / 2 vCPUs)"]
        subgraph PROXY_NET["Rede Externa do Proxy (EasyPanel / Traefik)"]
            DASH["dashboard (Streamlit) - 768m RAM, 1.0 vCPU, porta interna 8501"]
        end
        subgraph NET["Rede Interna: sro_network"]
            WORKER["scraper-worker (Prefect 3) - 512m RAM, sem streamlit/pandas"]
            PREFECT["prefect-server - 127.0.0.1:4200:4200"]
            PG["postgres (PostgreSQL 16) - 1g RAM, sem porta 5432 no host"]
        end
    end

    USER -- "HTTPS / WSS" --> PROXY
    PROXY -- "HTTP + Upgrade/Connection (/_stcore/stream)" --> DASH
    DASH -- "psycopg sincrono (sro_readonly, pool_size=2, read_only=on)" --> PG
    WORKER -- "asyncpg (read-write)" --> PG
    WORKER --> PREFECT
```

**Orçamento consolidado de recursos na VPS (8 GB RAM / 2 vCPUs / ~100 GB disco):**
*   `postgres`: `1 GB` (`mem_limit: 1g`)
*   `scraper-worker`: `512 MB` (`mem_limit: 512m`)
*   `prefect-server`: ~300–600 MB
*   `dashboard`: `768 MB` (`mem_limit: 768m`, `MALLOC_ARENA_MAX=2`), `cpus: "1.0"` — margem de segurança sobre o RSS base de Streamlit + Pandas + PyArrow (~180–250 MB) sem competir com as 2 vCPUs durante janelas de coleta.
*   **Total da stack:** ~2,8–3,0 GB de RAM, mantendo ~5 GB livres para EasyPanel/Traefik e Debian 13.

### 2. Separação de Módulos e Fronteiras de Responsabilidade

Para manter compatibilidade com `mypy --strict` e testes unitários em `pytest` sem carregar o runtime do Streamlit nos testes de dados, o pacote `src/dashboard/` é dividido em **cinco módulos com responsabilidades isoladas**:

1. **`src/dashboard/config.py` (`DashboardSettings`):** Configuração Pydantic v2 independente de `src/core/config.py` (`hide_input_in_errors=True`). Exige exclusivamente `DASHBOARD_DATABASE_URL` (`SecretStr`, aceitando apenas DSN PostgreSQL síncrono via `psycopg` e rejeitando drivers assíncronos), garantindo que o container `dashboard` nunca receba a `DATABASE_URL` read-write do `scraper-worker`. Expõe ainda `DASHBOARD_CACHE_TTL_SECONDS` e `LOG_LEVEL`.
2. **`src/dashboard/db.py` (Engine Síncrona Read-Only):** Constrói `sqlalchemy.Engine` síncrona com driver `psycopg` (`psycopg[binary]` já presente em `pyproject.toml`), pool enxuto (`pool_size=2, max_overflow=0, pool_timeout=5, pool_recycle=300, pool_pre_ping=True`) e guardrails de sessão fixos (`default_transaction_read_only=on`, `statement_timeout=15000`, `lock_timeout=3000`, `work_mem=16MB`).
3. **`src/dashboard/schemas.py` (DTOs Imutáveis — sem import de Streamlit/Pandas):** Contratos de saída tipados com `@dataclass(frozen=True, slots=True)` (seguindo o padrão de baixa sobrecarga de memória estabelecido no [Design 002](002_discord_notifications.md)) cobrindo servidores ativos (`is_active = TRUE`), frescor e as 5 visões analíticas.
4. **`src/dashboard/queries.py` (Camada Pura de Consultas — sem import de Streamlit/Pandas):** Funções síncronas tipadas recebendo `Session` do SQLAlchemy e parâmetros validados, executando SQL PostgreSQL 16 parametrizado via `sqlalchemy.text()` com *partition pruning* determinístico e retornando listas de DTOs.
5. **`src/dashboard/app.py` (Camada Fina de Renderização Streamlit + Altair):** Mantém a engine singleton via `@st.cache_resource` e wrappers cacheados via `@st.cache_data(ttl=..., max_entries=10)` por função. Renderiza o seletor de servidor ativo na barra lateral, subtítulo discreto de frescor (`Último snapshot: YYYY-MM-DD HH:MM UTC`), botão manual de atualização (`st.cache_data.clear()` + `st.rerun()`) e seletor de visão `st.segmented_control` com carregamento *lazy* exclusivo da visão ativa. Captura `SQLAlchemyError` registrando log JSON em `structlog` (via `configure_logging` de `src/core/logging.py`, sem credenciais) e exibindo aviso controlado na UI.

### 3. Estratégia SQL, *Partition Pruning* e Contrato das 5 Visões

As 5 views criadas em `alembic/versions/002_analytical_views.py` permanecem intactas para compatibilidade com consultas SQL manuais externas. O dashboard adiciona o índice parcial `idx_scrape_runs_server_latest` em `scrape_runs (server_id, created_at DESC) WHERE status IN ('SUCCESS', 'PARTIAL_SUCCESS')` (migração Alembic `003`) e executa consultas otimizadas em `src/dashboard/queries.py` que habilitam *partition pruning* nas 5 consultas analíticas por meio de filtros explícitos em `created_at`:

| Visão | Estratégia de *Partition Pruning* e Índice | Regra de Negócio e Casos de Borda |
| :--- | :--- | :--- |
| **Frescor (Cabeçalho)** | Consulta o último run válido via `idx_scrape_runs_server_latest` (`ORDER BY created_at DESC LIMIT 1`), evitando varredura de todas as partições mensais de `character_snapshots`. | Se não houver run válido, exibe `"Sem snapshots registrados"`. |
| **1. Evolução Diária de `item_points` (Top N)** | Filtra `server_id = :server_id AND created_at >= :window_start AND rank_position <= 100` (usa `idx_char_snapshots_rank (server_id, created_at, rank_position)` + pruning mensal). Deduplica por `(snapshot_day, character_id)` pegando o último snapshot UTC de cada dia. | Janela padrão `30d` (opções `30d`, `90d`, `180d`). Calcula Média, Mediana (P50) e P90 para Top 10, Top 50 e Top 100 + série única do Rank #1 (`rank1_item_points`). Drill-down opcional plota `item_points` e `rank_position` diários do Top 5–10 atual. |
| **2. Dominância de Guildas (Top 100)** | **Execução em 2 passos (plan-time pruning):** passo 1 obtém `(run_id, created_at)` via `idx_scrape_runs_server_latest`; passo 2 consulta `character_snapshots` com escalares bindados `server_id = :server_id AND scrape_run_id = :run_id AND rank_position <= 100 AND created_at BETWEEN :win_start AND :win_end` (±1h). | Agrega `players_in_top_100`, `players_in_top_50` e `total_top_item_points` sobre `rank_position <= 100`. Usa `NULLIF(sample_total_players, 0)` para `sample_share_pct` junto ao percentual nominal sobre 100 vagas (`nominal_top_100_pct`), cobrindo servidores `< 100` (ex.: `play_golden`). Sem guilda rotulado como `[SEM GUILDA]`. |
| **3. Balanço de Raças (`CH` vs `EU`)** | Mesma execução determinística em 2 passos com escalares bindados (`±1h` em `created_at`), fazendo `JOIN` em `characters` e agrupando por `COALESCE(cs.race, c.race, 'UNK')`. | Trata servidores 100% `CH` (0% `EU`) e registros `UNK` com domínio de cores fixo (`CH`, `EU`, `UNK`), sem divisão por zero ou quebra de legenda. |
| **4. Retenção Semanal** | CTE `windowed_snapshots` filtra `server_id = :server_id AND created_at >= :lookback_start` (`:window_start - INTERVAL '7 days'`) **antes** de calcular `LAG(...) OVER (PARTITION BY server_id, character_id ORDER BY created_at)`. `:window_start` é alinhado à segunda-feira 00:00 UTC (`DATE_TRUNC('week', ...)`). | Janela padrão `12 semanas` (opções `12w`, `26w`). Um personagem conta em `active_characters` se qualquer delta semanal for positivo (`delta_level > 0 OR delta_item_points > 0 OR delta_job_points > 0 OR delta_job_exp > 0`); `activity_retention_rate_pct = active_characters * 100.0 / NULLIF(total_tracked_characters, 0)`. Sinaliza visualmente `is_current_week = TRUE` e exibe estado vazio se `< 2` snapshots. |
| **5. Radar Bot vs Whale** | CTE `windowed_snapshots` filtra `server_id = :server_id AND created_at >= :reference_time - INTERVAL '9 days'` (7 dias + *lookback* de 2 dias) **antes** do `LAG(...)`, recortando `created_at >= :reference_time - INTERVAL '7 days'` e `HAVING COUNT(*) >= 6`. | `grind_persistence_ratio = active_grind_intervals / evaluated_intervals` (ativo quando delta de `level`, `job_exp`, `job_points` ou `honor_points` > 0). Segmentos: `>= 0.90` → `SUSPECTED_BOT_24_7`; `max_single_jump_item_points >= 150.0` e ratio `< 0.60` → `SUSPECTED_DONATOR_WHALE`; ratio `0.50..0.89` → `HARDCORE_HUMAN`; demais → `CASUAL_OR_BALANCED`. Estado vazio informativo se `< 7` snapshots em 7 dias. |

### 4. Defesa Read-Only em Duas Camadas e Provisionamento de `sro_readonly`

A proteção contra escrita acidental ou uso indevido da credencial opera no **servidor PostgreSQL 16** e no **cliente SQLAlchemy**:

1. **Camada 1 — Role Dedicada com Guardrails no Banco (`sro_readonly`):**
   *   Provisionada pelo script idempotente `scripts/provision_readonly_role.sh` (carrega `.env` automaticamente se presente e aceita variáveis já exportadas no shell; lê `POSTGRES_USER` — owner das tabelas —, `POSTGRES_DB` e `DASHBOARD_DB_PASSWORD`, sem senhas hardcoded em código ou migrações Alembic).
   *   A senha é passada via variável do `psql` (`-v`) e escapada com `format('... %L', ...)` (proteção contra SQL injection e aspas). Caracteres reservados em URLs (`@`, `#`, `:`, `/`) devem ser URL-encoded em `DASHBOARD_DATABASE_URL`.
   *   Privilégios e limites aplicados na role: `NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT LOGIN CONNECTION LIMIT 5`, `GRANT CONNECT`, `GRANT USAGE ON SCHEMA public`, `GRANT SELECT ON ALL TABLES IN SCHEMA public` (cobrindo automaticamente futuras partições criadas por `ensure_monthly_partitions` em `src/core/database.py`), `ALTER DEFAULT PRIVILEGES FOR ROLE <POSTGRES_USER>`, `REVOKE CREATE ON SCHEMA public`, e `ALTER ROLE sro_readonly SET` para `default_transaction_read_only=on`, `statement_timeout=15000`, `lock_timeout=3000` e `work_mem=16MB`.
2. **Camada 2 — Guardrails de Sessão SQLAlchemy (`connect_args`):**
   *   Reforça os mesmos 4 parâmetros de sessão em toda conexão aberta por `src/dashboard/db.py`.

### 5. Empacotamento Docker, Publicação Traefik/EasyPanel e Hardening Colateral

*   **Isolamento de dependências:** O extra opcional `[dashboard]` no `pyproject.toml` é instalado apenas no build do serviço `dashboard` (via `ARG INSTALL_EXTRAS` no `Dockerfile` compartilhado, onde `curl` já está instalado para o healthcheck). Trade-off consciente: a imagem do `dashboard` herda as dependências base do projeto (`prefect`, `lxml`, etc.) em troca de manter um único `Dockerfile` de manutenção simples, enquanto o `scraper-worker` permanece estritamente livre de `streamlit`, `pandas` e `pyarrow`.
*   **Publicação via Traefik / EasyPanel (VPS Hostinger KVM 2):**
    *   Topologia **dual-network**: o serviço `dashboard` entra em `sro_network` e na rede externa do proxy (configurada via arquivo override de produção na VPS); caso o proxy opere em modo *host network*, publica-se explicitamente `127.0.0.1:8501:8501` — nunca `0.0.0.0:8501`.
    *   O roteamento HTTPS (Let's Encrypt) aplica Basic Auth para 1 usuário + middleware de *rate-limiting* na borda, encaminha `Host`, `X-Forwarded-Proto: https`, `Upgrade` e `Connection` para WebSockets (`/_stcore/stream`) com idle timeout `>= 300s`, e mantém XSRF/CORS do Streamlit compatíveis com terminação TLS no proxy.
*   **Hardening Colateral (`prefect-server`):** A porta `4200` do `prefect-server` passa de exposição pública em `0.0.0.0` para bind exclusivo em `127.0.0.1:4200:4200` (acesso administrativo via túnel SSH ou publicação opcional atrás do Traefik com autenticação).

--------------------------------------------------------------------------------

## Alternatives Considered

### Alternative A: Metabase ou Ferramentas de BI Baseadas em JVM
*   **Pros:** Interface SQL pronta, construtor visual de dashboards sem código Python e gerenciamento embutido de perguntas salvas.
*   **Cons:** Exige runtime JVM e consumo base de RAM frequentemente superior a 1 GB, requer banco de metadados adicional e introduz uma segunda runtime em um repositório baseado exclusivamente em Python.
*   **Reason for Rejection:** O consumo de memória da JVM reduz a folga operacional da VPS Hostinger KVM 2 (8 GB RAM) e aumenta o custo de manutenção em comparação a um serviço Python/Streamlit sob limite de 768 MB.

### Alternative B: Painel Operacional Integrado no Mesmo Dashboard
*   **Pros:** Centralizaria saúde de coletas (`scrape_runs`), estado do Circuit Breaker e download de `./quarantine/*.html.gz` junto às visões de comunidade.
*   **Cons:** Exigiria montar o volume `./quarantine` no container exposto via HTTPS, ampliaria a superfície de acesso sobre payloads brutos e duplicaria funcionalidades já presentes na UI do Prefect.
*   **Reason for Rejection:** Telemetria operacional permanece na UI do Prefect e em consultas SQL/CLI, mantendo o dashboard focado em análise de comunidade e desacoplado da quarentena em disco.

### Alternative C: Container Sob Demanda via Docker Compose `profiles`
*   **Pros:** Zero consumo de RAM quando o dashboard não estiver em uso.
*   **Cons:** Exige acesso SSH para subir e derrubar o container antes e depois de cada consulta, inviabilizando o acesso direto contínuo pelo domínio HTTPS.
*   **Reason for Rejection:** Eliminação da dependência de acesso SSH para consultas recorrentes via HTTPS; o consumo contínuo é controlado por `mem_limit: 768m`, `cpus: "1.0"`, `pool_size=2` e cache limitado.

### Alternative D: Autenticação no App (`streamlit-authenticator`) ou Acesso Exclusivo via SSH Tunnel/VPN
*   **Pros:** `streamlit-authenticator` evitaria middleware no proxy; SSH Tunnel eliminaria qualquer exposição HTTP/HTTPS.
*   **Cons:** `streamlit-authenticator` adiciona dependência Python, gestão de hashes/cookies na aplicação e superfície de bugs de sessão; SSH Tunnel/VPN exige cliente/túnel prévio no dispositivo de acesso.
*   **Reason for Rejection:** A VPS Hostinger KVM 2 já executa Traefik/EasyPanel, permitindo aplicar TLS (Let's Encrypt), Basic Auth e rate-limiting na borda sem adicionar código ou dependências de autenticação ao Python.

### Alternative E: Materialized Views com Refresh Agendado no Prefect
*   **Pros:** Leituras diretas sem agregação em tempo de consulta mesmo com milhões de linhas.
*   **Cons:** Introduz nova task/flow de refresh no Prefect, maior uso de disco no PostgreSQL e defasagem entre o fim do scrape e o refresh da view.
*   **Reason for Rejection:** YAGNI para o escopo atual (até 10 servidores, 6 meses de histórico e 1 usuário); queries com *partition pruning* + cache TTL no Streamlit são projetadas para atender o SLA de `< 5s` sem nova orquestração.

### Alternative F: Biblioteca `plotly` para Visualização
*   **Pros:** Ampla variedade de tipos de gráficos interativos e controles prontos de zoom/exportação.
*   **Cons:** Adiciona uma dependência extra à imagem Docker e maior sobrecarga de serialização JSON/memória para gráficos de séries temporais, barras e dispersão.
*   **Reason for Rejection:** YAGNI; `altair` já vem embutido no Streamlit e cobre todas as 5 visões (séries temporais, barras e dispersão) sem ampliar dependências nem uso de RAM.

### Alternative G: Reusar Engine Assíncrona (`asyncpg`) de `src/core/database.py`
*   **Pros:** Reutilização direta da factory de conexão do `scraper-worker`.
*   **Cons:** O ciclo de execução do Streamlit é síncrono por rerun; encapsular `asyncpg` via `asyncio.run()` causa erros de *event loop* fechado e pool preso entre reruns. Além disso, `src/core/config.py` acoplaria o dashboard à variável read-write `DATABASE_URL`.
*   **Reason for Rejection:** O driver síncrono `psycopg` (`psycopg[binary]`) já está listado em `pyproject.toml` e opera nativamente com SQLAlchemy 2.0 síncrono e `@st.cache_resource` no Streamlit, mantendo `asyncpg` no `scraper-worker` e em `alembic/env.py`.

--------------------------------------------------------------------------------

## Decision Log

| ID | Decisão | Justificativa Resumida |
| :--- | :--- | :--- |
| **ADR-012** | **Streamlit + Altair em container sempre ativo (`dashboard`)** | Mantém stack 100% Python sem JVM, usa biblioteca de gráficos nativa e evita dependência de SSH para acesso HTTPS contínuo. |
| **ADR-013** | **Driver síncrono `psycopg` + `DashboardSettings` isolado em `src/dashboard/`** | Evita conflitos de *event loop* do `asyncpg` no Streamlit e impede que o container `dashboard` receba a `DATABASE_URL` read-write do `scraper-worker` (exceção registrada em `AGENTS.md`). |
| **ADR-014** | **Índice parcial `003` + *plan-time partition pruning* em 2 passos e DTOs `@dataclass(frozen=True, slots=True)`** | Garante pruning determinístico independente do algoritmo de join no PostgreSQL 16, preserva as views de `002` e reduz overhead de RAM seguindo o padrão do Design 002. |
| **ADR-015** | **Defesa read-only em duas camadas (`ALTER ROLE sro_readonly` + `connect_args`)** | Aplica `read_only`, timeouts, `work_mem=16MB` e `CONNECTION LIMIT 5` diretamente na role do banco e reforça na sessão do cliente. |
| **ADR-016** | **Publicação HTTPS via Traefik/EasyPanel em topologia *dual-network* + hardening do `prefect-server`** | Isola `postgres` exclusivamente em `sro_network`, delega TLS/Basic Auth/rate-limit à borda sem porta `8501` pública e restringe `prefect-server` a `127.0.0.1:4200`. |

--------------------------------------------------------------------------------

## Non-Functional Requirements

### 1. Segurança e Privacidade
*   **Menor Privilégio no Banco:** O container `dashboard` recebe exclusivamente `DASHBOARD_DATABASE_URL` (`sro_readonly`, `GRANT SELECT`, `CONNECTION LIMIT 5`), nunca a `DATABASE_URL` read-write.
*   **Defesa em Profundidade:** Limites de somente leitura, timeout e `work_mem` aplicados tanto na role PostgreSQL quanto na sessão SQLAlchemy.
*   **Proteção de Credenciais:** `DASHBOARD_DATABASE_URL` tipada como `SecretStr` com `hide_input_in_errors=True`; exceções e logs JSON nunca registram a URL ou senha.
*   **Isolamento de Rede na VPS:** Sem porta `8501` em `0.0.0.0`, sem porta `5432` no host e `prefect-server` restrito a `127.0.0.1:4200:4200`. TLS, Basic Auth e rate-limiting operam no Traefik/EasyPanel.

### 2. Performance, Volumetria e Recursos
*   **Volumetria Estimada:** Até 10 servidores × 2 scrapes/dia (3 em dias de Fortress War) × ~100–200 personagens × 180 dias ≈ **360k–720k linhas totais** em `character_snapshots` (**~60k–120k linhas por partição mensal**). Com *partition pruning* determinístico e índice parcial em `scrape_runs`, as consultas analíticas operam sobre 1–6 partições de pequeno porte dentro do SLA de `< 5s`.
*   **Carregamento *Lazy*:** `st.segmented_control` executa exclusivamente a query da visão ativa.
*   **Limites de Recursos:** `mem_limit: 768m` (`MALLOC_ARENA_MAX=2`), `cpus: "1.0"`, `pool_size=2, max_overflow=0, pool_timeout=5, pool_recycle=300`, `max_entries=10` por função cacheada (`ttl=600s` configurável) e rotação de logs Docker (`json-file`, `10m` × `3`).

--------------------------------------------------------------------------------

## Risks & Mitigations

| Risco / Modo de Falha | Impacto | Mitigação |
| :--- | :--- | :--- |
| **Falha de WebSocket (`/_stcore/stream`) atrás do Traefik/EasyPanel** | Tela travada em "Connecting..." ou quedas frequentes de sessão. | Configurar encaminhamento explícito de `Upgrade`/`Connection`, `X-Forwarded-Proto: https` e idle timeout `>= 300s` no proxy; validar via checklist de deploy. |
| **Configuração incorreta do Basic Auth no proxy (fail-open) ou força bruta** | Exposição pública não autorizada do dashboard. | Aplicar middleware de Basic Auth obrigatório + *rate-limiting* no Traefik e manter a role `sro_readonly` restrita apenas a dados públicos de ranking (sem credenciais nem quarentena). |
| **Crescimento de RSS do Streamlit/PyArrow próximo ao limite do container** | OOM-kill (`SIGKILL`) do container `dashboard` em consultas de 180 dias. | `mem_limit: 768m`, `MALLOC_ARENA_MAX=2`, DTOs com `@dataclass(frozen=True, slots=True)`, `max_entries=10` por função cacheada e carregamento *lazy*. |
| **Spill-to-disk em `work_mem=16MB` ou contenção durante coletas do `scraper-worker`** | Latência acima de 5s ou cancelamento da query. | *Partition pruning* determinístico + `statement_timeout=15000` + `lock_timeout=3000` + `pool_timeout=5`, com captura de `SQLAlchemyError` exibindo aviso controlado sem afetar o worker. |
| **Escala além do dimensionamento (> 10 servidores ou > 6 meses)** | Degradação gradual de latência nas Visões 1 e 4. | Monitorar latência via `EXPLAIN (ANALYZE, BUFFERS)`; gatilho planejado em `Future Improvements` para introduzir Materialized Views com refresh no Prefect. |
| **Acesso bloqueado à UI do Prefect após restrição de `4200` para loopback** | Operador perde acesso direto por `http://<ip-vps>:4200`. | Documentar acesso administrativo via túnel SSH (`ssh -L 4200:127.0.0.1:4200 ...`) ou rota autenticada no Traefik antes de reiniciar `prefect-server`. |

--------------------------------------------------------------------------------

## Rollout & Rollback

*   **Rollout:**
    1. Aplicar a migração Alembic `003` fora de uma janela ativa de coleta (a criação de `idx_scrape_runs_server_latest` sem `CONCURRENTLY` não bloqueia leituras, mas adquire `SHARE` lock breve sobre escritas em `scrape_runs`, estimado em `< 50 ms` para `< 5k` linhas).
    2. Executar `scripts/provision_readonly_role.sh` na VPS com `DASHBOARD_DB_PASSWORD` definida (garantindo URL-encoding em `DASHBOARD_DATABASE_URL`).
    3. Subir o serviço `dashboard` conectado à rede externa do Traefik/EasyPanel (override de produção), configurar a rota HTTPS + Basic Auth + rate-limiting + WebSockets (`/_stcore/stream`) e aplicar o hardening de `prefect-server` (`127.0.0.1:4200:4200`, validando acesso via túnel SSH).
*   **Rollback:**
    1. Remover a rota no Traefik/EasyPanel e derrubar o container `dashboard`.
    2. Se necessário, reverter a porta do `prefect-server` ou o índice da migração `003` e revogar a role `sro_readonly`. Nenhuma etapa impacta o `scraper-worker` ou os dados históricos.

--------------------------------------------------------------------------------

## Additional Resources

*   [SITEMAP.md](SITEMAP.md) — Índice mestre de designs aprovados.
*   [001_sro_ranking_scraper_pipeline.md](001_sro_ranking_scraper_pipeline.md) — Schema particionado e views analíticas originais.
*   [002_discord_notifications.md](002_discord_notifications.md) — Padrões de `SecretStr`, `hide_input_in_errors`, `@dataclass(frozen=True, slots=True)` e consultas com *partition pruning*.
*   [concepts_brief.md](../egm/003_analytical_dashboard/concepts_brief.md) — Brief de brainstorm original deste design.

## Future Improvements

*   **Materialized Views com refresh no Prefect:** Caso o escopo ultrapasse 10 servidores ou 6 meses de histórico e a latência se aproxime de 5s, introduzir views materializadas atualizadas ao final de `scheduled_12h_flow` (`src/flows/orchestrator.py`).
*   **Comparação Multi-Servidor Lado a Lado:** Permitir selecionar 2–3 servidores simultaneamente para comparar retenção semanal e curva média de `item_points`.
*   **Exportação CSV da Tabela Filtrada:** Botão `st.download_button` sobre os dados tabulares já em memória das visões 2 e 5.
*   **Multi-stage Docker Build Dedicado:** Separar targets `worker` e `dashboard` no `Dockerfile` caso se deseje eliminar `prefect`/`lxml`/`gcc` da imagem final do dashboard.

--------------------------------------------------------------------------------

## Detailed Implementation Plan

O trabalho é dividido em **4 fases incrementais** com checkpoints de validação ao final de cada fase, mantendo o repositório compilável e testável em todos os passos.

### Fase 1: Banco de Dados, ORM e Provisionamento Read-Only

#### 1. `src/models/db_models.py` (Modify)
*   **Type / Scope:** Metadados ORM SQLAlchemy 2.0 (`Base.metadata`).
*   **Proposed Changes:**
    *   Espelhar o novo índice parcial em `ScrapeRun.__table_args__`: `Index("idx_scrape_runs_server_latest", "server_id", text("created_at DESC"), postgresql_where=text("status IN ('SUCCESS', 'PARTIAL_SUCCESS')"))`, mantendo `Base.metadata` sincronizado com as migrações Alembic.

#### 2. `alembic/versions/003_dashboard_indexes.py` (Create)
*   **Type / Scope:** Migração Alembic de índice parcial no PostgreSQL 16.
*   **Proposed Changes:**
    *   Definir `revision = "003_dashboard_indexes"` e `down_revision = "002_analytical_views"`.
    *   Em `upgrade()`: executar `CREATE INDEX IF NOT EXISTS idx_scrape_runs_server_latest ON scrape_runs (server_id, created_at DESC) WHERE status IN ('SUCCESS', 'PARTIAL_SUCCESS')`.
    *   Em `downgrade()`: executar `DROP INDEX IF EXISTS idx_scrape_runs_server_latest`.

#### 3. `scripts/provision_readonly_role.sh` (Create)
*   **Type / Scope:** Script operacional idempotente para provisionamento da role `sro_readonly` na VPS.
*   **Proposed Changes:**
    *   Carregar `.env` de forma não-destrutiva (lendo chaves ausentes do ambiente sem sobrescrever variáveis já exportadas no shell), aplicar defaults `POSTGRES_USER="${POSTGRES_USER:-sro_user}"` e `POSTGRES_DB="${POSTGRES_DB:-sro_rankings}"`, e abortar com código de saída diferente de zero caso `DASHBOARD_DB_PASSWORD` esteja ausente ou vazia (nunca imprimir a senha).
    *   Invocar `docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -v readonly_pass="$DASHBOARD_DB_PASSWORD" -v db_name="$POSTGRES_DB" -v owner_role="$POSTGRES_USER"` usando `SELECT format(...) \gexec` (evitando interpolação de `psql -v` em blocos `DO $$`) para:
        *   Criar ou atualizar `sro_readonly` com `NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT LOGIN CONNECTION LIMIT 5` e senha escapada com `%L`.
        *   Executar `GRANT CONNECT ON DATABASE`, `GRANT USAGE ON SCHEMA public`, `GRANT SELECT ON ALL TABLES IN SCHEMA public`, `ALTER DEFAULT PRIVILEGES FOR ROLE <owner_role> IN SCHEMA public GRANT SELECT ON TABLES TO sro_readonly` e `REVOKE CREATE ON SCHEMA public FROM sro_readonly`.
        *   Executar **quatro instruções independentes**: `ALTER ROLE sro_readonly SET default_transaction_read_only = on;`, `ALTER ROLE sro_readonly SET statement_timeout = '15000';`, `ALTER ROLE sro_readonly SET lock_timeout = '3000';` e `ALTER ROLE sro_readonly SET work_mem = '16MB';`.

#### 4. `.env.example` (Modify)
*   **Type / Scope:** Modelo de variáveis de ambiente.
*   **Proposed Changes:**
    *   Documentar `DASHBOARD_DB_PASSWORD`, `DASHBOARD_DATABASE_URL` (`postgresql+psycopg://sro_readonly:CHANGE_ME_READONLY_PASSWORD@postgres:5432/sro_rankings`, com nota sobre URL-encoding de caracteres especiais) e `DASHBOARD_CACHE_TTL_SECONDS=600`.

*   **Checkpoint da Fase 1:** `uv run --no-sync ruff check src/models/db_models.py alembic/versions/003_dashboard_indexes.py && uv run --no-sync mypy src tests && uv run --no-sync pytest --cov=src tests/`

---

### Fase 2: Empacotamento, Camada Pura de Dados (`src/dashboard/`) e Testes Unitários

#### 5. `pyproject.toml` (Modify)
*   **Type / Scope:** Empacotamento Python, dependências opcionais, marcadores `pytest` e configuração `mypy`.
*   **Proposed Changes:**
    *   Adicionar `[project.optional-dependencies]` com `dashboard = ["streamlit>=1.40.0,<2.0.0", "altair>=5.0.0,<6.0.0"]`, mantendo a chave `dependencies` em `[project]` inalterada para que o `scraper-worker` não instale `streamlit`, `pandas` ou `pyarrow`.
    *   Adicionar dois blocos `[[tool.mypy.overrides]]`:
        *   `module = ["streamlit", "streamlit.*", "altair", "altair.*"]` com `ignore_missing_imports = true`.
        *   `module = ["src.dashboard.app"]` com `disallow_untyped_decorators = false` (necessário para `@st.cache_resource` e `@st.cache_data` quando o extra `[dashboard]` não estiver instalado no `.venv`).
    *   Registrar marcador `postgres` em `[tool.pytest.ini_options]` (`markers = ["postgres: integration tests requiring a live PostgreSQL 16 instance"]`).

#### 6. `src/dashboard/__init__.py` (Create)
*   **Type / Scope:** Inicialização do pacote `src.dashboard`.
*   **Proposed Changes:**
    *   Módulo marcador contendo apenas docstring (`"""Read-only analytical dashboard package."""`) e sem imports de UI.

#### 7. `src/dashboard/config.py` (Create)
*   **Type / Scope:** Configuração Pydantic v2 isolada para o dashboard.
*   **Proposed Changes:**
    *   Definir `DashboardSettings(BaseSettings)` com `SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore", hide_input_in_errors=True)`.
    *   Campos: `dashboard_database_url: SecretStr`, `dashboard_cache_ttl_seconds: int = Field(default=600, ge=30, le=3600)` e `log_level: str = "INFO"`.
    *   Validador de `dashboard_database_url`: rejeitar string vazia/espaços, normalizar `postgres://` e `postgresql://` para `postgresql+psycopg://`, aceitar `postgresql+psycopg://`, rejeitar esquemas assíncronos (`postgresql+asyncpg://`) ou não-PostgreSQL (`sqlite://`) e validar presença de host.
    *   Função `get_dashboard_settings() -> DashboardSettings`: instanciar `DashboardSettings()` diretamente (sem `@lru_cache`, mantendo paridade com `src/core/config.py` para testes com `monkeypatch`).

#### 8. `src/dashboard/db.py` (Create)
*   **Type / Scope:** Factory síncrona SQLAlchemy 2.0 com guardrails read-only.
*   **Proposed Changes:**
    *   Definir constantes de guardrail: `STATEMENT_TIMEOUT_MS = 15000`, `LOCK_TIMEOUT_MS = 3000`, `WORK_MEM_MB = 16`, `POOL_SIZE = 2`, `POOL_TIMEOUT_SECONDS = 5`, `POOL_RECYCLE_SECONDS = 300`.
    *   `build_postgres_options() -> str`: montar `"-c default_transaction_read_only=on -c statement_timeout=15000 -c lock_timeout=3000 -c work_mem=16MB"`.
    *   `create_dashboard_engine(settings: DashboardSettings) -> Engine`: extrair URL via `get_secret_value()`, criar engine síncrona com `pool_size=2`, `max_overflow=0`, `pool_timeout=5`, `pool_recycle=300`, `pool_pre_ping=True` e `connect_args={"options": build_postgres_options()}`.
    *   `create_dashboard_session_factory(engine: Engine) -> sessionmaker[Session]`: retornar factory síncrona com `expire_on_commit=False`.

#### 9. `src/dashboard/schemas.py` (Create)
*   **Type / Scope:** Contratos de dados imutáveis (stdlib `dataclass`, sem dependência de UI).
*   **Proposed Changes:**
    *   Definir DTOs com `@dataclass(frozen=True, slots=True)` e tipagem estrita (`mypy --strict`):
        *   `ServerOption(id: str, display_name: str, min_records_expected: int)`
        *   `ServerFreshness(server_id: str, last_snapshot_at: datetime | None)`
        *   `TopNAggregatePoint(snapshot_day: date, tier: Literal["Top 10", "Top 50", "Top 100"], avg_item_points: float, median_item_points: float, p90_item_points: float, rank1_item_points: float, sample_size: int)`
        *   `TopCharacterTrendPoint(snapshot_day: date, char_name: str, rank_position: int, item_points: float)`
        *   `GuildDominanceRow(guild_name: str, players_in_top_100: int, players_in_top_50: int, total_top_item_points: float, nominal_top_100_pct: float, sample_share_pct: float, sample_total_players: int)`
        *   `RaceBalanceRow(race: Literal["CH", "EU", "UNK"], total_ranked_characters: int, top_100_count: int, avg_level: float, avg_item_points: float, max_item_points: float)`
        *   `WeeklyRetentionRow(week_start: date, total_tracked_characters: int, active_characters: int, activity_retention_rate_pct: float, is_current_week: bool)`
        *   `BehaviorClassificationRow(char_name: str, race: Literal["CH", "EU", "UNK"], evaluated_intervals: int, total_item_points_gain: float, max_single_jump_item_points: float, grind_persistence_ratio: float, player_behavior_segment: Literal["SUSPECTED_BOT_24_7", "SUSPECTED_DONATOR_WHALE", "HARDCORE_HUMAN", "CASUAL_OR_BALANCED"])`

#### 10. `src/dashboard/queries.py` (Create)
*   **Type / Scope:** Camada pura de consultas SQL para PostgreSQL 16 com *partition pruning* (sem dependência de UI).
*   **Proposed Changes:**
    *   Exportar constantes SQL parametrizadas (`sqlalchemy.text`), incluir comentário referenciando `alembic/versions/002_analytical_views.py` como contrato espelhado nos limiares de classificação, e implementar 8 funções síncronas tipadas:
        *   `list_active_servers(session: Session) -> list[ServerOption]`: `WHERE is_active = TRUE ORDER BY display_name ASC`.
        *   `get_server_freshness(session: Session, server_id: str) -> ServerFreshness`: consulta `idx_scrape_runs_server_latest` (`ORDER BY created_at DESC LIMIT 1`).
        *   `get_top_n_item_points_evolution(session: Session, server_id: str, window_days: int = 30, reference_time: datetime | None = None) -> list[TopNAggregatePoint]`: filtra `created_at >= :window_start AND rank_position <= 100`, deduplica o último snapshot UTC por `(DATE_TRUNC('day', created_at, 'UTC')::date, character_id)`, calcula `AVG`, `PERCENTILE_CONT(0.5)`, `PERCENTILE_CONT(0.9)` por faixa (Top 10/50/100) e `MAX(item_points) FILTER (WHERE rank_position = 1)` (fallback `MAX(item_points)`) para `rank1_item_points`, ordenado por `snapshot_day ASC, tier ASC`.
        *   `get_top_characters_drilldown(session: Session, server_id: str, window_days: int = 30, top_n: int = 5, reference_time: datetime | None = None) -> list[TopCharacterTrendPoint]`: passo 1 obtém `(run_id, created_at)` via `idx_scrape_runs_server_latest` (retorna `[]` se ausente); passo 2 seleciona os `character_id` com `rank_position <= :top_n` na janela `±1h` (`:win_start`, `:win_end`) desse run e busca a série diária deduplicada em UTC (`DATE_TRUNC('day', created_at, 'UTC')::date`) desses personagens com `created_at >= :window_start`, ordenada por `snapshot_day ASC, rank_position ASC`.
        *   `get_guild_dominance(session: Session, server_id: str) -> list[GuildDominanceRow]`: 2 passos com escalares `:run_id`, `:win_start`, `:win_end`, filtro `rank_position <= 100`, `COALESCE(g.guild_name, '[SEM GUILDA]')`, `NULLIF(sample_total_players, 0)`, ordenado por `players_in_top_100 DESC, total_top_item_points DESC`.
        *   `get_race_balance(session: Session, server_id: str) -> list[RaceBalanceRow]`: 2 passos reutilizando apenas os escalares `:run_id`, `:win_start`, `:win_end` (sem filtro de `rank_position <= 100`, para preservar `total_ranked_characters` junto a `top_100_count`), `COALESCE(cs.race, c.race, 'UNK')`, ordenado por `total_ranked_characters DESC`.
        *   `get_weekly_retention(session: Session, server_id: str, window_weeks: int = 12, reference_time: datetime | None = None) -> list[WeeklyRetentionRow]`: calcula `current_week_start` em UTC (`DATE_TRUNC('week', ..., 'UTC')`), define `window_start = current_week_start - timedelta(weeks=window_weeks - 1)` (incluindo a semana corrente), filtra `:lookback_start = window_start - timedelta(days=7)` antes do `LAG()`, marca `is_current_week = (week_start == current_week_start)` e ordena por `week_start ASC`.
        *   `get_bot_vs_donor_classification(session: Session, server_id: str, reference_time: datetime | None = None) -> list[BehaviorClassificationRow]`: filtra `:lookback_start = reference_time - timedelta(days=9)` antes do `LAG()`, recorta `created_at >= :window_start` (`reference_time - 7 days`), aplica `HAVING COUNT(*) >= 6` e ordena por `grind_persistence_ratio DESC, max_single_jump_item_points DESC`.
    *   Validar `window_days` (`1 <= window_days <= 185`), `window_weeks` (`2 <= window_weeks <= 26`) e `top_n` (`1 <= top_n <= 10`), lançando `ValueError` em valores fora da faixa antes de executar SQL.

#### 11. `tests/test_dashboard_config.py` (Create)
*   **Type / Scope:** Testes unitários de configuração e engine read-only.
*   **Proposed Changes:**
    *   Validar normalização (`postgres://`, `postgresql://`, `postgresql+psycopg://`), rejeição de `postgresql+asyncpg://`, `sqlite://` e strings vazias, mascaramento `SecretStr` + ausência de credenciais em `ValidationError`.
    *   Validar `build_postgres_options()` e `create_dashboard_engine()` verificando `pool_size=2`, `max_overflow=0`, `pool_timeout=5`, `pool_recycle=300` e `connect_args["options"]`.

#### 12. `tests/test_dashboard_queries.py` (Create)
*   **Type / Scope:** Testes unitários da camada de queries/DTOs + suíte opt-in `@pytest.mark.postgres`.
*   **Proposed Changes:**
    *   Validar contratos SQL de *partition pruning* nas constantes de `src/dashboard/queries.py` (`created_at >= :lookback_start` antes de `LAG()`, parâmetros escalares nas Visões 2 e 3, `rank_position <= 100` na Visão 2, ausência desse filtro na Visão 3, `is_active = TRUE` e uso exclusivo de bind parameters).
    *   Validar mapeamento dos `@dataclass`, `ValueError` em parâmetros inválidos e casos de borda com `Session` mockada:
        *   Visão 1: deduplicação diária, faixas Top 10/50/100, `rank1_item_points` e drill-down vazio quando sem run válido.
        *   Visão 2: servidor `< 100` jogadores (ex.: `play_golden`) + `[SEM GUILDA]` + `NULLIF(sample_total_players, 0)`.
        *   Visão 3: servidor 100% `CH` (0% `EU`) e registros `UNK`.
        *   Visão 4: alinhamento ISO (`window_weeks - 1`), `is_current_week=True` e retorno vazio para `< 2` snapshots.
        *   Visão 5: retorno vazio (`0` linhas) e os 4 segmentos comportamentais.
        *   Frescor: `last_snapshot_at=None`.
    *   Suíte `@pytest.mark.postgres`: fixture lê `TEST_POSTGRES_URL` e executa `pytest.skip("TEST_POSTGRES_URL not set")` automaticamente quando ausente (mantendo `uv run --no-sync pytest --cov=src tests/` funcional offline); quando configurada, popula dados multi-mês/multi-semana em transação isolada com `rollback()` ao final, executa as 8 queries reais e assere sobre `EXPLAIN (FORMAT JSON)` que nenhuma partição mensal fora da janela temporal aparece nos nós do plano.

*   **Checkpoint da Fase 2:** `uv run --no-sync ruff check . && uv run --no-sync ruff format --check . && uv run --no-sync mypy src tests && uv run --no-sync pytest --cov=src tests/`

---

### Fase 3: Interface Streamlit (`app.py`) e Empacotamento Docker

#### 13. `src/dashboard/app.py` (Create)
*   **Type / Scope:** Aplicação Streamlit single-page + renderização `altair` (sem importar `pandas`).
*   **Proposed Changes:**
    *   Inicializar `get_dashboard_settings()`, `configure_logging` (`src/core/logging.py`), engine/sessionmaker (`@st.cache_resource`) e wrappers `@st.cache_data(ttl=..., max_entries=10)` de forma *lazy* dentro de `main()` (zero side-effects no `import`).
    *   Não importar `pandas`: passar listas de dicionários (`[asdict(row) for row in rows]`) diretamente para `alt.Data(values=...)` e `st.dataframe(...)`.
    *   Renderizar na barra lateral: seletor de servidor ativo (se `list_active_servers()` retornar `[]`, exibir `st.info` e encerrar o fluxo), legenda de frescor via `st.caption("Último snapshot: YYYY-MM-DD HH:MM UTC")` (ou `"Sem snapshots registrados"`) e botão **"Atualizar dados"** (`st.cache_data.clear()` + `st.rerun()`).
    *   Renderizar `st.segmented_control` com as 5 visões; se o usuário desmarcar a opção ativa (retorno `None` no Streamlit 1.40+), aplicar fallback automático para a Visão 1.
    *   Implementar os 5 renderizadores `altair`:
        1. **Visão 1:** seletor `30d`/`90d`/`180d`, linhas de Média/P50/P90 por faixa + linha do teto Rank #1; toggle opcional de drill-down (Top 5–10) plotando `item_points` e `rank_position` (eixo Y invertido: `alt.Scale(reverse=True)`).
        2. **Visão 2:** barras horizontais por guilda exibindo contagem absoluta (`players_in_top_100` / `sample_total_players`), percentual sobre amostra e nominal sobre 100 vagas, destacando `[SEM GUILDA]` + tabela auxiliar.
        3. **Visão 3:** barras/cards com domínio de cores fixo `alt.Scale(domain=["CH", "EU", "UNK"], range=...)` cobrindo 100% `CH` e `UNK`.
        4. **Visão 4:** seletor `12w`/`26w`, gráfico de barras/linha semanal diferenciando visualmente `is_current_week = True` (opacidade/anotação `"Semana em andamento"`).
        5. **Visão 5:** gráfico de dispersão (`grind_persistence_ratio` × `max_single_jump_item_points`, cor por segmento, tooltip com `char_name`, `race`, `total_item_points_gain`, `evaluated_intervals`) + `st.multiselect` por segmento + tabela filtrada.
    *   Tratar explicitamente os 4 estados vazios (`st.info`): servidor sem snapshots, `< 2` snapshots semanais na Visão 4, `0` registros na Visão 5 (`< 7` snapshots em 7 dias) e falha de consulta.
    *   Capturar `SQLAlchemyError` em cada visão, registrar evento JSON via `structlog` (`server_id`, `view_name`, `error_type`, sem credenciais) e exibir mensagem genérica de indisponibilidade via `st.warning` (sem detalhes técnicos ou credenciais).

#### 14. `Dockerfile` (Modify)
*   **Type / Scope:** Imagem Docker compartilhada entre `scraper-worker` e `dashboard`.
*   **Proposed Changes:**
    *   Declarar `ARG INSTALL_EXTRAS=""` antes da instalação do pacote e atualizar `RUN pip install --no-cache-dir .` para `RUN pip install --no-cache-dir ".${INSTALL_EXTRAS}"`.
    *   Preservar `curl` (já presente no `apt-get install`, usado pelo healthcheck do dashboard) e o `CMD` padrão do `scraper-worker`; o serviço `dashboard` sobrescreve o `command` no `docker-compose.yml` e passa `INSTALL_EXTRAS: "[dashboard]"` em `build.args`.

#### 15. `docker-compose.yml` (Modify)
*   **Type / Scope:** Orquestração de containers e hardening de rede na VPS Hostinger KVM 2.
*   **Proposed Changes:**
    *   Adicionar serviço `dashboard`:
        *   `build`: `context: .`, `dockerfile: Dockerfile`, `args: { INSTALL_EXTRAS: "[dashboard]" }`.
        *   Limites: `mem_limit: 768m`, `cpus: "1.0"`, `restart: unless-stopped`.
        *   `depends_on`: `postgres` (`condition: service_healthy`).
        *   `command`: `["streamlit", "run", "src/dashboard/app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true", "--browser.gatherUsageStats=false"]`.
        *   `environment`: `DASHBOARD_DATABASE_URL: ${DASHBOARD_DATABASE_URL:-}`, `DASHBOARD_CACHE_TTL_SECONDS: ${DASHBOARD_CACHE_TTL_SECONDS:-600}`, `LOG_LEVEL: ${LOG_LEVEL:-INFO}`, `MALLOC_ARENA_MAX: "2"` (sem `DATABASE_URL`; usar `:-` em vez de `:?` para não bloquear `docker compose exec postgres` durante o rollout inicial).
        *   `networks`: `[sro_network]`, sem portas publicadas em `0.0.0.0` (omitir `ports:`; o exemplo de override de VPS para anexar a rede externa do Traefik/EasyPanel é documentado no `README.md`, não versionado como arquivo no repositório).
        *   `healthcheck`: `["CMD-SHELL", "curl -fsS http://127.0.0.1:8501/_stcore/health || exit 1"]` (`interval: 30s`, `timeout: 5s`, `retries: 3`, `start_period: 20s`).
        *   `logging`: `driver: json-file`, `options: { max-size: "10m", max-file: "3" }`.
    *   Hardening colateral em `prefect-server`: alterar `ports` de `"4200:4200"` para `"127.0.0.1:4200:4200"`.

*   **Checkpoint da Fase 3:** `uv run --no-sync ruff check . && uv run --no-sync ruff format --check . && uv run --no-sync mypy src tests && uv run --no-sync pytest --cov=src tests/`

---

### Fase 4: Documentação e Governança

#### 16. Documentação do Repositório
*   **`AGENTS.md` (Modify):** Registrar na Seção 2 que `psycopg` síncrono (`postgresql+psycopg://`, reutilizando `psycopg[binary]` já presente em `pyproject.toml`) é permitido especificamente para `src/dashboard/` (mantendo `asyncpg` no `scraper-worker` e em `alembic/env.py`); incluir `src/dashboard/` e `scripts/` na árvore do repositório.
*   **`README.md` (Modify):** Documentar variáveis `DASHBOARD_*`, execução de `scripts/provision_readonly_role.sh`, exemplo de override Docker Compose para topologia *dual-network* no Traefik/EasyPanel (HTTPS, Basic Auth, rate-limiting e WebSockets `/_stcore/stream`), e acesso ao `prefect-server` via túnel SSH (`ssh -L 4200:127.0.0.1:4200 ...`).
*   **`src/README.md` (Modify):** Registrar o subpacote `./dashboard/README.md`.
*   **`src/dashboard/README.md` (Create):** Documentar responsabilidade, invariantes read-only e fronteiras de `config.py`, `db.py`, `schemas.py`, `queries.py` e `app.py`.
*   **`alembic/README.md` (Modify) e `alembic/versions/README.md` (Modify):** Atualizar a cadeia linear de migrações incluindo `003_dashboard_indexes.py`.

---

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
*   **Unit Tests & Coverage (Pytest — canonical offline gate):**
    ```bash
    uv run --no-sync pytest --cov=src tests/
    ```
    *   Os testes marcados `@pytest.mark.postgres` são pulados automaticamente quando `TEST_POSTGRES_URL` não estiver definida.
*   **PostgreSQL 16 Integration & Partition Pruning Tests (pre-deploy, against Compose):**
    ```bash
    TEST_POSTGRES_URL="postgresql+psycopg://..." uv run --no-sync pytest -m postgres tests/test_dashboard_queries.py
    ```
*   **Database Migration Verification (Alembic):**
    ```bash
    uv run --no-sync alembic upgrade head
    uv run --no-sync alembic downgrade 002_analytical_views && uv run --no-sync alembic upgrade head
    ```
    *   Confirmar existência e reversibilidade do índice `idx_scrape_runs_server_latest` em `scrape_runs`.
*   **Container Build, Isolation & VPS Integration Verification:**
    *   Confirmar ausência de `streamlit`, `pandas` e `pyarrow` no `scraper-worker`:
        ```bash
        docker compose run --rm --no-deps scraper-worker python -c "import importlib.util; assert all(importlib.util.find_spec(m) is None for m in ('streamlit', 'pandas', 'pyarrow'))"
        ```
    *   Confirmar status `healthy` do container `dashboard` (executando `curl -fsS http://127.0.0.1:8501/_stcore/health` dentro do container), ausência de publicação da porta `8501` em `0.0.0.0` e `prefect-server` restrito a `127.0.0.1:4200` (`docker compose ps`).
    *   Confirmar negação de `INSERT`/`UPDATE`/`DELETE` e os 4 limites de sessão (`SHOW default_transaction_read_only; SHOW statement_timeout; SHOW lock_timeout; SHOW work_mem;`) conectando como `sro_readonly`.
    *   Validar acesso via domínio HTTPS no Traefik/EasyPanel: Basic Auth + rate-limiting ativos, conexão WebSocket `/_stcore/stream` respondendo `HTTP 101` sem reconexões ao alternar entre as 5 visões, e carregamento de **cada visão** em `< 5s`.

--------------------------------------------------------------------------------

*Drafted with EGM*
