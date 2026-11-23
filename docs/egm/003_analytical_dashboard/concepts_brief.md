# 003 — Dashboard Analítico Read-Only (Streamlit)

* **Author:** richardman13
* **Date:** 2026-11-22
* **Status:** Brainstorm / RFC Brief

---

## Overview

Este documento consolida o brainstorming arquitetural para adicionar uma camada de consumo visual read-only ao `sro-ranking-scraper` usando **Streamlit**, voltada exclusivamente para análise de comunidade por **um único usuário**, rodando integralmente na **VPS Hostinger KVM 2** (Debian 13, 8 GB RAM, 2 vCPUs, ~100 GB disco) atrás de **domínio HTTPS via proxy reverso Traefik/EasyPanel**.

Hoje, as 5 views analíticas no PostgreSQL 16 (`v_character_activity_deltas`, `v_bot_vs_donor_classification`, `v_guild_turnover_dominance`, `v_race_balance_ch_vs_eu`, `v_server_retention_weekly`) exigem consultas SQL manuais para inspeção. O dashboard elimina esse atrito oferecendo 5 visões interativas para até **10 servidores** com histórico médio de **6 meses**, rodando em um container próprio sempre ativo no `docker-compose.yml` (sem exposição direta de porta pública), com dependências isoladas do `scraper-worker` e conexão estritamente `READ ONLY` ao PostgreSQL.

Telemetria operacional (`scrape_runs`, estado do Circuit Breaker e histórico de quarentenas em disco) fica explicitamente fora deste dashboard e permanece sob responsabilidade do Prefect UI e de consultas pontuais via SQL/CLI.

---

## Analysis

A revisão multi-lente (Goldfish) sobre o código atual (`docker-compose.yml`, `Dockerfile`, `pyproject.toml`, `alembic/versions/001_initial_schema.py` e `002_analytical_views.py`), somada às restrições da **VPS Hostinger KVM 2** (8 GB RAM / 2 vCPUs / Debian 13 + Traefik/EasyPanel), identificou seis riscos concretos e suas respectivas soluções.

### 1. Falha de *Partition Pruning* nas Views Existentes e Contenção de RAM no PostgreSQL (Risco Alto)
* **Problema no código atual:**
  * `v_character_activity_deltas` executa `LAG(...) OVER (PARTITION BY cs.server_id, cs.character_id ORDER BY cs.created_at)` sem filtro temporal interno. Como o otimizador do PostgreSQL não consegue empurrar predicados de data (`WHERE created_at >= ...`) através de uma função de janela `LAG()`, consultar `v_bot_vs_donor_classification` ou `v_server_retention_weekly` força a leitura e ordenação de **todas as partições mensais** do servidor nos 6 meses.
  * `v_guild_turnover_dominance` e `v_race_balance_ch_vs_eu` consultam `scrape_runs` sem índice em `(server_id, created_at DESC)` e fazem `JOIN` com `character_snapshots` apenas por `scrape_run_id` (sem `created_at`), impedindo *partition pruning*.
  * O container `postgres` tem `mem_limit: 1g`, `shared_buffers=512MB` e `work_mem=64MB`. Consultas concorrentes com ordenação/janela podem consumir múltiplos buffers de 64MB e acionar o OOM killer, derrubando coletas do `scraper-worker`.
* **Solução proposta:**
  * Na camada de queries do dashboard, usar CTEs com corte temporal explícito (`created_at >= :window_start - INTERVAL '2 days'`) **antes** de calcular `LAG()`, preservando o cálculo exato de deltas na borda e restaurando 100% do *partition pruning*.
  * Adicionar índice parcial em `scrape_runs (server_id, created_at DESC) WHERE status IN ('SUCCESS', 'PARTIAL_SUCCESS')` (via migração Alembic `003`) e incluir `server_id` + janela de `created_at` no `JOIN` do último snapshot.
  * Aplicar guardrails obrigatórios na sessão SQLAlchemy do dashboard: `default_transaction_read_only=on`, `statement_timeout=15000` (15s), `lock_timeout=3000` (3s) e `work_mem=16MB`, com pool limitado a `pool_size=2, max_overflow=0, pool_recycle=300`.

### 2. Execução *Eager* de `st.tabs` e Granularidade da Visão 1
* **Problema:** No Streamlit, `st.tabs` executa o código Python de **todas as 5 abas** a cada rerun ou troca de servidor, disparando as 5 consultas SQL simultaneamente e comprometendo a meta de `< 5s`. Além disso, plotar 100 linhas individuais de personagens ao longo de 6 meses gera gráficos ilegíveis e lentos no navegador.
* **Solução proposta:**
  * Usar `st.segmented_control` (ou `st.radio(..., horizontal=True)`) como seletor de visão para que **apenas a visão ativa** execute sua consulta SQL.
  * Na Visão 1 (evolução agregada de `item_points`/rank do Top N), agregar diariamente (`DATE_TRUNC('day', created_at)`) curvas de média/mediana/máximo para faixas configuráveis (ex.: Top 10, Top 50, Top 100) na janela padrão de 30 dias (expansível até 6 meses), com opção secundária de inspecionar individualmente o Top 5–10.
  * Exibir apenas um subtítulo discreto de frescor (`Último snapshot: YYYY-MM-DD HH:MM UTC`) obtido via `MAX(created_at)` em `character_snapshots`, sem importar dados operacionais de `scrape_runs`.

### 3. Isolamento de Dependências, Driver Síncrono e `mypy --strict`
* **Problema:** O `Dockerfile` atual instala o projeto via `pip install --no-cache-dir .` (não `uv`), portanto `[dependency-groups]` (PEP 735) não é resolvido por `pip`. Além disso, misturar código Streamlit na mesma camada das queries dificulta manter `mypy --strict` e testes unitários em `pytest`.
* **Solução proposta:**
  * Declarar um extra opcional `[project.optional-dependencies] dashboard = ["streamlit>=1.40.0", ...]` no `pyproject.toml`, mantendo `scraper-worker` livre de `streamlit`/`pandas`/`pyarrow` (preservando o teto de 512MB de RAM) e limitando o container `dashboard` a `mem_limit: 512m` e `cpus: "1.0"` (evitando competição excessiva nas 2 vCPUs da VPS durante coletas).
  * Executar o Streamlit em modo headless/remoto (`--server.address=0.0.0.0`, `--server.port=8501`, `--server.headless=true`, `--browser.gatherUsageStats=false`), com `healthcheck` em `http://127.0.0.1:8501/_stcore/health` e rotação de logs Docker (`json-file`, `max-size` / `max-file`) para operação 24/7 nos 100 GB de disco da VPS.
  * Usar o driver síncrono `psycopg` (`postgresql+psycopg://`), que **já está presente** em `pyproject.toml` (usado pelo Alembic), evitando conflitos de event loop do `asyncpg` nos reruns síncronos do Streamlit.
  * Separar o código em `src/dashboard/queries.py` + `src/dashboard/schemas.py` (sem import de `streamlit`, tipagem estrita `mypy --strict`, testável com `pytest`) e `src/dashboard/app.py` (camada fina de renderização com `@st.cache_data(ttl=600, max_entries=30)` e botão manual de atualização).

### 4. Provisionamento Obrigatório da Role `READ ONLY` na VPS
* **Problema:** Scripts em `/docker-entrypoint-initdb.d/` não executam sobre o volume `postgres_data` já existente, e `AGENTS.md` proíbe senhas hardcoded em migrações Alembic. Em uma VPS pública na internet, depender apenas de `default_transaction_read_only=on` com o superusuário/owner do banco reduz a defesa em profundidade.
* **Solução proposta (defesa em duas camadas obrigatórias na VPS):**
  1. **Menor privilégio no banco (padrão obrigatório na VPS):** Variável `DASHBOARD_DATABASE_URL` apontando para uma role dedicada `sro_readonly` com `GRANT SELECT` nas tabelas/views (no PostgreSQL, `GRANT SELECT` na tabela pai `character_snapshots` cobre automaticamente futuras partições mensais), provisionada por comando/script idempotente lendo a senha de variável de ambiente. O serviço `postgres` permanece sem porta `5432` publicada no host.
  2. **Guardrail de sessão:** `default_transaction_read_only=on`, `statement_timeout=15000`, `lock_timeout=3000` e `work_mem=16MB` via `connect_args` em toda conexão do dashboard.

### 5. Casos de Borda de Domínio nas Views 2–5
* **Visão 2 (`v_guild_turnover_dominance`):** A view divide por `/ 100` fixo; em servidores com menos de 100 registros (ex.: `play_golden`, `min_records_expected = 50`), a soma de percentuais fica em 50%. O dashboard deve exibir a contagem absoluta de jogadores ranqueados junto ao percentual e rotular `[SEM GUILDA]` com clareza.
* **Visão 3 (`v_race_balance_ch_vs_eu`):** Tratar servidores 100% CH (0% EU) e registros `UNK` sem erro de divisão ou quebra de legenda.
* **Visão 4 (`v_server_retention_weekly`):** Sinalizar visualmente a semana corrente incompleta (para não confundir queda parcial com perda real de retenção) e exibir estado vazio informativo para servidores recém-criados (< 2 snapshots).
* **Visão 5 (`v_bot_vs_donor_classification`):** Como exige `HAVING COUNT(*) >= 6` em 7 dias, servidores em cooldown ou com poucos scrapes retornam 0 linhas; exibir estado vazio explicando o requisito mínimo de 7 snapshots. Apresentar como gráfico de dispersão (`grind_persistence_ratio` × `max_single_jump_item_points`) + tabela filtrável.

### 6. Exposição Segura na VPS Hostinger via Traefik / EasyPanel e Orçamento de Recursos
* **Problema:** Publicar `ports: - "8501:8501"` no Docker expõe a porta em `0.0.0.0` (contornando o UFW via `iptables`) sem TLS nem autenticação. Além disso, o Streamlit depende de **WebSockets** (`/_stcore/stream`), que quebram atrás de proxies reversos mal configurados (sem headers `Upgrade`/`Connection` ou com timeouts curtos). Por fim, o `docker-compose.yml` atual publica `prefect-server` em `"4200:4200"` aberto em `0.0.0.0` na VPS.
* **Solução proposta:**
  * **Zero porta pública direta:** O container `dashboard` não publica a porta `8501` em `0.0.0.0`; o tráfego entra exclusivamente via **Traefik/EasyPanel** (rede interna Docker ou bind `127.0.0.1:8501:8501`).
  * **TLS + Autenticação na borda:** Terminação HTTPS (Let's Encrypt) e autenticação para usuário único aplicadas no **Traefik/EasyPanel** (ex.: Basic Auth middleware), mantendo o código Streamlit livre de lógica customizada de login.
  * **Compatibilidade WebSocket/Proxy:** Configurar o roteamento no Traefik/EasyPanel com suporte a WebSockets (`/_stcore/stream`) e ajustar flags do Streamlit compatíveis com proxy reverso HTTPS (XSRF/CORS).
  * **Orçamento consolidado na KVM 2 (8 GB RAM / 2 vCPUs / 100 GB disco):** `postgres` (`1g`) + `scraper-worker` (`512m`) + `prefect-server` (~300–600MB) + `dashboard` (`512m`) somam ~2,5–2,8 GB, operando com boa folga nos 8 GB de RAM da VPS mesmo junto ao EasyPanel/Traefik.
  * **Hardening colateral:** Restringir `prefect-server` (`4200`) para `127.0.0.1:4200:4200` ou protegê-lo sob o mesmo Traefik/EasyPanel com autenticação.

### Contradições Resolvidas & Ideal Final Result (IFR)
* **Contradição 1 (Deltas históricos vs. PostgreSQL de 1GB e < 5s):** Resolvida com *lookback buffer* de 2 dias antes do `LAG()` para restaurar *partition pruning*, `work_mem=16MB` na sessão e carregamento *lazy* por visão ativa (também reduzindo tráfego JSON/WebSocket pela internet).
* **Contradição 2 (Container sempre ativo vs. impacto zero no pipeline):** Resolvida com extra de dependências isolado, `mem_limit: 512m`, `cpus: "1.0"`, pool máximo de 2 conexões e cache em memória limitado (`max_entries=30`).
* **Contradição 3 (`mypy --strict` / `pytest` vs. UI Streamlit):** Resolvida isolando acesso a dados/DTOs em módulos puros sem dependência de Streamlit.
* **Contradição 4 (Acesso via domínio HTTPS na VPS vs. zero código de autenticação no app):** Resolvida delegando TLS e autenticação na borda ao **Traefik/EasyPanel**, sem expor a porta `8501` publicamente e com role `sro_readonly` obrigatória no PostgreSQL.
* **Ideal Final Result:** Abrir `https://<domínio-dashboard>` (autenticado na borda via Traefik/EasyPanel na VPS Hostinger KVM 2) e carregar qualquer uma das 5 visões do servidor selecionado em **< 1–5 segundos**, via SQL read-only particionado e cacheado, sem impactar `scraper-worker` (512MB) nem `postgres` (1GB) e mantendo 100% de aderência aos gates de `AGENTS.md`.

---

## Goals vs. Non-Goals

* **In-Scope Goals:**
  * Dashboard Streamlit single-page, read-only, para 1 usuário, hospedado na **VPS Hostinger KVM 2** (Debian 13, 8 GB RAM, 2 vCPUs) atrás de **domínio HTTPS via Traefik/EasyPanel** (TLS + autenticação na borda + suporte a WebSockets, sem exposição direta da porta `8501`).
  * Container dedicado sempre ativo no `docker-compose.yml` (`mem_limit: 512m`, `cpus: "1.0"`, `healthcheck` em `/_stcore/health`, rotação de logs) com dependências isoladas via `[project.optional-dependencies]`.
  * 5 visões analíticas da comunidade por servidor selecionado:
    1. Evolução agregada de `item_points`/rank do Top N (padrão 30 dias, expansível até 6 meses).
    2. Dominância de guildas no Top 100 (`v_guild_turnover_dominance`).
    3. Balanço de raças CH vs EU (`v_race_balance_ch_vs_eu`).
    4. Retenção semanal (`v_server_retention_weekly`).
    5. Radar comportamental Bot vs Whale (`v_bot_vs_donor_classification`).
  * Cache em aplicação (`st.cache_data`) com TTL configurável + botão manual de atualização.
  * Conexão PostgreSQL estritamente `READ ONLY` via role dedicada `sro_readonly` (`DASHBOARD_DATABASE_URL`) + `default_transaction_read_only=on`, `statement_timeout`, `lock_timeout` e `work_mem` reduzido.
  * Critério de sucesso: carregar qualquer visão de qualquer servidor (até 10 servidores / 6 meses de histórico) em **menos de 5 segundos** via domínio HTTPS na VPS, sem SQL manual e sem degradação no `scraper-worker` ou no `postgres`.

* **Out-of-Scope Non-Goals:**
  * Painel Operacional (`scrape_runs`, Circuit Breaker, leitura ou download de `./quarantine/*.html.gz`) — permanece no Prefect UI e SQL/CLI.
  * Autenticação customizada dentro do código Streamlit, autorização multi-usuário/RBAC ou acesso público anônimo sem proteção na borda (autenticação de usuário único fica delegada ao Traefik/EasyPanel).
  * Escrita no banco de dados (reset de Circuit Breaker, edição de servidores, anotação manual).
  * Materialized Views com job periódico no Prefect (adiado para fase futura caso o volume ultrapasse 10 servidores / 6 meses).
  * Bibliotecas extras de gráficos como `plotly` (usar `altair`, já embutido no Streamlit).

---

## Alternatives Weighed

* **Metabase / ferramentas de BI baseadas em JVM:** Rejeitado pelo autor por exigir JVM e alto consumo de RAM, destoando da stack Python enxuta do repositório.
* **Painel Operacional integrado no mesmo dashboard:** Rejeitado pelo autor; telemetria de execução fica no Prefect UI, evitando acoplamento com o volume `./quarantine/` e ampliação de escopo.
* **Container sob demanda via Docker Compose `profiles`:** Rejeitado pelo autor; preferência por container sempre ativo junto com `postgres` e `scraper-worker`, mitigado por `mem_limit: 512m` e pool de conexões mínimo.
* **Autenticação no app Streamlit (`streamlit-authenticator`) ou acesso restrito a SSH Tunnel/VPN:** Rejeitado em favor de **domínio HTTPS + autenticação na borda via Traefik/EasyPanel**, aproveitando a infraestrutura já ativa na VPS Hostinger KVM 2 sem adicionar dependências de auth ao Python.
* **Materialized Views com refresh agendado no Prefect:** Rejeitado nesta fase (YAGNI); para até 10 servidores, 6 meses e 1 usuário, queries com *partition pruning* + cache TTL no Streamlit atendem o SLA de `< 5s` sem nova orquestração.
* **Plotly para visualização:** Rejeitado (YAGNI); `altair` já vem instalado com o Streamlit e cobre séries temporais, barras e dispersão sem aumentar imagem nem RAM.
* **Reusar engine assíncrona (`asyncpg`) de `src/core/database.py`:** Rejeitado; `psycopg` já está no projeto e evita problemas de ciclo de vida de event loop nos reruns síncronos do Streamlit.

---

## Key Decisions & Staging for Next Steps

Decisões a confirmar antes de avançar para o design formal (`/egm-draft`):

1. **Otimização SQL (CTEs + Migração `003`):** Confirmar uso de CTEs com *lookback buffer* de 2 dias em `src/dashboard/queries.py` (para restaurar *partition pruning* sem quebrar as views existentes) + criação de migração Alembic `003` adicionando o índice parcial `idx_scrape_runs_server_latest` em `scrape_runs`.
2. **Visão 1 (Evolução Agregada do Top N):** Confirmar curvas diárias agregadas (Top 10 / Top 50 / Top 100 — média/mediana/máximo de `item_points`) com drill-down opcional para Top 5–10 personagens individuais.
3. **Seletor de Visão e Biblioteca de Gráficos:** Confirmar `st.segmented_control` (execução *lazy* apenas da visão ativa) + `altair` (nativo do Streamlit).
4. **Segurança Read-Only no PostgreSQL (Produção na VPS):** Confirmar defesa em duas camadas obrigatórias: role dedicada `sro_readonly` via `DASHBOARD_DATABASE_URL` + `default_transaction_read_only=on`, `statement_timeout=15000` e `work_mem=16MB` na sessão, mantendo a porta `5432` sem exposição no host.
5. **Publicação e Hardening na VPS Hostinger KVM 2 (Traefik / EasyPanel):** Confirmar roteamento interno do container `dashboard` sem porta pública direta (HTTPS + autenticação na borda + WebSockets `/_stcore/stream`), limites de container (`mem_limit: 512m`, `cpus: "1.0"`, healthcheck e rotação de logs) e restrição da porta `4200` do `prefect-server` (hoje publicada em `0.0.0.0`).
