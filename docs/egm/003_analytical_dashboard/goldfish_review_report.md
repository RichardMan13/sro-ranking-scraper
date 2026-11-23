# Goldfish Annotated Review Report

**Target:** `docs/designs/003_analytical_dashboard.md` (`FeatureRange`, lines 1–286)

## Prioritized Findings & AI Suggestions

### [Issue 1] [`egm-design`] - Severity: Blocker
*   **Affected Scope:** [Lines 112–171](../../designs/003_analytical_dashboard.md#L112-L171) (`Technical Plan §2 e §3`)
*   **Discrepancy Description:** Contradição arquitetural entre o suporte declarado a `sqlite://` em `config.py`/`db.py` para testes unitários e o uso de SQL exclusivo de PostgreSQL (`PERCENTILE_CONT`, `DATE_TRUNC`, `INTERVAL`, `::NUMERIC`) nas 5 consultas analíticas.
*   **Rationale:** `sqlite://` não executa nenhuma das 5 consultas analíticas, tornando a promessa de suporte SQLite enganosa ou código morto em produção.
*   **AI Suggestion:** PROPOSE FIX
*   **Justification:** A crítica é válida e verificável diretamente no texto do design. Remover `sqlite://` do código de produção (`config.py`/`db.py`) e separar explicitamente testes unitários offline (DTOs, configuração e contratos SQL com `Session` mockada) de testes de integração opt-in contra PostgreSQL 16 (`@pytest.mark.postgres`) resolve a contradição sem introduzir código morto.
*   **Proposed Edit:**
    *   Location: `Technical Plan §2` (`config.py`, `db.py`, `queries.py`) e `Detailed Implementation Plan` (#8, #9, #13, #14)
    *   Change: Remover menções a `sqlite://` em `DashboardSettings` e `create_dashboard_engine`; explicitar que o dashboard aceita apenas DSN PostgreSQL síncrono (`postgresql+psycopg://`) e que `queries.py` é validado em duas camadas: testes unitários offline com `Session` mockada + suíte opt-in `@pytest.mark.postgres` contra o `postgres:16-alpine` do Compose.

### [Issue 2] [`egm-design`] - Severity: Blocker
*   **Affected Scope:** [Lines 91, 195–202](../../designs/003_analytical_dashboard.md#L195-L202) (`Technical Plan §1 e §5`)
*   **Discrepancy Description:** Topologia de rede ambígua/quebrada entre Traefik/EasyPanel e o container `dashboard` (`sro_network` isolada sem `ports:` vs. fallback `127.0.0.1:8501` inoperante de dentro de outro container), além de lacunas nas flags de proxy reverso HTTPS (XSRF/CORS), timeouts de WebSocket (`/_stcore/stream`) e rate-limiting junto ao Basic Auth.
*   **Rationale:** Sem anexação explícita à rede externa do proxy ou bind de loopback para proxy no host, o Traefik não alcança o container `dashboard:8501`.
*   **AI Suggestion:** PROPOSE FIX
*   **Justification:** A crítica é válida. `docker-compose.yml` define `sro_network` como bridge local isolada. Definir a topologia determinística *dual-network* (container `dashboard` conectado à `sro_network` interna + rede externa do proxy Traefik/EasyPanel via override de produção, mantendo `postgres`, `scraper-worker` e `prefect-server` isolados em `sro_network`) elimina a ambiguidade sem expor `postgres`.
*   **Proposed Edit:**
    *   Location: `Technical Plan §1` (diagrama Mermaid) e `Technical Plan §5` (Publicação Traefik/EasyPanel)
    *   Change: Especificar topologia *dual-network* para `dashboard` (`sro_network` + rede externa do Traefik/EasyPanel via override de produção), mantendo `postgres` isolado; explicitar encaminhamento de `Host`, `X-Forwarded-Proto: https`, `Upgrade`/`Connection`, timeout mínimo de idle WebSocket (`>= 300s`) para `/_stcore/stream`, política de CORS/XSRF do Streamlit atrás de HTTPS e middleware de *rate-limiting* no Traefik junto ao Basic Auth.

### [Issue 3] [`egm-design`] - Severity: Blocker
*   **Affected Scope:** [Lines 35, 123, 177–187, 250–252](../../designs/003_analytical_dashboard.md#L173-L188) (`Technical Plan §4` e `NFRs`)
*   **Discrepancy Description:** Guardrails de sessão (`default_transaction_read_only=on`, `statement_timeout`, `lock_timeout`, `work_mem`, limite de conexões) aplicados apenas no cliente (`connect_args`) e risco de quoting/URL-encoding da senha `DASHBOARD_DB_PASSWORD`.
*   **Rationale:** Se a role `sro_readonly` for usada fora do `db.py`, os limites do cliente são contornados; caracteres especiais sem escape/encoding quebram SQL ou o DSN.
*   **AI Suggestion:** PROPOSE FIX
*   **Justification:** Aplicar os mesmos guardrails via `ALTER ROLE sro_readonly` no PostgreSQL 16 (junto com `CONNECTION LIMIT 5`) fecha a brecha no servidor de banco sem custo operacional e complementa o `connect_args` do cliente.
*   **Proposed Edit:**
    *   Location: `Technical Plan §4` (`sro_readonly`) e `Detailed Implementation Plan` (#6)
    *   Change: Incluir `ALTER ROLE sro_readonly CONNECTION LIMIT 5` e `ALTER ROLE sro_readonly SET default_transaction_read_only = on / statement_timeout = '15000' / lock_timeout = '3000' / work_mem = '16MB'` no contrato do script de provisionamento; exigir passagem via `psql -v` + `format('... %L', :'password')` e documentar URL-encoding da senha em `DASHBOARD_DATABASE_URL`.

### [Issue 4] [`egm-design`, `egm-structural`, `egm-style`] - Severity: Major
*   **Affected Scope:** [Lines 213–243](../../designs/003_analytical_dashboard.md#L210-L244) (`Alternatives Considered`)
*   **Discrepancy Description:** Todas as 7 entradas de `Alternatives Considered` começam com `"Rejeitado pelo autor:"`, violando a *Alternatives Rule* do `feature-spec-standard.md` e adicionando ruído editorial.
*   **Rationale:** Justificativas de rejeição devem apoiar-se exclusivamente em trade-offs técnicos ou de escopo, sem atribuição pessoal sem link de issue/PR.
*   **AI Suggestion:** PROPOSE FIX
*   **Justification:** A crítica é convergente entre os 3 revisores e objetivamente válida.
*   **Proposed Edit:**
    *   Location: `Alternatives Considered` (Alternativas A–G)
    *   Change: Remover o prefixo `"Rejeitado pelo autor:"` das 7 alternativas e iniciar cada `Reason for Rejection` diretamente pelo trade-off técnico/operacional (na Alternativa C, justificando pela eliminação de dependência de acesso SSH para consultas contínuas via HTTPS).

### [Issue 5] [`egm-design`] - Severity: Major
*   **Affected Scope:** [Lines 168–169](../../designs/003_analytical_dashboard.md#L168-L169) (`Technical Plan §3` — Visões 2 e 3)
*   **Discrepancy Description:** *Partition pruning* nas Visões 2 e 3 depende de *run-time pruning* via `JOIN` com CTE `latest_run` (que falha silenciosamente se o planejador escolher `Hash Join`).
*   **Rationale:** Uma consulta em 2 passos passando `:run_id`, `:window_start` e `:window_end` como parâmetros escalares garante *plan-time partition pruning* determinístico no PostgreSQL 16.
*   **AI Suggestion:** PROPOSE FIX
*   **Justification:** A crítica é tecnicamente precisa para o otimizador do PostgreSQL 16; o custo adicional é ~1 ms na rede local Docker apenas em *cache miss*.
*   **Proposed Edit:**
    *   Location: `Technical Plan §3` (tabela das Visões 2 e 3) e `Detailed Implementation Plan` (#11)
    *   Change: Substituir o `JOIN` com CTE `latest_run` por resolução prévia de `(run_id, created_at)` via `idx_scrape_runs_server_latest` seguida de query em `character_snapshots` com parâmetros escalares bindados (`:run_id`, `:window_start`, `:window_end`).

### [Issue 6] [`egm-design`] - Severity: Major
*   **Affected Scope:** [Lines 29, 32, 55, 134, 170](../../designs/003_analytical_dashboard.md#L170) (`Technical Plan §3` — Visão 4)
*   **Discrepancy Description:** Visão 4 (Retenção Semanal) usa `:window_start` sem definir janela padrão, trunca a semana mais antiga quando a janela não é alinhada à segunda-feira UTC e usa *lookback* de 2 dias (frágil para cooldowns > 48h).
*   **Rationale:** Janela não alinhada cria queda artificial na borda esquerda e lookback de 2 dias descarta personagens após cooldowns de > 48h.
*   **AI Suggestion:** PROPOSE FIX
*   **Justification:** A crítica é válida e melhora a acurácia analítica com custo desprezível (no máximo 1 partição mensal adjacente).
*   **Proposed Edit:**
    *   Location: `Goals (G1)` e `Technical Plan §3` (Visão 4)
    *   Change: Definir janela explícita para a Visão 4 (padrão 12 semanas, expansível até 26 semanas), alinhar `:window_start` ao início da semana ISO (`DATE_TRUNC('week', ...)` em UTC) e ampliar o *lookback buffer* para `7 days`. Incluir também a definição explícita de `active_characters` e `activity_retention_rate_pct` (alinhando com o achado do Stage 6).

### [Issue 7] [`egm-design`] - Severity: Major
*   **Affected Scope:** [Lines 29, 130–131, 167](../../designs/003_analytical_dashboard.md#L167) (`Goals` e `Technical Plan §3` — Visão 1)
*   **Discrepancy Description:** Múltiplos scrapes por dia geram oscilação de `sample_size` e zigue-zague no drill-down; `max_item_points` é idêntico para Top 10, Top 50 e Top 100 (sempre Rank #1); G1 cita `item_points`/rank mas o agregado não inclui métrica de rank.
*   **Rationale:** Sem regra explícita de deduplicação diária e separação do teto (Rank #1), a agregação diária e o gráfico ficam ambíguos.
*   **AI Suggestion:** PROPOSE FIX
*   **Justification:** A crítica é válida. Consolidar por `(snapshot_day, character_id)` usando o último snapshot do dia (ou `MAX(item_points)` / `MIN(rank_position)`), calcular Média, Mediana (P50) e P90 por faixa (Top 10/50/100) + série única do teto (Rank #1), e alinhar a redação de G1 deixa o contrato inequívoco.
*   **Proposed Edit:**
    *   Location: `Goals (G1)`, `Technical Plan §2–§3` (Visão 1) e `Detailed Implementation Plan` (#10, #11)
    *   Change: Especificar deduplicação diária por `(snapshot_day, character_id)` (último snapshot UTC do dia), substituir `max_item_points` repetido por `p90_item_points` por faixa (mantendo o máximo/Rank #1 como série única) e ajustar G1 para "evolução diária de `item_points` por faixa de rank (Top 10/50/100) com drill-down de `item_points` e `rank_position` do Top 5–10".

### [Issue 8] [`egm-design`] - Severity: Major
*   **Affected Scope:** [Lines 34, 97–102, 142, 193, 195, 258](../../designs/003_analytical_dashboard.md#L97-L102) (`Orçamento de Memória`, `Cache` e `Dockerfile`)
*   **Discrepancy Description:** Risco de OOM em `mem_limit: 512m` dado o RSS base de Streamlit+Pandas+PyArrow (~180–250 MB); `max_entries=30` aplica-se por função (180–240 entradas globais); e `pip install ".[dashboard]"` embarca `prefect`/`lxml`/`gcc` na imagem do dashboard.
*   **Rationale:** A VPS tem 8 GB de RAM (~5 GB livres), permitindo margem segura de `768m` + `MALLOC_ARENA_MAX=2` + `max_entries=10` por função; e documentar explicitamente o trade-off de imagem única (ou isolar via multi-stage) evita surpresas.
*   **AI Suggestion:** PROPOSE FIX
*   **Justification:** A crítica é válida. Adotar `mem_limit: 768m` com `MALLOC_ARENA_MAX=2` e `max_entries=10` por função cacheada elimina risco de OOM sem comprometer a folga da VPS de 8 GB (~2,8–3,0 GB total), e registrar explicitamente o trade-off do `Dockerfile` único (simplicidade de build vs. presença de libs do worker na imagem do dashboard) ou usar `pyproject.toml` com extras separados deixa a decisão transparente.
*   **Proposed Edit:**
    *   Location: `Overview`, `Goals (G2)`, `Technical Plan §1, §2, §5`, `NFRs`, `Detailed Implementation Plan` (#3, #12)
    *   Change: Atualizar `mem_limit` do container `dashboard` para `768m` (mantendo `scraper-worker` em `512m`), adicionar `MALLOC_ARENA_MAX=2`, reduzir `max_entries` para `10` por função cacheada e registrar o trade-off de imagem única (com opção de migração futura para multi-stage caso o tamanho da imagem se torne relevante).

### [Issue 9] [`egm-design`, `egm-structural`, `egm-style`] - Severity: Major
*   **Affected Scope:** [Lines 36, 156–158, 203–204, 247–269](../../designs/003_analytical_dashboard.md#L247-L269) (`NFRs` e `Rollout & Rollback`)
*   **Discrepancy Description:** Falta seção dedicada `## Risks & Mitigations` (exigida pelo *Feature Spec Standard*) com estimativa de volumetria; Rollout/Rollback omite o restart de `prefect-server` (`127.0.0.1:4200:4200`) + túnel SSH; e L263 omite que `CREATE INDEX` sem `CONCURRENTLY` adquire lock `SHARE` bloqueando brevemente escritas em `scrape_runs`.
*   **Rationale:** Riscos operacionais (Basic Auth falhando aberto, WebSocket caindo, spill-to-disk em `work_mem=16MB`, acesso à UI do Prefect após restrição da porta `4200`) e a volumetria que sustenta `< 5s` precisam estar documentados.
*   **AI Suggestion:** PROPOSE FIX
*   **Justification:** Crítica convergente dos 3 revisores e diretamente exigida pelo padrão do repositório.
*   **Proposed Edit:**
    *   Location: Nova seção `## Risks & Mitigations` após `## Non-Functional Requirements`, além de seção H2 própria `## Rollout & Rollback`
    *   Change: Adicionar `## Risks & Mitigations` (incluindo estimativa quantitativa: ~10 servidores × 2 scrapes/dia × ~100–200 chars × 180 dias ≈ 360k–720k linhas totais / ~60k–120k por partição mensal); incluir `prefect-server` (`127.0.0.1:4200:4200` + túnel SSH) no Rollout/Rollback; e calibrar o texto do `CREATE INDEX` explicitando o `SHARE` lock breve sobre escritas em `scrape_runs`.

### [Issue 10] [`egm-structural`] - Severity: Major
*   **Affected Scope:** [Lines 270–285](../../designs/003_analytical_dashboard.md#L270-L285) (`### Additional Resources` e `### Future Improvements`)
*   **Discrepancy Description:** `### Additional Resources` (L272) e `### Future Improvements` (L279) estão em H3 após `---`, aninhadas sob `## Non-Functional Requirements`.
*   **Rationale:** Quebra a hierarquia lógica de cabeçalhos no documento.
*   **AI Suggestion:** PROPOSE FIX
*   **Justification:** Correção estrutural direta.
*   **Proposed Edit:**
    *   Location: Linhas 272 e 279
    *   Change: Promover para `## Additional Resources` e `## Future Improvements`.

### [Issue 11] [`egm-structural`] - Severity: Major
*   **Affected Scope:** [Lines 244–247](../../designs/003_analytical_dashboard.md#L244-L247) (após `## Alternatives Considered`)
*   **Discrepancy Description:** Ausência de seção `## Decision Log` dando continuidade numérica aos ADRs dos Designs 001 (`ADR-001`–`ADR-006`) e 002 (`ADR-007`–`ADR-011`).
*   **Rationale:** Mantém rastreabilidade arquitetural e simetria de template com os demais design docs em `docs/designs/`.
*   **AI Suggestion:** PROPOSE FIX
*   **Justification:** Verificado em `docs/designs/002_discord_notifications.md` (L264–275).
*   **Proposed Edit:**
    *   Location: Após `## Alternatives Considered`
    *   Change: Inserir `## Decision Log` com `ADR-012` a `ADR-016` (Streamlit + Altair em container dedicado; `psycopg` síncrono + `DashboardSettings` isolado; consultas com *partition pruning* determinístico em 2 passos + índice parcial `003`; defesa read-only em duas camadas com `ALTER ROLE sro_readonly`; publicação HTTPS + Basic Auth + WebSockets atrás do Traefik/EasyPanel em topologia *dual-network*).

### [Issue 12] [`egm-structural`] - Severity: Major
*   **Affected Scope:** [Lines 108–160, 191–204, 261–268](../../designs/003_analytical_dashboard.md#L108-L204) (`Technical Plan` e `NFRs`)
*   **Discrepancy Description:** Vazamento de detalhes de implementação (DDL literal, nomes de arquivos de migração, limites `Field(ge/le)`, flags exatas de CLI, comando `curl` de healthcheck e runbook CLI de Rollout) no `FeatureRange`, duplicando `Detailed Implementation Plan`.
*   **Rationale:** Viola a separação de altitude (*Anti-Weeds*) entre `FeatureRange` (arquitetura/contratos) e `Detailed Implementation Plan` (especificação executável).
*   **AI Suggestion:** PROPOSE FIX
*   **Justification:** A crítica é válida e também convergente com o Stage 5 (`egm-zoom-out` Major #2).
*   **Proposed Edit:**
    *   Location: `Technical Plan §2, §3, §5` e `Rollout & Rollback`
    *   Change: Enxugar `FeatureRange` para manter apenas fronteiras de módulos, contratos de dados/segurança, invariantes de *partition pruning* e topologia de rede; concentrar DDL literal, assinaturas/campos exatos, flags CLI do Streamlit, `healthcheck`, `logging` e comandos de deploy exclusivamente no `Detailed Implementation Plan`.

### [Issue 13] [`egm-style`, `egm-structural`] - Severity: Major
*   **Affected Scope:** [Lines 106–147](../../designs/003_analytical_dashboard.md#L106-L147) (`Technical Plan §2`)
*   **Discrepancy Description:** L106 declara *"quatro responsabilidades isoladas"*, mas lista cinco módulos (`config.py`, `db.py`, `schemas.py`, `queries.py`, `app.py`).
*   **Rationale:** Inconsistência numérica direta.
*   **AI Suggestion:** PROPOSE FIX
*   **Justification:** Erro factual objetivo.
*   **Proposed Edit:**
    *   Location: Linha 106
    *   Change: Alterar `"quatro responsabilidades isoladas"` para `"cinco módulos com responsabilidades isoladas"`.

### [Issue 14] [`egm-style`] - Severity: Major
*   **Affected Scope:** [Lines 151, 233](../../designs/003_analytical_dashboard.md#L151) (`Technical Plan §3` e `Alternatives Considered`)
*   **Discrepancy Description:** Afirmações não calibradas: *"restauram 100% do partition pruning"* (L151, conflitando com consulta de frescor) e *"atendem o SLA de `< 5s`"* (L233, sem benchmark empírico em doc `Draft`).
*   **Rationale:** Calibrar declarações de desempenho e escopo técnico mantém precisão documental.
*   **AI Suggestion:** PROPOSE FIX
*   **Justification:** A crítica é objetivamente válida.
*   **Proposed Edit:**
    *   Location: Linhas 151 e 233
    *   Change: Ajustar L151 para *"habilitam partition pruning nas 5 consultas analíticas por meio de filtros explícitos em `created_at`"* e L233 para *"são projetadas para atender o SLA de `< 5s`"*.

### [Issue 15] [`egm-design`, `egm-structural`, `egm-style`] - Severity: Nit
*   **Affected Scope:** [Lines 14, 21–22, 76–95, 106, 128, 132, 144–145, 166, 168, 210–261, 283](../../designs/003_analytical_dashboard.md#L14) (`FeatureRange`)
*   **Discrepancy Description:** Conjunto de nits convergentes: explicitar filtro `is_active = TRUE` em `ServerOption`; adicionar `NULLIF(sample_total_players, 0)` em `GuildDominanceRow`; reutilizar `idx_scrape_runs_server_latest` na consulta de frescor; registrar atualização em `AGENTS.md` sobre `psycopg` síncrono; remover fallback morto `st.radio`; padronizar idioma/numeração de subcabeçalhos (`Alternative A..G` / `NFRs` numerados em PT-BR); usar rótulos em linha única no Mermaid; padronizar unidades de memória (`512 MB`, `1 GB`, `8 GB`), remover termos subjetivos (`enxuta`, `pesada adicional`, `instantâneas`), definir seletor de servidor na barra lateral e trocar `DataFrames` por `dados tabulares` na L283.
*   **Rationale:** Pequenos ajustes de precisão, consistência com o Design 002 e higiene editorial.
*   **AI Suggestion:** PROPOSE FIX
*   **Justification:** Todos os ajustes são de baixo risco e aumentam diretamente a clareza e a consistência com o restante do repositório.
*   **Proposed Edit:**
    *   Location: Trechos correspondentes no `FeatureRange` (e item de documentação `AGENTS.md` no `Detailed Implementation Plan`)
    *   Change: Aplicar os ajustes pontuais listados acima em bloco único.
