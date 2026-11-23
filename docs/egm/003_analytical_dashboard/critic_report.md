# Stage 7 — Critic Review Report (`FeatureRange`, linhas 1–286)

*   **Target Document:** [003_analytical_dashboard.md](../../designs/003_analytical_dashboard.md) (`FeatureRange`, linhas 1–286)
*   **Date:** 2026-11-23
*   **Reviewers:** `egm-design`, `egm-structural`, `egm-style`
*   **Standard Applied:** `feature-spec-standard.md`
*   **Summary:** `3` 🔴 Blocker | `11` 🟡 Major | `10` 🟢 Nit (consolidados e deduplicados)

---

## 🔴 Blockers

### B1. Contradição Arquitetural: SQL Exclusivo de PostgreSQL vs. Suporte Declarado a `sqlite://` (`egm-design`)
* **Escopo afetado:** Linhas 112, 124, 138–139, 167–171.
* **Achado:** `DashboardSettings` e `db.py` declaram suporte a `sqlite://` *"exclusivo para testes unitários"*, mas as 5 consultas da Seção 3 usam recursos inexistentes ou inválidos no SQLite (`PERCENTILE_CONT(0.5)`, `DATE_TRUNC`, `INTERVAL`, `CURRENT_TIMESTAMP AT TIME ZONE 'UTC'`, `::NUMERIC`).
* **Ação requerida:** Remover a promessa de `sqlite://` em produção (`config.py`/`db.py`) e separar explicitamente testes unitários (DTOs/config/contratos SQL com `Session` mockada) de testes de integração opt-in contra PostgreSQL 16 (`@pytest.mark.postgres`).

### B2. Topologia de Rede Ambígua/Quebrada entre Traefik/EasyPanel e `dashboard` + Lacunas de Proxy Streamlit (`egm-design`)
* **Escopo afetado:** Linhas 91, 195–202; `docker-compose.yml` (linhas 94–96).
* **Achado:** `sro_network` é uma bridge local isolada. O Traefik gerenciado pelo EasyPanel não enxerga `dashboard:8501` sem anexação à rede externa do proxy, e `127.0.0.1:8501` dentro de um container Traefik aponta para o próprio container, não para o host. Além disso, o comando Streamlit (L196) omite a especificação de compatibilidade XSRF/CORS atrás de proxy HTTPS, timeouts de idle para WebSocket (`/_stcore/stream`) e rate-limit no Basic Auth.
* **Ação requerida:** Definir um único modelo determinístico de conectividade com Traefik/EasyPanel (ex.: topologia *dual-network* anexando `dashboard` à rede externa do proxy mantendo `postgres` isolado em `sro_network`, ou bind explícito `127.0.0.1:8501:8501` se o proxy operar em modo host), explicitar flags XSRF/CORS compatíveis com HTTPS, timeouts mínimos de WebSocket e rate-limiting junto ao Basic Auth.

### B3. Guardrails Aplicados Apenas no Cliente + Risco de Quoting/URL-Encoding em `scripts/provision_readonly_role.sh` (`egm-design`)
* **Escopo afetado:** Linhas 35, 123, 177–187, 250–252.
* **Achado:** `default_transaction_read_only=on`, `statement_timeout=15000`, `lock_timeout=3000` e `work_mem=16MB` são aplicados apenas via `connect_args` do SQLAlchemy — se `sro_readonly` for usada fora do `db.py`, os limites são ignorados. Além disso, o script precisa garantir passagem segura da senha sem interpolação SQL vulnerável e documentar restrição/URL-encoding de caracteres especiais (`@`, `#`, `:`, `/`) em `DASHBOARD_DATABASE_URL`.
* **Ação requerida:** Aplicar os guardrails também no nível da role PostgreSQL (`ALTER ROLE sro_readonly CONNECTION LIMIT 5; ALTER ROLE sro_readonly SET ...;`) no script de provisionamento, usar `-v password=...` + `format('... %L', :'password')` no `psql`, e documentar URL-encoding da senha.

---

## 🟡 Majors

### M1. Violação da *Alternatives Rule* ("Rejeitado pelo autor:") (`egm-design`, `egm-structural`, `egm-style`)
* **Escopo afetado:** Linhas 213, 218, 223, 228, 233, 238, 243.
* **Achado:** Todas as 7 entradas em `Alternatives Considered` começam com `"Rejeitado pelo autor:"`, violando o *Feature Spec Standard* (não atribuir rejeições a autor/mantenedor sem link de issue/PR; basear-se apenas em trade-offs técnicos/escopo).
* **Ação requerida:** Remover o prefixo `"Rejeitado pelo autor:"` e iniciar diretamente pela justificativa técnica objetiva (na Alternativa C, justificando pela eliminação de dependência de SSH para acesso HTTPS contínuo).

### M2. *Partition Pruning* Não Determinístico em CTE `JOIN` nas Visões 2 e 3 (`egm-design`)
* **Escopo afetado:** Linhas 168–169.
* **Achado:** `JOIN` com CTE `latest_run` depende de *run-time partition pruning* (só ocorre se o otimizador escolher `Nested Loop`). Sob `Hash Join`, todas as partições são varridas silenciosamente.
* **Ação requerida:** Resolver `(run_id, created_at)` em passo 1 (~1ms via `idx_scrape_runs_server_latest`) e passar `:run_id`, `:window_start` e `:window_end` como parâmetros escalares bindados para garantir *plan-time partition pruning*.

### M3. Visão 4 (Retenção Semanal): `:window_start` Indefinido, Borda Esquerda Truncada e Lookback de 2 Dias Frágil (`egm-design`)
* **Escopo afetado:** Linhas 29, 32, 55, 134, 170.
* **Achado:** `:window_start` não está definido para a Visão 4; janelas relativas truncam a semana mais antiga à esquerda; e *lookback* de 2 dias descarta personagens quando há cooldown > 48h.
* **Ação requerida:** Definir janela padrão explícita da Visão 4 (ex.: 12 semanas / até 26 semanas), alinhar `:window_start` ao início da semana ISO (`DATE_TRUNC('week', ...)` UTC) e ampliar o *lookback buffer* para `7 days`.

### M4. Visão 1 (Evolução Top N): Múltiplos Scrapes/Dia, `max_item_points` Redundante e Alinhamento com G1 (`egm-design`)
* **Escopo afetado:** Linhas 29, 130–131, 167.
* **Achado:** Com 2–3 scrapes/dia, `DATE_TRUNC('day', created_at)` sem deduplicação faz `sample_size` oscilar e gera múltiplos pontos por dia no drill-down; `max_item_points` é idêntico para Top 10, Top 50 e Top 100 (Rank #1); G1 cita `item_points`/rank mas o agregado só tem `item_points`.
* **Ação requerida:** Definir deduplicação diária explícita (ex.: último snapshot do dia ou `MAX(item_points)` / `MIN(rank_position)` por `(snapshot_day, character_id)`), separar Rank #1 (ou usar P90/P50/Média por faixa) e alinhar a redação de G1.

### M5. Orçamento de Memória (`512m`), Semântica de `max_entries=30` e Acoplamento de Imagem (`egm-design`)
* **Escopo afetado:** Linhas 34, 97–102, 142, 193, 195, 258.
* **Achado:** Baseline RSS de Streamlit+Pandas+PyArrow (~180–250 MB) pode acionar OOM-kill em `512m` sob reruns de 180 dias; `@st.cache_data(max_entries=30)` aplica-se **por função** (180–240 entradas globais); `pip install ".[dashboard]"` embarca `prefect`/`lxml`/`gcc` na imagem do dashboard.
* **Ação requerida:** Ajustar `mem_limit` para `768m` (ou validar RSS empiricamente com `MALLOC_ARENA_MAX=2` e `max_entries=10` por função) e documentar/resolver o acoplamento de imagem Docker (multi-stage ou trade-off explícito).

### M6. Ausência de `## Risks & Mitigations`, Volumetria e Omissão de `prefect-server` / `SHARE` Lock no Rollout (`egm-design`, `egm-structural`, `egm-style`)
* **Escopo afetado:** Linhas 36, 156–158, 203–204, 247–269.
* **Achado:** Falta seção dedicada `## Risks & Mitigations` exigida pelo *Feature Spec Standard* (com estimativa de volumetria, risco de Basic Auth falhar aberto, spill-to-disk em `work_mem=16MB`, falha de WebSocket); Rollout/Rollback omite o restart do `prefect-server` com `127.0.0.1:4200:4200` + túnel SSH; e L263 omite que `CREATE INDEX` (sem `CONCURRENTLY`) adquire `SHARE` lock bloqueando brevemente escritas em `scrape_runs`.
* **Ação requerida:** Adicionar `## Risks & Mitigations`, incluir estimativa de volumetria, cobrir `prefect-server` no Rollout/Rollback e calibrar a ressalva do lock em `scrape_runs`.

### M7. Violação de Hierarquia de Cabeçalhos (`###` Órfãos após `---`) (`egm-structural`)
* **Escopo afetado:** Linhas 270–285 (`### Additional Resources` L272 e `### Future Improvements` L279).
* **Achado:** Declarados em H3 após `---`, ficam aninhados sob `## Non-Functional Requirements`.
* **Ação requerida:** Promover ambos para H2 (`## Additional Resources` e `## Future Improvements`).

### M8. Quebra de Simetria de Template: Ausência de `## Decision Log` (ADRs `ADR-012+`) (`egm-structural`)
* **Escopo afetado:** Linhas 244–247 (em contraste com `002_discord_notifications.md`).
* **Achado:** Design 001 (`ADR-001`–`ADR-006`) e Design 002 (`ADR-007`–`ADR-011`) mantêm `## Decision Log`; Design 003 omite a seção.
* **Ação requerida:** Adicionar `## Decision Log` após `## Alternatives Considered` a partir de `ADR-012`.

### M9. Vazamento de Detalhes de Implementação no `FeatureRange` (`egm-structural`)
* **Escopo afetado:** Linhas 153–160, 191–204, 261–268.
* **Achado:** DDL literal, nomes de arquivos de migração, strings de versão, comandos `RUN`/`curl`/CLI e runbook de Rollout vazam no `FeatureRange` e duplicam `Detailed Implementation Plan`.
* **Ação requerida:** Elevar a altitude no `FeatureRange` (invariantes, topologia e contratos) e concentrar detalhes literais/comandos em `Detailed Implementation Plan`; extrair `Rollout & Rollback` de NFRs para seção H2 própria (em alto nível no `FeatureRange`).

### M10. Contagem Inconsistente de Módulos em `src/dashboard/` (`egm-style`, `egm-structural`)
* **Escopo afetado:** Linhas 106–147.
* **Achado:** L106 declara *"quatro responsabilidades isoladas"*, mas lista **cinco** módulos (`config.py`, `db.py`, `schemas.py`, `queries.py`, `app.py`).
* **Ação requerida:** Corrigir para `"cinco"`.

### M11. Afirmações Não Calibradas (`CALIBRATE`) (`egm-style`)
* **Escopo afetado:** Linhas 151, 233.
* **Achado:** L151 afirma *"restauram 100% do partition pruning"* (conflitando com L166 sem filtro temporal); L233 afirma *"atendem o SLA de `< 5s`"* sem benchmark em documento `Draft`.
* **Ação requerida:** Calibrar L151 para o escopo real das consultas analíticas e L233 para *"são projetadas para atender o SLA de `< 5s`"*.

---

## 🟢 Nits

1. **`is_active` em `ServerOption` (L128):** Explicitar filtro `WHERE is_active = TRUE` ou exibição com badge `[Inativo]`.
2. **`NULLIF` em `GuildDominanceRow` (L132) & consulta de frescor (L166):** Usar `NULLIF(sample_total_players, 0)` e preferir `ORDER BY created_at DESC LIMIT 1` (ou reutilizar `idx_scrape_runs_server_latest`) em vez de `MAX(created_at)` sem filtro temporal.
3. **Alinhamento documental com `AGENTS.md` (L119–120, L240–243):** Registrar atualização em `AGENTS.md` documentando o uso de `psycopg` síncrono em Alembic e `src/dashboard/`.
4. **Remover fallback morto `st.radio` (L145, L192):** `streamlit>=1.40.0` já inclui `st.segmented_control` estável.
5. **Consistência idiomática e numeração de subcabeçalhos (L210–261):** Alinhar idioma e numeração de `Alternatives` e `NFRs` com o Design 002.
6. **Portabilidade de `\n` no diagrama Mermaid (L76–95):** Usar rótulos em linha única ou *markdown strings* (`` "`...`" ``).
7. **Clareza e concisão editorial (L14, L21–22, L106, L144, L168, L212, L231, L237, L283):** Padronizar unidades de memória em prosa (`512 MB`, `1 GB`, `8 GB`), remover pleonasmo `"Adiciona dependência pesada adicional"`, substituir `"enxuta"` e `"instantâneas"`, definir local único do seletor de servidor (barra lateral), trocar `"DataFrames"` por `"dados tabulares"` (L283), padronizar `"6 meses de histórico"` (L14) e evitar repetir os 4 guardrails literalmente 5 vezes.
