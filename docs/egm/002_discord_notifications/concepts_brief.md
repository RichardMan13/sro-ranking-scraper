# Alertas Operacionais & Resumo Pós-FW via Discord (v2.0)

* **Author:** richardman13
* **Date:** 2026-11-22

## Overview

A v2.0 adiciona uma camada leve de notificações assíncronas via webhooks do Discord dentro do container `scraper-worker` já existente, reutilizando `httpx.AsyncClient` sem novos containers, sem portas públicas e sem alteração de schema no PostgreSQL.

O recurso atende dois públicos separados em canais independentes:
1. **Operador (canal privado via `DISCORD_OPS_WEBHOOK_URL`):** recebe alertas imediatos em toda execução com falha crítica (`SCHEMA_DRIFT`, `LOW_COUNT_QUARANTINE`, `BLOCKED_CHALLENGE`, `HTTP_ERROR`), incluindo execuções manuais com identificação de `trigger_type`, eliminando a necessidade de monitorar manualmente a UI do Prefect ou consultar `scrape_runs`.
2. **Comunidade / Guilda (canal público via `DISCORD_COMMUNITY_WEBHOOK_URL`):** recebe um resumo automático pós-Fortress War ao final de `post_fw_flow` (quando bem-sucedido), comparando o snapshot atual com o baseline pré-FW nas últimas 24h (mudanças de `fortress_held` e variação do Top 5 oficial de guildas em `guild_snapshots`).

## Analysis

A revisão pelas 7 lentes Goldfish (`egm-design`, `egm-codebase-details`, `egm-zoom-out`, `egm-comprehension`, `egm-implementation-readiness`, `egm-structural` e `egm-inventive-catalyst`) definiu os guardrails de segurança, integração no código atual e simplificação arquitetural:

### Segurança, Limites da API e Isolamento de Falhas
* **Proteção do token do webhook:** A URL do Discord contém o segredo no próprio caminho (`/api/webhooks/{id}/{token}`), e exceções do `httpx` (`HTTPStatusError`, `RequestError`) incluem a URL completa em `str(exc)`. Os campos em `Settings` devem usar `SecretStr | None = None`, e logs de erro devem registrar apenas `error_type`, `http_status_code` e rótulo de canal (`ops` ou `community`), nunca a URL ou `str(exc)` bruto.
* **Menções indesejadas e limite de payload:** Nomes de guildas ou mensagens de erro podem conter `@everyone`, `@here` ou `<@&...>`. Todo payload deve enviar `"allowed_mentions": {"parse": []}` e truncar strings variáveis para respeitar os limites do Discord (2.000 chars em `content`, 1.024 por field, 4.096 em description).
* **Isolamento temporal e orçamento de 180s:** O `asyncio.timeout(180.0)` em `BaseRankingScraper.run()` cobre apenas o fetch. A notificação roda somente após o `commit()` e fechamento da sessão de escrita, dentro de `try/except Exception`, com timeout explícito curto (`<= 10s`) e `0 retries`. Falha no Discord nunca falha `scrape_target_task`/`post_fw_flow` nem reverte dados persistidos.
* **Cobertura de exceções não tratadas:** Se o PostgreSQL cair (`OperationalError`) ou ocorrer `LookupError` antes do retorno de `ScrapeRunSummary`, o alerta ops não dispara porque o erro ocorre fora do contrato de status canônico. Esse limite ficará explícito no design doc `002`.

### Integração no Repositório Atual
* **Variáveis vazias no Docker Compose:** `docker-compose.yml` passa variáveis opcionais como `${VAR:-}` (string vazia). `Settings` (`src/core/config.py`) normaliza `""` / espaços para `None` via `@field_validator(..., mode="before")` antes da validação de URL HTTPS, evitando `ValidationError` na subida do container.
* **Contexto do `structlog` após `scraper.run()`:** `BaseRankingScraper.run()` chama `clear_run_context()` no `finally`. O notificador recebe e registra `server_id` e `run_id` explicitamente nos logs estruturados.
* **Quarentena ausente em `HTTP_ERROR`:** Apenas `BLOCKED_CHALLENGE`, `SCHEMA_DRIFT` e `LOW_COUNT_QUARANTINE` geram `.html.gz`. Para `HTTP_ERROR`, `quarantine_file_path` é `None` e o alerta exibe `N/A` ou omite o campo.
* **Consulta unificada em `guild_snapshots` com partition pruning e compatibilidade SQLite:** Com a escolha do Top 5 oficial de guildas (`guild_snapshots.rank_position <= 5`), tanto `fortress_held` quanto o Top 5 vêm exclusivamente de `guild_snapshots` (sem tocar em `character_snapshots`). A query filtra `scrape_run_id` **e** faixa de `created_at` (garantindo partition pruning no PostgreSQL) usando SQLAlchemy portável (sem `DISTINCT ON` ou `INTERVAL` literal) para continuar executando em SQLite nos testes unitários.
* **Múltiplas fortalezas por servidor:** Um servidor pode ter várias fortalezas ativas em `guild_snapshots.fortress_held` (ex.: Jangan, Bandit, Hotan) ou nenhuma preenchida. A comparação mapeia `fortress_held -> guild_name` por fortaleza, reportando trocas, manutenção de posse ou ausência de dados.

### Contradições Resolvidas (Resultado Final Ideal)
* **Stateless vs. spam em falhas repetidas:** Sem criar tabela de estado, o Circuit Breaker de 6h (`SKIPPED_CIRCUIT_BREAKER` excluído dos alertas) e a cadência de 12h já limitam naturalmente os alertas a no máximo 2 mensagens/dia por servidor quebrado.
* **Reexecução manual de `post_fw_flow` vs. baseline pré-FW:** A busca de baseline prioriza o último run válido (`SUCCESS`/`PARTIAL_SUCCESS`) das últimas 24h com `trigger_type != 'post_fw_extra'`, usando qualquer run válido anterior (`id != current_run_id`) nas últimas 24h como fallback e publicando apenas o retrato atual sem deltas se não houver baseline.

## Goals vs. Non-Goals

* **In-Scope Goals:**
  * Implementar `src/core/notifications.py` (sem alterar a estrutura de pacotes definida em `AGENTS.md`).
  * Enviar alertas operacionais em português (timestamps em `YYYY-MM-DD HH:MM UTC`) via `DISCORD_OPS_WEBHOOK_URL` ao final de `scrape_target_task` para `SCHEMA_DRIFT`, `LOW_COUNT_QUARANTINE`, `BLOCKED_CHALLENGE` e `HTTP_ERROR`, incluindo execuções manuais com `trigger_type` explícito.
  * Enviar resumo pós-Fortress War em português (timestamps em `YYYY-MM-DD HH:MM UTC`) via `DISCORD_COMMUNITY_WEBHOOK_URL` ao final de `post_fw_flow` quando `post_fw_extra` termina em `SUCCESS` ou `PARTIAL_SUCCESS`, comparando `fortress_held` e Top 5 oficial de guildas (`guild_snapshots.rank_position <= 5`) contra o baseline pré-FW de até 24h.
  * Operar em modo no-op silencioso quando uma URL de webhook não estiver configurada e em modo best-effort (`SecretStr`, `allowed_mentions: {"parse": []}`, timeout `<= 10s`, `0 retries`) sem impactar persistência ou status de execução.
  * Manter zero novos containers, zero portas públicas, zero novas dependências Python, zero migrações Alembic e conformidade total com `mypy --strict`, `ruff` e testes unitários com `respx` + SQLite.
* **Out-of-Scope Non-Goals:**
  * Camada de visualização / Dashboard Analítico (Opção B — Streamlit, Metabase, Power BI), adiada para ciclo futuro.
  * Suporte a Telegram ou outros canais além do Discord na v2.0.
  * Deduplicação stateful de alertas, tabela de histórico de notificações ou alertas de recuperação (`falha -> saudável`).
  * Alertas para `SUCCESS`, `PARTIAL_SUCCESS`, `TARGET_MAINTENANCE` e `SKIPPED_CIRCUIT_BREAKER`.
  * Cálculo de dominância por jogadores no Top 100 (`character_snapshots.rank_position <= 100`) no resumo pós-FW.
  * Criação de novas `@task` do Prefect para envio de notificações ou automações manuais na UI do Prefect.

## Alternatives Weighed

* **Opção B — Dashboard Analítico (Streamlit / Metabase / Power BI) na v2.0:** Rejected by author: priorizou resolver primeiro a detecção de quebras operacionais mantendo orçamento atual de RAM, zero portas públicas e zero manutenção de front-end; ficará para fase posterior.
* **Suporte simultâneo a Discord e Telegram:** Rejected by author: o operador utiliza apenas Discord; suportar dois provedores dobraria formatação, validação e testes sem ganho atual.
* **Alertas stateful (apenas na transição de estado) com tabela de controle:** Rejected by author: com cadência de 12h e Circuit Breaker de 6h, alertar em toda falha já limita o ruído a `<= 2` mensagens/dia por servidor e evita migração de schema e máquina de estados.
* **Top 5 de guildas derivado de `character_snapshots.rank_position <= 100` (alinhado à `v_guild_turnover_dominance`):** Rejected by author: optou por ler diretamente as 5 primeiras posições do ranking oficial de guildas (`guild_snapshots.rank_position <= 5`), simplificando a consulta pós-FW para uma única tabela (`guild_snapshots`).
* **Suprimir alertas ops em `manual_scrape_flow`:** Rejected by author: manter o disparo com `trigger_type` explícito preserva a regra stateless e permite testar o webhook via `--mode manual` (usando `.env` sem URL em desenvolvimento local).
* **Criar `@task` dedicadas no Prefect para envio de webhooks:** Descartado na análise YAGNI (`egm-zoom-out`): adiciona ruído no SQLite do `prefect-server` e overhead de estado para uma operação best-effort com `retries=0`.

## Key Decisions & Staging for Next Steps

Todas as decisões em aberto foram fechadas e seguem para o design doc formal (`docs/designs/002_discord_notifications.md`):
1. **Provedor e canais:** Apenas Discord, com dois webhooks independentes e opcionais em `Settings` (`DISCORD_OPS_WEBHOOK_URL` e `DISCORD_COMMUNITY_WEBHOOK_URL`, tipados como `SecretStr | None = None`, com normalização de `"" -> None` e validação HTTPS).
2. **Política de alertas ops:** Disparar ao final de `scrape_target_task` (incluindo `trigger_type="manual"`, exibindo `trigger_type` na mensagem) para `SCHEMA_DRIFT`, `LOW_COUNT_QUARANTINE`, `BLOCKED_CHALLENGE` e `HTTP_ERROR`, sempre após `commit()` e de forma best-effort (timeout `<= 10s`, `0 retries`).
3. **Métrica do resumo pós-FW:** Consultar exclusivamente `guild_snapshots` (com partition pruning em `created_at`) para comparar `fortress_held` (todas as fortalezas presentes) e o Top 5 oficial de guildas (`rank_position <= 5`).
4. **Regra de baseline pós-FW:** Último run válido (`SUCCESS`/`PARTIAL_SUCCESS`) das últimas 24h com `trigger_type != 'post_fw_extra'`; fallback para qualquer run válido anterior (`id != current_run_id`) nas últimas 24h; sem baseline, publicar apenas o retrato atual pós-FW com nota curta.
5. **Estrutura e formatação:** Módulo único `src/core/notifications.py`, mensagens em português, timestamps em `YYYY-MM-DD HH:MM UTC`, `"allowed_mentions": {"parse": []}` e truncamento seguro aos limites da API do Discord.
