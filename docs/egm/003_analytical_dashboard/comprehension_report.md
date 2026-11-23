## O que este documento propõe (FeatureRange — linhas 1–286)

O documento propõe a **v3.0** do **SRO Ranking Scraper**, adicionando um dashboard analítico *single-page* e estritamente somente leitura (*read-only*) em **Streamlit** (usando gráficos `altair`), voltado para análise de comunidade por um único usuário. Segundo o documento, o sistema existente já conta com um `scraper-worker` (Prefect 3, `asyncpg`, limite de 512 MB de RAM) que grava snapshots em tabelas particionadas mensais no PostgreSQL 16 (`1g` de RAM), 5 views analíticas da v1.0 (que hoje sofrem por não aplicar *partition pruning*) e alertas no Discord da v2.0, tudo operando em uma VPS Hostinger KVM 2 com Traefik/EasyPanel. A nova camada rodará em um container dedicado e sempre ativo (`dashboard`, `512m` RAM, `1.0` vCPU), sem expor a porta `8501` em `0.0.0.0`, atrás do Traefik/EasyPanel com HTTPS, Basic Auth e WebSockets (`/_stcore/stream`), e com hardening colateral restringindo o `prefect-server` a `127.0.0.1:4200`. O acesso ao banco usará defesa em duas camadas — role dedicada `sro_readonly` via `DASHBOARD_DATABASE_URL` mais guardrails de sessão (`default_transaction_read_only=on`, `statement_timeout=15000`, `lock_timeout=3000`, `work_mem=16MB`) — alimentando 5 visões com carregamento *lazy*, cache em memória e queries otimizadas com *partition pruning* para carregar em menos de 5 segundos (até 10 servidores e 6 meses de histórico), mantendo telemetria operacional (`scrape_runs`, Circuit Breaker, `./quarantine/`) fora do escopo.

## Como o plano será executado (DetailedRange — linhas 287–446)

A execução está estruturada em 15 entregas de arquivos cobrindo empacotamento, infraestrutura, banco de dados, aplicação, testes e documentação. As dependências de UI (`streamlit`, `altair`) serão isoladas em `[project.optional-dependencies]` (`dashboard`) no `pyproject.toml`, instaladas apenas na imagem do serviço `dashboard` via `ARG INSTALL_EXTRAS` no `Dockerfile`, preservando o `scraper-worker` livre dessas bibliotecas. No banco, será criada a migração Alembic `003_dashboard_indexes.py` (índice parcial `idx_scrape_runs_server_latest`) e o script operacional `scripts/provision_readonly_role.sh` para provisionar `sro_readonly`; no código, o pacote `src/dashboard/` será dividido em configuração (`config.py`), engine síncrona `psycopg` (`db.py`), DTOs imutáveis (`schemas.py`), 8 funções SQL parametrizadas com *partition pruning* (`queries.py`) e a interface Streamlit (`app.py`). A verificação combina `ruff`, `mypy --strict`, `pytest` focado na camada de dados/configuração (com `Session` mockada e SQLite em memória, sem PostgreSQL ativo), validação de migração/reversibilidade via `alembic` e checagens de integração na VPS (isolamento da imagem do worker, healthcheck, permissões de `sro_readonly`, `EXPLAIN (ANALYZE, BUFFERS)` e acesso HTTPS/WebSocket via Traefik/EasyPanel).

---

## Achados de Compreensão (Priorizados por Severidade)

*(Nenhum **🔴 Blocker** identificado: todas as seções se conectam logicamente e o fluxo arquitetural é compreensível de ponta a ponta.)*

### 🟡 Major

1. **Regras de negócio e fórmulas não definidas para a Visão 4 (Retenção Semanal) e a Visão 5 (Radar Bot vs Whale)**
   * **Escopo afetado:** `Goals` (linhas 32–33), `Technical Plan §2 e §3` (linhas 134–135, 170–171) e `Detailed Implementation Plan §10 e §11` (linhas 365–366, 378–379).
   * **Descrição e justificativa:** Na Visão 5, o documento referencia as métricas `grind_persistence_ratio`, `max_single_jump_item_points` e `evaluated_intervals` e afirma que a query aplica *"as mesmas regras de segmentação de `v_bot_vs_donor_classification`"*, mas não define inline como essas métricas são calculadas nem quais são os limiares para classificar `SUSPECTED_BOT_24_7`, `SUSPECTED_DONATOR_WHALE`, `HARDCORE_HUMAN` e `CASUAL_OR_BALANCED`. De forma análoga, na Visão 4, o texto explica o recorte temporal com `LAG()`, mas não define o critério que classifica um personagem como ativo (`active_characters`) nem a fórmula de `activity_retention_rate_pct`. Um leitor novo (ou implementador trabalhando apenas com este doc) não consegue entender o significado exato dessas métricas nem implementar `src/dashboard/queries.py` sem consultar artefatos externos.

2. **Pré-requisito não declarado de `curl` no `Dockerfile` para o `healthcheck`**
   * **Escopo afetado:** `Technical Plan §5` (linha 197) e `Detailed Implementation Plan §2 e §3` (linhas 297–301, 313).
   * **Descrição e justificativa:** O serviço `dashboard` em `docker-compose.yml` define `healthcheck` executando `curl -fsS http://127.0.0.1:8501/_stcore/health || exit 1`. Entretanto, a seção de alterações no `Dockerfile` lista apenas `ARG INSTALL_EXTRAS=""` e `pip install`, sem informar se `curl` já está presente na imagem base ou se precisa ser instalado. Caso a imagem base seja `python:3.11-slim` sem `curl`, o healthcheck falhará em runtime.

3. **Ambiguidade sobre leitura de `.env` vs variáveis já exportadas em `scripts/provision_readonly_role.sh`**
   * **Escopo afetado:** `Technical Plan §4` (linha 178), `Rollout` (linha 264) e `Detailed Implementation Plan §6` (linha 332).
   * **Descrição e justificativa:** A linha 178 afirma que o script opera *"lendo `POSTGRES_USER`, `POSTGRES_DB` e `DASHBOARD_DB_PASSWORD` do `.env`"*, e o procedimento de Rollout (linha 264) diz apenas para executar o script com `DASHBOARD_DB_PASSWORD` definido no `.env`. Já a linha 332 especifica *"Ler `POSTGRES_USER`, `POSTGRES_DB` e `DASHBOARD_DB_PASSWORD` do ambiente"*. Não fica claro se o próprio script carrega o arquivo `.env` automaticamente ou se o operador precisa exportar as variáveis no shell antes de executá-lo.

4. **Critério de servidor "ativo" não especificado em `list_active_servers` e filtro de rank implícito na Visão 2**
   * **Escopo afetado:** `Technical Plan §3` (linha 168) e `Detailed Implementation Plan §11` (linha 372).
   * **Descrição e justificativa:** A função `list_active_servers` não especifica qual coluna ou condição na tabela `servers` define um servidor como ativo (ex.: `is_active = TRUE`, `enabled`, etc.). Além disso, na tabela de contrato SQL da Visão 2 (linha 168), embora o título seja "Dominância de Guildas (Top 100)" e o DTO contenha `players_in_top_100`, `players_in_top_50` e `total_top_item_points`, a coluna de estratégia SQL não explicita o filtro `rank_position <= 100` nem esclarece se `total_top_item_points` agrega sobre o Top 100 ou o Top 50.

5. **Referência ao índice `idx_char_snapshots_rank` sem sua definição de colunas**
   * **Escopo afetado:** `Technical Plan §3` (linhas 166–167).
   * **Descrição e justificativa:** O documento afirma que a consulta de frescor (`SELECT MAX(created_at) FROM character_snapshots WHERE server_id = :server_id`) e a Visão 1 utilizam `idx_char_snapshots_rank`, mas nunca informa quais colunas (e em que ordem) compõem esse índice. Sem essa informação, um leitor novo não consegue validar por que um `MAX(created_at)` sem filtro temporal consegue ser eficiente em uma tabela particionada mensalmente.

---

### 🟢 Nit

1. **Contagem inconsistente de módulos em `src/dashboard/`**
   * **Escopo afetado:** `Technical Plan §2` (linhas 106–147).
   * **Descrição e justificativa:** A linha 106 declara que o pacote `src/dashboard/` é dividido em *"quatro responsabilidades isoladas"*, mas a lista numerada imediatamente abaixo apresenta **cinco** módulos (`config.py`, `db.py`, `schemas.py`, `queries.py` e `app.py`).

2. **Siglas e termos de domínio sem definição breve na primeira ocorrência**
   * **Escopo afetado:** Título e `Overview` (linhas 1, 12), `The Problem` (linhas 18, 20), `Goals` (linha 31) e `CUJ 3` (linha 64).
   * **Descrição e justificativa:** Termos específicos do domínio do jogo — `SRO`, `CH`, `EU`, `UNK`, `item_points` e `Fortress War` — são usados sem uma breve definição inline (por exemplo, expandir `SRO` como *Silkroad Online* e explicar `CH`/`EU`/`UNK` como raças *Chinese*/*European*/*Unknown* e `item_points` como métrica de pontuação de equipamento/personagem).

3. **Identificadores existentes citados sem origem ou definição**
   * **Escopo afetado:** `Technical Plan §4` (linha 183), `Future Improvements` (linha 281) e `Detailed Implementation Plan §12` (linha 386).
   * **Descrição e justificativa:** `ensure_monthly_partitions` (linha 183), `scheduled_12h_flow` (linha 281) e `configure_logging` (linha 386) são referenciados sem indicar o módulo de origem (ex.: `src/core/logging.py`) ou seu papel no sistema existente.

4. **Placeholder `<owner>` sem mapeamento explícito para `POSTGRES_USER`**
   * **Escopo afetado:** `Technical Plan §4` (linha 184) e `Detailed Implementation Plan §6` (linha 333).
   * **Descrição e justificativa:** O comando `ALTER DEFAULT PRIVILEGES FOR ROLE <owner> ...` usa `<owner>` sem explicitar textualmente que esse valor corresponde à variável `POSTGRES_USER` mencionada na frase anterior.

5. **Alternativa `127.0.0.1:8501` no proxy sem instrução correspondente no `docker-compose.yml`**
   * **Escopo afetado:** `Technical Plan §1 e §5` (linhas 91, 202) e `Detailed Implementation Plan §3` (linha 312).
   * **Descrição e justificativa:** As linhas 91 e 202 mencionam `127.0.0.1:8501` como alternativa caso o Traefik/EasyPanel opere fora da rede do Compose, mas o plano detalhado do `docker-compose.yml` (linha 312) menciona apenas `networks: [sro_network]` sem `ports`, omitindo a necessidade de publicar `127.0.0.1:8501:8501` nesse cenário.
