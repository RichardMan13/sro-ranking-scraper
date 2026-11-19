# AGENTS.md — Governança & Memória Global do Repositório

Este arquivo define as regras obrigatórias para qualquer agente de execução (Goldfish) operando neste repositório sob o **Elephant-Goldfish Model (EGM)**.

---

## 1. Regra de Ouro do SDD (Spec-Driven Development)

> **Nunca escreva código sem antes ler `docs/designs/SITEMAP.md` e o design doc correspondente em `docs/designs/`.**

- O design doc aprovado é a **fonte única de verdade** arquitetural.
- Não invente convenções, tabelas, colunas, nomes de módulos ou dependências fora do especificado em `docs/designs/`.
- Caso uma mudança de implementação exija alterar o design ou registrar uma nova decisão técnica, atualize o design doc e o `docs/designs/SITEMAP.md` antes ou em conjunto com o código.

---

## 2. Restrições Arquiteturais Não Negociáveis

### Stack Obrigatória
- **Linguagem:** Python 3.11
- **Orquestração:** Prefect 3 (self-hosted)
- **Cliente HTTP & Parsing:** `httpx[http2]` (`AsyncClient`), `lxml`, `beautifulsoup4`, `orjson`
- **Validação & Configuração:** Pydantic v2 (`pydantic`, `pydantic-settings`)
- **Persistência & Migrações:** SQLAlchemy 2.0 (`Mapped[...]`, `asyncpg` via `postgresql+asyncpg://`), Alembic, PostgreSQL 16
- **Observabilidade:** `structlog` (logs estruturados em JSON contextualizados com `server_id` e `run_id`)

### Proibições Estritas
- **PROIBIDO usar browsers headless:** Nunca introduza Playwright, Selenium, Puppeteer ou qualquer navegador automatizado.
- **PROIBIDO usar infraestrutura pesada ou bypasses pagos:** Nunca introduza Apache Airflow, Apache Spark, serviços comerciais de resolução de CAPTCHA ou pools de proxies pagos.
- **PROIBIDO gravar HTML bruto no PostgreSQL:** Payloads inválidos ou bloqueados nunca devem ir para colunas `TEXT`/`BYTEA`; use exclusivamente a quarentena local em disco.
- **PROIBIDO secrets hardcoded:** Todas as credenciais e tokens (`DATABASE_URL`, `PREFECT_API_URL`, `cf_clearance`, etc.) devem vir de variáveis de ambiente (`.env` / `pydantic-settings`).

### Padrões de Dados, Resiliência & Recursos
- **Append-Only & Auditoria:** Tabelas de fatos (`character_snapshots`, `guild_snapshots`) são estritamente aditivas, particionadas por `RANGE (created_at)` com chave primária composta `PRIMARY KEY (id, created_at)` e colunas `created_at` / `updated_at`. 100% das execuções devem ser registradas em `scrape_runs`.
- **Circuit Breaker Persistente:** Antes de qualquer requisição HTTP, consulte `servers.circuit_breaker_until`. Em caso de HTTP 403/429 ou desafio Cloudflare/Turnstile, ative o cooldown (padrão 6h) na tabela `servers`, grave status `BLOCKED_CHALLENGE` em `scrape_runs` e aborte sem retentativas agressivas.
- **Top-Rank Integrity Guard & Quarentena:** Rejeite o lote se posições 1, 2 ou 3 falharem na validação, se a taxa de erro exceder 5% ou se a contagem ficar abaixo de `min_records_expected`. Compacte payloads rejeitados em `./quarantine/{server_id}/{run_id}.html.gz`.
- **Limites de Memória e Performance:**
  - Container `scraper-worker`: teto estrito de **512MB de RAM**.
  - Tempo máximo de execução por alvo: **180 segundos** (semáforo HTTP <= 3 conexões concorrentes).
  - Persistência em lote via pré-resolução de dimensões + bulk insert (< 500ms por snapshot).

---

## 3. Estrutura do Repositório

```text
.
├── docs/designs/          # Memória do Elefante (SITEMAP.md e design docs aprovados)
├── src/
│   ├── core/              # config.py, database.py, logging.py
│   ├── models/            # pydantic_schemas.py, db_models.py
│   ├── scrapers/          # base.py, chronicle.py, electus.py, golden.py
│   └── flows/             # tasks.py, orchestrator.py
├── alembic/               # Migrações de schema, partições e views analíticas SQL
├── tests/                 # Testes unitários (parsers, validação) e fixtures/
├── quarantine/            # Dead-letter local (.html.gz, ignorado no git)
└── pyproject.toml         # Dependências + configuração de ruff, mypy (strict) e pytest
```

---

## 4. Padrões de Código e Checklist de Verificação

- Todo código Python deve possuir tipagem estática completa compatível com `mypy --strict`.
- Toda lógica de parsing e validação deve ter cobertura de testes unitários usando fixtures estáticas em `tests/fixtures/` (sem chamadas de rede reais nos testes unitários).
- **Gate obrigatório antes de concluir qualquer tarefa:**

```bash
# 1. Lint e formatação (Ruff)
ruff check .
ruff format --check .

# 2. Verificação de tipos estrita (Mypy)
mypy src tests

# 3. Suíte de testes unitários e cobertura (Pytest)
pytest --cov=src tests/

# 4. Migrações de banco de dados (quando houver alterações de schema/views)
alembic upgrade head
```

> Nenhuma tarefa deve ser considerada concluída se qualquer um dos comandos aplicáveis acima falhar.
