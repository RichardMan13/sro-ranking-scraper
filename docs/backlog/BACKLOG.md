# Backlog — Pequenos Fixes & Melhorias

> **Escopo Estrito:** Este backlog registra **apenas mudanças pequenas e pontuais** (bugfixes e pequenas melhorias incrementais) que **não cabem em `docs/designs/`**.
>
> **O que NÃO entra aqui:** Features estruturais ou mudanças arquiteturais (novas tabelas, novos serviços/módulos principais, novas dependências relevantes ou redesenho de fluxo) devem ir **diretamente** para o fluxo EGM em `docs/designs/` (indexado em `docs/designs/SITEMAP.md`).

---

## Regras de Organização e Fluxo

### 1. Estrutura de Diretórios

```text
docs/backlog/
├── BACKLOG.md          # Índice mestre (este arquivo) + regras de governança
├── fix/                # Um arquivo por correção/bug: FIX-NNN_<slug>.md
└── feature/            # Um arquivo por pequena melhoria/ajuste: FEAT-NNN_<slug>.md
```

### 2. Regras para `fix` (`docs/backlog/fix/`)

- **Um arquivo por fix:** Todo bug ou correção pontual pendente deve ter seu próprio arquivo em `docs/backlog/fix/FIX-NNN_<slug>.md` e ser listado na tabela **Fixes Pendentes** abaixo.
- **Conteúdo mínimo do arquivo:**
  - **Contexto / Sintoma:** O que está falhando e onde (arquivo/módulo/serviço).
  - **Causa Raiz (quando conhecida):** Origem do problema.
  - **Plano de Correção:** O que precisa ser alterado.
  - **Critério de Aceite & Regressão:** Teste unitário/fixture estática em `tests/` que reproduz o bug e garante que ele não volte.
- **Relação com `docs/designs/`:**
  - **Bug de implementação** (código divergiu da spec sem mudar arquitetura): **não** cria design doc novo; basta corrigir o código, adicionar teste de regressão e marcar o item como `Done` neste arquivo.
  - **Ajuste pontual que toca contrato já documentado:** atualizar a seção correspondente no design doc existente (`docs/designs/NNN_*.md`) em conjunto com o código.

### 3. Regras para `feature` (`docs/backlog/feature/`)

- **Apenas melhorias pequenas:** Use `docs/backlog/feature/FEAT-NNN_<slug>.md` somente para ajustes incrementais e melhorias de baixo escopo que **não** justifiquem um novo Design Doc (ex.: novo filtro/coluna no dashboard, melhoria de mensagem de alerta, ajuste de CLI/log, pequeno helper).
- **Conteúdo mínimo do arquivo:**
  - **Objetivo / Problema:** Por que a melhoria é necessária.
  - **Escopo Proposto:** Comportamento esperado e componentes impactados.
  - **Critérios de Aceite:** Como validar que está pronto.
- **Limite de escopo:** Se durante a análise a mudança deixar de ser pequena (exigir nova tabela, novo serviço, nova dependência ou alteração arquitetural relevante), **ela não pertence ao backlog** — encerre o item aqui como `Moved to Design` e abra o Design Doc diretamente em `docs/designs/NNN_<nome>.md`.

### 4. Ciclo de Status

- **Pending:** Registrado no backlog, aguardando priorização/execução.
- **In Progress:** Em investigação ou implementação ativa.
- **Done:** Implementado, verificado pelos gates do `AGENTS.md` e concluído.
- **Moved to Design:** Cresceu além de uma mudança pequena e foi movido para `docs/designs/NNN_*.md` (referenciar o link).
- **Won't Fix:** Descartado (registrar motivo no arquivo do item).

### 5. Templates Rápidos

#### Template `FIX-NNN_<slug>.md`

```markdown
# FIX-NNN: <Título curto>

- **Status:** Pending
- **Prioridade:** Alta | Média | Baixa
- **Componente:** scraper | flows | db | notifications | dashboard | infra
- **Design Relacionado:** [001_sro_ranking_scraper_pipeline.md](../../designs/001_sro_ranking_scraper_pipeline.md)
- **Criado em:** YYYY-MM-DD

## Sintoma
Descreva o problema observado.

## Causa Raiz
(Preencher durante a investigação.)

## Plano de Correção
- [ ] Passo 1
- [ ] Adicionar teste de regressão em `tests/`

## Critério de Aceite
- Gates do `AGENTS.md` passando (`ruff`, `mypy`, `pytest`).
```

#### Template `FEAT-NNN_<slug>.md`

```markdown
# FEAT-NNN: <Título curto>

- **Status:** Pending
- **Prioridade:** Alta | Média | Baixa
- **Componente:** scraper | flows | db | notifications | dashboard | infra
- **Criado em:** YYYY-MM-DD

## Objetivo
Por que esta pequena melhoria é necessária?

## Escopo Proposto
- ...

## Critérios de Aceite
- ...
```

---

## Fixes Pendentes (`docs/backlog/fix/`)

| ID | Arquivo | Componente | Prioridade | Status | Resumo |
| :--- | :--- | :--- | :--- | :--- | :--- |
| — | *Nenhum fix pendente registrado ainda* | — | — | — | — |

---

## Melhorias Pequenas Pendentes (`docs/backlog/feature/`)

| ID | Arquivo | Componente | Prioridade | Status | Resumo |
| :--- | :--- | :--- | :--- | :--- | :--- |
| FEAT-001 | [FEAT-001_chronicle_full_pagination_and_job_filters.md](feature/FEAT-001_chronicle_full_pagination_and_job_filters.md) | scraper | Alta | Pending | Extrair todas as páginas de `play-chronicle.com` com filtros `top-player`, `top-guild`, `top-trader`, `top-hunter` e `top-thieves` |
| FEAT-002 | [FEAT-002_rsilkroad_full_pagination_scraper.md](feature/FEAT-002_rsilkroad_full_pagination_scraper.md) | scraper | Alta | Pending | Adicionar extração completa de todas as páginas de `rsilkroad.com` (`/Rank/Player` e `/Rank/Guild` com `pageSize=100`) |

---

## Histórico de Concluídos

| ID | Tipo | Arquivo | Data | Status | Referência |
| :--- | :--- | :--- | :--- | :--- | :--- |
| FIX-001 | fix | [FIX-001_hyperfilter_js_challenge_detection.md](fix/FIX-001_hyperfilter_js_challenge_detection.md) | 2026-11-23 | Done | `src/scrapers/base.py`, `tests/fixtures/electus_hyperfilter_200.html` |
| FIX-002 | fix | [FIX-002_diamwall_513_challenge_detection.md](fix/FIX-002_diamwall_513_challenge_detection.md) | 2026-11-23 | Done | `src/scrapers/base.py`, `tests/fixtures/golden_diamwall_513.html` |
| FIX-003 | fix | [FIX-003_chronicle_ranking_routes_and_layout.md](fix/FIX-003_chronicle_ranking_routes_and_layout.md) | 2026-11-23 | Done | `src/scrapers/chronicle.py`, `src/models/pydantic_schemas.py`, `src/scrapers/base.py` |
