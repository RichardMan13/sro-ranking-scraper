# Stage 8 — Intermediate Zoom Out Review Report

* **Documento Auditado:** [003_analytical_dashboard.md](../../designs/003_analytical_dashboard.md) (linhas 1–418; `FeatureRange` 1–256, `DetailedRange` 257–418)
* **Referência Anterior:** [zoom_out_report.md](zoom_out_report.md) (Stage 5)
* **Veredito Geral:** **Aprovado com ressalva mínima (0 🔴 Blocker | 1 🟡 Major | 3 🟢 Nit).**

As revisões dos Stages 5–7 eliminaram de forma limpa os principais problemas do Stage 5 sem introduzir *bloat*, YAGNI ou *over-engineering*:
* **Resolvidos do Stage 5:** remoção das 3 variáveis de ambiente YAGNI de guardrails do PostgreSQL (linhas 110, 318, 325); migração dos DTOs de `BaseModel` para `@dataclass(frozen=True, slots=True)` alinhada ao Design 002 (linhas 112, 333); remoção de código morto (`sqlite://` nas linhas 319/369 e fallback `st.radio` nas linhas 55/114/362); restauração de altitude em `Technical Plan §2` (linhas 106–115) e `§5` (linhas 140–146); contrato determinístico de topologia *dual-network* (linhas 36, 74, 144, 283); `pool_timeout=5` (linhas 35, 111, 212, 325) e frescor via `idx_scrape_runs_server_latest` (linha 122).
* **Decisões conscientes mantidas sem inchaço:** aumento de `mem_limit` para `768m` + `MALLOC_ARENA_MAX=2` (justificado pelo RSS real de Streamlit/PyArrow na linha 103, ainda dentro da folga da KVM 2 de 8 GB), manutenção de `Dockerfile` único e preservação das views de `002_analytical_views.py` via ADR-014 (plan-time pruning em 2 passos).

---

## 🔴 Blocker

Nenhum *Blocker* encontrado.

---

## 🟡 Major

### 1. Atrito entre a nova suíte `@pytest.mark.postgres` e o gate obrigatório do `AGENTS.md`
* **Escopo Afetado:** `Detailed Implementation Plan #1` (linha 266), `#14` (linhas 373, 377) e `Verification Plan` (linhas 397–404).
* **Racional de Simplicidade / Contradição:** A inclusão da suíte opt-in `@pytest.mark.postgres` introduz um novo comando de teste (`uv run --no-sync pytest -m "not postgres" --cov=src tests/`, linha 399) que diverge do gate obrigatório padrão definido em `AGENTS.md` Seção 4 (`uv run --no-sync pytest --cov=src tests/`). Como `pyproject.toml` (`addopts = "-ra -q"`) não filtra o marcador `postgres` por padrão, executar o comando canônico do `AGENTS.md` em ambiente local/offline sem PostgreSQL ativo quebrará a suíte.
* **Recomendação:** Garantir no design (Item #14) que a fixture dos testes `@pytest.mark.postgres` execute `pytest.skip(...)` automaticamente quando a variável de conexão de teste/Postgres não estiver configurada, mantendo `uv run --no-sync pytest --cov=src tests/` funcional offline e preservando `pytest -m postgres` para execução explícita pré-deploy.

---

## 🟢 Nit

### 1. Novo diretório de topo `scripts/` omitido na atualização da árvore do `AGENTS.md`
* **Escopo Afetado:** `Detailed Implementation Plan #6` (linha 300) e `#15` (linha 382).
* **Racional:** O item #6 cria `scripts/provision_readonly_role.sh` (novo diretório raiz `scripts/`), mas o item #15 atualiza a árvore de diretórios do `AGENTS.md` apenas com `src/dashboard/`. Incluir `scripts/` na mesma atualização mantém o mapa do repositório consistente.

### 2. Risco residual de *drift* entre regras de domínio duplicadas em `queries.py` e `002_analytical_views.py`
* **Escopo Afetado:** `Technical Plan §3` (linhas 118, 127) e `Decision Log` (`ADR-014`, linha 195).
* **Racional:** Como os limiares de classificação (`0.90`, `150.0`, `0.60`, `0.50..0.89`, `HAVING COUNT(*) >= 6`) passam a existir em dois lugares, registrar em comentário no módulo `queries.py` a referência explícita à migração `002_analytical_views.py` como contrato espelhado.

### 3. Pequena repetição de literais de pool/guardrails ao longo de `FeatureRange`
* **Escopo Afetado:** `Goals G3` (linha 35), `Technical Plan §2` (linha 111), `§4` (linhas 136–138), `NFR §2` (linha 212) e `Detailed Implementation Plan #9` (linhas 325–327).
* **Racional:** Opcionalmente condensar nas seções de resumo e manter a enumeração completa apenas em `Technical Plan §4` e `Detailed Implementation Plan #9`.
