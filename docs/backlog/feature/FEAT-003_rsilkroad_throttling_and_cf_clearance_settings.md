# FEAT-003: Configurar throttling, `page_size` e `cf_clearance` do `RSilkroadScraper` via `Settings`

- **Status:** Pending
- **Prioridade:** Média
- **Componente:** scraper
- **Design Relacionado:** [001_sro_ranking_scraper_pipeline.md](../../designs/001_sro_ranking_scraper_pipeline.md)
- **Criado em:** 2026-11-24

## Objetivo
Permitir ajustar em runtime (via variáveis de ambiente / `.env`) o ritmo de paginação e a eventual autenticação Cloudflare do `RSilkroadScraper` ([src/scrapers/rsilkroad.py](../../../src/scrapers/rsilkroad.py)), sem precisar alterar código nem rebuildar a imagem Docker caso o alvo volte a aplicar `HTTP 429` (`BLOCKED_CHALLENGE`) sob a configuração padrão atual (`DEFAULT_INTER_BATCH_DELAY_RANGE = (0.35, 0.75)` e `page_size = 100`).

## Escopo Proposto
- **Configuração em [src/core/config.py](../../../src/core/config.py) (`Settings`):**
  - `rsilkroad_page_size: int = Field(default=100, ge=1, le=100)` (`RSILKROAD_PAGE_SIZE`).
  - `rsilkroad_inter_batch_delay_min: float = Field(default=0.35, ge=0.0)` e `rsilkroad_inter_batch_delay_max: float = Field(default=0.75, ge=0.0)` (`RSILKROAD_INTER_BATCH_DELAY_MIN` / `RSILKROAD_INTER_BATCH_DELAY_MAX`), validando `min <= max`.
  - `rsilkroad_cf_clearance: SecretStr | None = None` (`RSILKROAD_CF_CLEARANCE`), normalizando string vazia/espaços para `None` (mesmo padrão seguro de `Settings` com `hide_input_in_errors=True`).
- **Integração em [src/scrapers/rsilkroad.py](../../../src/scrapers/rsilkroad.py):**
  - Ler `page_size` e `inter_batch_delay_range` de `self.settings` como padrão em `RSilkroadScraper.__init__` quando não forem passados explicitamente por argumento.
  - Sobrescrever `_build_client_cookies()` (seguindo o padrão de `ElectusScraper`) para injetar o cookie `cf_clearance` quando `settings.rsilkroad_cf_clearance` estiver definido, sem logar o valor do cookie.
- **Infraestrutura e documentação:**
  - Documentar as variáveis opcionais em [.env.example](../../../.env.example) e repassá-las em `scraper-worker.environment` no [docker-compose.yml](../../../docker-compose.yml).
- **Testes unitários:**
  - Validar carregamento e regras de borda das novas variáveis (`min > max`, `page_size < 1`, string vazia -> `None`) em [tests/test_models_db.py](../../../tests/test_models_db.py).
  - Validar que `RSilkroadScraper` aplica os defaults de `Settings`, permite override via `__init__` e injeta o cookie `cf_clearance` apenas quando configurado em [tests/test_parsers.py](../../../tests/test_parsers.py).

## Critérios de Aceite
- É possível ajustar `RSILKROAD_PAGE_SIZE`, `RSILKROAD_INTER_BATCH_DELAY_MIN`, `RSILKROAD_INTER_BATCH_DELAY_MAX` e `RSILKROAD_CF_CLEARANCE` exclusivamente via `.env` / ambiente do `scraper-worker`, preservando o comportamento atual quando omitidos.
- Nenhum valor de `RSILKROAD_CF_CLEARANCE` vaza em `ValidationError` ou logs JSON estruturados.
- Gates do `AGENTS.md` passando (`ruff check .`, `ruff format --check .`, `mypy src tests`, `pytest --cov=src tests/`).
