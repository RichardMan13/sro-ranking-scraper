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
  - `rsilkroad_inter_batch_delay_min: float = Field(default=0.35, ge=0.0, allow_inf_nan=False)` e `rsilkroad_inter_batch_delay_max: float = Field(default=0.75, ge=0.0, allow_inf_nan=False)` (`RSILKROAD_INTER_BATCH_DELAY_MIN` / `RSILKROAD_INTER_BATCH_DELAY_MAX`), validando `min <= max` via `@model_validator(mode="after")`.
  - `rsilkroad_cf_clearance: SecretStr | None = None` (`RSILKROAD_CF_CLEARANCE`), validando **antes** do `strip()` a ausência de `\r`, `\n` e `;` (levantando `ValueError` com mensagem estática sem ecoar o valor, mantendo `hide_input_in_errors=True`, para evitar header/cookie injection) e normalizando string vazia/espaços (`" \t"`) para `None` (reaproveitando o padrão `mode="before"` de `normalize_discord_webhook_url`). Mantém `electus_cf_clearance: str | None` inalterado para preservar compatibilidade.
- **Integração em [src/scrapers/rsilkroad.py](../../../src/scrapers/rsilkroad.py):**
  - Alterar `page_size: int | None = None` e `inter_batch_delay_range: tuple[float, float] | None = None` em `RSilkroadScraper.__init__` (seguindo `max_pages_per_category: int | None = None`), lendo os valores de `self.settings` quando omitidos; validar `1 <= page_size <= 100` e `math.isfinite` nos limites de delay.
  - Adicionar `_cf_cookies(self) -> dict[str, str] | None` (seguindo o padrão real de `ElectusScraper._cf_cookies()`) lendo `self.settings.rsilkroad_cf_clearance.get_secret_value()` quando definido, e repassar `cookies=self._cf_cookies()` em `_fetch_single_page()` para `self._fetch_url(...)`, sem logar o valor do cookie.
  - Manter `_RSILKROAD_PAGE_SIZE` e `DEFAULT_INTER_BATCH_DELAY_RANGE` alinhados aos defaults de `Settings` e atualizar as docstrings de `fetch_player_pages` / `fetch_guild_pages` que citam `pageSize=100` fixo.
- **Infraestrutura e documentação:**
  - Documentar as variáveis em [.env.example](../../../.env.example) com defaults explícitos para as numéricas (`RSILKROAD_PAGE_SIZE=100`, `RSILKROAD_INTER_BATCH_DELAY_MIN=0.35`, `RSILKROAD_INTER_BATCH_DELAY_MAX=0.75`) e `RSILKROAD_CF_CLEARANCE=` vazia.
  - Repassá-las em `scraper-worker.environment` no [docker-compose.yml](../../../docker-compose.yml) com fallback `:-` para as numéricas (`${RSILKROAD_PAGE_SIZE:-100}`, etc.) e `${RSILKROAD_CF_CLEARANCE:-}` para o cookie.
  - Sincronizar [001_sro_ranking_scraper_pipeline.md](../../designs/001_sro_ranking_scraper_pipeline.md) atualizando a tabela de alvos da Seção 3 (`rsilkroad`, removendo `pageSize=100` fixo e citando o cookie/throttling configurável) e as seções de variáveis de ambiente ao lado de `ELECTUS_CF_CLEARANCE`.
- **Testes unitários:**
  - Em [tests/test_models_db.py](../../../tests/test_models_db.py): incluir as 4 variáveis `RSILKROAD_*` no `monkeypatch.delenv(...)` de `test_settings_defaults_and_env_overrides`, asserindo defaults em `Settings(_env_file=None)` e overrides via `monkeypatch.setenv(...)` em `get_settings()` (incluindo `isinstance(..., SecretStr)` e `.get_secret_value()`), além de regras de borda (`page_size < 1` e `> 100`, delays `< 0`, `min > max`, `inf`/`nan`, string vazia -> `None`, rejeição de `\r`/`\n`/`;` mesmo nas extremidades e ausência de vazamento do segredo em `ValidationError`).
  - Em [tests/test_parsers.py](../../../tests/test_parsers.py): validar defaults via `Settings`, override e validações de erro via `__init__` (`page_size < 1` / `> 100`, `inter_batch_delay_range` negativo, invertido ou não-finito), propagação real de `rsilkroad_page_size` na query string (`?page=1&pageSize=<N>`), injeção condicional do cookie `cf_clearance` via `httpx.MockTransport` (padrão `test_electus_cf_clearance_cookie_injection`) e ausência do valor de `RSILKROAD_CF_CLEARANCE` nos logs JSON estruturados.

## Critérios de Aceite
- É possível ajustar `RSILKROAD_PAGE_SIZE`, `RSILKROAD_INTER_BATCH_DELAY_MIN`, `RSILKROAD_INTER_BATCH_DELAY_MAX` e `RSILKROAD_CF_CLEARANCE` exclusivamente via `.env` / ambiente do `scraper-worker`, preservando o comportamento atual quando omitidos.
- Nenhum valor de `RSILKROAD_CF_CLEARANCE` vaza em `ValidationError` ou logs JSON estruturados.
- Gates do `AGENTS.md` passando (`ruff check .`, `ruff format --check .`, `mypy src tests`, `pytest --cov=src tests/`).
