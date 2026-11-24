# FIX-001: Detectar desafio JS/cookie HyperFilter (HTTP 200) como `BLOCKED_CHALLENGE`

- **Status:** Done
- **Prioridade:** Alta
- **Componente:** scraper
- **Design Relacionado:** [001_sro_ranking_scraper_pipeline.md](../../designs/001_sro_ranking_scraper_pipeline.md)
- **Criado em:** 2026-11-23

## Sintoma
As execuções do servidor `electus` (`https://electus.online`) falham com status `SCHEMA_DRIFT` (`"Zero records extracted across all ranking categories"`) e HTTP `200`, sem ativar o circuit breaker de 6h (`servers.circuit_breaker_until` permanece `NULL`).

## Causa Raiz
O servidor `electus` responde com **HTTP 200** contendo uma página de desafio anti-bot JS/cookie do **HyperFilter** (`/hf-sys/hf-lib.php`, `/hf-sys/hf-help.php`, `createCookie("__HFUID"`, `<noscript>This website requires javascript/cookies support in order to operate properly.</noscript>`).  
Em `src/scrapers/base.py`, `CHALLENGE_HTML_MARKERS` verifica apenas assinaturas Cloudflare/Turnstile, fazendo o scraper ignorar o desafio e seguir para o parsing HTML vazio.

## Plano de Correção
- [x] Adicionar marcadores específicos do desafio HyperFilter em `CHALLENGE_HTML_MARKERS` (`src/scrapers/base.py`: `"/hf-sys/hf-lib.php"` e `'createcookie("__hfuid"'`), evitando falsos positivos em páginas normais com `<noscript>`.
- [x] Garantir que HTTP `200` com desafio HyperFilter dispare `BlockedChallengeError` (`BLOCKED_CHALLENGE`), grave payload em quarentena e ative `servers.circuit_breaker_until` (sem retentativas nem tentativa de bypass).
- [x] Adicionar fixture estática (`tests/fixtures/electus_hyperfilter_200.html`) e testes unitários de regressão em `tests/test_parsers.py`.

## Critério de Aceite
- `ElectusScraper` / `BaseRankingScraper` classifica a página HyperFilter como `BLOCKED_CHALLENGE` na primeira requisição (`player`), grava quarentena `.html.gz` e ativa o circuit breaker.
- Gates do `AGENTS.md` passando (`ruff`, `mypy`, `pytest`).
