# FIX-002: Detectar bloqueio anti-bot DiamWall (HTTP 513) como `BLOCKED_CHALLENGE`

- **Status:** Done
- **Prioridade:** Alta
- **Componente:** scraper
- **Design Relacionado:** [001_sro_ranking_scraper_pipeline.md](../../designs/001_sro_ranking_scraper_pipeline.md)
- **Criado em:** 2026-11-23

## Sintoma
As execuções do servidor `play_golden` (`https://play-golden.online`) falham com status `HTTP_ERROR` e HTTP `513` (`"HTTP 513 fetching https://play-golden.online/ranking/characters?page=1"`), sem gravar quarentena e sem ativar o circuit breaker de 6h.

## Causa Raiz
A sondagem ao vivo em `https://play-golden.online/` e `/ranking/characters?page=1` confirmou que o **HTTP 513** é devolvido pelo WAF/anti-bot **DiamWall** (`server: openresty`, `set-cookie: __diamwall=...`, `<title>Verifying your browser | DiamWall</title>`, `iframe src="/.well-known/diamwall/load/html/5s.html"`, `cdn.diamwall.com`).  
Em `src/scrapers/base.py`, `_is_challenge_response` não reconhece os marcadores do DiamWall nem o status `513`, fazendo `_fetch_url` cair em `TransportFetchError` (`HTTP_ERROR`).

## Plano de Correção
- [x] Adicionar marcadores específicos do DiamWall em `CHALLENGE_HTML_MARKERS` (`"verifying your browser | diamwall"`, `"/.well-known/diamwall/"`) e checagem correlacionada de HTTP `513` + cookie `__diamwall` via `response.headers.get_list("set-cookie")` em `_is_challenge_response`.
- [x] Garantir que a resposta DiamWall HTTP `513` dispare `BlockedChallengeError` (`BLOCKED_CHALLENGE`), grave payload em quarentena e ative `servers.circuit_breaker_until`.
- [x] Adicionar fixture estática (`tests/fixtures/golden_diamwall_513.html`) e testes unitários de regressão (positivos e negativos) em `tests/test_parsers.py`.

## Critério de Aceite
- `PlayGoldenScraper` / `BaseRankingScraper` classifica o desafio DiamWall HTTP `513` como `BLOCKED_CHALLENGE`, grava quarentena `.html.gz` e ativa o circuit breaker de 6h.
- Gates do `AGENTS.md` passando (`ruff`, `mypy`, `pytest`).
