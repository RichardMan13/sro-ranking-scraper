from __future__ import annotations

import asyncio
import gzip
import hashlib
import selectors
import socket
import uuid
from collections.abc import Coroutine, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from typing import Any, TypeVar, cast

import httpx
import pytest
import respx
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.dml import Insert, Update
from src.core.config import Settings
from src.core.database import create_engine_from_url
from src.models.db_models import (
    ScrapeRun,
    Server,
)
from src.scrapers.base import (
    DEFAULT_HEADER_PROFILES,
    BaseRankingScraper,
    BlockedChallengeError,
    CategoryFetchResult,
    PagePayload,
    ScraperFetchError,
    ScrapeRunSummary,
    TargetMaintenanceError,
    TransportFetchError,
)

_T = TypeVar("_T")


# ---------------------------------------------------------------------------
# Dual-mode async runner for sandboxed Windows & normal host/Linux CI
# ---------------------------------------------------------------------------


@lru_cache(maxsize=1)
def _loopback_sockets_allowed() -> bool:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.settimeout(0.5)
        conn.settimeout(0.5)
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        addr = sock.getsockname()
        conn.connect(addr)
        accepted, _ = sock.accept()
        accepted.close()
        return True
    except OSError:
        return False
    finally:
        conn.close()
        sock.close()


class _DummySelector(selectors.BaseSelector):
    """Minimal selector that avoids OS sockets/pipes inside Windows sandboxes."""

    def register(
        self,
        fileobj: Any,
        events: int,
        data: Any = None,
    ) -> selectors.SelectorKey:
        raise NotImplementedError("Offline selector does not register file descriptors")

    def unregister(self, fileobj: Any) -> selectors.SelectorKey:
        raise NotImplementedError("Offline selector does not unregister file descriptors")

    def modify(
        self,
        fileobj: Any,
        events: int,
        data: Any = None,
    ) -> selectors.SelectorKey:
        raise NotImplementedError("Offline selector does not modify file descriptors")

    def select(
        self,
        timeout: float | None = None,
    ) -> list[tuple[selectors.SelectorKey, int]]:
        if timeout is not None and timeout > 0:
            import time

            time.sleep(min(timeout, 0.02))
        return []

    def get_map(self) -> dict[Any, selectors.SelectorKey]:
        return {}


class _OfflineEventLoop(asyncio.BaseEventLoop):
    """Pure-Python asyncio loop avoiding `socket.socketpair()` in restricted sandboxes."""

    def __init__(self) -> None:
        super().__init__()
        self._selector = _DummySelector()

    def _write_to_self(self) -> None:
        return None

    def _process_events(self, event_list: list[tuple[selectors.SelectorKey, int]]) -> None:
        return None

    def close(self) -> None:
        if not self.is_closed():
            self._selector.close()
            super().close()


def _run_async(coro: Coroutine[Any, Any, _T]) -> _T:
    if _loopback_sockets_allowed():
        return asyncio.run(coro)
    loop = _OfflineEventLoop()
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(coro)
    finally:
        asyncio.set_event_loop(None)
        loop.close()


# ---------------------------------------------------------------------------
# Fake AsyncSession & Test Helpers
# ---------------------------------------------------------------------------


class _FakeResult:
    def __init__(self, rows: list[tuple[uuid.UUID, str]]) -> None:
        self._rows = rows

    def all(self) -> list[tuple[uuid.UUID, str]]:
        return self._rows


class FakeAsyncSession:
    """In-memory typed AsyncSession double capturing SQL statements and chunks."""

    def __init__(
        self,
        server: Server | None,
        *,
        fail_on_snapshot_insert: Exception | None = None,
    ) -> None:
        self.server = server
        self.fail_on_snapshot_insert = fail_on_snapshot_insert
        self.added_runs: list[ScrapeRun] = []
        self.server_updates: list[dict[str, Any]] = []
        self.guild_upsert_chunks: list[list[dict[str, Any]]] = []
        self.char_upsert_chunks: list[list[dict[str, Any]]] = []
        self.char_snapshot_chunks: list[list[dict[str, Any]]] = []
        self.guild_snapshot_chunks: list[list[dict[str, Any]]] = []
        self.executed_statements: list[Any] = []
        self.guild_ids: dict[str, uuid.UUID] = {}
        self.char_ids: dict[str, uuid.UUID] = {}
        self.flush_count = 0
        self.commit_count = 0
        self.rollback_count = 0

    async def scalar(self, stmt: Any) -> Server | None:
        self.executed_statements.append(stmt)
        return self.server

    def add(self, instance: object) -> None:
        if isinstance(instance, ScrapeRun):
            self.added_runs.append(instance)

    async def flush(self) -> None:
        self.flush_count += 1

    async def commit(self) -> None:
        self.commit_count += 1

    async def rollback(self) -> None:
        self.rollback_count += 1

    async def execute(self, stmt: Any, params: Any = None) -> _FakeResult:
        self.executed_statements.append(stmt)
        if isinstance(stmt, Update) and getattr(stmt.table, "name", "") == "servers":
            pg_engine = create_engine_from_url("postgresql+asyncpg://localhost/sro")
            try:
                compiled = stmt.compile(dialect=pg_engine.dialect)
                self.server_updates.append(dict(compiled.params))
            finally:
                pg_engine.sync_engine.dispose()
            return _FakeResult([])

        if isinstance(stmt, Insert):
            table_name = stmt.table.name
            if table_name == "guilds":
                multi = getattr(stmt, "_multi_values", ())
                raw_rows = list(multi[0]) if multi else []
                rows: list[dict[str, Any]] = [
                    {getattr(k, "name", str(k)): v for k, v in row.items()} for row in raw_rows
                ]
                self.guild_upsert_chunks.append(rows)
                returned: list[tuple[uuid.UUID, str]] = []
                for row in rows:
                    name = str(row["guild_name"])
                    gid = self.guild_ids.setdefault(name, uuid.uuid4())
                    returned.append((gid, name))
                return _FakeResult(returned)

            if table_name == "characters":
                multi = getattr(stmt, "_multi_values", ())
                raw_rows = list(multi[0]) if multi else []
                rows = [{getattr(k, "name", str(k)): v for k, v in row.items()} for row in raw_rows]
                self.char_upsert_chunks.append(rows)
                returned = []
                for row in rows:
                    name = str(row["char_name"])
                    cid = self.char_ids.setdefault(name, uuid.uuid4())
                    returned.append((cid, name))
                return _FakeResult(returned)

            if table_name == "character_snapshots":
                if self.fail_on_snapshot_insert is not None:
                    raise self.fail_on_snapshot_insert
                chunk = [dict(item) for item in cast(Sequence[dict[str, Any]], params or [])]
                self.char_snapshot_chunks.append(chunk)
                return _FakeResult([])

            if table_name == "guild_snapshots":
                if self.fail_on_snapshot_insert is not None:
                    raise self.fail_on_snapshot_insert
                chunk = [dict(item) for item in cast(Sequence[dict[str, Any]], params or [])]
                self.guild_snapshot_chunks.append(chunk)
                return _FakeResult([])

        return _FakeResult([])


def _make_settings(tmp_path: Path, cooldown_hours: int = 6) -> Settings:
    return Settings(
        _env_file=None,
        database_url="postgresql+asyncpg://user:pass@localhost:5432/sro_test",
        quarantine_dir=tmp_path / "quarantine",
        circuit_breaker_cooldown_hours=cooldown_hours,
    )


def _make_server(
    *,
    server_id: str = "chronicle",
    min_records_expected: int = 10,
    circuit_breaker_until: datetime | None = None,
) -> Server:
    return Server(
        id=server_id,
        display_name="Chronicle SRO",
        base_url="https://play-chronicle.com",
        min_records_expected=min_records_expected,
        circuit_breaker_until=circuit_breaker_until,
    )


def _make_player_records(count: int, *, start_rank: int = 1) -> list[dict[str, Any]]:
    return [
        {
            "rank": start_rank + idx,
            "char_name": f"Player_{start_rank + idx}",
            "level": 110,
            "guild_name": f"Guild_{(idx % 5) + 1}",
            "race": "CH" if idx % 2 == 0 else "EU",
            "item_points": 1000.0 + idx,
            "extra_metrics": {"pvp_kd": "5/1"},
        }
        for idx in range(count)
    ]


def _make_guild_records(count: int, *, start_rank: int = 1) -> list[dict[str, Any]]:
    return [
        {
            "rank": start_rank + idx,
            "guild_name": f"Guild_{start_rank + idx}",
            "master_name": f"Player_{start_rank + idx}",
            "members_count": 40 + idx,
            "total_item_points": 50000.0 + idx,
            "fortress_held": "Jangan" if idx == 0 else None,
        }
        for idx in range(count)
    ]


class _StubScraper(BaseRankingScraper):
    server_id = "chronicle"

    def __init__(
        self,
        settings: Settings,
        *,
        player_result: CategoryFetchResult | None = None,
        job_result: CategoryFetchResult | None = None,
        guild_result: CategoryFetchResult | None = None,
        retry_delays: tuple[float, ...] = (0.0, 0.0),
        timeout_budget_seconds: float = 180.0,
    ) -> None:
        super().__init__(
            settings,
            retry_delays=retry_delays,
            timeout_budget_seconds=timeout_budget_seconds,
        )
        self._player_result = player_result or CategoryFetchResult()
        self._job_result = job_result or CategoryFetchResult()
        self._guild_result = guild_result or CategoryFetchResult()

    async def fetch_player_pages(
        self,
        client: httpx.AsyncClient,
        base_url: str,
    ) -> CategoryFetchResult:
        return self._player_result

    async def fetch_job_pages(
        self,
        client: httpx.AsyncClient,
        base_url: str,
    ) -> CategoryFetchResult:
        return self._job_result

    async def fetch_guild_pages(
        self,
        client: httpx.AsyncClient,
        base_url: str,
    ) -> CategoryFetchResult:
        return self._guild_result


class _HttpFetchingScraper(BaseRankingScraper):
    server_id = "chronicle"

    def __init__(
        self,
        settings: Settings,
        *,
        retry_delays: tuple[float, ...] = (2.0, 5.0),
        timeout_budget_seconds: float = 180.0,
    ) -> None:
        super().__init__(
            settings,
            retry_delays=retry_delays,
            timeout_budget_seconds=timeout_budget_seconds,
        )

    async def fetch_player_pages(
        self,
        client: httpx.AsyncClient,
        base_url: str,
    ) -> CategoryFetchResult:
        resp = await self._fetch_url(client, f"{base_url}/ranking/player", category="player")
        return CategoryFetchResult(
            raw_records=_make_player_records(10),
            raw_payloads=[
                PagePayload(
                    url=str(resp.url),
                    content=resp.content,
                    status_code=resp.status_code,
                    category="player",
                )
            ],
            http_status_code=resp.status_code,
        )

    async def fetch_job_pages(
        self,
        client: httpx.AsyncClient,
        base_url: str,
    ) -> CategoryFetchResult:
        resp = await self._fetch_url(client, f"{base_url}/ranking/job", category="job")
        return CategoryFetchResult(
            raw_records=[
                {
                    "rank": 1,
                    "char_name": "Player_1",
                    "job_type": "TRADER",
                    "job_level": 100,
                    "job_exp": 99999,
                }
            ],
            raw_payloads=[
                PagePayload(
                    url=str(resp.url),
                    content=resp.content,
                    status_code=resp.status_code,
                    category="job",
                )
            ],
            http_status_code=resp.status_code,
        )

    async def fetch_guild_pages(
        self,
        client: httpx.AsyncClient,
        base_url: str,
    ) -> CategoryFetchResult:
        resp = await self._fetch_url(client, f"{base_url}/ranking/guild", category="guild")
        return CategoryFetchResult(
            raw_records=_make_guild_records(5),
            raw_payloads=[
                PagePayload(
                    url=str(resp.url),
                    content=resp.content,
                    status_code=resp.status_code,
                    category="guild",
                )
            ],
            http_status_code=resp.status_code,
        )


def _verify_quarantine_artifact(
    *,
    summary: ScrapeRunSummary,
    run_row: ScrapeRun,
    expected_dir: Path,
    expected_substrings: Sequence[bytes] = (),
) -> None:
    assert summary.quarantine_file_path is not None
    assert summary.quarantine_sha256 is not None
    assert summary.quarantine_size_bytes is not None
    q_path = Path(summary.quarantine_file_path)
    assert q_path.parent == expected_dir / summary.server_id
    assert q_path.name == f"{summary.run_id}.html.gz"
    assert q_path.exists()

    gz_bytes = q_path.read_bytes()
    assert len(gz_bytes) == summary.quarantine_size_bytes
    assert hashlib.sha256(gz_bytes).hexdigest() == summary.quarantine_sha256
    assert run_row.quarantine_file_path == summary.quarantine_file_path
    assert run_row.quarantine_sha256 == summary.quarantine_sha256
    assert run_row.quarantine_size_bytes == summary.quarantine_size_bytes
    assert run_row.records_persisted == 0

    decompressed = gzip.decompress(gz_bytes)
    for snippet in expected_substrings:
        assert snippet in decompressed


# ---------------------------------------------------------------------------
# 1. Pre-HTTP Circuit Breaker Tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "cb_offset",
    [
        timedelta(hours=2),
        # Also exercise naive datetime normalization when DB driver returns naive timestamp
        timedelta(minutes=30),
    ],
)
def test_pre_http_circuit_breaker_skips_without_network_or_scrape_runs(
    tmp_path: Path,
    cb_offset: timedelta,
) -> None:
    settings = _make_settings(tmp_path)
    cb_time = datetime.now(UTC) + cb_offset
    if cb_offset == timedelta(minutes=30):
        cb_time = cb_time.replace(tzinfo=None)

    server = _make_server(circuit_breaker_until=cb_time)
    session = FakeAsyncSession(server)
    http_calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal http_calls
        http_calls += 1
        return httpx.Response(200, text="<html></html>")

    scraper = _HttpFetchingScraper(settings)
    client = httpx.AsyncClient(http2=True, transport=httpx.MockTransport(handler))

    async def execute() -> ScrapeRunSummary:
        async with client:
            return await scraper.run(cast(AsyncSession, session), client=client)

    summary = _run_async(execute())

    assert summary.skipped is True
    assert summary.status == "SKIPPED_CIRCUIT_BREAKER"
    assert summary.circuit_breaker_until is not None
    assert summary.circuit_breaker_until.tzinfo == UTC
    assert http_calls == 0
    assert session.added_runs == []
    assert session.commit_count == 0
    assert session.rollback_count == 1


def test_expired_circuit_breaker_proceeds_with_scrape(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    expired_cb = datetime.now(UTC) - timedelta(seconds=1)
    server = _make_server(circuit_breaker_until=expired_cb)
    session = FakeAsyncSession(server)

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="<html>ok</html>")

    scraper = _HttpFetchingScraper(settings, retry_delays=(0.0, 0.0))
    client = httpx.AsyncClient(http2=True, transport=httpx.MockTransport(handler))

    async def execute() -> ScrapeRunSummary:
        async with client:
            return await scraper.run(cast(AsyncSession, session), client=client)

    summary = _run_async(execute())
    assert summary.skipped is False
    assert summary.status == "SUCCESS"
    assert session.commit_count == 1


# ---------------------------------------------------------------------------
# 2. Anti-Bot Challenge Detection (`BLOCKED_CHALLENGE`)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("status_code", "headers", "body"),
    [
        (403, {}, "<html><body>Forbidden</body></html>"),
        (429, {}, "<html><body>Too Many Requests</body></html>"),
        (200, {"cf-mitigated": "challenge"}, "<html><body>Challenge</body></html>"),
        (200, {}, '<html><body><div id="turnstile-wrapper"></div></body></html>'),
        (200, {}, '<html><body><div class="cf-turnstile"></div></body></html>'),
        (200, {}, "<html><body><script>cf-chl-bypass</script></body></html>"),
        (200, {}, "<html><title>Just a moment...</title></html>"),
        (200, {}, "<html><title>Attention Required! | Cloudflare</title></html>"),
    ],
)
def test_challenge_detection_trips_circuit_breaker_without_retry_and_quarantines(
    tmp_path: Path,
    status_code: int,
    headers: dict[str, str],
    body: str,
) -> None:
    settings = _make_settings(tmp_path, cooldown_hours=6)
    server = _make_server()
    session = FakeAsyncSession(server)
    attempts = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(status_code, headers=headers, text=body)

    scraper = _HttpFetchingScraper(settings, retry_delays=(2.0, 5.0))
    client = httpx.AsyncClient(http2=True, transport=httpx.MockTransport(handler))

    async def execute() -> ScrapeRunSummary:
        async with client:
            return await scraper.run(cast(AsyncSession, session), client=client)

    before_ts = datetime.now(UTC)
    summary = _run_async(execute())

    assert attempts == 1  # Never retried
    assert summary.status == "BLOCKED_CHALLENGE"
    assert summary.http_status_code == status_code
    assert summary.records_persisted == 0
    assert summary.circuit_breaker_until is not None
    assert summary.circuit_breaker_until >= before_ts + timedelta(hours=5, minutes=59)

    assert len(session.server_updates) == 1
    assert session.commit_count == 1
    assert len(session.added_runs) == 1
    run_row = session.added_runs[0]
    assert run_row.status == "BLOCKED_CHALLENGE"

    _verify_quarantine_artifact(
        summary=summary,
        run_row=run_row,
        expected_dir=settings.quarantine_dir,
        expected_substrings=[body.encode("utf-8")],
    )


# ---------------------------------------------------------------------------
# 3. Maintenance Detection (`TARGET_MAINTENANCE`)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("status_code", "body"),
    [
        (503, "<html><body>Scheduled maintenance in progress</body></html>"),
        (200, "<html><body><h1>Server Under Maintenance</h1></body></html>"),
        (200, "<html><body>Website under maintenance - back soon</body></html>"),
    ],
)
def test_maintenance_detection_skips_retry_quarantine_and_circuit_breaker(
    tmp_path: Path,
    status_code: int,
    body: str,
) -> None:
    settings = _make_settings(tmp_path)
    server = _make_server()
    session = FakeAsyncSession(server)
    attempts = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(status_code, text=body)

    scraper = _HttpFetchingScraper(settings, retry_delays=(2.0, 5.0))
    client = httpx.AsyncClient(http2=True, transport=httpx.MockTransport(handler))

    async def execute() -> ScrapeRunSummary:
        async with client:
            return await scraper.run(cast(AsyncSession, session), client=client)

    summary = _run_async(execute())

    assert attempts == 1
    assert summary.status == "TARGET_MAINTENANCE"
    assert summary.http_status_code == status_code
    assert summary.quarantine_file_path is None
    assert summary.circuit_breaker_until is None
    assert session.server_updates == []
    assert session.commit_count == 1
    assert len(session.added_runs) == 1
    assert session.added_runs[0].status == "TARGET_MAINTENANCE"
    assert not (settings.quarantine_dir / "chronicle").exists()


# ---------------------------------------------------------------------------
# 4. Transient Retries & `HTTP_ERROR`
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("transient_mode", ["connect_error", "read_timeout", 502, 503, 504])
def test_transient_failures_retry_twice_with_exact_delays_and_succeed_on_third_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    transient_mode: str | int,
) -> None:
    settings = _make_settings(tmp_path)
    server = _make_server()
    session = FakeAsyncSession(server)
    sleep_delays: list[float] = []
    player_attempts = 0

    async def fake_sleep(delay: float) -> None:
        sleep_delays.append(delay)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal player_attempts
        if request.url.path == "/ranking/player":
            player_attempts += 1
            if player_attempts <= 2:
                if transient_mode == "connect_error":
                    raise httpx.ConnectError("connection reset", request=request)
                if transient_mode == "read_timeout":
                    raise httpx.ReadTimeout("timed out", request=request)
                return httpx.Response(int(transient_mode), text="Transient gateway error")
        return httpx.Response(200, text="<html>ok</html>")

    scraper = _HttpFetchingScraper(settings, retry_delays=(2.0, 5.0))
    client = httpx.AsyncClient(http2=True, transport=httpx.MockTransport(handler))

    async def execute() -> ScrapeRunSummary:
        async with client:
            return await scraper.run(cast(AsyncSession, session), client=client)

    summary = _run_async(execute())

    assert player_attempts == 3
    assert sleep_delays == [2.0, 5.0]
    assert summary.status == "SUCCESS"
    assert summary.records_persisted == 15


def test_exhausted_retries_record_http_error_without_circuit_breaker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _make_settings(tmp_path)
    server = _make_server()
    session = FakeAsyncSession(server)
    sleep_delays: list[float] = []
    attempts = 0

    async def fake_sleep(delay: float) -> None:
        sleep_delays.append(delay)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(502, text="Bad Gateway")

    scraper = _HttpFetchingScraper(settings, retry_delays=(2.0, 5.0))
    client = httpx.AsyncClient(http2=True, transport=httpx.MockTransport(handler))

    async def execute() -> ScrapeRunSummary:
        async with client:
            return await scraper.run(cast(AsyncSession, session), client=client)

    summary = _run_async(execute())

    assert attempts == 3
    assert sleep_delays == [2.0, 5.0]
    assert summary.status == "HTTP_ERROR"
    assert summary.http_status_code == 502
    assert summary.quarantine_file_path is None
    assert session.server_updates == []
    assert session.commit_count == 1
    assert session.added_runs[0].status == "HTTP_ERROR"


def test_non_retryable_500_records_http_error_immediately(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    server = _make_server()
    session = FakeAsyncSession(server)
    attempts = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(500, text="Internal Server Error")

    scraper = _HttpFetchingScraper(settings, retry_delays=(2.0, 5.0))
    client = httpx.AsyncClient(http2=True, transport=httpx.MockTransport(handler))

    async def execute() -> ScrapeRunSummary:
        async with client:
            return await scraper.run(cast(AsyncSession, session), client=client)

    summary = _run_async(execute())

    assert attempts == 1
    assert summary.status == "HTTP_ERROR"
    assert summary.http_status_code == 500
    assert session.server_updates == []


# ---------------------------------------------------------------------------
# 5. Per-Target Timeout Budget Enforcement
# ---------------------------------------------------------------------------


def test_timeout_budget_enforces_limit_and_records_http_error(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    server = _make_server()
    session = FakeAsyncSession(server)

    class _SlowScraper(_StubScraper):
        async def fetch_job_pages(
            self,
            client: httpx.AsyncClient,
            base_url: str,
        ) -> CategoryFetchResult:
            await asyncio.sleep(0.25)
            return CategoryFetchResult()

    scraper = _SlowScraper(
        settings,
        player_result=CategoryFetchResult(raw_records=_make_player_records(10)),
        timeout_budget_seconds=0.02,
    )

    summary = _run_async(
        scraper.run(
            cast(AsyncSession, session),
            client=httpx.AsyncClient(
                http2=True,
                transport=httpx.MockTransport(lambda req: httpx.Response(200)),
            ),
        )
    )

    assert summary.status == "HTTP_ERROR"
    assert summary.error_details is not None
    assert summary.error_details.failed_category == "job"
    assert "timeout budget" in summary.error_details.reason
    assert session.server_updates == []
    assert session.commit_count == 1


# ---------------------------------------------------------------------------
# 6. Semaphore(3), Deterministic Header Rotation & Internal Client (`respx`)
# ---------------------------------------------------------------------------


def test_semaphore_limits_concurrency_to_three_and_rotates_headers(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    in_flight = 0
    max_in_flight = 0
    seen_user_agents: list[str] = []
    seen_accept_langs: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        seen_user_agents.append(request.headers["User-Agent"])
        seen_accept_langs.append(request.headers["Accept-Language"])
        await asyncio.sleep(0.015)
        in_flight -= 1
        return httpx.Response(200, text="ok")

    scraper = _HttpFetchingScraper(settings, retry_delays=(0.0, 0.0))

    async def execute() -> None:
        client = httpx.AsyncClient(http2=True, transport=httpx.MockTransport(handler))
        async with client:
            tasks = [
                scraper._fetch_url(client, f"https://play-chronicle.com/page/{i}") for i in range(6)
            ]
            responses = await asyncio.gather(*tasks)
            assert len(responses) == 6

    _run_async(execute())

    assert max_in_flight == 3
    expected_uas = [
        DEFAULT_HEADER_PROFILES[i % len(DEFAULT_HEADER_PROFILES)]["User-Agent"] for i in range(6)
    ]
    expected_langs = [
        DEFAULT_HEADER_PROFILES[i % len(DEFAULT_HEADER_PROFILES)]["Accept-Language"]
        for i in range(6)
    ]
    assert seen_user_agents == expected_uas
    assert seen_accept_langs == expected_langs


def test_run_builds_internal_http2_client_when_client_is_none(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    server = _make_server()
    session = FakeAsyncSession(server)
    scraper = _HttpFetchingScraper(settings, retry_delays=(0.0, 0.0))

    with respx.mock(assert_all_called=True) as mock_router:
        mock_router.get("https://play-chronicle.com/ranking/player").mock(
            return_value=httpx.Response(200, text="<html>player</html>")
        )
        mock_router.get("https://play-chronicle.com/ranking/job").mock(
            return_value=httpx.Response(200, text="<html>job</html>")
        )
        mock_router.get("https://play-chronicle.com/ranking/guild").mock(
            return_value=httpx.Response(200, text="<html>guild</html>")
        )

        summary = _run_async(scraper.run(cast(AsyncSession, session), trigger_type="manual"))

    assert summary.status == "SUCCESS"
    assert summary.trigger_type == "manual"
    assert summary.records_persisted == 15


# ---------------------------------------------------------------------------
# 7. Top-Rank Integrity Guard & Zero Records (`SCHEMA_DRIFT`)
# ---------------------------------------------------------------------------


def test_zero_records_extracted_triggers_schema_drift_without_zero_division(
    tmp_path: Path,
) -> None:
    settings = _make_settings(tmp_path)
    server = _make_server()
    session = FakeAsyncSession(server)
    scraper = _StubScraper(
        settings,
        player_result=CategoryFetchResult(
            raw_records=[],
            raw_payloads=[
                PagePayload(url="https://play-chronicle.com/p1", content=b"<html></html>")
            ],
        ),
    )

    summary = _run_async(
        scraper.run(
            cast(AsyncSession, session),
            client=httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200))),
        )
    )

    assert summary.status == "SCHEMA_DRIFT"
    assert summary.records_extracted == 0
    assert summary.records_persisted == 0
    assert summary.error_details is not None
    assert summary.error_details.failed_category == "player"
    _verify_quarantine_artifact(
        summary=summary,
        run_row=session.added_runs[0],
        expected_dir=settings.quarantine_dir,
    )


@pytest.mark.parametrize("missing_rank", [1, 2, 3])
def test_top_rank_integrity_guard_rejects_missing_player_podium_even_below_5_percent(
    tmp_path: Path,
    missing_rank: int,
) -> None:
    settings = _make_settings(tmp_path)
    server = _make_server(min_records_expected=10)
    session = FakeAsyncSession(server)

    players = _make_player_records(100)
    # Remove/corrupt podium rank (1/105 < 1% error rate, yet must trigger SCHEMA_DRIFT!)
    players = [p for p in players if p["rank"] != missing_rank]
    players.append({"rank": missing_rank, "char_name": "BrokenTop", "level": 999})

    scraper = _StubScraper(
        settings,
        player_result=CategoryFetchResult(
            raw_records=players,
            raw_payloads=[("https://play-chronicle.com/ranking/player", b"<table>broken</table>")],
        ),
        guild_result=CategoryFetchResult(raw_records=_make_guild_records(5)),
    )

    summary = _run_async(
        scraper.run(
            cast(AsyncSession, session),
            client=httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200))),
        )
    )

    assert summary.status == "SCHEMA_DRIFT"
    assert summary.validation_errors_count == 1
    assert summary.records_persisted == 0
    assert summary.error_details is not None
    assert summary.error_details.failed_category == "player"
    _verify_quarantine_artifact(
        summary=summary,
        run_row=session.added_runs[0],
        expected_dir=settings.quarantine_dir,
        expected_substrings=[b"<table>broken</table>"],
    )


def test_top_rank_integrity_guard_rejects_corrupted_guild_podium_when_three_or_more(
    tmp_path: Path,
) -> None:
    settings = _make_settings(tmp_path)
    server = _make_server(min_records_expected=10)
    session = FakeAsyncSession(server)

    guilds = _make_guild_records(10)
    guilds[1] = {"rank": 2, "guild_name": ""}  # Corrupt rank 2 guild

    scraper = _StubScraper(
        settings,
        player_result=CategoryFetchResult(raw_records=_make_player_records(50)),
        guild_result=CategoryFetchResult(
            raw_records=guilds,
            raw_payloads=[PagePayload(url="https://play-chronicle.com/g", content=b"guild-html")],
        ),
    )

    summary = _run_async(
        scraper.run(
            cast(AsyncSession, session),
            client=httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200))),
        )
    )

    assert summary.status == "SCHEMA_DRIFT"
    assert summary.records_persisted == 0
    assert summary.error_details is not None
    assert summary.error_details.failed_category == "guild"


# ---------------------------------------------------------------------------
# 8. Error Rate Boundary (`5.0%` PARTIAL_SUCCESS vs `>5.0%` SCHEMA_DRIFT)
# ---------------------------------------------------------------------------


def test_error_rate_exactly_5_percent_persists_partial_success(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    server = _make_server(min_records_expected=50)
    session = FakeAsyncSession(server)

    # 95 players + 5 guilds = 100 total records; 5 invalid players (ranks 10..14) = 5.0%
    players = _make_player_records(95)
    for bad_idx in range(10, 15):
        players[bad_idx]["level"] = 999  # Invalid (>140)

    scraper = _StubScraper(
        settings,
        player_result=CategoryFetchResult(raw_records=players),
        guild_result=CategoryFetchResult(raw_records=_make_guild_records(5)),
    )

    summary = _run_async(
        scraper.run(
            cast(AsyncSession, session),
            client=httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200))),
        )
    )

    assert summary.status == "PARTIAL_SUCCESS"
    assert summary.records_extracted == 100
    assert summary.validation_errors_count == 5
    assert summary.records_persisted == 95  # 90 valid players + 5 valid guilds
    assert summary.quarantine_file_path is None
    assert summary.error_details is not None
    assert len(summary.error_details.sample_errors) == 5
    assert session.commit_count == 1


def test_error_rate_above_5_percent_quarantines_as_schema_drift(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    server = _make_server(min_records_expected=50)
    session = FakeAsyncSession(server)

    # 95 players + 5 guilds = 100 total records; 6 invalid players (ranks 10..15) = 6.0%
    players = _make_player_records(95)
    for bad_idx in range(10, 16):
        players[bad_idx]["level"] = 999

    scraper = _StubScraper(
        settings,
        player_result=CategoryFetchResult(
            raw_records=players,
            raw_payloads=[PagePayload(url="https://play-chronicle.com/p", content=b"drift-html")],
        ),
        guild_result=CategoryFetchResult(raw_records=_make_guild_records(5)),
    )

    summary = _run_async(
        scraper.run(
            cast(AsyncSession, session),
            client=httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200))),
        )
    )

    assert summary.status == "SCHEMA_DRIFT"
    assert summary.validation_errors_count == 6
    assert summary.records_persisted == 0
    _verify_quarantine_artifact(
        summary=summary,
        run_row=session.added_runs[0],
        expected_dir=settings.quarantine_dir,
        expected_substrings=[b"drift-html"],
    )


# ---------------------------------------------------------------------------
# 9. Minimum Count Thresholds (`LOW_COUNT_QUARANTINE`)
# ---------------------------------------------------------------------------


def test_low_player_count_triggers_low_count_quarantine(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    server = _make_server(min_records_expected=100)
    session = FakeAsyncSession(server)

    scraper = _StubScraper(
        settings,
        player_result=CategoryFetchResult(
            raw_records=_make_player_records(20),
            raw_payloads=[PagePayload(url="https://play-chronicle.com/p", content=b"low-players")],
        ),
        guild_result=CategoryFetchResult(raw_records=_make_guild_records(5)),
    )

    summary = _run_async(
        scraper.run(
            cast(AsyncSession, session),
            client=httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200))),
        )
    )

    assert summary.status == "LOW_COUNT_QUARANTINE"
    assert summary.records_persisted == 0
    assert summary.error_details is not None
    assert summary.error_details.failed_category == "player"
    _verify_quarantine_artifact(
        summary=summary,
        run_row=session.added_runs[0],
        expected_dir=settings.quarantine_dir,
        expected_substrings=[b"low-players"],
    )


def test_low_guild_count_triggers_low_count_quarantine(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    server = _make_server(min_records_expected=10)
    session = FakeAsyncSession(server)

    scraper = _StubScraper(
        settings,
        player_result=CategoryFetchResult(raw_records=_make_player_records(15)),
        guild_result=CategoryFetchResult(
            raw_records=_make_guild_records(4),  # < MIN_GUILD_RECORDS (5)
            raw_payloads=[PagePayload(url="https://play-chronicle.com/g", content=b"low-guilds")],
        ),
    )

    summary = _run_async(
        scraper.run(
            cast(AsyncSession, session),
            client=httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200))),
        )
    )

    assert summary.status == "LOW_COUNT_QUARANTINE"
    assert summary.records_persisted == 0
    assert summary.error_details is not None
    assert summary.error_details.failed_category == "guild"
    _verify_quarantine_artifact(
        summary=summary,
        run_row=session.added_runs[0],
        expected_dir=settings.quarantine_dir,
        expected_substrings=[b"low-guilds"],
    )


# ---------------------------------------------------------------------------
# 10. Reconciliation, Intra-Run Deduplication & Shared Timestamp
# ---------------------------------------------------------------------------


def test_job_to_player_reconciliation_deduplication_and_truncation(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    server = _make_server(min_records_expected=4)
    session = FakeAsyncSession(server)

    players = [
        {
            "rank": 1,
            "char_name": "Alpha",
            "level": 110,
            "guild_name": None,
            "race": "CH",
            "item_points": 1234.567,
            "title": "X" * 80,  # Truncated to 64
            "extra_metrics": {"unique_points": 42},
        },
        {
            "rank": 2,
            "char_name": "Beta",
            "level": 109,
            "guild_name": "PlayerOnlyGuild",
            "race": None,
            "item_points": None,
        },
        {
            "rank": 3,
            "char_name": "Gamma",
            "level": 108,
            "guild_name": "Guild_1",
            "race": "EU",
        },
        # Duplicate Gamma on page 2 with worse rank 4 -> dropped in favor of rank 3
        {
            "rank": 4,
            "char_name": "Gamma",
            "level": 108,
            "guild_name": "Guild_1",
            "race": "EU",
        },
        {
            "rank": 4,
            "char_name": "Delta",
            "level": 107,
            "guild_name": "Guild_2",
            "race": "CH",
        },
    ]
    jobs = [
        {
            "rank": 7,
            "char_name": "Alpha",
            "job_type": "THIEF",
            "job_level": 99,
            "job_exp": 555000,
            "guild_name": "Guild_1",
            "extra_metrics": {"caravan_kills": 12},
        },
        # Duplicate Alpha in job ranking with worse rank 9 -> ignored
        {
            "rank": 9,
            "char_name": "Alpha",
            "job_type": "TRADER",
            "job_level": 10,
        },
        # Orphan job character absent from player ranking -> must NOT create orphan snapshot
        {
            "rank": 1,
            "char_name": "OrphanJobOnly",
            "job_type": "HUNTER",
            "job_level": 110,
        },
    ]
    guilds = _make_guild_records(5)
    guilds[0]["fortress_held"] = "F" * 50  # Truncated to 32

    scraper = _StubScraper(
        settings,
        player_result=CategoryFetchResult(raw_records=players),
        job_result=CategoryFetchResult(raw_records=jobs),
        guild_result=CategoryFetchResult(raw_records=guilds),
    )

    summary = _run_async(
        scraper.run(
            cast(AsyncSession, session),
            client=httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200))),
        )
    )

    assert summary.status == "SUCCESS"
    assert summary.records_extracted == 13  # 5 raw players + 3 raw jobs + 5 raw guilds
    assert summary.records_persisted == 9  # 4 deduped players + 5 deduped guilds

    char_snaps = session.char_snapshot_chunks[0]
    guild_snaps = session.guild_snapshot_chunks[0]
    assert len(char_snaps) == 4
    assert len(guild_snaps) == 5

    # Orphan job must not exist in characters or character_snapshots
    assert "OrphanJobOnly" not in session.char_ids

    # Alpha reconciled with Job rank 7, guild fallback, merged extra_metrics, and truncation
    alpha_id = session.char_ids["Alpha"]
    alpha_snap = next(row for row in char_snaps if row["character_id"] == alpha_id)
    assert alpha_snap["job_type"] == "THIEF"
    assert alpha_snap["job_level"] == 99
    assert alpha_snap["job_exp"] == 555000
    assert alpha_snap["guild_id"] == session.guild_ids["Guild_1"]
    assert alpha_snap["extra_metrics"] == {
        "unique_points": 42,
        "caravan_kills": 12,
        "job_rank": 7,
    }
    assert alpha_snap["title"] == "X" * 64
    assert alpha_snap["item_points"] == Decimal("1234.57")

    # Beta guild_name came only from Player ranking and was included in guilds upsert
    assert "PlayerOnlyGuild" in session.guild_ids
    beta_snap = next(row for row in char_snaps if row["character_id"] == session.char_ids["Beta"])
    assert beta_snap["guild_id"] == session.guild_ids["PlayerOnlyGuild"]
    assert beta_snap["item_points"] == Decimal("0.00")

    # Fortress held truncated defensively to 32 chars
    assert guild_snaps[0]["fortress_held"] == "F" * 32

    # Shared snapshot_ts across ScrapeRun, character_snapshots, and guild_snapshots
    run_ts = session.added_runs[0].created_at
    assert all(row["created_at"] == run_ts for row in char_snaps)
    assert all(row["created_at"] == run_ts for row in guild_snaps)


# ---------------------------------------------------------------------------
# 11. Chunked Bulk Persistence (>1000 rows), PG Dialect Compile & Rollback
# ---------------------------------------------------------------------------


def test_chunked_bulk_upsert_and_snapshot_insert_splits_at_1000(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    server = _make_server(min_records_expected=100)
    session = FakeAsyncSession(server)

    scraper = _StubScraper(
        settings,
        player_result=CategoryFetchResult(raw_records=_make_player_records(2050)),
        guild_result=CategoryFetchResult(raw_records=_make_guild_records(12)),
    )

    summary = _run_async(
        scraper.run(
            cast(AsyncSession, session),
            client=httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200))),
        )
    )

    assert summary.status == "SUCCESS"
    assert summary.records_persisted == 2062
    assert [len(chunk) for chunk in session.char_upsert_chunks] == [1000, 1000, 50]
    assert [len(chunk) for chunk in session.char_snapshot_chunks] == [1000, 1000, 50]
    assert [len(chunk) for chunk in session.guild_upsert_chunks] == [12]
    assert [len(chunk) for chunk in session.guild_snapshot_chunks] == [12]
    assert session.flush_count == 1
    assert session.commit_count == 1
    assert session.rollback_count == 0


def test_persistence_failure_rolls_back_transaction(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    server = _make_server(min_records_expected=5)
    session = FakeAsyncSession(
        server,
        fail_on_snapshot_insert=RuntimeError("simulated asyncpg failure"),
    )
    scraper = _StubScraper(
        settings,
        player_result=CategoryFetchResult(raw_records=_make_player_records(10)),
        guild_result=CategoryFetchResult(raw_records=_make_guild_records(5)),
    )

    with pytest.raises(RuntimeError, match="simulated asyncpg failure"):
        _run_async(
            scraper.run(
                cast(AsyncSession, session),
                client=httpx.AsyncClient(
                    transport=httpx.MockTransport(lambda r: httpx.Response(200))
                ),
            )
        )

    assert session.commit_count == 0
    assert session.rollback_count == 1


def test_postgres_upsert_sql_compilation_and_guard_inputs(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    server = _make_server(min_records_expected=5)
    session = FakeAsyncSession(server)

    scraper = _StubScraper(
        settings,
        player_result=CategoryFetchResult(raw_records=_make_player_records(5)),
        guild_result=CategoryFetchResult(raw_records=_make_guild_records(5)),
    )
    _run_async(
        scraper.run(
            cast(AsyncSession, session),
            client=httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200))),
        )
    )

    pg_engine = create_engine_from_url("postgresql+asyncpg://localhost/sro")
    try:
        pg_dialect = pg_engine.dialect
        compiled_inserts = [
            (stmt.table.name, str(stmt.compile(dialect=pg_dialect)))
            for stmt in session.executed_statements
            if isinstance(stmt, Insert)
        ]
    finally:
        pg_engine.sync_engine.dispose()
    guild_sql = next(sql for tbl, sql in compiled_inserts if tbl == "guilds")
    char_sql = next(sql for tbl, sql in compiled_inserts if tbl == "characters")

    assert "ON CONFLICT (server_id, guild_name) DO UPDATE SET" in guild_sql
    assert "coalesce(excluded.master_char_name, guilds.master_char_name)" in guild_sql.lower()
    assert "RETURNING guilds.id, guilds.guild_name" in guild_sql

    assert "ON CONFLICT (server_id, char_name) DO UPDATE SET" in char_sql
    assert "ON CONFLICT (server_id, char_name) DO UPDATE SET race = CASE" in char_sql
    assert "RETURNING characters.id, characters.char_name" in char_sql

    # Constructor and input validation guards
    class _MissingServerIdScraper(_StubScraper):
        server_id = "   "

    with pytest.raises(ValueError, match="server_id"):
        _MissingServerIdScraper(settings)

    with pytest.raises(ValueError, match="timeout_budget_seconds"):
        _StubScraper(settings, timeout_budget_seconds=0)

    valid_scraper = _StubScraper(settings)
    with pytest.raises(ValueError, match="trigger_type"):
        _run_async(valid_scraper.run(cast(AsyncSession, FakeAsyncSession(_make_server())), "bad"))

    with pytest.raises(LookupError, match="not found"):
        _run_async(valid_scraper.run(cast(AsyncSession, FakeAsyncSession(None))))


def test_fetch_url_exception_hierarchy_and_semaphore_released_during_backoff(
    tmp_path: Path,
) -> None:
    settings = _make_settings(tmp_path)
    scraper = _HttpFetchingScraper(settings, retry_delays=(0.04,))

    async def verify_exceptions() -> None:
        seen_cf_cookies: list[str | None] = []

        async def err_handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/cf":
                seen_cf_cookies.append(request.headers.get("Cookie"))
                return httpx.Response(403, text="blocked")
            if request.url.path == "/maint":
                return httpx.Response(503, text="Server under maintenance")
            return httpx.Response(404, text="not found")

        client = httpx.AsyncClient(http2=True, transport=httpx.MockTransport(err_handler))
        async with client:
            with pytest.raises(BlockedChallengeError) as cf_exc:
                await scraper._fetch_url(
                    client,
                    "https://play-chronicle.com/cf",
                    cookies={"cf_clearance": "tok"},
                    category="player",
                )
            assert isinstance(cf_exc.value, ScraperFetchError)
            assert cf_exc.value.category == "player"
            assert cf_exc.value.status_code == 403
            assert seen_cf_cookies == ["cf_clearance=tok"]

            with pytest.raises(TargetMaintenanceError) as maint_exc:
                await scraper._fetch_url(
                    client,
                    "https://play-chronicle.com/maint",
                    category="job",
                )
            assert isinstance(maint_exc.value, ScraperFetchError)
            assert maint_exc.value.category == "job"

            with pytest.raises(TransportFetchError) as http_exc:
                await scraper._fetch_url(
                    client,
                    "https://play-chronicle.com/missing",
                    category="guild",
                )
            assert isinstance(http_exc.value, ScraperFetchError)
            assert http_exc.value.status_code == 404

    _run_async(verify_exceptions())

    # Verify semaphore slots are released while a retrying task sleeps
    async def verify_semaphore_not_held_during_sleep() -> None:
        sleep_started = asyncio.Event()
        fast_completed_while_sleeping = False
        retrying_attempts = 0

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal retrying_attempts, fast_completed_while_sleeping
            if request.url.path.startswith("/slow-retry"):
                retrying_attempts += 1
                if retrying_attempts <= 3:
                    sleep_started.set()
                    return httpx.Response(502, text="retry")
                return httpx.Response(200, text="recovered")
            await sleep_started.wait()
            fast_completed_while_sleeping = True
            return httpx.Response(200, text="fast")

        client = httpx.AsyncClient(http2=True, transport=httpx.MockTransport(handler))
        async with client:
            await asyncio.gather(
                scraper._fetch_url(client, "https://play-chronicle.com/slow-retry/1"),
                scraper._fetch_url(client, "https://play-chronicle.com/slow-retry/2"),
                scraper._fetch_url(client, "https://play-chronicle.com/slow-retry/3"),
                scraper._fetch_url(client, "https://play-chronicle.com/fast"),
            )
        assert fast_completed_while_sleeping is True

    _run_async(verify_semaphore_not_held_during_sleep())


# ---------------------------------------------------------------------------
# 12. Regression Tests (Header/Cookie Merging, HTTP_ERROR Status, Float Rank)
# ---------------------------------------------------------------------------


def test_fetch_url_merges_headers_and_cookies_case_insensitively(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    scraper = _HttpFetchingScraper(settings, retry_delays=())
    captured_raw_headers: list[list[tuple[bytes, bytes]]] = []
    captured_headers: list[httpx.Headers] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        captured_raw_headers.append(list(request.headers.raw))
        captured_headers.append(request.headers)
        return httpx.Response(200, text="ok")

    async def execute() -> None:
        client = httpx.AsyncClient(http2=True, transport=httpx.MockTransport(handler))
        async with client:
            await scraper._fetch_url(
                client,
                "https://play-chronicle.com/merged",
                headers={"cookie": "foo=bar", "user-agent": "Custom/1.0"},
                cookies={"cf_clearance": "abc"},
            )
            await scraper._fetch_url(
                client,
                "https://play-chronicle.com/cookie-only",
                cookies={"cf_clearance": "tok"},
            )

    _run_async(execute())

    merged_raw = captured_raw_headers[0]
    cookie_raw = [(k, v) for k, v in merged_raw if k.lower() == b"cookie"]
    ua_raw = [(k, v) for k, v in merged_raw if k.lower() == b"user-agent"]
    assert len(cookie_raw) == 1
    assert cookie_raw[0][1] == b"foo=bar; cf_clearance=abc"
    assert len(ua_raw) == 1
    assert ua_raw[0][1] == b"Custom/1.0"
    assert captured_headers[0]["cookie"] == "foo=bar; cf_clearance=abc"
    assert captured_headers[0]["user-agent"] == "Custom/1.0"

    cookie_only_raw = [(k, v) for k, v in captured_raw_headers[1] if k.lower() == b"cookie"]
    assert len(cookie_only_raw) == 1
    assert cookie_only_raw[0][1] == b"cf_clearance=tok"
    assert captured_headers[1]["Cookie"] == "cf_clearance=tok"


def test_http_error_does_not_leak_prior_200_status_on_transport_or_budget_timeout(
    tmp_path: Path,
) -> None:
    settings = _make_settings(tmp_path)

    class _MultiPageScraper(_HttpFetchingScraper):
        def __init__(self, *, mode: str, timeout_budget_seconds: float = 180.0) -> None:
            super().__init__(
                settings,
                retry_delays=(),
                timeout_budget_seconds=timeout_budget_seconds,
            )
            self._mode = mode

        async def fetch_player_pages(
            self,
            client: httpx.AsyncClient,
            base_url: str,
        ) -> CategoryFetchResult:
            await self._fetch_url(client, f"{base_url}/p1", category="player")
            if self._mode == "slow":
                await asyncio.sleep(0.15)
                return CategoryFetchResult()
            await self._fetch_url(client, f"{base_url}/p2", category="player")
            return CategoryFetchResult()

    async def transport_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/p2":
            raise httpx.ConnectError("connection reset", request=request)
        return httpx.Response(200, text="ok")

    async def gateway_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/p2":
            return httpx.Response(504, text="Gateway Timeout")
        return httpx.Response(200, text="ok")

    # 1. Page 1 200 OK -> Page 2 ConnectError => http_status_code must be None
    sess_transport = FakeAsyncSession(_make_server())
    summary_transport = _run_async(
        _MultiPageScraper(mode="p2").run(
            cast(AsyncSession, sess_transport),
            client=httpx.AsyncClient(http2=True, transport=httpx.MockTransport(transport_handler)),
        )
    )
    assert summary_transport.status == "HTTP_ERROR"
    assert summary_transport.http_status_code is None
    assert sess_transport.added_runs[0].http_status_code is None

    # 2. Page 1 200 OK -> budget TimeoutError => http_status_code must be None
    sess_timeout = FakeAsyncSession(_make_server())
    summary_timeout = _run_async(
        _MultiPageScraper(mode="slow", timeout_budget_seconds=0.02).run(
            cast(AsyncSession, sess_timeout),
            client=httpx.AsyncClient(
                http2=True,
                transport=httpx.MockTransport(lambda req: httpx.Response(200, text="ok")),
            ),
        )
    )
    assert summary_timeout.status == "HTTP_ERROR"
    assert summary_timeout.http_status_code is None
    assert sess_timeout.added_runs[0].http_status_code is None

    # 3. Page 1 200 OK -> Page 2 HTTP 504 => http_status_code must still record 504
    sess_gateway = FakeAsyncSession(_make_server())
    summary_gateway = _run_async(
        _MultiPageScraper(mode="p2").run(
            cast(AsyncSession, sess_gateway),
            client=httpx.AsyncClient(http2=True, transport=httpx.MockTransport(gateway_handler)),
        )
    )
    assert summary_gateway.status == "HTTP_ERROR"
    assert summary_gateway.http_status_code == 504
    assert sess_gateway.added_runs[0].http_status_code == 504


@pytest.mark.parametrize("corrupted_rank", [2.0, "2.0"])
def test_top_rank_integrity_guard_catches_float_formatted_podium_rank_with_duplicate(
    tmp_path: Path,
    corrupted_rank: float | str,
) -> None:
    settings = _make_settings(tmp_path)
    server = _make_server(min_records_expected=10)
    session = FakeAsyncSession(server)

    players = _make_player_records(105)
    # Pair corrupted float/decimal-string rank 2 with an existing valid rank 2 (<1% error rate)
    players.append({"rank": corrupted_rank, "char_name": "BrokenFloatTop", "level": 999})

    scraper = _StubScraper(
        settings,
        player_result=CategoryFetchResult(
            raw_records=players,
            raw_payloads=[PagePayload(url="https://play-chronicle.com/p", content=b"float-podium")],
        ),
        guild_result=CategoryFetchResult(raw_records=_make_guild_records(5)),
    )

    summary = _run_async(
        scraper.run(
            cast(AsyncSession, session),
            client=httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200))),
        )
    )

    assert summary.status == "SCHEMA_DRIFT"
    assert summary.validation_errors_count == 1
    assert summary.records_persisted == 0
    assert summary.error_details is not None
    assert summary.error_details.failed_category == "player"
    _verify_quarantine_artifact(
        summary=summary,
        run_row=session.added_runs[0],
        expected_dir=settings.quarantine_dir,
        expected_substrings=[b"float-podium"],
    )


def test_run_fetches_job_and_guild_concurrently_after_player_probe(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    server = _make_server(min_records_expected=10)
    session = FakeAsyncSession(server)

    in_flight = 0
    max_in_flight = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        await asyncio.sleep(0.02)
        in_flight -= 1
        return httpx.Response(200, text=f"ok:{request.url.path}")

    scraper = _HttpFetchingScraper(settings, retry_delays=(0.0, 0.0))
    summary = _run_async(
        scraper.run(
            cast(AsyncSession, session),
            client=httpx.AsyncClient(http2=True, transport=httpx.MockTransport(handler)),
        )
    )

    assert summary.status == "SUCCESS"
    assert max_in_flight == 2


def test_run_cancels_sibling_category_on_challenge_error(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    server = _make_server(min_records_expected=10)
    session = FakeAsyncSession(server)
    cancelled_siblings: list[str] = []

    class _ChallengeCancellingScraper(_StubScraper):
        async def fetch_job_pages(
            self,
            client: httpx.AsyncClient,
            base_url: str,
        ) -> CategoryFetchResult:
            await self._fetch_url(client, f"{base_url}/ranking/job", category="job")
            return CategoryFetchResult()

        async def fetch_guild_pages(
            self,
            client: httpx.AsyncClient,
            base_url: str,
        ) -> CategoryFetchResult:
            try:
                await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                cancelled_siblings.append("guild")
                raise
            return CategoryFetchResult()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, text='<div id="turnstile-wrapper">blocked</div>')

    scraper = _ChallengeCancellingScraper(
        settings,
        player_result=CategoryFetchResult(raw_records=_make_player_records(10)),
    )
    summary = _run_async(
        scraper.run(
            cast(AsyncSession, session),
            client=httpx.AsyncClient(http2=True, transport=httpx.MockTransport(handler)),
        )
    )

    assert summary.status == "BLOCKED_CHALLENGE"
    assert summary.error_details is not None
    assert summary.error_details.failed_category == "job"
    assert cancelled_siblings == ["guild"]
