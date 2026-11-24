from __future__ import annotations

import asyncio
import gzip
import hashlib
import time
import uuid
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Final, TypeVar

import httpx
from pydantic import ValidationError
from sqlalchemy import case, func, select, update
from sqlalchemy import insert as sa_insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import Settings, get_settings
from src.core.logging import bind_run_context, clear_run_context, get_logger
from src.models.db_models import (
    Character,
    CharacterSnapshot,
    Guild,
    GuildSnapshot,
    ScrapeRun,
    Server,
)
from src.models.pydantic_schemas import (
    CANONICAL_TRIGGER_TYPES,
    MAX_ERROR_RATE,
    MIN_GUILD_RECORDS,
    ErrorDetails,
    RawGuildRankingItem,
    RawJobRankingItem,
    RawPlayerRankingItem,
)

DEFAULT_MAX_CONCURRENT_REQUESTS: Final[int] = 3
DEFAULT_RETRY_DELAYS: Final[tuple[float, ...]] = (2.0, 5.0)
DEFAULT_TIMEOUT_BUDGET_SECONDS: Final[float] = 180.0
SNAPSHOT_INSERT_CHUNK_SIZE: Final[int] = 1000
MAX_SAMPLE_ERRORS: Final[int] = 25

CHALLENGE_HTTP_STATUSES: Final[frozenset[int]] = frozenset({403, 429})
RETRYABLE_HTTP_STATUSES: Final[frozenset[int]] = frozenset({502, 503, 504})

CHALLENGE_HTML_MARKERS: Final[tuple[str, ...]] = (
    '<div id="turnstile-wrapper">',
    "cf-turnstile",
    "cf-chl-bypass",
    "just a moment...",
    "attention required! | cloudflare",
    "/hf-sys/hf-lib.php",
    'createcookie("__hfuid"',
    "verifying your browser | diamwall",
    "/.well-known/diamwall/",
)

MAINTENANCE_HTML_MARKERS: Final[tuple[str, ...]] = (
    "server under maintenance",
    "website under maintenance",
)

DEFAULT_HEADER_PROFILES: Final[tuple[dict[str, str], ...]] = (
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.8,*/*;q=0.7"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Sec-Ch-Ua": '"Chromium";v="131", "Not_A Brand";v="24"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
    },
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.8,*/*;q=0.7"
        ),
        "Accept-Language": "en-GB,en;q=0.9,pt-BR;q=0.8",
        "Sec-Ch-Ua": '"Microsoft Edge";v="130", "Chromium";v="130", "Not_A Brand";v="24"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
    },
    {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.8,*/*;q=0.7"
        ),
        "Accept-Language": "en-US,en;q=0.8,de;q=0.6",
        "Sec-Ch-Ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Linux"',
    },
)

_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class PagePayload:
    """Raw HTTP page payload captured for dead-letter quarantine diagnostics."""

    url: str
    content: bytes
    status_code: int = 200
    category: str = ""


@dataclass(slots=True)
class CategoryFetchResult:
    """Extracted raw records and payloads returned by a category fetch hook."""

    raw_records: list[dict[str, Any]] = field(default_factory=list)
    raw_payloads: list[PagePayload | tuple[str, bytes]] = field(default_factory=list)
    http_status_code: int | None = None


@dataclass(frozen=True, slots=True)
class ScrapeRunSummary:
    """Typed summary returned by `BaseRankingScraper.run()` for orchestration & tests."""

    run_id: uuid.UUID
    server_id: str
    status: str
    trigger_type: str
    skipped: bool = False
    records_extracted: int = 0
    records_persisted: int = 0
    validation_errors_count: int = 0
    execution_time_ms: int = 0
    http_status_code: int | None = None
    quarantine_file_path: str | None = None
    quarantine_sha256: str | None = None
    quarantine_size_bytes: int | None = None
    circuit_breaker_until: datetime | None = None
    error_details: ErrorDetails | None = None


class ScraperFetchError(Exception):
    """Base exception raised on HTTP/challenge/maintenance failures during fetch."""

    def __init__(
        self,
        message: str,
        *,
        url: str = "",
        status_code: int | None = None,
        payload: bytes = b"",
        category: str = "",
    ) -> None:
        super().__init__(message)
        self.url = url
        self.status_code = status_code
        self.payload = payload
        self.category = category


class BlockedChallengeError(ScraperFetchError):
    """Raised immediately on HTTP 403/429 or anti-bot challenge signatures (no retry)."""


class TargetMaintenanceError(ScraperFetchError):
    """Raised immediately on server maintenance 503 or maintenance banners (no 6h CB)."""


class TransportFetchError(ScraperFetchError):
    """Raised on unrecoverable HTTP or transport errors after configured retries."""


def _chunked(items: list[_T], size: int) -> Iterator[list[_T]]:
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def _flatten_exceptions(exc: BaseException) -> list[BaseException]:
    if isinstance(exc, BaseExceptionGroup):
        leaves: list[BaseException] = []
        for sub in exc.exceptions:
            leaves.extend(_flatten_exceptions(sub))
        return leaves
    return [exc]


def _try_extract_raw_rank(raw_record: dict[str, Any]) -> int | None:
    raw_rank = raw_record.get("rank")
    if isinstance(raw_rank, bool) or raw_rank is None:
        return None
    try:
        return RawPlayerRankingItem.clean_required_int(raw_rank)
    except (ValueError, TypeError):
        return None


def _serialize_validation_error(
    category: str,
    index: int,
    raw_record: dict[str, Any],
    exc: ValidationError,
) -> dict[str, Any]:
    errors_list: list[dict[str, Any]] = [
        {
            "loc": [str(part) for part in err.get("loc", ())],
            "msg": str(err.get("msg", "")),
            "type": str(err.get("type", "")),
        }
        for err in exc.errors(include_url=False)
    ]
    raw_rank = raw_record.get("rank")
    return {
        "category": category,
        "index": index,
        "raw_rank": str(raw_rank) if raw_rank is not None else None,
        "errors": errors_list,
    }


def _deduplicate_players(items: list[RawPlayerRankingItem]) -> list[RawPlayerRankingItem]:
    best_by_name: dict[str, tuple[int, int, RawPlayerRankingItem]] = {}
    for idx, item in enumerate(items):
        existing = best_by_name.get(item.char_name)
        if existing is None or item.rank < existing[0]:
            best_by_name[item.char_name] = (item.rank, idx, item)
    return [
        entry[2] for entry in sorted(best_by_name.values(), key=lambda entry: (entry[0], entry[1]))
    ]


def _deduplicate_jobs(items: list[RawJobRankingItem]) -> list[RawJobRankingItem]:
    best_by_name: dict[str, tuple[int, int, RawJobRankingItem]] = {}
    for idx, item in enumerate(items):
        existing = best_by_name.get(item.char_name)
        if existing is None or item.rank < existing[0]:
            best_by_name[item.char_name] = (item.rank, idx, item)
    return [
        entry[2] for entry in sorted(best_by_name.values(), key=lambda entry: (entry[0], entry[1]))
    ]


def _deduplicate_guilds(items: list[RawGuildRankingItem]) -> list[RawGuildRankingItem]:
    best_by_name: dict[str, tuple[int, int, RawGuildRankingItem]] = {}
    for idx, item in enumerate(items):
        existing = best_by_name.get(item.guild_name)
        if existing is None or item.rank < existing[0]:
            best_by_name[item.guild_name] = (item.rank, idx, item)
    return [
        entry[2] for entry in sorted(best_by_name.values(), key=lambda entry: (entry[0], entry[1]))
    ]


def _reconcile_jobs_into_players(
    players: list[RawPlayerRankingItem],
    jobs: list[RawJobRankingItem],
) -> list[RawPlayerRankingItem]:
    """Left-join validated job items onto player rankings by case-sensitive `char_name`."""
    job_by_char: dict[str, RawJobRankingItem] = {job.char_name: job for job in jobs}
    reconciled: list[RawPlayerRankingItem] = []
    for player in players:
        job = job_by_char.get(player.char_name)
        if job is None:
            reconciled.append(player)
            continue
        merged_extras: dict[str, Any] = {
            **player.extra_metrics,
            **job.extra_metrics,
            "job_rank": job.rank,
        }
        reconciled.append(
            player.model_copy(
                update={
                    "job_type": job.job_type if job.job_type is not None else player.job_type,
                    "job_level": job.job_level if job.job_level is not None else player.job_level,
                    "job_points": (
                        job.job_points if job.job_points is not None else player.job_points
                    ),
                    "job_exp": job.job_exp if job.job_exp is not None else player.job_exp,
                    "guild_name": (
                        player.guild_name if player.guild_name is not None else job.guild_name
                    ),
                    "extra_metrics": merged_extras,
                }
            )
        )
    return reconciled


class BaseRankingScraper(ABC):
    """Abstract base scraper handling HTTPX, circuit breaker, validation, quarantine, and DB."""

    server_id: str

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        retry_delays: tuple[float, ...] = DEFAULT_RETRY_DELAYS,
        timeout_budget_seconds: float = DEFAULT_TIMEOUT_BUDGET_SECONDS,
    ) -> None:
        raw_server_id = getattr(self, "server_id", "")
        if not isinstance(raw_server_id, str) or not raw_server_id.strip():
            raise ValueError("BaseRankingScraper subclass must define a non-empty 'server_id'")
        if timeout_budget_seconds <= 0:
            raise ValueError("timeout_budget_seconds must be positive")
        self.settings: Settings = settings if settings is not None else get_settings()
        self._retry_delays: tuple[float, ...] = tuple(retry_delays)
        self._timeout_budget_seconds: float = timeout_budget_seconds
        self._semaphore: asyncio.Semaphore | None = None
        self._header_index: int = 0
        self._last_http_status_code: int | None = None
        self._logger = get_logger(self.__class__.__name__)

    @abstractmethod
    async def fetch_player_pages(
        self,
        client: httpx.AsyncClient,
        base_url: str,
    ) -> CategoryFetchResult:
        """Fetch and parse player ranking pages for the target server."""

    @abstractmethod
    async def fetch_job_pages(
        self,
        client: httpx.AsyncClient,
        base_url: str,
    ) -> CategoryFetchResult:
        """Fetch and parse job ranking pages for the target server."""

    @abstractmethod
    async def fetch_guild_pages(
        self,
        client: httpx.AsyncClient,
        base_url: str,
    ) -> CategoryFetchResult:
        """Fetch and parse guild ranking pages for the target server."""

    def _get_semaphore(self) -> asyncio.Semaphore:
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(DEFAULT_MAX_CONCURRENT_REQUESTS)
        return self._semaphore

    def _next_headers(self, extra_headers: Mapping[str, str] | None = None) -> dict[str, str]:
        profile = DEFAULT_HEADER_PROFILES[self._header_index % len(DEFAULT_HEADER_PROFILES)]
        self._header_index += 1
        merged = dict(profile)
        if extra_headers:
            for key, value in extra_headers.items():
                for existing_key in [k for k in merged if k.lower() == key.lower()]:
                    del merged[existing_key]
                merged[key] = value
        return merged

    @staticmethod
    def _is_challenge_response(response: httpx.Response) -> bool:
        if response.status_code in CHALLENGE_HTTP_STATUSES:
            return True
        if response.headers.get("cf-mitigated", "").strip().lower() == "challenge":
            return True
        if response.status_code == 513 and any(
            "__diamwall" in cookie.lower() for cookie in response.headers.get_list("set-cookie")
        ):
            return True
        lower_text = response.text.lower()
        return any(marker in lower_text for marker in CHALLENGE_HTML_MARKERS)

    @staticmethod
    def _is_maintenance_response(response: httpx.Response) -> bool:
        lower_text = response.text.lower()
        if any(marker in lower_text for marker in MAINTENANCE_HTML_MARKERS):
            return True
        return response.status_code == 503 and "maintenance" in lower_text

    @staticmethod
    def _build_http_client() -> httpx.AsyncClient:
        return httpx.AsyncClient(
            http2=True,
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0),
            follow_redirects=True,
        )

    @staticmethod
    def _elapsed_ms(start_monotonic: float) -> int:
        return max(0, int((time.monotonic() - start_monotonic) * 1000))

    async def _fetch_url(
        self,
        client: httpx.AsyncClient,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        cookies: Mapping[str, str] | None = None,
        category: str = "",
    ) -> httpx.Response:
        request_headers = self._next_headers(headers)
        if cookies:
            cookie_header = "; ".join(f"{k}={v}" for k, v in cookies.items())
            cookie_keys = [k for k in request_headers if k.lower() == "cookie"]
            existing_parts = [request_headers.pop(k).strip() for k in cookie_keys]
            existing_cookie = "; ".join(part for part in existing_parts if part)
            request_headers["Cookie"] = (
                f"{existing_cookie}; {cookie_header}" if existing_cookie else cookie_header
            )
        semaphore = self._get_semaphore()
        max_attempts = len(self._retry_delays) + 1

        for attempt in range(max_attempts):
            try:
                async with semaphore:
                    response = await client.get(
                        url,
                        headers=request_headers,
                    )
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                if attempt < len(self._retry_delays):
                    delay = self._retry_delays[attempt]
                    self._logger.warning(
                        "http_transport_retry",
                        url=url,
                        attempt=attempt + 1,
                        delay_seconds=delay,
                        error=str(exc),
                    )
                    await asyncio.sleep(delay)
                    continue
                raise TransportFetchError(
                    f"Transport error fetching {url}: {exc}",
                    url=url,
                    status_code=None,
                    category=category,
                ) from exc

            self._last_http_status_code = response.status_code

            if self._is_challenge_response(response):
                raise BlockedChallengeError(
                    f"Anti-bot challenge detected at {url} (HTTP {response.status_code})",
                    url=url,
                    status_code=response.status_code,
                    payload=response.content,
                    category=category,
                )

            if self._is_maintenance_response(response):
                raise TargetMaintenanceError(
                    f"Target maintenance detected at {url} (HTTP {response.status_code})",
                    url=url,
                    status_code=response.status_code,
                    payload=response.content,
                    category=category,
                )

            if response.status_code in RETRYABLE_HTTP_STATUSES:
                if attempt < len(self._retry_delays):
                    delay = self._retry_delays[attempt]
                    self._logger.warning(
                        "http_status_retry",
                        url=url,
                        status_code=response.status_code,
                        attempt=attempt + 1,
                        delay_seconds=delay,
                    )
                    await asyncio.sleep(delay)
                    continue
                raise TransportFetchError(
                    f"HTTP {response.status_code} fetching {url} "
                    f"after {len(self._retry_delays)} retries",
                    url=url,
                    status_code=response.status_code,
                    payload=response.content,
                    category=category,
                )

            if response.status_code >= 400:
                raise TransportFetchError(
                    f"HTTP {response.status_code} fetching {url}",
                    url=url,
                    status_code=response.status_code,
                    payload=response.content,
                    category=category,
                )

            return response

        raise TransportFetchError(
            f"Exhausted retries fetching {url}",
            url=url,
            status_code=self._last_http_status_code,
            category=category,
        )

    def _write_quarantine_file(
        self,
        run_id: uuid.UUID,
        payloads: list[PagePayload],
    ) -> tuple[str, str, int]:
        target_dir = Path(self.settings.quarantine_dir) / self.server_id
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / f"{run_id}.html.gz"

        parts: list[bytes] = []
        for payload in payloads:
            banner = (
                f"<!-- === QUARANTINE PAGE: category={payload.category} "
                f"url={payload.url} status={payload.status_code} === -->\n"
            ).encode()
            parts.append(banner + payload.content + b"\n")

        combined_bytes = b"".join(parts) if parts else b"<!-- === EMPTY PAYLOAD === -->\n"
        compressed_bytes = gzip.compress(combined_bytes, mtime=0)
        target_path.write_bytes(compressed_bytes)

        sha256_hex = hashlib.sha256(compressed_bytes).hexdigest()
        return target_path.as_posix(), sha256_hex, len(compressed_bytes)

    @staticmethod
    def _normalize_category_payloads(
        result: CategoryFetchResult,
        category: str,
    ) -> list[PagePayload]:
        normalized: list[PagePayload] = []
        for entry in result.raw_payloads:
            if isinstance(entry, PagePayload):
                normalized.append(
                    entry
                    if entry.category
                    else PagePayload(
                        url=entry.url,
                        content=entry.content,
                        status_code=entry.status_code,
                        category=category,
                    )
                )
            else:
                url, content = entry
                normalized.append(
                    PagePayload(
                        url=url,
                        content=content,
                        status_code=result.http_status_code or 200,
                        category=category,
                    )
                )
        return normalized

    async def run(
        self,
        session: AsyncSession,
        trigger_type: str = "scheduled_12h",
        client: httpx.AsyncClient | None = None,
    ) -> ScrapeRunSummary:
        """Execute a complete isolated scrape run for `self.server_id`."""
        if trigger_type not in CANONICAL_TRIGGER_TYPES:
            raise ValueError(
                f"Invalid trigger_type {trigger_type!r}; "
                f"expected one of {sorted(CANONICAL_TRIGGER_TYPES)}"
            )

        run_id = uuid.uuid4()
        start_mono = time.monotonic()
        self._semaphore = asyncio.Semaphore(DEFAULT_MAX_CONCURRENT_REQUESTS)
        self._last_http_status_code = None

        bind_run_context(server_id=self.server_id, run_id=run_id)
        try:
            server = await session.scalar(select(Server).where(Server.id == self.server_id))
            if server is None:
                raise LookupError(f"Server {self.server_id!r} not found")

            now_utc = datetime.now(UTC)
            if server.circuit_breaker_until is not None:
                cb_utc = (
                    server.circuit_breaker_until.replace(tzinfo=UTC)
                    if server.circuit_breaker_until.tzinfo is None
                    else server.circuit_breaker_until.astimezone(UTC)
                )
                if now_utc < cb_utc:
                    remaining_seconds = max(1, int((cb_utc - now_utc).total_seconds()))
                    self._logger.warning(
                        "circuit_breaker_active",
                        circuit_breaker_until=cb_utc.isoformat(),
                        remaining_seconds=remaining_seconds,
                    )
                    await session.rollback()
                    return ScrapeRunSummary(
                        run_id=run_id,
                        server_id=self.server_id,
                        status="SKIPPED_CIRCUIT_BREAKER",
                        trigger_type=trigger_type,
                        skipped=True,
                        execution_time_ms=self._elapsed_ms(start_mono),
                        circuit_breaker_until=cb_utc,
                    )

            pending_categories: list[str] = ["player", "job", "guild"]

            async def _run_category(
                category_name: str,
                coro: Awaitable[CategoryFetchResult],
            ) -> CategoryFetchResult:
                try:
                    result = await coro
                except ScraperFetchError as exc:
                    if not exc.category:
                        exc.category = category_name
                    raise
                else:
                    if category_name in pending_categories:
                        pending_categories.remove(category_name)
                    return result

            async def _fetch_categories(
                active_client: httpx.AsyncClient,
            ) -> tuple[CategoryFetchResult, CategoryFetchResult, CategoryFetchResult]:
                # Fetch `player` first as a single health/anti-bot probe so blocked
                # or maintenance targets abort immediately without extra requests.
                player_res = await _run_category(
                    "player",
                    self.fetch_player_pages(active_client, server.base_url),
                )
                try:
                    async with asyncio.TaskGroup() as tg:
                        job_task = tg.create_task(
                            _run_category(
                                "job",
                                self.fetch_job_pages(active_client, server.base_url),
                            )
                        )
                        guild_task = tg.create_task(
                            _run_category(
                                "guild",
                                self.fetch_guild_pages(active_client, server.base_url),
                            )
                        )
                except ExceptionGroup as eg:
                    leaves = _flatten_exceptions(eg)
                    for exc_type in (
                        BlockedChallengeError,
                        TargetMaintenanceError,
                        TransportFetchError,
                    ):
                        for leaf in leaves:
                            if isinstance(leaf, exc_type):
                                raise leaf from None
                    raise leaves[0] from None
                return player_res, job_task.result(), guild_task.result()

            try:
                async with asyncio.timeout(self._timeout_budget_seconds):
                    if client is not None:
                        player_result, job_result, guild_result = await _fetch_categories(client)
                    else:
                        async with self._build_http_client() as owned_client:
                            player_result, job_result, guild_result = await _fetch_categories(
                                owned_client
                            )
            except BlockedChallengeError as exc:
                failed_category = exc.category or (
                    pending_categories[0] if pending_categories else "player"
                )
                challenge_payload = PagePayload(
                    url=exc.url or server.base_url,
                    content=exc.payload,
                    status_code=exc.status_code or 403,
                    category=failed_category,
                )
                q_path, q_sha256, q_size = self._write_quarantine_file(run_id, [challenge_payload])
                now_ts = datetime.now(UTC)
                cb_until = now_ts + timedelta(hours=self.settings.circuit_breaker_cooldown_hours)
                err_details = ErrorDetails(
                    reason=str(exc),
                    failed_category=failed_category,
                    sample_errors=[{"url": exc.url, "status_code": exc.status_code}],
                    quarantine_sha256=q_sha256,
                    quarantine_size_bytes=q_size,
                )
                elapsed_ms = self._elapsed_ms(start_mono)
                try:
                    await session.execute(
                        update(Server)
                        .where(Server.id == self.server_id)
                        .values(circuit_breaker_until=cb_until, updated_at=now_ts)
                    )
                    session.add(
                        ScrapeRun(
                            id=run_id,
                            server_id=self.server_id,
                            ranking_category="all",
                            trigger_type=trigger_type,
                            status="BLOCKED_CHALLENGE",
                            records_extracted=0,
                            records_persisted=0,
                            validation_errors_count=0,
                            execution_time_ms=elapsed_ms,
                            http_status_code=exc.status_code,
                            quarantine_file_path=q_path,
                            quarantine_sha256=q_sha256,
                            quarantine_size_bytes=q_size,
                            error_details=err_details.model_dump(mode="json"),
                            created_at=now_ts,
                            updated_at=now_ts,
                        )
                    )
                    await session.commit()
                except Exception:
                    await session.rollback()
                    raise
                self._logger.warning(
                    "scrape_blocked_challenge",
                    status="BLOCKED_CHALLENGE",
                    http_status_code=exc.status_code,
                    circuit_breaker_until=cb_until.isoformat(),
                    quarantine_file_path=q_path,
                )
                return ScrapeRunSummary(
                    run_id=run_id,
                    server_id=self.server_id,
                    status="BLOCKED_CHALLENGE",
                    trigger_type=trigger_type,
                    execution_time_ms=elapsed_ms,
                    http_status_code=exc.status_code,
                    quarantine_file_path=q_path,
                    quarantine_sha256=q_sha256,
                    quarantine_size_bytes=q_size,
                    circuit_breaker_until=cb_until,
                    error_details=err_details,
                )
            except TargetMaintenanceError as exc:
                failed_category = exc.category or (
                    pending_categories[0] if pending_categories else "player"
                )
                now_ts = datetime.now(UTC)
                err_details = ErrorDetails(
                    reason=str(exc),
                    failed_category=failed_category,
                    sample_errors=[{"url": exc.url, "status_code": exc.status_code}],
                )
                elapsed_ms = self._elapsed_ms(start_mono)
                try:
                    session.add(
                        ScrapeRun(
                            id=run_id,
                            server_id=self.server_id,
                            ranking_category="all",
                            trigger_type=trigger_type,
                            status="TARGET_MAINTENANCE",
                            records_extracted=0,
                            records_persisted=0,
                            validation_errors_count=0,
                            execution_time_ms=elapsed_ms,
                            http_status_code=exc.status_code,
                            error_details=err_details.model_dump(mode="json"),
                            created_at=now_ts,
                            updated_at=now_ts,
                        )
                    )
                    await session.commit()
                except Exception:
                    await session.rollback()
                    raise
                self._logger.warning(
                    "scrape_target_maintenance",
                    status="TARGET_MAINTENANCE",
                    http_status_code=exc.status_code,
                )
                return ScrapeRunSummary(
                    run_id=run_id,
                    server_id=self.server_id,
                    status="TARGET_MAINTENANCE",
                    trigger_type=trigger_type,
                    execution_time_ms=elapsed_ms,
                    http_status_code=exc.status_code,
                    error_details=err_details,
                )
            except (TransportFetchError, TimeoutError) as exc:
                now_ts = datetime.now(UTC)
                if isinstance(exc, TransportFetchError):
                    failed_category = exc.category or (
                        pending_categories[0] if pending_categories else "player"
                    )
                    http_status: int | None = exc.status_code
                    reason = str(exc)
                    sample: list[dict[str, Any]] = [
                        {"url": exc.url, "status_code": exc.status_code}
                    ]
                else:
                    failed_category = pending_categories[0] if pending_categories else "player"
                    http_status = None
                    reason = (
                        f"Target scrape exceeded {self._timeout_budget_seconds}s timeout budget"
                    )
                    sample = [{"timeout_budget_seconds": self._timeout_budget_seconds}]

                err_details = ErrorDetails(
                    reason=reason,
                    failed_category=failed_category,
                    sample_errors=sample,
                )
                elapsed_ms = self._elapsed_ms(start_mono)
                try:
                    session.add(
                        ScrapeRun(
                            id=run_id,
                            server_id=self.server_id,
                            ranking_category="all",
                            trigger_type=trigger_type,
                            status="HTTP_ERROR",
                            records_extracted=0,
                            records_persisted=0,
                            validation_errors_count=0,
                            execution_time_ms=elapsed_ms,
                            http_status_code=http_status,
                            error_details=err_details.model_dump(mode="json"),
                            created_at=now_ts,
                            updated_at=now_ts,
                        )
                    )
                    await session.commit()
                except Exception:
                    await session.rollback()
                    raise
                self._logger.error(
                    "scrape_http_error",
                    status="HTTP_ERROR",
                    http_status_code=http_status,
                    reason=reason,
                )
                return ScrapeRunSummary(
                    run_id=run_id,
                    server_id=self.server_id,
                    status="HTTP_ERROR",
                    trigger_type=trigger_type,
                    execution_time_ms=elapsed_ms,
                    http_status_code=http_status,
                    error_details=err_details,
                )

            return await self._validate_and_persist(
                session=session,
                server=server,
                run_id=run_id,
                trigger_type=trigger_type,
                start_mono=start_mono,
                player_result=player_result,
                job_result=job_result,
                guild_result=guild_result,
            )
        finally:
            clear_run_context()

    async def _validate_and_persist(
        self,
        *,
        session: AsyncSession,
        server: Server,
        run_id: uuid.UUID,
        trigger_type: str,
        start_mono: float,
        player_result: CategoryFetchResult,
        job_result: CategoryFetchResult,
        guild_result: CategoryFetchResult,
    ) -> ScrapeRunSummary:
        raw_players: list[RawPlayerRankingItem] = []
        raw_jobs: list[RawJobRankingItem] = []
        raw_guilds: list[RawGuildRankingItem] = []

        player_errors = 0
        job_errors = 0
        guild_errors = 0
        sample_errors: list[dict[str, Any]] = []
        failed_player_podium_ranks: set[int] = set()
        failed_guild_podium_ranks: set[int] = set()

        for idx, raw_rec in enumerate(player_result.raw_records):
            try:
                raw_players.append(RawPlayerRankingItem.model_validate(raw_rec))
            except ValidationError as exc:
                player_errors += 1
                raw_rank = _try_extract_raw_rank(raw_rec)
                if raw_rank in {1, 2, 3}:
                    failed_player_podium_ranks.add(raw_rank)
                if len(sample_errors) < MAX_SAMPLE_ERRORS:
                    sample_errors.append(_serialize_validation_error("player", idx, raw_rec, exc))

        for idx, raw_rec in enumerate(job_result.raw_records):
            try:
                raw_jobs.append(RawJobRankingItem.model_validate(raw_rec))
            except ValidationError as exc:
                job_errors += 1
                if len(sample_errors) < MAX_SAMPLE_ERRORS:
                    sample_errors.append(_serialize_validation_error("job", idx, raw_rec, exc))

        for idx, raw_rec in enumerate(guild_result.raw_records):
            try:
                raw_guilds.append(RawGuildRankingItem.model_validate(raw_rec))
            except ValidationError as exc:
                guild_errors += 1
                raw_rank = _try_extract_raw_rank(raw_rec)
                if raw_rank in {1, 2, 3}:
                    failed_guild_podium_ranks.add(raw_rank)
                if len(sample_errors) < MAX_SAMPLE_ERRORS:
                    sample_errors.append(_serialize_validation_error("guild", idx, raw_rec, exc))

        dedup_players = _deduplicate_players(raw_players)
        dedup_jobs = _deduplicate_jobs(raw_jobs)
        dedup_guilds = _deduplicate_guilds(raw_guilds)

        records_extracted = (
            len(player_result.raw_records)
            + len(job_result.raw_records)
            + len(guild_result.raw_records)
        )
        total_errors = player_errors + job_errors + guild_errors
        http_status = (
            player_result.http_status_code
            or job_result.http_status_code
            or guild_result.http_status_code
            or self._last_http_status_code
            or 200
        )

        # Evaluate integrity guards in strict order
        rejected_status: str | None = None
        reject_reason = ""
        failed_category = "player"

        error_counts = {"player": player_errors, "job": job_errors, "guild": guild_errors}
        dominant_error_category = max(error_counts, key=lambda key: error_counts[key])

        if records_extracted == 0:
            rejected_status = "SCHEMA_DRIFT"
            failed_category = "player"
            reject_reason = "Zero records extracted across all ranking categories"
        else:
            player_ranks = {p.rank for p in dedup_players}
            guild_ranks = {g.rank for g in dedup_guilds}
            if failed_player_podium_ranks or not {1, 2, 3}.issubset(player_ranks):
                rejected_status = "SCHEMA_DRIFT"
                failed_category = "player"
                reject_reason = "Top-Rank Integrity Guard failed for player ranks 1-3"
            elif len(guild_result.raw_records) >= 3 and (
                failed_guild_podium_ranks or not {1, 2, 3}.issubset(guild_ranks)
            ):
                rejected_status = "SCHEMA_DRIFT"
                failed_category = "guild"
                reject_reason = "Top-Rank Integrity Guard failed for guild ranks 1-3"
            elif (total_errors / records_extracted) > MAX_ERROR_RATE:
                rejected_status = "SCHEMA_DRIFT"
                failed_category = dominant_error_category
                reject_reason = (
                    f"Validation error rate {total_errors / records_extracted:.2%} "
                    f"exceeds MAX_ERROR_RATE ({MAX_ERROR_RATE:.0%})"
                )
            elif len(dedup_players) < server.min_records_expected:
                rejected_status = "LOW_COUNT_QUARANTINE"
                failed_category = "player"
                reject_reason = (
                    f"Valid player count {len(dedup_players)} below "
                    f"min_records_expected ({server.min_records_expected})"
                )
            elif len(dedup_guilds) < MIN_GUILD_RECORDS:
                rejected_status = "LOW_COUNT_QUARANTINE"
                failed_category = "guild"
                reject_reason = (
                    f"Valid guild count {len(dedup_guilds)} below "
                    f"MIN_GUILD_RECORDS ({MIN_GUILD_RECORDS})"
                )

        if rejected_status is not None:
            all_payloads = [
                *self._normalize_category_payloads(player_result, "player"),
                *self._normalize_category_payloads(job_result, "job"),
                *self._normalize_category_payloads(guild_result, "guild"),
            ]
            q_path, q_sha256, q_size = self._write_quarantine_file(run_id, all_payloads)
            now_ts = datetime.now(UTC)
            err_details = ErrorDetails(
                reason=reject_reason,
                failed_category=failed_category,
                sample_errors=sample_errors,
                quarantine_sha256=q_sha256,
                quarantine_size_bytes=q_size,
            )
            elapsed_ms = self._elapsed_ms(start_mono)
            try:
                session.add(
                    ScrapeRun(
                        id=run_id,
                        server_id=self.server_id,
                        ranking_category="all",
                        trigger_type=trigger_type,
                        status=rejected_status,
                        records_extracted=records_extracted,
                        records_persisted=0,
                        validation_errors_count=total_errors,
                        execution_time_ms=elapsed_ms,
                        http_status_code=http_status,
                        quarantine_file_path=q_path,
                        quarantine_sha256=q_sha256,
                        quarantine_size_bytes=q_size,
                        error_details=err_details.model_dump(mode="json"),
                        created_at=now_ts,
                        updated_at=now_ts,
                    )
                )
                await session.commit()
            except Exception:
                await session.rollback()
                raise
            self._logger.warning(
                "scrape_quarantined",
                status=rejected_status,
                reason=reject_reason,
                records_extracted=records_extracted,
                validation_errors_count=total_errors,
                quarantine_file_path=q_path,
            )
            return ScrapeRunSummary(
                run_id=run_id,
                server_id=self.server_id,
                status=rejected_status,
                trigger_type=trigger_type,
                records_extracted=records_extracted,
                records_persisted=0,
                validation_errors_count=total_errors,
                execution_time_ms=elapsed_ms,
                http_status_code=http_status,
                quarantine_file_path=q_path,
                quarantine_sha256=q_sha256,
                quarantine_size_bytes=q_size,
                error_details=err_details,
            )

        reconciled_players = _reconcile_jobs_into_players(dedup_players, dedup_jobs)
        final_status = "PARTIAL_SUCCESS" if total_errors > 0 else "SUCCESS"
        accepted_error_details: ErrorDetails | None = (
            ErrorDetails(
                reason=(
                    f"Partial validation success with {total_errors}/{records_extracted} "
                    f"invalid records"
                ),
                failed_category=dominant_error_category,
                sample_errors=sample_errors,
            )
            if total_errors > 0
            else None
        )

        snapshot_ts = datetime.now(UTC)
        records_persisted = len(reconciled_players) + len(dedup_guilds)
        elapsed_ms = self._elapsed_ms(start_mono)

        try:
            session.add(
                ScrapeRun(
                    id=run_id,
                    server_id=self.server_id,
                    ranking_category="all",
                    trigger_type=trigger_type,
                    status=final_status,
                    records_extracted=records_extracted,
                    records_persisted=records_persisted,
                    validation_errors_count=total_errors,
                    execution_time_ms=elapsed_ms,
                    http_status_code=http_status,
                    error_details=(
                        accepted_error_details.model_dump(mode="json")
                        if accepted_error_details is not None
                        else None
                    ),
                    created_at=snapshot_ts,
                    updated_at=snapshot_ts,
                )
            )
            await session.flush()

            guild_master_by_name: dict[str, str | None] = {}
            for g in dedup_guilds:
                guild_master_by_name[g.guild_name] = (
                    g.master_name[:64] if g.master_name is not None else None
                )
            for p in reconciled_players:
                if p.guild_name is not None:
                    truncated_guild = p.guild_name[:64]
                    if truncated_guild not in guild_master_by_name:
                        guild_master_by_name[truncated_guild] = None

            guild_rows: list[dict[str, Any]] = [
                {
                    "server_id": self.server_id,
                    "guild_name": guild_name,
                    "master_char_name": master_name,
                    "first_seen_at": snapshot_ts,
                    "last_seen_at": snapshot_ts,
                    "is_active": True,
                }
                for guild_name, master_name in guild_master_by_name.items()
            ]
            guild_id_map: dict[str, uuid.UUID] = {}
            for guild_chunk in _chunked(guild_rows, SNAPSHOT_INSERT_CHUNK_SIZE):
                guild_stmt = pg_insert(Guild).values(guild_chunk)
                guild_upsert = guild_stmt.on_conflict_do_update(
                    index_elements=[Guild.server_id, Guild.guild_name],
                    set_={
                        "master_char_name": func.coalesce(
                            guild_stmt.excluded.master_char_name,
                            Guild.master_char_name,
                        ),
                        "last_seen_at": guild_stmt.excluded.last_seen_at,
                        "is_active": True,
                    },
                ).returning(Guild.id, Guild.guild_name)
                result = await session.execute(guild_upsert)
                for row in result.all():
                    guild_id_map[str(row[1])] = row[0]

            char_rows: list[dict[str, Any]] = [
                {
                    "server_id": self.server_id,
                    "char_name": p.char_name,
                    "race": p.race if p.race in ("CH", "EU") else "UNK",
                    "first_seen_at": snapshot_ts,
                    "last_seen_at": snapshot_ts,
                }
                for p in reconciled_players
            ]
            char_id_map: dict[str, uuid.UUID] = {}
            for char_chunk in _chunked(char_rows, SNAPSHOT_INSERT_CHUNK_SIZE):
                char_stmt = pg_insert(Character).values(char_chunk)
                char_upsert = char_stmt.on_conflict_do_update(
                    index_elements=[Character.server_id, Character.char_name],
                    set_={
                        "race": case(
                            (char_stmt.excluded.race.in_(("CH", "EU")), char_stmt.excluded.race),
                            else_=Character.race,
                        ),
                        "last_seen_at": char_stmt.excluded.last_seen_at,
                    },
                ).returning(Character.id, Character.char_name)
                result = await session.execute(char_upsert)
                for row in result.all():
                    char_id_map[str(row[1])] = row[0]

            char_snapshot_rows: list[dict[str, Any]] = [
                {
                    "scrape_run_id": run_id,
                    "server_id": self.server_id,
                    "character_id": char_id_map[p.char_name],
                    "guild_id": (
                        guild_id_map.get(p.guild_name[:64]) if p.guild_name is not None else None
                    ),
                    "rank_position": p.rank,
                    "level": p.level,
                    "race": p.race,
                    "item_points": (
                        Decimal(f"{p.item_points:.2f}")
                        if p.item_points is not None
                        else Decimal("0.00")
                    ),
                    "strength": p.strength,
                    "intellect": p.intellect,
                    "title": p.title[:64] if p.title is not None else None,
                    "job_type": p.job_type,
                    "job_level": p.job_level,
                    "job_points": p.job_points,
                    "job_exp": p.job_exp,
                    "honor_points": p.honor_points,
                    "extra_metrics": dict(p.extra_metrics),
                    "created_at": snapshot_ts,
                    "updated_at": snapshot_ts,
                }
                for p in reconciled_players
            ]
            for char_snap_chunk in _chunked(char_snapshot_rows, SNAPSHOT_INSERT_CHUNK_SIZE):
                await session.execute(sa_insert(CharacterSnapshot), char_snap_chunk)

            guild_snapshot_rows: list[dict[str, Any]] = [
                {
                    "scrape_run_id": run_id,
                    "server_id": self.server_id,
                    "guild_id": guild_id_map[g.guild_name],
                    "rank_position": g.rank,
                    "members_count": g.members_count if g.members_count is not None else 0,
                    "total_item_points": (
                        Decimal(f"{g.total_item_points:.2f}")
                        if g.total_item_points is not None
                        else Decimal("0.00")
                    ),
                    "fortress_held": g.fortress_held[:32] if g.fortress_held is not None else None,
                    "extra_metrics": dict(g.extra_metrics),
                    "created_at": snapshot_ts,
                    "updated_at": snapshot_ts,
                }
                for g in dedup_guilds
            ]
            for guild_snap_chunk in _chunked(guild_snapshot_rows, SNAPSHOT_INSERT_CHUNK_SIZE):
                await session.execute(sa_insert(GuildSnapshot), guild_snap_chunk)

            await session.commit()
        except Exception:
            await session.rollback()
            raise

        self._logger.info(
            "scrape_completed",
            status=final_status,
            records_extracted=records_extracted,
            records_persisted=records_persisted,
            validation_errors_count=total_errors,
            execution_time_ms=elapsed_ms,
        )
        return ScrapeRunSummary(
            run_id=run_id,
            server_id=self.server_id,
            status=final_status,
            trigger_type=trigger_type,
            records_extracted=records_extracted,
            records_persisted=records_persisted,
            validation_errors_count=total_errors,
            execution_time_ms=elapsed_ms,
            http_status_code=http_status,
            error_details=accepted_error_details,
        )
