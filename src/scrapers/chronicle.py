from __future__ import annotations

import asyncio
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Final, Literal

import httpx
from lxml import etree

from src.core.config import Settings
from src.models.pydantic_schemas import _EMPTY_PLACEHOLDERS
from src.scrapers._html_utils import (
    cell_text as _cell_text,
)
from src.scrapers._html_utils import (
    decode_html_bytes as _decode_html_bytes,
)
from src.scrapers._html_utils import (
    extract_char_name as _extract_char_name,
)
from src.scrapers._html_utils import (
    extract_race_from_cell as _extract_race_from_cell,
)
from src.scrapers._html_utils import (
    extract_table_rows as _extract_tree_rows,
)
from src.scrapers._html_utils import (
    is_empty_notice_row as _is_empty_notice_row,
)
from src.scrapers._html_utils import (
    parse_html_tree as _parse_html_tree,
)
from src.scrapers._html_utils import (
    parse_optional_numeric as _parse_optional_numeric,
)
from src.scrapers._html_utils import (
    strip_rank_prefix as _strip_rank_prefix,
)
from src.scrapers._html_utils import (
    xpath_elements as _xpath_elements,
)
from src.scrapers.base import (
    DEFAULT_MAX_CONCURRENT_REQUESTS,
    DEFAULT_RETRY_DELAYS,
    DEFAULT_TIMEOUT_BUDGET_SECONDS,
    BaseRankingScraper,
    BlockedChallengeError,
    CategoryFetchResult,
    PagePayload,
    TargetMaintenanceError,
    TransportFetchError,
    _flatten_exceptions,
)

_DEFAULT_MAX_PAGES_SAFETY_CAP: Final[int] = 400
_CHRONICLE_TABLE_CLASSES: Final[tuple[str, ...]] = ("table", "table-striped", "table-hover")

_CHRONICLE_JOB_ROUTES: Final[tuple[tuple[Literal["TRADER", "HUNTER", "THIEF"], str], ...]] = (
    ("TRADER", "/ranking/job/top-trader-{page}.html"),
    ("HUNTER", "/ranking/job/top-hunter-{page}.html"),
    ("THIEF", "/ranking/job/top-thieves-{page}.html"),
)

_PAGINATOR_AJAX_RE: Final[re.Pattern[str]] = re.compile(
    r"paginatorAjax\s*\(\s*['\"][^'\"]+['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)",
    re.IGNORECASE,
)
_PAGE_NUMBER_SUFFIX_RE: Final[re.Pattern[str]] = re.compile(
    r"-(\d+)\.html(?:[?#].*)?$", re.IGNORECASE
)


@dataclass(frozen=True, slots=True)
class _PaginatorInfo:
    """Pagination hints extracted from Chronicle `paginatorAjax` calls."""

    has_paginator: bool
    max_page: int | None
    has_next: bool


@dataclass(frozen=True, slots=True)
class _FetchedPage:
    """Parsed single-page outcome used for ordered batch processing."""

    page: int
    status_code: int | None
    payload: PagePayload | None
    records: list[dict[str, Any]]
    paginator: _PaginatorInfo
    not_found: bool = False


def _extract_table_rows(
    content: bytes,
    table_classes: tuple[str, ...],
    encoding: str | None,
) -> list[etree._Element]:
    tree = _parse_html_tree(content, encoding)
    return _extract_tree_rows(tree, table_classes)


def _extract_paginator_info(
    content: bytes,
    encoding: str | None,
    *,
    current_page: int,
) -> _PaginatorInfo:
    if not content.strip():
        return _PaginatorInfo(has_paginator=False, max_page=None, has_next=False)
    decoded = _decode_html_bytes(content, encoding)
    matches = _PAGINATOR_AJAX_RE.findall(decoded)
    if not matches:
        return _PaginatorInfo(has_paginator=False, max_page=None, has_next=False)

    page_numbers: list[int] = []
    for target_url in matches:
        page_match = _PAGE_NUMBER_SUFFIX_RE.search(target_url.strip())
        if page_match is not None:
            page_numbers.append(int(page_match.group(1)))

    if not page_numbers:
        return _PaginatorInfo(has_paginator=True, max_page=None, has_next=False)

    max_page = max(page_numbers)
    return _PaginatorInfo(
        has_paginator=True,
        max_page=max_page,
        has_next=max_page > current_page,
    )


def _page_fingerprint(records: list[dict[str, Any]]) -> tuple[tuple[tuple[str, str], ...], ...]:
    return tuple(
        tuple((key, repr(val)) for key, val in sorted(record.items())) for record in records
    )


class ChronicleScraper(BaseRankingScraper):
    """Scraper for Chronicle SRO (`https://play-chronicle.com`)."""

    server_id: str = "chronicle"

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        retry_delays: tuple[float, ...] = DEFAULT_RETRY_DELAYS,
        timeout_budget_seconds: float = DEFAULT_TIMEOUT_BUDGET_SECONDS,
        max_pages_per_category: int | None = None,
    ) -> None:
        super().__init__(
            settings=settings,
            retry_delays=retry_delays,
            timeout_budget_seconds=timeout_budget_seconds,
        )
        resolved_max_pages = (
            max_pages_per_category
            if max_pages_per_category is not None
            else self.settings.max_pages_per_category
        )
        if resolved_max_pages < 1:
            raise ValueError("max_pages_per_category must be >= 1")
        self._max_pages_per_category: int = resolved_max_pages

    async def _fetch_single_page(
        self,
        client: httpx.AsyncClient,
        clean_base: str,
        *,
        route_template: str,
        page: int,
        category: str,
        row_mapper: Callable[[etree._Element, list[etree._Element]], dict[str, Any]],
    ) -> _FetchedPage:
        url = f"{clean_base}{route_template.format(page=page)}"
        try:
            response = await self._fetch_url(client, url, category=category)
        except TransportFetchError as exc:
            if page > 1 and exc.status_code == 404:
                return _FetchedPage(
                    page=page,
                    status_code=None,
                    payload=None,
                    records=[],
                    paginator=_PaginatorInfo(has_paginator=False, max_page=None, has_next=False),
                    not_found=True,
                )
            raise

        payload = PagePayload(
            url=str(response.url),
            content=response.content,
            status_code=response.status_code,
            category=category,
        )
        rows = _extract_table_rows(
            response.content,
            _CHRONICLE_TABLE_CLASSES,
            response.encoding,
        )
        page_records: list[dict[str, Any]] = []
        for tr in rows:
            cells = _xpath_elements(tr, "./td")
            if not cells or _is_empty_notice_row(cells):
                continue
            page_records.append(row_mapper(tr, cells))

        paginator = _extract_paginator_info(
            response.content,
            response.encoding,
            current_page=page,
        )
        return _FetchedPage(
            page=page,
            status_code=response.status_code,
            payload=payload,
            records=page_records,
            paginator=paginator,
        )

    async def _fetch_paginated_html(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        *,
        route_template: str,
        category: str,
        row_mapper: Callable[[etree._Element, list[etree._Element]], dict[str, Any]],
    ) -> CategoryFetchResult:
        clean_base = base_url.rstrip("/")
        raw_records: list[dict[str, Any]] = []
        raw_payloads: list[PagePayload | tuple[str, bytes]] = []
        last_status: int | None = None
        max_pages = self._max_pages_per_category

        first = await self._fetch_single_page(
            client,
            clean_base,
            route_template=route_template,
            page=1,
            category=category,
            row_mapper=row_mapper,
        )
        last_status = first.status_code
        if first.payload is not None:
            raw_payloads.append(first.payload)
        if not first.records:
            return CategoryFetchResult(
                raw_records=raw_records,
                raw_payloads=raw_payloads,
                http_status_code=last_status,
            )

        raw_records.extend(first.records)
        prev_fingerprint = _page_fingerprint(first.records)

        if first.paginator.has_paginator:
            if not first.paginator.has_next or first.paginator.max_page is None:
                return CategoryFetchResult(
                    raw_records=raw_records,
                    raw_payloads=raw_payloads,
                    http_status_code=last_status,
                )

            known_max_page = min(first.paginator.max_page, max_pages)
            next_page = 2
            while next_page <= known_max_page:
                batch_end = min(
                    next_page + DEFAULT_MAX_CONCURRENT_REQUESTS - 1,
                    known_max_page,
                )
                batch_pages = list(range(next_page, batch_end + 1))

                if len(batch_pages) == 1:
                    fetched_batch = [
                        await self._fetch_single_page(
                            client,
                            clean_base,
                            route_template=route_template,
                            page=batch_pages[0],
                            category=category,
                            row_mapper=row_mapper,
                        )
                    ]
                else:
                    try:
                        async with asyncio.TaskGroup() as tg:
                            tasks = [
                                tg.create_task(
                                    self._fetch_single_page(
                                        client,
                                        clean_base,
                                        route_template=route_template,
                                        page=p,
                                        category=category,
                                        row_mapper=row_mapper,
                                    )
                                )
                                for p in batch_pages
                            ]
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
                    fetched_batch = [task.result() for task in tasks]

                stop = False
                for fetched in fetched_batch:
                    if fetched.not_found:
                        stop = True
                        break
                    if fetched.status_code is not None:
                        last_status = fetched.status_code
                    if fetched.payload is not None:
                        raw_payloads.append(fetched.payload)
                    if not fetched.records:
                        stop = True
                        break
                    curr_fingerprint = _page_fingerprint(fetched.records)
                    if curr_fingerprint == prev_fingerprint:
                        stop = True
                        break
                    raw_records.extend(fetched.records)
                    prev_fingerprint = curr_fingerprint

                    if fetched.paginator.has_paginator:
                        if fetched.paginator.max_page is not None:
                            known_max_page = min(
                                max(known_max_page, fetched.paginator.max_page),
                                max_pages,
                            )
                        if not fetched.paginator.has_next:
                            stop = True
                            break

                if stop:
                    break
                next_page = batch_end + 1

            return CategoryFetchResult(
                raw_records=raw_records,
                raw_payloads=raw_payloads,
                http_status_code=last_status,
            )

        # Fallback when HTML has no `paginatorAjax` links: sequential probing with
        # anti-clamping fingerprint guard, 404 stop, and empty-page stop.
        for page in range(2, max_pages + 1):
            fetched = await self._fetch_single_page(
                client,
                clean_base,
                route_template=route_template,
                page=page,
                category=category,
                row_mapper=row_mapper,
            )
            if fetched.not_found:
                break
            if fetched.status_code is not None:
                last_status = fetched.status_code
            if fetched.payload is not None:
                raw_payloads.append(fetched.payload)
            if not fetched.records:
                break
            curr_fingerprint = _page_fingerprint(fetched.records)
            if curr_fingerprint == prev_fingerprint:
                break
            raw_records.extend(fetched.records)
            prev_fingerprint = curr_fingerprint

        return CategoryFetchResult(
            raw_records=raw_records,
            raw_payloads=raw_payloads,
            http_status_code=last_status,
        )

    @staticmethod
    def _map_player_row(_tr: etree._Element, cells: list[etree._Element]) -> dict[str, Any]:
        char_name = _extract_char_name(cells[1]) if len(cells) > 1 else None
        race = _extract_race_from_cell(cells[1]) if len(cells) > 1 else None

        return {
            "rank": _strip_rank_prefix(_cell_text(cells, 0)),
            "char_name": char_name,
            "race": race,
            "guild_name": _cell_text(cells, 2),
            "level": _cell_text(cells, 3),
            "item_points": _cell_text(cells, 4),
            "extra_metrics": {},
        }

    @staticmethod
    def _make_job_row_mapper(
        job_type: Literal["TRADER", "HUNTER", "THIEF"],
    ) -> Callable[[etree._Element, list[etree._Element]], dict[str, Any]]:
        def _map_row(_tr: etree._Element, cells: list[etree._Element]) -> dict[str, Any]:
            char_name = _extract_char_name(cells[1]) if len(cells) > 1 else None
            return {
                "rank": _strip_rank_prefix(_cell_text(cells, 0)),
                "char_name": char_name,
                "guild_name": _cell_text(cells, 2),
                "job_type": job_type,
                "job_level": _cell_text(cells, 3),
                "job_exp": _cell_text(cells, 4),
                "extra_metrics": {},
            }

        return _map_row

    @staticmethod
    def _map_guild_row(_tr: etree._Element, cells: list[etree._Element]) -> dict[str, Any]:
        guild_name = _extract_char_name(cells[1]) if len(cells) > 1 else None
        extra_metrics: dict[str, Any] = {}
        guild_level = _cell_text(cells, 2)
        if guild_level is not None and guild_level not in _EMPTY_PLACEHOLDERS:
            extra_metrics["guild_level"] = _parse_optional_numeric(guild_level)

        raw_members = _cell_text(cells, 3)
        if raw_members is not None and "/" in raw_members:
            raw_members = raw_members.split("/", maxsplit=1)[0].strip()

        return {
            "rank": _strip_rank_prefix(_cell_text(cells, 0)),
            "guild_name": guild_name,
            "master_name": None,
            "members_count": raw_members,
            "total_item_points": _cell_text(cells, 4),
            "fortress_held": None,
            "extra_metrics": extra_metrics,
        }

    async def fetch_player_pages(
        self,
        client: httpx.AsyncClient,
        base_url: str,
    ) -> CategoryFetchResult:
        """Fetch and parse all Chronicle player ranking pages."""
        return await self._fetch_paginated_html(
            client,
            base_url,
            route_template="/ranking/top-player-{page}.html",
            category="player",
            row_mapper=self._map_player_row,
        )

    async def fetch_job_pages(
        self,
        client: httpx.AsyncClient,
        base_url: str,
    ) -> CategoryFetchResult:
        """Fetch and parse all Chronicle job ranking filters (trader, hunter, thieves)."""
        try:
            async with asyncio.TaskGroup() as tg:
                tasks = [
                    tg.create_task(
                        self._fetch_paginated_html(
                            client,
                            base_url,
                            route_template=route_template,
                            category="job",
                            row_mapper=self._make_job_row_mapper(job_type),
                        )
                    )
                    for job_type, route_template in _CHRONICLE_JOB_ROUTES
                ]
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

        combined_records: list[dict[str, Any]] = []
        combined_payloads: list[PagePayload | tuple[str, bytes]] = []
        last_status: int | None = None
        for task in tasks:
            sub_result = task.result()
            combined_records.extend(sub_result.raw_records)
            combined_payloads.extend(sub_result.raw_payloads)
            if sub_result.http_status_code is not None:
                last_status = sub_result.http_status_code

        return CategoryFetchResult(
            raw_records=combined_records,
            raw_payloads=combined_payloads,
            http_status_code=last_status,
        )

    async def fetch_guild_pages(
        self,
        client: httpx.AsyncClient,
        base_url: str,
    ) -> CategoryFetchResult:
        """Fetch and parse all Chronicle guild ranking pages."""
        return await self._fetch_paginated_html(
            client,
            base_url,
            route_template="/ranking/top-guild-{page}.html",
            category="guild",
            row_mapper=self._map_guild_row,
        )
