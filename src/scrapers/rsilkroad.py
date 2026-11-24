from __future__ import annotations

import asyncio
import math
import random
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Final
from urllib.parse import parse_qs, urlsplit

import httpx
from lxml import etree

from src.core.config import Settings
from src.models.pydantic_schemas import _EMPTY_PLACEHOLDERS
from src.scrapers._html_utils import (
    cell_text as _cell_text,
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
_RSILKROAD_PAGE_SIZE: Final[int] = 100
_RSILKROAD_TABLE_CLASSES: Final[tuple[str, ...]] = ("table",)
DEFAULT_INTER_BATCH_DELAY_RANGE: Final[tuple[float, float]] = (0.35, 0.75)


@dataclass(frozen=True, slots=True)
class _PagedListInfo:
    """Pagination hints extracted from ASP.NET MVC PagedList `<ul class="pagination">`."""

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
    paginator: _PagedListInfo
    not_found: bool = False


def _extract_main_table_rows(tree: etree._Element | None) -> list[etree._Element]:
    return _extract_tree_rows(tree, _RSILKROAD_TABLE_CLASSES)


def _extract_page_param(href: str) -> int | None:
    query = parse_qs(urlsplit(href.strip()).query)
    raw_values = query.get("page")
    if not raw_values:
        return None
    raw_page = raw_values[0].strip()
    if raw_page.isascii() and raw_page.isdigit():
        page_num = int(raw_page)
        return page_num if page_num >= 1 else None
    return None


def _extract_pagedlist_info(
    tree: etree._Element | None,
    *,
    current_page: int,
) -> _PagedListInfo:
    if tree is None:
        return _PagedListInfo(has_paginator=False, max_page=None, has_next=False)

    paginators = _xpath_elements(
        tree,
        "//ul[contains(concat(' ', normalize-space(@class), ' '), ' pagination ')]",
    )
    if not paginators:
        return _PagedListInfo(has_paginator=False, max_page=None, has_next=False)

    page_numbers: list[int] = []
    for paginator in paginators:
        for li in _xpath_elements(paginator, "./li"):
            li_classes = set((li.get("class") or "").split())
            if "disabled" in li_classes:
                continue
            for anchor in _xpath_elements(li, ".//a[@href]"):
                href = anchor.get("href")
                if not href:
                    continue
                parsed_page = _extract_page_param(href)
                if parsed_page is not None:
                    page_numbers.append(parsed_page)

    if not page_numbers:
        return _PagedListInfo(has_paginator=True, max_page=None, has_next=False)

    max_page = max(page_numbers)
    return _PagedListInfo(
        has_paginator=True,
        max_page=max_page,
        has_next=max_page > current_page,
    )


class RSilkroadScraper(BaseRankingScraper):
    """Scraper for RSilkroad (`https://www.rsilkroad.com`)."""

    server_id: str = "rsilkroad"

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        retry_delays: tuple[float, ...] = DEFAULT_RETRY_DELAYS,
        timeout_budget_seconds: float = DEFAULT_TIMEOUT_BUDGET_SECONDS,
        max_pages_per_category: int | None = None,
        page_size: int | None = None,
        inter_batch_delay_range: tuple[float, float] | None = None,
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

        resolved_page_size = (
            page_size if page_size is not None else self.settings.rsilkroad_page_size
        )
        if resolved_page_size < 1 or resolved_page_size > 100:
            raise ValueError("page_size must be between 1 and 100")

        resolved_delay_range = (
            inter_batch_delay_range
            if inter_batch_delay_range is not None
            else (
                self.settings.rsilkroad_inter_batch_delay_min,
                self.settings.rsilkroad_inter_batch_delay_max,
            )
        )
        min_delay, max_delay = resolved_delay_range
        if (
            not math.isfinite(min_delay)
            or not math.isfinite(max_delay)
            or min_delay < 0
            or max_delay < min_delay
        ):
            raise ValueError(
                "inter_batch_delay_range must contain finite values "
                "satisfying 0 <= min_delay <= max_delay"
            )

        self._max_pages_per_category: int = resolved_max_pages
        self._page_size: int = resolved_page_size
        self._inter_batch_delay_range: tuple[float, float] = (min_delay, max_delay)

    def _cf_cookies(self) -> dict[str, str] | None:
        secret = self.settings.rsilkroad_cf_clearance
        if secret is None:
            return None
        token = secret.get_secret_value().strip()
        return {"cf_clearance": token} if token else None

    async def _sleep_between_batches(self) -> None:
        min_delay, max_delay = self._inter_batch_delay_range
        if max_delay <= 0.0:
            return
        delay = min_delay if min_delay == max_delay else random.uniform(min_delay, max_delay)
        if delay > 0.0:
            await asyncio.sleep(delay)

    async def _fetch_single_page(
        self,
        client: httpx.AsyncClient,
        clean_base: str,
        *,
        route_path: str,
        page: int,
        category: str,
        row_mapper: Callable[[etree._Element, list[etree._Element]], dict[str, Any]],
    ) -> _FetchedPage:
        url = f"{clean_base}{route_path}?page={page}&pageSize={self._page_size}"
        try:
            response = await self._fetch_url(
                client,
                url,
                cookies=self._cf_cookies(),
                category=category,
            )
        except TransportFetchError as exc:
            if page > 1 and exc.status_code == 404:
                return _FetchedPage(
                    page=page,
                    status_code=None,
                    payload=None,
                    records=[],
                    paginator=_PagedListInfo(has_paginator=False, max_page=None, has_next=False),
                    not_found=True,
                )
            raise

        payload = PagePayload(
            url=str(response.url),
            content=response.content,
            status_code=response.status_code,
            category=category,
        )
        tree = _parse_html_tree(response.content, response.encoding)
        rows = _extract_main_table_rows(tree)
        page_records: list[dict[str, Any]] = []
        for tr in rows:
            cells = _xpath_elements(tr, "./td")
            if not cells or _is_empty_notice_row(cells):
                continue
            page_records.append(row_mapper(tr, cells))

        paginator = _extract_pagedlist_info(tree, current_page=page)
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
        route_path: str,
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
            route_path=route_path,
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
                            route_path=route_path,
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
                                        route_path=route_path,
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
                    raw_records.extend(fetched.records)

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
                if next_page <= known_max_page:
                    await self._sleep_between_batches()

            return CategoryFetchResult(
                raw_records=raw_records,
                raw_payloads=raw_payloads,
                http_status_code=last_status,
            )

        # Fallback when HTML has no `<ul class="pagination">`: sequential probing until empty/404.
        for page in range(2, max_pages + 1):
            await self._sleep_between_batches()
            fetched = await self._fetch_single_page(
                client,
                clean_base,
                route_path=route_path,
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
            raw_records.extend(fetched.records)

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
        """Fetch and parse all RSilkroad player ranking pages using configured `pageSize`."""
        return await self._fetch_paginated_html(
            client,
            base_url,
            route_path="/Rank/Player",
            category="player",
            row_mapper=self._map_player_row,
        )

    async def fetch_job_pages(
        self,
        client: httpx.AsyncClient,
        base_url: str,
    ) -> CategoryFetchResult:
        """RSilkroad does not expose a dedicated Job ranking route under `/Rank/*`."""
        return CategoryFetchResult()

    async def fetch_guild_pages(
        self,
        client: httpx.AsyncClient,
        base_url: str,
    ) -> CategoryFetchResult:
        """Fetch and parse all RSilkroad guild ranking pages using configured `pageSize`."""
        return await self._fetch_paginated_html(
            client,
            base_url,
            route_path="/Rank/Guild",
            category="guild",
            row_mapper=self._map_guild_row,
        )
