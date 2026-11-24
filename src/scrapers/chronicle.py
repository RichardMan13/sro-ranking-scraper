from __future__ import annotations

import math
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any, Final
from urllib.parse import urlsplit

import httpx
from lxml import etree
from lxml import html as lxml_html

from src.models.pydantic_schemas import _EMPTY_PLACEHOLDERS
from src.scrapers.base import (
    BaseRankingScraper,
    CategoryFetchResult,
    PagePayload,
    TransportFetchError,
)

_PLAYER_MAX_PAGES: Final[int] = 10
_JOB_MAX_PAGES: Final[int] = 5
_GUILD_MAX_PAGES: Final[int] = 2
_CHRONICLE_TABLE_CLASSES: Final[tuple[str, ...]] = ("table", "table-striped", "table-hover")

_EMPTY_NOTICE_RE: Final[re.Pattern[str]] = re.compile(
    r"(?:"
    r"\bno\s+(?:records?|results?|data|entries|rows|items|matches|players?|guilds?|jobs?|characters?)\b"
    r"|\b0\s+(?:records?|results?|data|entries|rows|items|matches|players?|guilds?|jobs?|characters?)\b"
    r"|\bshowing\s+0\b"
    r"|\btotal\s*[:=]?\s*0\b"
    r"|\b(?:empty|nothing\s+found)\b"
    r")",
    re.IGNORECASE,
)


def _decode_html_bytes(content: bytes, encoding: str | None) -> str:
    chosen = (encoding or "").strip() or "utf-8"
    try:
        return content.decode(chosen, errors="replace")
    except LookupError:
        return content.decode("utf-8", errors="replace")


def _xpath_elements(node: etree._Element, expr: str) -> list[etree._Element]:
    raw = node.xpath(expr)
    if not isinstance(raw, list):
        return []
    return [item for item in raw if isinstance(item, etree._Element)]


def _element_text(element: etree._Element) -> str:
    parts: list[str] = []
    try:
        for chunk in element.itertext():
            if isinstance(chunk, bytes):
                parts.append(chunk.decode("utf-8", errors="replace"))
            else:
                parts.append(chunk)
    except UnicodeDecodeError:
        raw_html = etree.tostring(element, encoding="utf-8", method="html")
        decoded = raw_html.decode("utf-8", errors="replace")
        fallback_tree = lxml_html.fromstring(
            decoded.encode("utf-8"),
            parser=lxml_html.HTMLParser(encoding="utf-8"),
        )
        parts = [
            chunk.decode("utf-8", errors="replace") if isinstance(chunk, bytes) else chunk
            for chunk in fallback_tree.itertext()
        ]
    return " ".join("".join(parts).split())


def _cell_text(cells: list[etree._Element], index: int) -> str | None:
    if index >= len(cells):
        return None
    return _element_text(cells[index])


def _strip_rank_prefix(raw_rank: str | None) -> str | None:
    if raw_rank is None:
        return None
    cleaned = raw_rank.strip()
    if cleaned.startswith("#"):
        return cleaned.lstrip("#").strip()
    return cleaned


def _extract_table_rows(
    content: bytes,
    table_classes: tuple[str, ...],
    encoding: str | None,
) -> list[etree._Element]:
    if not content.strip():
        return []
    decoded = _decode_html_bytes(content, encoding)
    if not decoded.strip():
        return []
    try:
        tree = lxml_html.fromstring(
            decoded.encode("utf-8"),
            parser=lxml_html.HTMLParser(encoding="utf-8"),
        )
    except etree.ParserError:
        return []

    class_predicates = " and ".join(
        f"contains(concat(' ', normalize-space(@class), ' '), ' {cls} ')" for cls in table_classes
    )
    tables = _xpath_elements(tree, f"//table[{class_predicates}]")
    rows: list[etree._Element] = []
    for table in tables:
        if _xpath_elements(table, "./tbody"):
            rows.extend(_xpath_elements(table, "./tbody/tr"))
        else:
            rows.extend(_xpath_elements(table, "./tr[td]"))
    return rows


def _is_empty_notice_row(cells: list[etree._Element]) -> bool:
    if len(cells) != 1 or cells[0].get("colspan") is None:
        return False
    text = _element_text(cells[0])
    if not text or not any(ch.isdigit() for ch in text):
        return True
    return bool(_EMPTY_NOTICE_RE.search(text))


def _extract_char_name(cell: etree._Element) -> str:
    char_links = _xpath_elements(
        cell,
        ".//a[contains(concat(' ', normalize-space(@class), ' '), ' char-link ')]",
    )
    if char_links:
        return _element_text(char_links[0])
    links = _xpath_elements(cell, ".//a")
    if links:
        return _element_text(links[0])
    return _element_text(cell)


def _extract_race_from_cell(cell: etree._Element) -> str | None:
    for img in _xpath_elements(cell, ".//img"):
        for attr in ("data-race", "alt", "title"):
            raw_val = img.get(attr)
            if raw_val and raw_val.strip():
                return raw_val.strip()
        src = img.get("src")
        if src and src.strip():
            filename = Path(urlsplit(src.strip()).path).name
            if filename:
                return filename
    return None


def _parse_optional_numeric(raw_val: str) -> int | float | str:
    compact = raw_val.replace(",", "").strip()
    if compact.isascii() and compact.isdigit():
        try:
            return int(compact)
        except ValueError:
            return raw_val
    try:
        parsed = float(compact)
    except ValueError:
        return raw_val
    return parsed if math.isfinite(parsed) else raw_val


class ChronicleScraper(BaseRankingScraper):
    """Scraper for Chronicle SRO (`https://play-chronicle.com`)."""

    server_id: str = "chronicle"

    async def _fetch_paginated_html(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        *,
        route_template: str,
        max_pages: int,
        category: str,
        row_mapper: Callable[[etree._Element, list[etree._Element]], dict[str, Any]],
    ) -> CategoryFetchResult:
        clean_base = base_url.rstrip("/")
        raw_records: list[dict[str, Any]] = []
        raw_payloads: list[PagePayload | tuple[str, bytes]] = []
        last_status: int | None = None

        for page in range(1, max_pages + 1):
            url = f"{clean_base}{route_template.format(page=page)}"
            try:
                response = await self._fetch_url(client, url, category=category)
            except TransportFetchError as exc:
                if page > 1 and exc.status_code == 404:
                    break
                raise
            last_status = response.status_code
            raw_payloads.append(
                PagePayload(
                    url=str(response.url),
                    content=response.content,
                    status_code=response.status_code,
                    category=category,
                )
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

            if not page_records:
                break
            raw_records.extend(page_records)

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
    def _map_job_row(_tr: etree._Element, cells: list[etree._Element]) -> dict[str, Any]:
        char_name = _extract_char_name(cells[1]) if len(cells) > 1 else None
        extra_metrics: dict[str, Any] = {}
        kd_ratio = _cell_text(cells, 3)
        if kd_ratio is not None and kd_ratio not in _EMPTY_PLACEHOLDERS:
            extra_metrics["job_kd_ratio"] = kd_ratio

        return {
            "rank": _strip_rank_prefix(_cell_text(cells, 0)),
            "char_name": char_name,
            "guild_name": _cell_text(cells, 2),
            "job_type": None,
            "job_level": None,
            "job_points": _cell_text(cells, 4),
            "extra_metrics": extra_metrics,
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
        """Fetch and parse Chronicle player ranking pages (1..10)."""
        return await self._fetch_paginated_html(
            client,
            base_url,
            route_template="/ranking/top-player-{page}.html",
            max_pages=_PLAYER_MAX_PAGES,
            category="player",
            row_mapper=self._map_player_row,
        )

    async def fetch_job_pages(
        self,
        client: httpx.AsyncClient,
        base_url: str,
    ) -> CategoryFetchResult:
        """Fetch and parse Chronicle job ranking pages (1..5)."""
        return await self._fetch_paginated_html(
            client,
            base_url,
            route_template="/ranking/top-job-{page}.html",
            max_pages=_JOB_MAX_PAGES,
            category="job",
            row_mapper=self._map_job_row,
        )

    async def fetch_guild_pages(
        self,
        client: httpx.AsyncClient,
        base_url: str,
    ) -> CategoryFetchResult:
        """Fetch and parse Chronicle guild ranking pages (1..2)."""
        return await self._fetch_paginated_html(
            client,
            base_url,
            route_template="/ranking/top-guild-{page}.html",
            max_pages=_GUILD_MAX_PAGES,
            category="guild",
            row_mapper=self._map_guild_row,
        )
