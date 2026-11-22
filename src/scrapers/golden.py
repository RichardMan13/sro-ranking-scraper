from __future__ import annotations

import re
from collections.abc import Callable
from pathlib import Path
from typing import Any, Final
from urllib.parse import urlsplit

import httpx
from lxml import etree
from lxml import html as lxml_html

from src.models.pydantic_schemas import _EMPTY_PLACEHOLDERS
from src.scrapers.base import BaseRankingScraper, CategoryFetchResult, PagePayload

_PLAYER_MAX_PAGES: Final[int] = 10
_JOB_MAX_PAGES: Final[int] = 5
_GUILD_MAX_PAGES: Final[int] = 2

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
    table_class: str,
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

    tables = _xpath_elements(
        tree,
        f"//table[contains(concat(' ', normalize-space(@class), ' '), ' {table_class} ')]",
    )
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


def _extract_name(cell: etree._Element) -> str:
    links = _xpath_elements(cell, ".//a")
    if links:
        return _element_text(links[0])
    return _element_text(cell)


def _extract_golden_race(cell: etree._Element) -> str | None:
    for img in _xpath_elements(cell, ".//img"):
        for attr in ("alt", "data-race", "title"):
            raw_val = img.get(attr)
            if raw_val and raw_val.strip():
                return raw_val.strip()
        src = img.get("src")
        if src and src.strip():
            filename = Path(urlsplit(src.strip()).path).name
            if filename:
                return filename
    text = _element_text(cell)
    return text or None


def _extract_extra_value(
    row: etree._Element,
    cells: list[etree._Element],
    *,
    tr_attr: str,
    field_name: str,
    css_class: str,
    col_index: int,
) -> str | None:
    attr_val = row.get(tr_attr)
    if attr_val is not None:
        cleaned = attr_val.strip()
        if cleaned and cleaned not in _EMPTY_PLACEHOLDERS:
            return cleaned
    tagged = _xpath_elements(
        row,
        f".//*[@data-field='{field_name}' or "
        f"contains(concat(' ', normalize-space(@class), ' '), ' {css_class} ')]",
    )
    if tagged:
        text = _element_text(tagged[0])
        if text and text not in _EMPTY_PLACEHOLDERS:
            return text
    col_val = _cell_text(cells, col_index)
    if col_val and col_val not in _EMPTY_PLACEHOLDERS:
        return col_val
    return None


def _parse_optional_int(raw_val: str) -> int | str:
    compact = raw_val.replace(",", "").strip()
    if compact.isascii() and compact.isdigit():
        try:
            return int(compact)
        except ValueError:
            return raw_val
    return raw_val


class PlayGoldenScraper(BaseRankingScraper):
    """Scraper for Play Golden (`https://play-golden.online`)."""

    server_id: str = "play_golden"

    async def _fetch_paginated_html(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        *,
        route_path: str,
        max_pages: int,
        category: str,
        row_mapper: Callable[[etree._Element, list[etree._Element]], dict[str, Any]],
    ) -> CategoryFetchResult:
        clean_base = base_url.rstrip("/")
        raw_records: list[dict[str, Any]] = []
        raw_payloads: list[PagePayload | tuple[str, bytes]] = []
        last_status: int | None = None

        for page in range(1, max_pages + 1):
            url = f"{clean_base}{route_path}?page={page}"
            response = await self._fetch_url(client, url, category=category)
            last_status = response.status_code
            raw_payloads.append(
                PagePayload(
                    url=str(response.url),
                    content=response.content,
                    status_code=response.status_code,
                    category=category,
                )
            )

            rows = _extract_table_rows(response.content, "table-ranking", response.encoding)
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
    def _map_player_row(tr: etree._Element, cells: list[etree._Element]) -> dict[str, Any]:
        race = _extract_golden_race(cells[1]) if len(cells) > 1 else None
        char_name = _extract_name(cells[2]) if len(cells) > 2 else None

        extra_metrics: dict[str, Any] = {}
        gold_raw = _extract_extra_value(
            tr,
            cells,
            tr_attr="data-gold",
            field_name="gold",
            css_class="gold",
            col_index=9,
        )
        if gold_raw is not None:
            extra_metrics["gold"] = _parse_optional_int(gold_raw)

        last_logout = _extract_extra_value(
            tr,
            cells,
            tr_attr="data-last-logout",
            field_name="last_logout",
            css_class="last-logout",
            col_index=10,
        )
        if last_logout is not None:
            extra_metrics["last_logout"] = last_logout

        return {
            "rank": _strip_rank_prefix(_cell_text(cells, 0)),
            "race": race,
            "char_name": char_name,
            "guild_name": _cell_text(cells, 3),
            "level": _cell_text(cells, 4),
            "strength": _cell_text(cells, 5),
            "intellect": _cell_text(cells, 6),
            "item_points": _cell_text(cells, 7),
            "honor_points": _cell_text(cells, 8),
            "extra_metrics": extra_metrics,
        }

    @staticmethod
    def _map_job_row(_tr: etree._Element, cells: list[etree._Element]) -> dict[str, Any]:
        char_name = _extract_name(cells[1]) if len(cells) > 1 else None
        return {
            "rank": _strip_rank_prefix(_cell_text(cells, 0)),
            "char_name": char_name,
            "job_type": _cell_text(cells, 2),
            "job_level": _cell_text(cells, 3),
            "job_exp": _cell_text(cells, 4),
            "guild_name": _cell_text(cells, 5),
        }

    @staticmethod
    def _map_guild_row(_tr: etree._Element, cells: list[etree._Element]) -> dict[str, Any]:
        guild_name = _extract_name(cells[1]) if len(cells) > 1 else None
        raw_members = _cell_text(cells, 3)
        if raw_members is not None and "/" in raw_members:
            raw_members = raw_members.split("/", maxsplit=1)[0].strip()

        return {
            "rank": _strip_rank_prefix(_cell_text(cells, 0)),
            "guild_name": guild_name,
            "master_name": _cell_text(cells, 2),
            "members_count": raw_members,
            "total_item_points": _cell_text(cells, 4),
            "fortress_held": _cell_text(cells, 5),
        }

    async def fetch_player_pages(
        self,
        client: httpx.AsyncClient,
        base_url: str,
    ) -> CategoryFetchResult:
        """Fetch and parse Play Golden player ranking pages (1..10)."""
        return await self._fetch_paginated_html(
            client,
            base_url,
            route_path="/ranking/characters",
            max_pages=_PLAYER_MAX_PAGES,
            category="player",
            row_mapper=self._map_player_row,
        )

    async def fetch_job_pages(
        self,
        client: httpx.AsyncClient,
        base_url: str,
    ) -> CategoryFetchResult:
        """Fetch and parse Play Golden job ranking pages (1..5)."""
        return await self._fetch_paginated_html(
            client,
            base_url,
            route_path="/ranking/jobs",
            max_pages=_JOB_MAX_PAGES,
            category="job",
            row_mapper=self._map_job_row,
        )

    async def fetch_guild_pages(
        self,
        client: httpx.AsyncClient,
        base_url: str,
    ) -> CategoryFetchResult:
        """Fetch and parse Play Golden guild ranking pages (1..2)."""
        return await self._fetch_paginated_html(
            client,
            base_url,
            route_path="/ranking/guilds",
            max_pages=_GUILD_MAX_PAGES,
            category="guild",
            row_mapper=self._map_guild_row,
        )
