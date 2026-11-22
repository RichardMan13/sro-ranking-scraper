from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, Final
from urllib.parse import urlsplit

import httpx
import orjson
from lxml import etree
from lxml import html as lxml_html

from src.scrapers.base import (
    BaseRankingScraper,
    BlockedChallengeError,
    CategoryFetchResult,
    PagePayload,
    TargetMaintenanceError,
    TransportFetchError,
)

_PLAYER_MAX_PAGES: Final[int] = 10
_PLAYER_LIMIT: Final[int] = 100
_JOB_MAX_PAGES: Final[int] = 5
_JOB_LIMIT: Final[int] = 100
_GUILD_MAX_PAGES: Final[int] = 2
_GUILD_LIMIT: Final[int] = 50

_JSON_ACCEPT_HEADERS: Final[dict[str, str]] = {
    "Accept": "application/json, text/plain, */*",
}


def _first_present(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _map_electus_race(raw_race: Any) -> Any:
    if isinstance(raw_race, bool):
        return None
    if raw_race in (1, "1") or (isinstance(raw_race, str) and raw_race.strip() == "1"):
        return "CH"
    if raw_race in (2, "2") or (isinstance(raw_race, str) and raw_race.strip() == "2"):
        return "EU"
    return raw_race


def _merge_extra_metrics(row: Mapping[str, Any], extra_keys: tuple[str, ...]) -> dict[str, Any]:
    extras: dict[str, Any] = {}
    nested = row.get("extra_metrics")
    if isinstance(nested, Mapping):
        extras.update(nested)
    for key in extra_keys:
        if key in row and row[key] is not None:
            extras[key] = row[key]
    return extras


def _extract_json_rows(payload: Any) -> list[Any] | None:
    if isinstance(payload, dict):
        if "data" in payload:
            rows = payload["data"]
        elif "items" in payload:
            rows = payload["items"]
        else:
            return None
    elif isinstance(payload, list):
        rows = payload
    else:
        return None
    return rows if isinstance(rows, list) else None


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


def _extract_fallback_rows(content: bytes, encoding: str | None) -> list[etree._Element]:
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

    tables = _xpath_elements(tree, "//table[@id='ranking-table']")
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


def _extract_race_from_cell(cell: etree._Element) -> Any:
    for img in _xpath_elements(cell, ".//img"):
        for attr in ("data-race", "alt", "title"):
            raw_val = img.get(attr)
            if raw_val and raw_val.strip():
                return _map_electus_race(raw_val.strip())
        src = img.get("src")
        if src and src.strip():
            filename = Path(urlsplit(src.strip()).path).name
            if filename:
                return filename
    return None


class ElectusScraper(BaseRankingScraper):
    """Scraper for Electus Online (`https://electus.online`) via JSON API + HTML fallback."""

    server_id: str = "electus"

    def _cf_cookies(self) -> dict[str, str] | None:
        token = (self.settings.electus_cf_clearance or "").strip()
        return {"cf_clearance": token} if token else None

    async def _fetch_category(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        *,
        category: str,
        max_pages: int,
        limit: int,
        json_mapper: Callable[[Mapping[str, Any]], dict[str, Any]],
        html_mapper: Callable[[list[etree._Element]], dict[str, Any]],
    ) -> CategoryFetchResult:
        clean_base = base_url.rstrip("/")
        cookies = self._cf_cookies()
        raw_records: list[dict[str, Any]] = []
        raw_payloads: list[PagePayload | tuple[str, bytes]] = []
        last_status: int | None = None

        for page in range(1, max_pages + 1):
            api_url = f"{clean_base}/api/ranking/{category}?page={page}&limit={limit}"
            try:
                response = await self._fetch_url(
                    client,
                    api_url,
                    headers=_JSON_ACCEPT_HEADERS,
                    cookies=cookies,
                    category=category,
                )
            except (BlockedChallengeError, TargetMaintenanceError):
                raise
            except TransportFetchError as exc:
                if page == 1 and exc.status_code == 404:
                    self._logger.warning(
                        "electus_api_fallback",
                        category=category,
                        reason="http_404",
                        status_code=404,
                    )
                    return await self._fetch_html_category(
                        client,
                        clean_base,
                        category=category,
                        max_pages=max_pages,
                        cookies=cookies,
                        html_mapper=html_mapper,
                    )
                raise

            last_status = response.status_code
            try:
                payload = orjson.loads(response.content)
            except orjson.JSONDecodeError as exc:
                if page == 1:
                    self._logger.warning(
                        "electus_api_fallback",
                        category=category,
                        reason="non_json_response",
                        status_code=response.status_code,
                    )
                    return await self._fetch_html_category(
                        client,
                        clean_base,
                        category=category,
                        max_pages=max_pages,
                        cookies=cookies,
                        html_mapper=html_mapper,
                    )
                raise TransportFetchError(
                    f"Non-JSON response on page {page} for {api_url}",
                    url=str(response.url),
                    status_code=response.status_code,
                    payload=response.content,
                    category=category,
                ) from exc

            raw_payloads.append(
                PagePayload(
                    url=str(response.url),
                    content=response.content,
                    status_code=response.status_code,
                    category=category,
                )
            )

            rows = _extract_json_rows(payload)
            if not rows:
                break

            for row in rows:
                if isinstance(row, Mapping):
                    raw_records.append(json_mapper(row))
                else:
                    raw_records.append({"_invalid_row": row})

        return CategoryFetchResult(
            raw_records=raw_records,
            raw_payloads=raw_payloads,
            http_status_code=last_status,
        )

    async def _fetch_html_category(
        self,
        client: httpx.AsyncClient,
        clean_base: str,
        *,
        category: str,
        max_pages: int,
        cookies: dict[str, str] | None,
        html_mapper: Callable[[list[etree._Element]], dict[str, Any]],
    ) -> CategoryFetchResult:
        raw_records: list[dict[str, Any]] = []
        raw_payloads: list[PagePayload | tuple[str, bytes]] = []
        last_status: int | None = None

        for page in range(1, max_pages + 1):
            html_url = f"{clean_base}/ranking/{category}?page={page}"
            response = await self._fetch_url(
                client,
                html_url,
                cookies=cookies,
                category=category,
            )
            last_status = response.status_code
            raw_payloads.append(
                PagePayload(
                    url=str(response.url),
                    content=response.content,
                    status_code=response.status_code,
                    category=category,
                )
            )

            rows = _extract_fallback_rows(response.content, response.encoding)
            page_records: list[dict[str, Any]] = []
            for tr in rows:
                cells = _xpath_elements(tr, "./td")
                if not cells or _is_empty_notice_row(cells):
                    continue
                page_records.append(html_mapper(cells))

            if not page_records:
                break
            raw_records.extend(page_records)

        return CategoryFetchResult(
            raw_records=raw_records,
            raw_payloads=raw_payloads,
            http_status_code=last_status,
        )

    @staticmethod
    def _map_player_json(row: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "rank": _first_present(row, "rank", "position"),
            "char_name": _first_present(row, "char_name", "charname", "character_name", "name"),
            "guild_name": _first_present(row, "guild_name", "guild", "guildname"),
            "race": _map_electus_race(_first_present(row, "race", "race_id")),
            "level": _first_present(row, "level", "char_level"),
            "item_points": _first_present(row, "item_points", "itempoints", "item_point"),
            "strength": _first_present(row, "strength", "str"),
            "intellect": _first_present(row, "intellect", "int"),
            "title": _first_present(row, "title", "hwan_level"),
            "job_type": _first_present(row, "job_type", "job", "job_name"),
            "job_level": _first_present(row, "job_level", "joblevel"),
            "job_points": _first_present(row, "job_points", "jobpoints"),
            "job_exp": _first_present(row, "job_exp", "jobexp"),
            "honor_points": _first_present(row, "honor_points", "honor", "honorpoints"),
            "extra_metrics": _merge_extra_metrics(
                row,
                ("elo_points", "sox_parts", "hours_played"),
            ),
        }

    @staticmethod
    def _map_job_json(row: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "rank": _first_present(row, "rank", "position"),
            "char_name": _first_present(row, "char_name", "charname", "character_name", "name"),
            "job_type": _first_present(row, "job_type", "job", "job_name", "type"),
            "job_level": _first_present(row, "job_level", "joblevel", "level"),
            "job_exp": _first_present(row, "job_exp", "jobexp", "exp"),
            "guild_name": _first_present(row, "guild_name", "guild", "guildname"),
            "extra_metrics": _merge_extra_metrics(
                row,
                ("job_points", "jobpoints", "elo_points", "caravan_kills", "kills"),
            ),
        }

    @staticmethod
    def _map_guild_json(row: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "rank": _first_present(row, "rank", "position"),
            "guild_name": _first_present(row, "guild_name", "guild", "guildname", "name"),
            "master_name": _first_present(
                row,
                "master_name",
                "master",
                "leader",
                "master_char_name",
                "guild_master",
            ),
            "members_count": _first_present(
                row,
                "members_count",
                "members",
                "member_count",
                "total_members",
            ),
            "total_item_points": _first_present(
                row,
                "total_item_points",
                "item_points",
                "itempoints",
                "points",
                "total_points",
            ),
            "fortress_held": _first_present(row, "fortress_held", "fortress", "fw_fortress"),
            "extra_metrics": _merge_extra_metrics(
                row,
                ("elo_points", "guild_level", "level"),
            ),
        }

    @staticmethod
    def _map_player_html(cells: list[etree._Element]) -> dict[str, Any]:
        char_name = _extract_char_name(cells[1]) if len(cells) > 1 else None
        race = _extract_race_from_cell(cells[1]) if len(cells) > 1 else None
        return {
            "rank": _strip_rank_prefix(_cell_text(cells, 0)),
            "char_name": char_name,
            "race": race,
            "guild_name": _cell_text(cells, 2),
            "level": _cell_text(cells, 3),
            "item_points": _cell_text(cells, 4),
            "honor_points": _cell_text(cells, 5),
            "title": _cell_text(cells, 6),
        }

    @staticmethod
    def _map_job_html(cells: list[etree._Element]) -> dict[str, Any]:
        char_name = _extract_char_name(cells[1]) if len(cells) > 1 else None
        return {
            "rank": _strip_rank_prefix(_cell_text(cells, 0)),
            "char_name": char_name,
            "guild_name": _cell_text(cells, 2),
            "job_type": _cell_text(cells, 3),
            "job_level": _cell_text(cells, 4),
            "job_exp": _cell_text(cells, 5),
        }

    @staticmethod
    def _map_guild_html(cells: list[etree._Element]) -> dict[str, Any]:
        guild_name = _extract_char_name(cells[1]) if len(cells) > 1 else None
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
        """Fetch and parse Electus player ranking pages (1..10)."""
        return await self._fetch_category(
            client,
            base_url,
            category="player",
            max_pages=_PLAYER_MAX_PAGES,
            limit=_PLAYER_LIMIT,
            json_mapper=self._map_player_json,
            html_mapper=self._map_player_html,
        )

    async def fetch_job_pages(
        self,
        client: httpx.AsyncClient,
        base_url: str,
    ) -> CategoryFetchResult:
        """Fetch and parse Electus job ranking pages (1..5)."""
        return await self._fetch_category(
            client,
            base_url,
            category="job",
            max_pages=_JOB_MAX_PAGES,
            limit=_JOB_LIMIT,
            json_mapper=self._map_job_json,
            html_mapper=self._map_job_html,
        )

    async def fetch_guild_pages(
        self,
        client: httpx.AsyncClient,
        base_url: str,
    ) -> CategoryFetchResult:
        """Fetch and parse Electus guild ranking pages (1..2)."""
        return await self._fetch_category(
            client,
            base_url,
            category="guild",
            max_pages=_GUILD_MAX_PAGES,
            limit=_GUILD_LIMIT,
            json_mapper=self._map_guild_json,
            html_mapper=self._map_guild_html,
        )
