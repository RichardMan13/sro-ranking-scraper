from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Final
from urllib.parse import urlsplit

from lxml import etree
from lxml import html as lxml_html

EMPTY_NOTICE_RE: Final[re.Pattern[str]] = re.compile(
    r"(?:"
    r"\bno\s+(?:records?|results?|data|entries|rows|items|matches|players?|guilds?|jobs?|characters?)\b"
    r"|\b0\s+(?:records?|results?|data|entries|rows|items|matches|players?|guilds?|jobs?|characters?)\b"
    r"|\bshowing\s+0\b"
    r"|\btotal\s*[:=]?\s*0\b"
    r"|\b(?:empty|nothing\s+found)\b"
    r")",
    re.IGNORECASE,
)


def decode_html_bytes(content: bytes, encoding: str | None) -> str:
    """Decode raw HTML response bytes safely with UTF-8 fallback."""
    chosen = (encoding or "").strip() or "utf-8"
    try:
        return content.decode(chosen, errors="replace")
    except LookupError:
        return content.decode("utf-8", errors="replace")


def parse_html_tree(content: bytes, encoding: str | None) -> etree._Element | None:
    """Parse HTML bytes into an lxml element tree, returning `None` on empty/invalid input."""
    if not content.strip():
        return None
    decoded = decode_html_bytes(content, encoding)
    if not decoded.strip():
        return None
    try:
        return lxml_html.fromstring(
            decoded.encode("utf-8"),
            parser=lxml_html.HTMLParser(encoding="utf-8"),
        )
    except etree.ParserError:
        return None


def xpath_elements(node: etree._Element, expr: str) -> list[etree._Element]:
    """Evaluate an XPath expression and return only `etree._Element` results."""
    raw = node.xpath(expr)
    if not isinstance(raw, list):
        return []
    return [item for item in raw if isinstance(item, etree._Element)]


def element_text(element: etree._Element) -> str:
    """Extract normalized whitespace-collapsed text from an lxml element."""
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


def cell_text(cells: list[etree._Element], index: int) -> str | None:
    """Return normalized text of `cells[index]` or `None` when out of bounds."""
    if index >= len(cells):
        return None
    return element_text(cells[index])


def strip_rank_prefix(raw_rank: str | None) -> str | None:
    """Strip leading `#` characters and surrounding whitespace from rank strings."""
    if raw_rank is None:
        return None
    cleaned = raw_rank.strip()
    if cleaned.startswith("#"):
        return cleaned.lstrip("#").strip()
    return cleaned


def extract_table_rows(
    tree: etree._Element | None,
    table_classes: tuple[str, ...],
) -> list[etree._Element]:
    """Extract data `<tr>` rows from all tables matching every CSS class in `table_classes`."""
    if tree is None:
        return []
    class_predicates = " and ".join(
        f"contains(concat(' ', normalize-space(@class), ' '), ' {cls} ')" for cls in table_classes
    )
    tables = xpath_elements(tree, f"//table[{class_predicates}]")
    rows: list[etree._Element] = []
    for table in tables:
        if xpath_elements(table, "./tbody"):
            rows.extend(xpath_elements(table, "./tbody/tr"))
        else:
            rows.extend(xpath_elements(table, "./tr[td]"))
    return rows


def is_empty_notice_row(cells: list[etree._Element]) -> bool:
    """Return `True` for single-cell `colspan` rows indicating an empty table."""
    if len(cells) != 1 or cells[0].get("colspan") is None:
        return False
    text = element_text(cells[0])
    if not text or not any(ch.isdigit() for ch in text):
        return True
    return bool(EMPTY_NOTICE_RE.search(text))


def extract_char_name(cell: etree._Element) -> str:
    """Extract character or guild name prioritizing `.char-link` or first `<a>`."""
    char_links = xpath_elements(
        cell,
        ".//a[contains(concat(' ', normalize-space(@class), ' '), ' char-link ')]",
    )
    if char_links:
        return element_text(char_links[0])
    links = xpath_elements(cell, ".//a")
    if links:
        return element_text(links[0])
    return element_text(cell)


def extract_race_from_cell(cell: etree._Element) -> str | None:
    """Extract race hint from `<img>` attributes (`data-race`, `alt`, `title`) or filename."""
    for img in xpath_elements(cell, ".//img"):
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


def parse_optional_numeric(raw_val: str) -> int | float | str:
    """Parse ASCII integer or finite float strings, falling back to the raw string."""
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
