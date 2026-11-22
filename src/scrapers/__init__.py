from __future__ import annotations

from typing import Final

from src.core.config import Settings
from src.scrapers.base import BaseRankingScraper
from src.scrapers.chronicle import ChronicleScraper
from src.scrapers.electus import ElectusScraper
from src.scrapers.golden import PlayGoldenScraper

__all__ = [
    "BaseRankingScraper",
    "ChronicleScraper",
    "ElectusScraper",
    "PlayGoldenScraper",
    "get_scraper",
]

_SCRAPER_REGISTRY: Final[dict[str, type[BaseRankingScraper]]] = {
    "chronicle": ChronicleScraper,
    "electus": ElectusScraper,
    "play_golden": PlayGoldenScraper,
}


def get_scraper(server_id: str, settings: Settings | None = None) -> BaseRankingScraper:
    """Instantiate the target scraper for a configured `server_id`."""
    scraper_cls = _SCRAPER_REGISTRY.get(server_id)
    if scraper_cls is None:
        supported = ", ".join(sorted(_SCRAPER_REGISTRY))
        raise ValueError(f"Unknown server_id {server_id!r}; expected one of: {supported}")
    return scraper_cls(settings=settings)
