from __future__ import annotations

import os
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path


def _ensure_accessible_prefect_home() -> None:
    """Redirect `PREFECT_HOME` to temp storage only if `~/.prefect` is inaccessible."""
    if os.environ.get("PREFECT_HOME"):
        return
    try:
        (Path.home() / ".prefect" / "profiles.toml").exists()
    except OSError:
        fallback = Path(tempfile.gettempdir()) / "sro_ranking_scraper_prefect_home"
        fallback.mkdir(parents=True, exist_ok=True)
        os.environ["PREFECT_HOME"] = str(fallback)


_ensure_accessible_prefect_home()

from prefect import task  # noqa: E402

from src.core.config import get_settings  # noqa: E402
from src.core.database import (  # noqa: E402
    create_engine_from_url,
    create_session_factory,
    ensure_monthly_partitions,
)
from src.core.logging import get_logger  # noqa: E402
from src.scrapers import get_scraper  # noqa: E402
from src.scrapers.base import ScrapeRunSummary  # noqa: E402


@task(retries=0, name="ensure-partitions")
async def ensure_partitions_task(reference_time: datetime | None = None) -> list[str]:
    """Ensure current and next UTC monthly partitions exist for snapshot tables."""
    settings = get_settings()
    logger = get_logger("tasks")
    engine = create_engine_from_url(str(settings.database_url))
    session_factory = create_session_factory(engine)

    try:
        async with session_factory() as session:
            try:
                partitions = await ensure_monthly_partitions(session, reference_time=reference_time)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
        logger.info("partitions_ensured", partitions=partitions)
        return partitions
    finally:
        await engine.dispose()


@task(retries=0, name="scrape-target")
async def scrape_target_task(
    server_id: str,
    trigger_type: str = "scheduled_12h",
) -> ScrapeRunSummary:
    """Execute a complete ranking scrape run for a single target server."""
    settings = get_settings()
    scraper = get_scraper(server_id, settings=settings)
    engine = create_engine_from_url(str(settings.database_url))
    session_factory = create_session_factory(engine)

    try:
        async with session_factory() as session:
            return await scraper.run(session, trigger_type=trigger_type)
    finally:
        await engine.dispose()


@task(retries=0, name="purge-old-quarantine")
def purge_old_quarantine_task(
    retention_days: int = 30,
    quarantine_dir: Path | None = None,
) -> int:
    """Delete compressed `.html.gz` quarantine payloads older than `retention_days`."""
    if retention_days < 0:
        raise ValueError(f"retention_days must be non-negative, got {retention_days}")

    target_dir = quarantine_dir if quarantine_dir is not None else get_settings().quarantine_dir
    logger = get_logger("tasks")

    if not target_dir.exists() or not target_dir.is_dir():
        logger.info(
            "quarantine_purge_skipped_missing_dir",
            quarantine_dir=str(target_dir),
            retention_days=retention_days,
        )
        return 0

    cutoff_ts = (datetime.now(UTC) - timedelta(days=retention_days)).timestamp()
    deleted_count = 0

    for artifact in target_dir.rglob("*.html.gz"):
        if not artifact.is_file():
            continue
        if artifact.stat().st_mtime < cutoff_ts:
            artifact.unlink(missing_ok=True)
            deleted_count += 1

    logger.info(
        "quarantine_purge_completed",
        quarantine_dir=str(target_dir),
        retention_days=retention_days,
        deleted_count=deleted_count,
    )
    return deleted_count
