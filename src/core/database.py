from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

PARTITIONED_TABLES: tuple[str, ...] = ("character_snapshots", "guild_snapshots")


def _normalize_async_database_url(database_url: str) -> str:
    url = database_url.strip()
    if "://" in url:
        scheme, rest = url.split("://", maxsplit=1)
        url = f"{scheme.lower()}://{rest}"
    if url.startswith("postgres://"):
        return "postgresql+asyncpg://" + url.removeprefix("postgres://")
    if url.startswith("postgresql://"):
        return "postgresql+asyncpg://" + url.removeprefix("postgresql://")
    return url


def create_engine_from_url(database_url: str) -> AsyncEngine:
    """Create an async SQLAlchemy engine configured for the given database URL."""
    url = _normalize_async_database_url(database_url)
    if url.startswith("sqlite"):
        return create_async_engine(url)
    return create_async_engine(
        url,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=5,
    )


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Create an async session factory bound to the given AsyncEngine."""
    return async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )


async def ensure_monthly_partitions(
    session: AsyncSession,
    reference_time: datetime | None = None,
) -> list[str]:
    """Idempotently create current and next UTC monthly partitions for snapshot tables."""
    ref = reference_time if reference_time is not None else datetime.now(UTC)
    ref = ref.replace(tzinfo=UTC) if ref.tzinfo is None else ref.astimezone(UTC)

    y1, m1 = ref.year, ref.month
    y2, m2 = (y1 + 1, 1) if m1 == 12 else (y1, m1 + 1)

    created: list[str] = []
    for year, month in ((y1, m1), (y2, m2)):
        end_year, end_month = (year + 1, 1) if month == 12 else (year, month + 1)
        start_str = f"{year:04d}-{month:02d}-01 00:00:00+00"
        end_str = f"{end_year:04d}-{end_month:02d}-01 00:00:00+00"

        for table in PARTITIONED_TABLES:
            partition_name = f"{table}_{year:04d}_{month:02d}"
            ddl = (
                f"CREATE TABLE IF NOT EXISTS {partition_name} "
                f"PARTITION OF {table} "
                f"FOR VALUES FROM ('{start_str}') TO ('{end_str}')"
            )
            await session.execute(text(ddl))
            created.append(partition_name)

    return created
