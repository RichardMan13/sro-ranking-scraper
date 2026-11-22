"""Pure opaque-box interface contracts (typing.Protocol / TypedDict) for E2E tests.

Derived strictly from docs/designs/001_sro_ranking_scraper_pipeline.md and PROJECT.md.
Contains ZERO imports from `src.*`.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from datetime import datetime, time
from pathlib import Path
from typing import Any, Protocol

import httpx
from pydantic import BaseModel
from sqlalchemy import MetaData, Table
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker


class SettingsProto(Protocol):
    """Contract for `src.core.config.Settings`."""

    database_url: Any
    prefect_api_url: str
    log_level: str
    quarantine_dir: Path
    quarantine_retention_days: int
    circuit_breaker_cooldown_hours: int
    electus_cf_clearance: str | None


class SettingsFactoryProto(Protocol):
    """Callable factory / class protocol for `Settings`."""

    def __call__(self, **kwargs: Any) -> SettingsProto: ...


class GetSettingsProto(Protocol):
    """Callable protocol for `src.core.config.get_settings`."""

    def __call__(self) -> SettingsProto: ...


class StructlogLoggerProto(Protocol):
    """Contract for logger returned by `src.core.logging.get_logger`."""

    def info(self, event: str, **kw: Any) -> Any: ...

    def warning(self, event: str, **kw: Any) -> Any: ...

    def error(self, event: str, **kw: Any) -> Any: ...

    def debug(self, event: str, **kw: Any) -> Any: ...

    def exception(self, event: str, **kw: Any) -> Any: ...


class ConfigureLoggingProto(Protocol):
    """Contract for `src.core.logging.configure_logging`."""

    def __call__(self, log_level: str = "INFO") -> None: ...


class GetLoggerProto(Protocol):
    """Contract for `src.core.logging.get_logger`."""

    def __call__(self, name: str | None = None) -> StructlogLoggerProto: ...


class BindRunContextProto(Protocol):
    """Contract for `src.core.logging.bind_run_context`."""

    def __call__(self, server_id: str, run_id: str) -> None: ...


class ClearRunContextProto(Protocol):
    """Contract for `src.core.logging.clear_run_context`."""

    def __call__(self) -> None: ...


class CreateEngineFromUrlProto(Protocol):
    """Contract for `src.core.database.create_engine_from_url`."""

    def __call__(self, database_url: str) -> AsyncEngine: ...


class CreateSessionFactoryProto(Protocol):
    """Contract for `src.core.database.create_session_factory`."""

    def __call__(self, engine: AsyncEngine) -> async_sessionmaker[AsyncSession]: ...


class EnsureMonthlyPartitionsProto(Protocol):
    """Contract for `src.core.database.ensure_monthly_partitions`."""

    def __call__(
        self,
        session: AsyncSession,
        reference_time: datetime | None = None,
    ) -> Awaitable[list[str]]: ...


class RawPlayerItemProto(Protocol):
    """Contract for `RawPlayerRankingItem` instance."""

    rank: int
    char_name: str
    level: int
    guild_name: str | None
    race: str | None
    item_points: float | None
    strength: int | None
    intellect: int | None
    title: str | None
    job_type: str | None
    job_level: int | None
    job_points: int | None
    job_exp: int | None
    honor_points: int | None
    extra_metrics: dict[str, Any]


class RawJobItemProto(Protocol):
    """Contract for `RawJobRankingItem` instance."""

    rank: int
    char_name: str
    job_type: str
    job_level: int
    job_exp: int | None
    guild_name: str | None
    extra_metrics: dict[str, Any]


class RawGuildItemProto(Protocol):
    """Contract for `RawGuildRankingItem` instance."""

    rank: int
    guild_name: str
    master_name: str | None
    members_count: int | None
    total_item_points: float | None
    fortress_held: str | None
    extra_metrics: dict[str, Any]


class ErrorDetailsProto(Protocol):
    """Contract for `ErrorDetails` instance."""

    reason: str
    failed_category: str
    sample_errors: list[dict[str, Any]]
    quarantine_sha256: str | None
    quarantine_size_bytes: int | None

    def model_dump(self) -> dict[str, Any]: ...


class PydanticModelClsProto(Protocol):
    """Generic Pydantic v2 model class protocol."""

    def __call__(self, **data: Any) -> BaseModel: ...

    def model_validate(self, obj: Any) -> BaseModel: ...


class BaseModelDeclProto(Protocol):
    """Contract for SQLAlchemy `Base` declarative base."""

    metadata: MetaData


class OrmTableModelProto(Protocol):
    """Contract for SQLAlchemy ORM table model class."""

    __tablename__: str
    __table__: Table


class CategoryFetchResultProto(Protocol):
    """Contract for `CategoryFetchResult` returned by `fetch_*_pages`."""

    raw_records: list[dict[str, Any]]
    raw_payloads: list[tuple[str, bytes]]


class ScrapeRunSummaryProto(Protocol):
    """Contract for return value of `BaseRankingScraper.run(...)`."""

    status: str


class ScraperProto(Protocol):
    """Contract for `BaseRankingScraper` and its target subclasses."""

    server_id: str

    async def fetch_player_pages(
        self, client: httpx.AsyncClient, base_url: str
    ) -> CategoryFetchResultProto: ...

    async def fetch_job_pages(
        self, client: httpx.AsyncClient, base_url: str
    ) -> CategoryFetchResultProto: ...

    async def fetch_guild_pages(
        self, client: httpx.AsyncClient, base_url: str
    ) -> CategoryFetchResultProto: ...

    async def run(
        self,
        session: AsyncSession,
        trigger_type: str = "scheduled_12h",
        client: httpx.AsyncClient | None = None,
    ) -> ScrapeRunSummaryProto: ...


class ScraperClsProto(Protocol):
    """Constructor contract for target scraper classes."""

    def __call__(self, settings: SettingsProto) -> ScraperProto: ...


class GetScraperProto(Protocol):
    """Factory contract for `get_scraper(server_id, settings)`."""

    def __call__(self, server_id: str, settings: SettingsProto) -> ScraperProto: ...


class PrefectTaskProto(Protocol):
    """Minimal contract for Prefect 3 `@task` objects."""

    retries: int
    fn: Callable[..., Any]

    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...


class ComputePostFwCronProto(Protocol):
    """Contract for `compute_post_fw_cron(fw_day_of_week, fw_end_time_utc)`."""

    def __call__(self, fw_day_of_week: int, fw_end_time_utc: time) -> str: ...


class PrefectFlowProto(Protocol):
    """Minimal contract for Prefect 3 `@flow` objects."""

    fn: Callable[..., Any]

    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...

    def to_deployment(self, **kwargs: Any) -> Any: ...


def assert_sequence_of_str(value: object) -> Sequence[str]:
    """Narrow unknown constant to Sequence[str] in runtime checks."""
    if isinstance(value, (list, tuple, set, frozenset)):
        return [str(x) for x in value]
    msg = f"Expected sequence/set of strings, got {type(value)!r}"
    raise TypeError(msg)
