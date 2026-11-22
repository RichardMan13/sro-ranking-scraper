from __future__ import annotations

import asyncio
import contextlib
import io
import selectors
import socket
from collections.abc import Coroutine
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypeVar
from unittest.mock import AsyncMock
from uuid import uuid4

import orjson
import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from pydantic import ValidationError
from sqlalchemy import Table
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.schema import CreateIndex, CreateTable
from src.core.config import Settings, get_settings
from src.core.database import (
    create_engine_from_url,
    create_session_factory,
    ensure_monthly_partitions,
)
from src.core.logging import (
    bind_run_context,
    clear_run_context,
    configure_logging,
    get_logger,
)
from src.models.db_models import (
    Base,
    Character,
    Guild,
    ScrapeRun,
    Server,
)
from src.models.pydantic_schemas import (
    CANONICAL_STATUSES,
    CANONICAL_TRIGGER_TYPES,
    MAX_ERROR_RATE,
    MIN_GUILD_RECORDS,
    ErrorDetails,
    RawGuildRankingItem,
    RawJobRankingItem,
    RawPlayerRankingItem,
)

# ---------------------------------------------------------------------------
# 1. Settings (`src/core/config.py`)
# ---------------------------------------------------------------------------


def test_settings_requires_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(ValidationError):
        Settings(_env_file=None)


def test_settings_rejects_empty_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "   ")
    with pytest.raises(ValidationError):
        Settings(_env_file=None)


def test_settings_defaults_and_env_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://user:pass@localhost:5432/sro")
    monkeypatch.delenv("PREFECT_API_URL", raising=False)
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    monkeypatch.delenv("QUARANTINE_DIR", raising=False)
    monkeypatch.delenv("QUARANTINE_RETENTION_DAYS", raising=False)
    monkeypatch.delenv("CIRCUIT_BREAKER_COOLDOWN_HOURS", raising=False)
    monkeypatch.delenv("ELECTUS_CF_CLEARANCE", raising=False)

    defaults = Settings(_env_file=None)
    assert str(defaults.database_url) == "postgresql+asyncpg://user:pass@localhost:5432/sro"
    assert defaults.prefect_api_url == "http://localhost:4200/api"
    assert defaults.log_level == "INFO"
    assert defaults.quarantine_dir == Path("./quarantine")
    assert defaults.quarantine_retention_days == 30
    assert defaults.circuit_breaker_cooldown_hours == 6
    assert defaults.electus_cf_clearance is None

    monkeypatch.setenv("PREFECT_API_URL", "http://prefect-server:4200/api")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("QUARANTINE_DIR", "/tmp/quarantine")
    monkeypatch.setenv("QUARANTINE_RETENTION_DAYS", "14")
    monkeypatch.setenv("CIRCUIT_BREAKER_COOLDOWN_HOURS", "12")
    monkeypatch.setenv("ELECTUS_CF_CLEARANCE", "cf-token-xyz")

    overridden = get_settings()
    assert overridden.prefect_api_url == "http://prefect-server:4200/api"
    assert overridden.log_level == "DEBUG"
    assert overridden.quarantine_dir == Path("/tmp/quarantine")
    assert overridden.quarantine_retention_days == 14
    assert overridden.circuit_breaker_cooldown_hours == 12
    assert overridden.electus_cf_clearance == "cf-token-xyz"


def test_settings_rejects_invalid_numeric_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://user:pass@localhost:5432/sro")
    with pytest.raises(ValidationError):
        Settings(_env_file=None, quarantine_retention_days=0)
    with pytest.raises(ValidationError):
        Settings(_env_file=None, circuit_breaker_cooldown_hours=0)


@pytest.mark.parametrize(
    "bad_url",
    [
        "not-a-url",
        "http://example.com",
        "mysql://user:pass@localhost:3306/sro",
        "postgresql_fake://user:pass@localhost:5432/sro",
        "postgresql://",
        "postgresql+asyncpg://",
        "postgresql://user:pass@",
        "sqlite://",
        "sqlite+aiosqlite://",
        "sqlite:///",
    ],
)
def test_settings_rejects_unsupported_or_empty_authority_database_urls(bad_url: str) -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, database_url=bad_url)


@pytest.mark.parametrize(
    ("raw_url", "expected_url"),
    [
        (
            "postgresql://user:pass@localhost:5432/sro",
            "postgresql://user:pass@localhost:5432/sro",
        ),
        (
            "postgresql+asyncpg://user:pass@localhost:5432/sro",
            "postgresql+asyncpg://user:pass@localhost:5432/sro",
        ),
        (
            "postgresql+psycopg://user:pass@localhost:5432/sro",
            "postgresql+psycopg://user:pass@localhost:5432/sro",
        ),
        (
            "postgres://user:pass@localhost:5432/sro",
            "postgres://user:pass@localhost:5432/sro",
        ),
        (
            "  POSTGRES://user:pass@localhost:5432/sro  ",
            "postgres://user:pass@localhost:5432/sro",
        ),
        (
            "  PostgreSQL+AsyncPG://user:pass@localhost:5432/sro  ",
            "postgresql+asyncpg://user:pass@localhost:5432/sro",
        ),
        ("sqlite:///:memory:", "sqlite:///:memory:"),
        ("sqlite+aiosqlite:///:memory:", "sqlite+aiosqlite:///:memory:"),
        ("sqlite:///./test.db", "sqlite:///./test.db"),
    ],
)
def test_settings_normalizes_and_accepts_supported_database_urls(
    raw_url: str,
    expected_url: str,
) -> None:
    settings = Settings(_env_file=None, database_url=raw_url)
    assert str(settings.database_url) == expected_url


# ---------------------------------------------------------------------------
# 2. Structured Logging (`src/core/logging.py`)
# ---------------------------------------------------------------------------


def test_logging_json_and_run_context(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO")
    clear_run_context()

    run_uuid = uuid4()
    bind_run_context(server_id="chronicle", run_id=run_uuid)
    logger = get_logger("test_m1")
    logger.info("scrape_started", target_count=100)

    captured = capsys.readouterr()
    payload = orjson.loads(captured.out.strip())
    assert payload["event"] == "scrape_started"
    assert payload["level"] == "info"
    assert payload["server_id"] == "chronicle"
    assert payload["run_id"] == str(run_uuid)
    assert payload["logger"] == "test_m1"
    assert payload["target_count"] == 100
    assert "timestamp" in payload

    clear_run_context()
    get_logger().info("after_clear")
    second_payload = orjson.loads(capsys.readouterr().out.strip())
    assert second_payload["event"] == "after_clear"
    assert "server_id" not in second_payload
    assert "run_id" not in second_payload


def test_logging_level_filtering(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("WARNING")
    clear_run_context()
    logger = get_logger("level_test")
    logger.info("ignored_info")
    logger.warning("emitted_warning", code=503)

    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert len(lines) == 1
    payload = orjson.loads(lines[0])
    assert payload["event"] == "emitted_warning"
    assert payload["level"] == "warning"
    assert payload["code"] == 503


# ---------------------------------------------------------------------------
# 3. Database Engine & Partition Provisioning (`src/core/database.py`)
# ---------------------------------------------------------------------------

_T = TypeVar("_T")


def _loopback_sockets_allowed() -> bool:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.settimeout(0.5)
        conn.settimeout(0.5)
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        addr = sock.getsockname()
        conn.connect(addr)
        accepted, _ = sock.accept()
        accepted.close()
        return True
    except OSError:
        return False
    finally:
        conn.close()
        sock.close()


class _DummySelector(selectors.BaseSelector):
    """Minimal selector that avoids OS sockets/pipes inside Windows sandboxes."""

    def register(
        self,
        fileobj: Any,
        events: int,
        data: Any = None,
    ) -> selectors.SelectorKey:
        raise NotImplementedError("Offline selector does not register file descriptors")

    def unregister(self, fileobj: Any) -> selectors.SelectorKey:
        raise NotImplementedError("Offline selector does not unregister file descriptors")

    def modify(
        self,
        fileobj: Any,
        events: int,
        data: Any = None,
    ) -> selectors.SelectorKey:
        raise NotImplementedError("Offline selector does not modify file descriptors")

    def select(
        self,
        timeout: float | None = None,
    ) -> list[tuple[selectors.SelectorKey, int]]:
        if timeout is not None and timeout > 0:
            import time

            time.sleep(min(timeout, 0.02))
        return []

    def get_map(self) -> dict[Any, selectors.SelectorKey]:
        return {}


class _OfflineEventLoop(asyncio.BaseEventLoop):
    """Pure-Python asyncio loop avoiding `socket.socketpair()` in restricted sandboxes."""

    def __init__(self) -> None:
        super().__init__()
        self._selector = _DummySelector()

    def _write_to_self(self) -> None:
        return None

    def _process_events(self, event_list: list[tuple[selectors.SelectorKey, int]]) -> None:
        return None

    def close(self) -> None:
        if not self.is_closed():
            self._selector.close()
            super().close()


def _run_async(coro: Coroutine[Any, Any, _T]) -> _T:
    if _loopback_sockets_allowed():
        return asyncio.run(coro)
    loop = _OfflineEventLoop()
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(coro)
    finally:
        asyncio.set_event_loop(None)
        loop.close()


def test_create_engine_and_session_factory() -> None:
    async def _run() -> None:
        pg_engine = create_engine_from_url("postgresql://user:pass@localhost:5432/sro_db")
        try:
            assert isinstance(pg_engine, AsyncEngine)
            assert pg_engine.url.drivername == "postgresql+asyncpg"
            factory = create_session_factory(pg_engine)
            assert isinstance(factory, async_sessionmaker)
        finally:
            await pg_engine.dispose()

        heroku_engine = create_engine_from_url("postgres://user:pass@localhost:5432/sro_db")
        try:
            assert heroku_engine.url.drivername == "postgresql+asyncpg"
        finally:
            await heroku_engine.dispose()

        sqlite_engine = create_engine_from_url("sqlite+aiosqlite:///:memory:")
        try:
            assert isinstance(sqlite_engine, AsyncEngine)
        finally:
            await sqlite_engine.dispose()

    _run_async(_run())


def test_ensure_monthly_partitions_mid_year() -> None:
    async def _run() -> None:
        executed_sql: list[str] = []
        mock_session = AsyncMock(spec=AsyncSession)

        async def record_execute(stmt: Any) -> None:
            executed_sql.append(str(stmt))

        mock_session.execute.side_effect = record_execute

        created = await ensure_monthly_partitions(
            mock_session,
            reference_time=datetime(2026, 11, 15, 10, 0, tzinfo=UTC),
        )

        assert created == [
            "character_snapshots_2026_11",
            "guild_snapshots_2026_11",
            "character_snapshots_2026_12",
            "guild_snapshots_2026_12",
        ]
        assert len(executed_sql) == 4
        assert (
            "CREATE TABLE IF NOT EXISTS character_snapshots_2026_11 "
            "PARTITION OF character_snapshots "
            "FOR VALUES FROM ('2026-11-01 00:00:00+00') TO ('2026-12-01 00:00:00+00')"
        ) in executed_sql[0]
        assert (
            "CREATE TABLE IF NOT EXISTS guild_snapshots_2026_12 "
            "PARTITION OF guild_snapshots "
            "FOR VALUES FROM ('2026-12-01 00:00:00+00') TO ('2027-01-01 00:00:00+00')"
        ) in executed_sql[3]

    _run_async(_run())


def test_ensure_monthly_partitions_december_rollover() -> None:
    async def _run() -> None:
        executed_sql: list[str] = []
        mock_session = AsyncMock(spec=AsyncSession)

        async def record_execute(stmt: Any) -> None:
            executed_sql.append(str(stmt))

        mock_session.execute.side_effect = record_execute

        created = await ensure_monthly_partitions(
            mock_session,
            reference_time=datetime(2026, 12, 31, 23, 59, tzinfo=UTC),
        )

        assert created == [
            "character_snapshots_2026_12",
            "guild_snapshots_2026_12",
            "character_snapshots_2027_01",
            "guild_snapshots_2027_01",
        ]
        assert "2027_01" in executed_sql[2]
        assert (
            "FOR VALUES FROM ('2027-01-01 00:00:00+00') TO ('2027-02-01 00:00:00+00')"
            in (executed_sql[2])
        )

    _run_async(_run())


def test_ensure_monthly_partitions_naive_and_default_time() -> None:
    async def _run() -> None:
        mock_session = AsyncMock(spec=AsyncSession)

        naive_created = await ensure_monthly_partitions(
            mock_session,
            reference_time=datetime(2026, 5, 10, 12, 0),
        )
        assert naive_created == [
            "character_snapshots_2026_05",
            "guild_snapshots_2026_05",
            "character_snapshots_2026_06",
            "guild_snapshots_2026_06",
        ]

        default_created = await ensure_monthly_partitions(mock_session)
        assert len(default_created) == 4

    _run_async(_run())


# ---------------------------------------------------------------------------
# 4. Pydantic v2 Schemas & Validators (`src/models/pydantic_schemas.py`)
# ---------------------------------------------------------------------------


def test_canonical_constants_and_error_details() -> None:
    assert MAX_ERROR_RATE == 0.05
    assert MIN_GUILD_RECORDS == 5
    assert (
        frozenset(
            {
                "SUCCESS",
                "PARTIAL_SUCCESS",
                "SCHEMA_DRIFT",
                "LOW_COUNT_QUARANTINE",
                "BLOCKED_CHALLENGE",
                "TARGET_MAINTENANCE",
                "HTTP_ERROR",
            }
        )
        == CANONICAL_STATUSES
    )
    assert frozenset({"scheduled_12h", "post_fw_extra", "manual"}) == CANONICAL_TRIGGER_TYPES

    details = ErrorDetails(
        reason="Top rank missing",
        failed_category="player",
        sample_errors=[{"rank": 1, "error": "missing"}],
        quarantine_sha256="a" * 64,
        quarantine_size_bytes=2048,
    )
    assert details.failed_category == "player"
    assert details.quarantine_size_bytes == 2048


def test_raw_player_ranking_item_normalization_and_bounds() -> None:
    item = RawPlayerRankingItem(
        rank="1",
        char_name="  HeroOne  ",
        level="110",
        guild_name="-",
        race="Chinese",
        item_points="1.250,50",
        strength="450",
        intellect="1,200",
        title="N/A",
        job_type="Mercador",
        job_level=100.0,
        job_points="",
        job_exp="98,765",
        honor_points=True,
    )
    assert item.rank == 1
    assert item.char_name == "HeroOne"
    assert item.level == 110
    assert item.guild_name is None
    assert item.race == "CH"
    assert item.item_points == 1250.5
    assert item.strength == 450
    assert item.intellect == 1200
    assert item.title is None
    assert item.job_type == "TRADER"
    assert item.job_level == 100
    assert item.job_points is None
    assert item.job_exp == 98765
    assert item.honor_points is None
    assert item.extra_metrics == {}

    item2 = RawPlayerRankingItem(
        rank=2,
        char_name="HeroTwo",
        level=120,
        guild_name="No Name",
        race="european_flag",
        item_points="1,250.50",
        job_type="Caçador",
        title="XXXX",
    )
    assert item2.guild_name is None
    assert item2.race == "EU"
    assert item2.item_points == 1250.5
    assert item2.job_type == "HUNTER"
    assert item2.title is None
    assert item2.extra_metrics is not item.extra_metrics

    item3 = RawPlayerRankingItem(
        rank=3,
        char_name="HeroThree",
        level=105,
        race="unknown",
        job_type="unknown",
        item_points="1250,7",
        strength="-",
    )
    assert item3.race is None
    assert item3.job_type is None
    assert item3.item_points == 1250.7
    assert item3.strength is None

    item4 = RawPlayerRankingItem(
        rank=4,
        char_name="HeroFour",
        level=105,
        item_points="1,250",
        title="None",
    )
    assert item4.item_points == 1250.0
    assert item4.title is None

    item5 = RawPlayerRankingItem(
        rank=5,
        char_name="HeroFive",
        level=105,
        guild_name=None,
        race=None,
        job_type=None,
        item_points="invalid_float",
    )
    assert item5.guild_name is None
    assert item5.race is None
    assert item5.job_type is None
    assert item5.item_points is None

    item6 = RawPlayerRankingItem(
        rank=6,
        char_name="HeroSix",
        level=105,
        item_points="   ",
    )
    assert item6.item_points is None

    item7 = RawPlayerRankingItem(
        rank=7,
        char_name="HeroSeven",
        level=105,
        item_points="nan",
    )
    assert item7.item_points is None

    item8 = RawPlayerRankingItem(
        rank=8,
        char_name="HeroEight",
        level=105,
        item_points=None,
    )
    assert item8.item_points is None


@pytest.mark.parametrize(
    "bad_kwargs",
    [
        {"rank": 0, "char_name": "Hero", "level": 100},
        {"rank": 1, "char_name": "   ", "level": 100},
        {"rank": 1, "char_name": "A" * 65, "level": 100},
        {"rank": 1, "char_name": "Hero", "level": 0},
        {"rank": 1, "char_name": "Hero", "level": 141},
        {"rank": 1, "char_name": "Hero", "level": 100, "item_points": -1.0},
        {"rank": 1, "char_name": "Hero", "level": 100, "item_points": float("nan")},
        {"rank": 1, "char_name": "Hero", "level": 100, "strength": "-5"},
        {"rank": 1, "char_name": "Hero", "level": 100, "strength": 12.5},
    ],
)
def test_raw_player_ranking_item_rejects_invalid_inputs(bad_kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        RawPlayerRankingItem(**bad_kwargs)


def test_raw_job_ranking_item_validation() -> None:
    job = RawJobRankingItem(
        rank=1,
        char_name="ShadowThief",
        job_type="Ladrão",
        job_level="95",
        job_exp="1,500,000",
        guild_name="N/A",
    )
    assert job.job_type == "THIEF"
    assert job.job_level == 95
    assert job.job_exp == 1500000
    assert job.guild_name is None

    with pytest.raises(ValidationError):
        RawJobRankingItem(
            rank=1,
            char_name="ShadowThief",
            job_type="UNKNOWN",
            job_level=50,
        )

    with pytest.raises(ValidationError):
        RawJobRankingItem(
            rank=1,
            char_name="ShadowThief",
            job_type="TRADER",
            job_level="",
        )


def test_raw_guild_ranking_item_validation() -> None:
    guild = RawGuildRankingItem(
        rank=1,
        guild_name="  Olympus  ",
        master_name="-",
        members_count="45/50",
        total_item_points="9.876,54",
        fortress_held="XXXX",
    )
    assert guild.guild_name == "Olympus"
    assert guild.master_name is None
    assert guild.members_count == 45
    assert guild.total_item_points == 9876.54
    assert guild.fortress_held is None

    guild2 = RawGuildRankingItem(
        rank=2,
        guild_name="Titans",
        members_count="1,234/2,000",
    )
    assert guild2.members_count == 1234

    guild3 = RawGuildRankingItem(
        rank=3,
        guild_name="Vanguard",
        members_count="N/A",
    )
    assert guild3.members_count is None

    for bad_name in ("", "-", "No Name", "N/A"):
        with pytest.raises(ValidationError):
            RawGuildRankingItem(rank=1, guild_name=bad_name)


@pytest.mark.parametrize(
    ("raw_race", "expected"),
    [
        ("char_eu.png", "EU"),
        ("european_char.png", "EU"),
        ("eu_character.gif", "EU"),
        ("archers_eu.png", "EU"),
        ("Archer", None),
        ("Zeus", None),
        ("Neutral", None),
        ("chinese.png", "CH"),
        ("ChineseCharacter", "CH"),
        ("European", "EU"),
        ("CH", "CH"),
        ("EU", "EU"),
        ("chinese_vs_european", None),
    ],
)
def test_normalize_race_token_boundaries(raw_race: str, expected: str | None) -> None:
    player = RawPlayerRankingItem(rank=1, char_name="Hero", level=100, race=raw_race)
    assert player.race == expected


@pytest.mark.parametrize("placeholder", ["", "   ", "-", "None", "No Name", "XXXX", "N/A"])
def test_char_name_rejects_empty_placeholders(placeholder: str) -> None:
    with pytest.raises(ValidationError):
        RawPlayerRankingItem(rank=1, char_name=placeholder, level=100)
    with pytest.raises(ValidationError):
        RawJobRankingItem(rank=1, char_name=placeholder, job_type="TRADER", job_level=50)


def test_clean_int_numbers_and_required_bool_guards() -> None:
    player = RawPlayerRankingItem(
        rank=1,
        char_name="Hero",
        level=100,
        strength="1,250 pts",
        intellect="1,234.00",
        job_points="1.234.567",
        job_exp="1.234",
        honor_points=True,
    )
    assert player.strength == 1250
    assert player.intellect == 1234
    assert player.job_points == 1234567
    assert player.job_exp == 1234
    assert player.honor_points is None

    guild_dash = RawGuildRankingItem(rank=1, guild_name="Olympus", members_count="-/50")
    assert guild_dash.members_count is None

    for bad_int_str in ("100.5", "1,234.56", "1-5", "1.23"):
        with pytest.raises(ValidationError):
            RawPlayerRankingItem(rank=1, char_name="Hero", level=100, strength=bad_int_str)
        with pytest.raises(ValidationError):
            RawGuildRankingItem(rank=1, guild_name="Olympus", members_count=bad_int_str)

    with pytest.raises(ValidationError):
        RawPlayerRankingItem(rank=True, char_name="Hero", level=100)
    with pytest.raises(ValidationError):
        RawPlayerRankingItem(rank=1, char_name="Hero", level=True)
    with pytest.raises(ValidationError):
        RawJobRankingItem(rank=True, char_name="Hero", job_type="TRADER", job_level=50)
    with pytest.raises(ValidationError):
        RawJobRankingItem(rank=1, char_name="Hero", job_type="TRADER", job_level=True)
    with pytest.raises(ValidationError):
        RawGuildRankingItem(rank=True, guild_name="Olympus")


def test_required_int_fields_normalize_thousands_and_reject_invalid_values() -> None:
    for rank_raw in ("1.000", "1,000"):
        player = RawPlayerRankingItem(rank=rank_raw, char_name="Hero", level="100.0")
        assert player.rank == 1000
        assert player.level == 100

        job = RawJobRankingItem(
            rank=rank_raw,
            char_name="Shadow",
            job_type="HUNTER",
            job_level="95.0",
        )
        assert job.rank == 1000
        assert job.job_level == 95

        guild = RawGuildRankingItem(rank=rank_raw, guild_name="Olympus")
        assert guild.rank == 1000

    for bad_required in ("100.5", True, False, "", "-"):
        with pytest.raises(ValidationError):
            RawPlayerRankingItem(rank=bad_required, char_name="Hero", level=100)
        with pytest.raises(ValidationError):
            RawPlayerRankingItem(rank=1, char_name="Hero", level=bad_required)
        with pytest.raises(ValidationError):
            RawJobRankingItem(
                rank=bad_required,
                char_name="Shadow",
                job_type="TRADER",
                job_level=50,
            )
        with pytest.raises(ValidationError):
            RawJobRankingItem(
                rank=1,
                char_name="Shadow",
                job_type="TRADER",
                job_level=bad_required,
            )
        with pytest.raises(ValidationError):
            RawGuildRankingItem(rank=bad_required, guild_name="Olympus")


def test_clean_float_numbers_multi_dot_thousands() -> None:
    player_multi = RawPlayerRankingItem(
        rank=1, char_name="Hero", level=100, item_points="1.234.567"
    )
    assert player_multi.item_points == 1234567.0

    player_single = RawPlayerRankingItem(rank=2, char_name="Hero2", level=100, item_points="1.234")
    assert player_single.item_points == 1.234

    guild_multi = RawGuildRankingItem(
        rank=1, guild_name="Olympus", total_item_points="1.234.567,89"
    )
    assert guild_multi.total_item_points == 1234567.89

    with pytest.raises(ValidationError):
        RawPlayerRankingItem(rank=1, char_name="Hero", level=100, item_points="-1.234.567")


# ---------------------------------------------------------------------------
# 5. SQLAlchemy 2.0 ORM Metadata (`src/models/db_models.py`)
# ---------------------------------------------------------------------------


def test_orm_tables_constraints_and_postgres_ddl() -> None:
    assert set(Base.metadata.tables.keys()) == {
        "servers",
        "scrape_runs",
        "characters",
        "guilds",
        "character_snapshots",
        "guild_snapshots",
    }

    assert Server.__tablename__ == "servers"
    assert ScrapeRun.__tablename__ == "scrape_runs"
    assert Character.__tablename__ == "characters"
    assert Guild.__tablename__ == "guilds"

    char_snap_table: Table = Base.metadata.tables["character_snapshots"]
    guild_snap_table: Table = Base.metadata.tables["guild_snapshots"]
    scrape_runs_table: Table = Base.metadata.tables["scrape_runs"]
    characters_table: Table = Base.metadata.tables["characters"]
    guilds_table: Table = Base.metadata.tables["guilds"]

    assert char_snap_table.kwargs["postgresql_partition_by"] == "RANGE (created_at)"
    assert guild_snap_table.kwargs["postgresql_partition_by"] == "RANGE (created_at)"
    assert [col.name for col in char_snap_table.primary_key.columns] == ["id", "created_at"]
    assert [col.name for col in guild_snap_table.primary_key.columns] == ["id", "created_at"]

    char_snap_constraints = {c.name for c in char_snap_table.constraints}
    assert {"pk_character_snapshots", "uq_char_snapshot_run"} <= char_snap_constraints

    guild_snap_constraints = {c.name for c in guild_snap_table.constraints}
    assert {"pk_guild_snapshots", "uq_guild_snapshot_run"} <= guild_snap_constraints

    assert {"ck_scrape_runs_status"} <= {c.name for c in scrape_runs_table.constraints}
    assert {"ck_characters_race", "uq_char_per_server"} <= {
        c.name for c in characters_table.constraints
    }
    assert {"uq_guild_per_server"} <= {c.name for c in guilds_table.constraints}

    char_index_names = {idx.name for idx in char_snap_table.indexes}
    assert char_index_names == {
        "idx_char_snapshots_lookup",
        "idx_char_snapshots_rank",
        "idx_char_snapshots_brin_time",
        "idx_char_snapshots_extra_metrics",
    }
    guild_index_names = {idx.name for idx in guild_snap_table.indexes}
    assert guild_index_names == {
        "idx_guild_snapshots_lookup",
        "idx_guild_snapshots_rank",
        "idx_guild_snapshots_brin_time",
    }

    pg_engine = create_engine_from_url("postgresql+asyncpg://localhost/sro")
    try:
        dialect = pg_engine.dialect
        compiled_tables = {
            table.name: str(CreateTable(table).compile(dialect=dialect))
            for table in Base.metadata.sorted_tables
        }
        assert "GENERATED ALWAYS AS IDENTITY" in compiled_tables["character_snapshots"]
        assert "PARTITION BY RANGE (created_at)" in compiled_tables["character_snapshots"]
        assert "PARTITION BY RANGE (created_at)" in compiled_tables["guild_snapshots"]

        compiled_indexes = [
            str(CreateIndex(idx).compile(dialect=dialect))
            for table in (char_snap_table, guild_snap_table)
            for idx in table.indexes
        ]
        joined_indexes = "\n".join(compiled_indexes)
        assert "USING brin" in joined_indexes
        assert "USING gin" in joined_indexes
    finally:
        pg_engine.sync_engine.dispose()


# ---------------------------------------------------------------------------
# 6. Alembic Migrations (`alembic.ini`, `alembic/env.py`, `001`, `002`)
# ---------------------------------------------------------------------------


def test_alembic_script_history() -> None:
    cfg = Config("alembic.ini")
    script = ScriptDirectory.from_config(cfg)
    assert script.get_heads() == ["002_analytical_views"]
    revisions = [rev.revision for rev in script.walk_revisions()]
    assert revisions == ["002_analytical_views", "001_initial_schema"]


def test_alembic_offline_upgrade_sql(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    output_no_env = io.StringIO()
    cfg_no_env = Config("alembic.ini")
    with contextlib.redirect_stdout(output_no_env):
        command.upgrade(cfg_no_env, "head", sql=True)
    sql_no_env = output_no_env.getvalue()

    assert 'CREATE EXTENSION IF NOT EXISTS "pgcrypto"' in sql_no_env
    for table_name in (
        "servers",
        "scrape_runs",
        "characters",
        "guilds",
        "character_snapshots",
        "guild_snapshots",
    ):
        assert f"CREATE TABLE {table_name}" in sql_no_env

    for seed_id in ("'chronicle'", "'electus'", "'play_golden'"):
        assert seed_id in sql_no_env
    assert "ON CONFLICT (id) DO NOTHING" in sql_no_env

    for partition_name in (
        "character_snapshots_2026_11",
        "character_snapshots_2026_12",
        "character_snapshots_default",
        "guild_snapshots_2026_11",
        "guild_snapshots_2026_12",
        "guild_snapshots_default",
    ):
        assert f"CREATE TABLE {partition_name} PARTITION OF" in sql_no_env

    for index_name in (
        "idx_char_snapshots_lookup",
        "idx_char_snapshots_rank",
        "idx_char_snapshots_brin_time",
        "idx_char_snapshots_extra_metrics",
        "idx_guild_snapshots_lookup",
        "idx_guild_snapshots_rank",
        "idx_guild_snapshots_brin_time",
    ):
        assert f"CREATE INDEX {index_name}" in sql_no_env

    for view_name in (
        "v_character_activity_deltas",
        "v_bot_vs_donor_classification",
        "v_guild_turnover_dominance",
        "v_race_balance_ch_vs_eu",
        "v_server_retention_weekly",
    ):
        assert f"CREATE OR REPLACE VIEW {view_name}" in sql_no_env

    # Verify percent-encoded password in DATABASE_URL works without ConfigParser errors
    monkeypatch.setenv("DATABASE_URL", "postgresql://sro:p%40ss%25word@localhost:5432/sro_db")
    output_with_env = io.StringIO()
    cfg_with_env = Config("alembic.ini")
    with contextlib.redirect_stdout(output_with_env):
        command.upgrade(cfg_with_env, "head", sql=True)
    assert "CREATE OR REPLACE VIEW v_server_retention_weekly" in output_with_env.getvalue()


def test_alembic_offline_downgrade_sql(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    output = io.StringIO()
    cfg = Config("alembic.ini")
    with contextlib.redirect_stdout(output):
        command.downgrade(cfg, "002_analytical_views:base", sql=True)
    sql = output.getvalue()

    idx_retention = sql.index("DROP VIEW IF EXISTS v_server_retention_weekly")
    idx_bot = sql.index("DROP VIEW IF EXISTS v_bot_vs_donor_classification")
    idx_deltas = sql.index("DROP VIEW IF EXISTS v_character_activity_deltas")
    assert idx_retention < idx_deltas
    assert idx_bot < idx_deltas
    assert "DROP VIEW IF EXISTS v_race_balance_ch_vs_eu" in sql
    assert "DROP VIEW IF EXISTS v_guild_turnover_dominance" in sql

    for index_name in (
        "idx_guild_snapshots_brin_time",
        "idx_guild_snapshots_rank",
        "idx_guild_snapshots_lookup",
        "idx_char_snapshots_extra_metrics",
        "idx_char_snapshots_brin_time",
        "idx_char_snapshots_rank",
        "idx_char_snapshots_lookup",
    ):
        assert f"DROP INDEX IF EXISTS {index_name}" in sql

    for table_name in (
        "guild_snapshots_default",
        "guild_snapshots_2026_12",
        "guild_snapshots_2026_11",
        "guild_snapshots",
        "character_snapshots_default",
        "character_snapshots_2026_12",
        "character_snapshots_2026_11",
        "character_snapshots",
        "guilds",
        "characters",
        "scrape_runs",
        "servers",
    ):
        assert f"DROP TABLE IF EXISTS {table_name}" in sql


def test_uppercase_scheme_url_normalization_for_engine_and_alembic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pg_engine = create_engine_from_url("  POSTGRES://user:pass@localhost:5432/sro_db  ")
    try:
        assert pg_engine.url.drivername == "postgresql+asyncpg"
    finally:
        pg_engine.sync_engine.dispose()

    monkeypatch.setenv("DATABASE_URL", "POSTGRES://user:pass@localhost:5432/sro_db")
    output = io.StringIO()
    cfg = Config("alembic.ini")
    with contextlib.redirect_stdout(output):
        command.upgrade(cfg, "head", sql=True)
    assert "CREATE OR REPLACE VIEW v_server_retention_weekly" in output.getvalue()


@pytest.mark.parametrize(
    "invalid_url",
    [
        "http://bad",
        "not-a-url",
        "",
        "postgresql://",
    ],
)
def test_alembic_offline_sql_fails_on_invalid_database_url(
    monkeypatch: pytest.MonkeyPatch,
    invalid_url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", invalid_url)
    cfg = Config("alembic.ini")
    with pytest.raises(ValidationError):
        command.upgrade(cfg, "head", sql=True)
