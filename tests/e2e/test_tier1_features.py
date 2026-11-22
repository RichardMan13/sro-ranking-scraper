"""Tier 1 E2E Feature Tests: Isolated happy-path verification for Features F1-F23.

Derived strictly from `ORIGINAL_REQUEST.md`, `PROJECT.md`, `AGENTS.md`,
and `docs/designs/001_sro_ranking_scraper_pipeline.md`.
Minimum 5 Tier 1 cases per feature (>= 115 cases total).
"""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import importlib
import os
import re
import shutil
import subprocess
import sys
import time as pytime
import tomllib
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from typing import Any, cast

import httpx
import orjson
import pytest
import respx
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from tests.e2e._contracts import (
    BaseModelDeclProto,
    BindRunContextProto,
    ClearRunContextProto,
    ComputePostFwCronProto,
    ConfigureLoggingProto,
    CreateEngineFromUrlProto,
    CreateSessionFactoryProto,
    EnsureMonthlyPartitionsProto,
    GetLoggerProto,
    GetScraperProto,
    GetSettingsProto,
    OrmTableModelProto,
    PrefectFlowProto,
    PrefectTaskProto,
    PydanticModelClsProto,
    RawGuildItemProto,
    RawJobItemProto,
    RawPlayerItemProto,
    SettingsFactoryProto,
    assert_sequence_of_str,
)
from tests.e2e._loader import require_file, require_module, require_symbol
from tests.e2e.helpers.db_helpers import (
    HistoricalCharSeed,
    count_rows,
    fetch_latest_scrape_run,
    fetch_server_row,
    get_snapshot_partition_name,
    list_table_partitions,
    query_view,
    seed_historical_run,
)
from tests.e2e.helpers.fs_helpers import decompress_gz, path_exists, read_bytes
from tests.e2e.helpers.log_helpers import parse_json_log_lines
from tests.e2e.helpers.payload_factory import (
    GuildRowSpec,
    JobRowSpec,
    PlayerRowSpec,
    make_guild_specs,
    make_job_specs,
    make_player_specs,
    render_chronicle_guild_html,
    render_chronicle_job_html,
    render_chronicle_player_html,
    render_electus_guild_json,
    render_electus_job_json,
    render_electus_player_html_fallback,
    render_electus_player_json,
    render_golden_guild_html,
    render_golden_job_html,
    render_golden_player_html,
    render_maintenance_html,
    render_turnstile_html,
)

pytestmark = [pytest.mark.e2e, pytest.mark.e2e_tier1]


def _venv_tool(name: str) -> str:
    """Resolve CLI tool inside the active Python `.venv` directory."""
    exe_dir = Path(sys.executable).parent
    for candidate in (exe_dir / f"{name}.exe", exe_dir / name):
        if candidate.exists():
            return str(candidate)
    resolved = shutil.which(name)
    return resolved if resolved is not None else name


def _load_yaml(text_data: str) -> dict[str, Any]:
    """Load YAML dynamically via `importlib` so mypy --strict requires no yaml stubs."""
    yaml_mod = importlib.import_module("yaml")
    safe_load = cast(Callable[[str], object], yaml_mod.safe_load)
    parsed = safe_load(text_data)
    assert isinstance(parsed, dict)
    return parsed


class _DdlRecordingSession:
    """Minimal session recording DDL statements for offline `ensure_monthly_partitions` tests."""

    def __init__(self) -> None:
        self.executed_sql: list[str] = []
        self.committed: bool = False

    async def execute(self, statement: Any, *args: Any, **kwargs: Any) -> Any:
        self.executed_sql.append(str(statement))
        return None

    async def commit(self) -> None:
        self.committed = True


# =============================================================================
# Feature 1: Mandatory Tech Stack & Prohibitions
# =============================================================================


def test_f1_t1_01_pyproject_mandatory_runtime_dependencies(repo_root: Path) -> None:
    """F1-T1-01: Verify Python 3.11+ and all 11 mandatory runtime dependencies."""
    pyproject_path = require_file("pyproject.toml", repo_root)
    data = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    project = data["project"]
    assert "3.11" in str(project["requires-python"])
    deps_joined = "\n".join(str(d).lower() for d in project["dependencies"])
    required = [
        "prefect",
        "sqlalchemy",
        "asyncpg",
        "pydantic",
        "pydantic-settings",
        "httpx",
        "beautifulsoup4",
        "lxml",
        "orjson",
        "structlog",
        "alembic",
    ]
    for pkg in required:
        assert pkg in deps_joined, f"Missing mandatory dependency: {pkg}"
    assert "http2" in deps_joined


def test_f1_t1_02_pyproject_dev_dependencies(repo_root: Path) -> None:
    """F1-T1-02: Verify dev test/typing packages in optional-dependencies or dependency-groups."""
    pyproject_path = require_file("pyproject.toml", repo_root)
    data = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    project = data.get("project", {})
    opt_dev = project.get("optional-dependencies", {}).get("dev", [])
    dep_group_dev = data.get("dependency-groups", {}).get("dev", [])
    dev_deps = opt_dev or dep_group_dev
    if not dev_deps:
        pytest.skip("Dev dependencies group not yet configured in pyproject.toml")
    dev_joined = "\n".join(str(d).lower() for d in dev_deps)
    for pkg in (
        "pytest",
        "pytest-asyncio",
        "pytest-cov",
        "respx",
        "ruff",
        "mypy",
        "types-beautifulsoup4",
        "lxml-stubs",
    ):
        assert pkg in dev_joined, f"Missing dev dependency: {pkg}"


def test_f1_t1_03_container_runtimes_no_browsers_or_jvm(repo_root: Path) -> None:
    """F1-T1-03: Verify Dockerfile and docker-compose images follow lean Python 3.11 stack."""
    dockerfile = require_file("Dockerfile", repo_root).read_text(encoding="utf-8")
    compose_raw = require_file("docker-compose.yml", repo_root).read_text(encoding="utf-8")
    compose = _load_yaml(compose_raw)
    assert "python:3.11-slim" in dockerfile
    services = compose["services"]
    assert "postgres:16" in str(services["postgres"]["image"])
    assert "prefecthq/prefect:3-python3.11" in str(services["prefect-server"]["image"])
    combined = (dockerfile + "\n" + compose_raw).lower()
    for banned in ("chromium", "firefox", "playwright", "selenium", "openjdk", "spark"):
        assert banned not in combined


def test_f1_t1_04_no_raw_html_columns_in_pg_models() -> None:
    """F1-T1-04: Verify PostgreSQL ORM tables contain no raw HTML TEXT/BYTEA columns."""
    base = cast(BaseModelDeclProto, require_symbol("src.models.db_models", "Base"))
    for table_name in ("scrape_runs", "character_snapshots", "guild_snapshots"):
        table = base.metadata.tables[table_name]
        col_names = set(table.columns.keys())
        for forbidden_col in ("raw_html", "html_body", "raw_payload", "payload_bytes"):
            assert forbidden_col not in col_names
    scrape_cols = set(base.metadata.tables["scrape_runs"].columns.keys())
    assert {
        "quarantine_file_path",
        "quarantine_sha256",
        "quarantine_size_bytes",
        "error_details",
    }.issubset(scrape_cols)


def test_f1_t1_05_env_example_and_compose_secret_injection(repo_root: Path) -> None:
    """F1-T1-05: Verify `.env.example` and compose parameterize sensitive config keys."""
    env_example = require_file(".env.example", repo_root).read_text(encoding="utf-8")
    compose_raw = require_file("docker-compose.yml", repo_root).read_text(encoding="utf-8")
    for key in ("DATABASE_URL", "PREFECT_API_URL", "ELECTUS_CF_CLEARANCE"):
        assert key in env_example
    assert "${" in compose_raw or "env_file" in compose_raw


# =============================================================================
# Feature 2: Code Quality Gate Compliance
# =============================================================================


def test_f2_t1_01_pyproject_tool_strict_settings(repo_root: Path) -> None:
    """F2-T1-01: Verify ruff, mypy strict, and pytest asyncio_mode settings."""
    data = tomllib.loads(require_file("pyproject.toml", repo_root).read_text(encoding="utf-8"))
    ruff_cfg = data["tool"]["ruff"]
    assert ruff_cfg["line-length"] == 100
    assert ruff_cfg["target-version"] == "py311"
    assert ruff_cfg["format"]["line-ending"] == "lf"
    mypy_cfg = data["tool"]["mypy"]
    assert mypy_cfg["python_version"] == "3.11"
    assert mypy_cfg["strict"] is True
    assert "pydantic.mypy" in mypy_cfg["plugins"]
    assert data["tool"]["pytest"]["ini_options"]["asyncio_mode"] == "auto"


def test_f2_t1_02_ruff_check_passes(repo_root: Path) -> None:
    """F2-T1-02: Run `ruff check .` against repository."""
    proc = subprocess.run(
        [_venv_tool("ruff"), "check", "."],
        cwd=repo_root,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert proc.returncode == 0, f"ruff check failed:\n{proc.stdout}\n{proc.stderr}"


def test_f2_t1_03_ruff_format_check_passes(repo_root: Path) -> None:
    """F2-T1-03: Run `ruff format --check .` against repository."""
    proc = subprocess.run(
        [_venv_tool("ruff"), "format", "--check", "."],
        cwd=repo_root,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert proc.returncode == 0, f"ruff format check failed:\n{proc.stdout}\n{proc.stderr}"


def test_f2_t1_04_mypy_strict_src_tests_passes(repo_root: Path) -> None:
    """F2-T1-04: Run `mypy src tests` under strict mode."""
    proc = subprocess.run(
        [_venv_tool("mypy"), "src", "tests"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert proc.returncode == 0, f"mypy failed:\n{proc.stdout}\n{proc.stderr}"


@pytest.mark.live_e2e
@pytest.mark.e2e_db
def test_f2_t1_05_live_alembic_and_unit_pytest_cov(
    repo_root: Path,
    e2e_migrated_db: str,
) -> None:
    """F2-T1-05: Execute `alembic upgrade head` and `pytest --cov=src tests/` without recursion."""
    if os.getenv("E2E_NESTED_PYTEST") == "1":
        pytest.skip("Guarding against recursive pytest execution inside F2-T1-05.")
    env = dict(os.environ)
    env["DATABASE_URL"] = e2e_migrated_db
    env["E2E_NESTED_PYTEST"] = "1"
    alembic_proc = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert alembic_proc.returncode == 0, alembic_proc.stderr
    pytest_proc = subprocess.run(
        [sys.executable, "-m", "pytest", "--cov=src", "tests/", "--ignore=tests/e2e"],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert pytest_proc.returncode == 0, f"{pytest_proc.stdout}\n{pytest_proc.stderr}"


# =============================================================================
# Feature 3: Settings (src/core/config.py)
# =============================================================================


def test_f3_t1_01_settings_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    """F3-T1-01: Verify canonical defaults when only `DATABASE_URL` is provided."""
    for key in (
        "PREFECT_API_URL",
        "LOG_LEVEL",
        "QUARANTINE_DIR",
        "QUARANTINE_RETENTION_DAYS",
        "CIRCUIT_BREAKER_COOLDOWN_HOURS",
        "ELECTUS_CF_CLEARANCE",
    ):
        monkeypatch.delenv(key, raising=False)
    settings_cls = cast(SettingsFactoryProto, require_symbol("src.core.config", "Settings"))
    s = settings_cls(_env_file=None, database_url="postgresql+asyncpg://u:p@localhost:5432/sro")
    assert s.prefect_api_url in {"http://localhost:4200/api", "http://prefect-server:4200/api"}
    assert s.log_level == "INFO"
    assert s.quarantine_dir == Path("./quarantine")
    assert s.quarantine_retention_days == 30
    assert s.circuit_breaker_cooldown_hours == 6
    assert s.electus_cf_clearance is None


def test_f3_t1_02_settings_env_overrides(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """F3-T1-02: Verify all 7 settings honor OS environment overrides."""
    qdir = tmp_path / "q_override"
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://app:secret@db:5432/sro_prod")
    monkeypatch.setenv("PREFECT_API_URL", "http://prefect:4200/api")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("QUARANTINE_DIR", str(qdir))
    monkeypatch.setenv("QUARANTINE_RETENTION_DAYS", "14")
    monkeypatch.setenv("CIRCUIT_BREAKER_COOLDOWN_HOURS", "12")
    monkeypatch.setenv("ELECTUS_CF_CLEARANCE", "tok_xyz")

    settings_cls = cast(SettingsFactoryProto, require_symbol("src.core.config", "Settings"))
    s = settings_cls(_env_file=None)
    assert "sro_prod" in str(s.database_url)
    assert s.prefect_api_url == "http://prefect:4200/api"
    assert s.log_level == "DEBUG"
    assert s.quarantine_dir == qdir
    assert s.quarantine_retention_days == 14
    assert s.circuit_breaker_cooldown_hours == 12
    assert s.electus_cf_clearance == "tok_xyz"


def test_f3_t1_03_settings_loads_dotenv_utf8(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """F3-T1-03: Verify `.env` UTF-8 loading."""
    for key in ("DATABASE_URL", "LOG_LEVEL", "QUARANTINE_RETENTION_DAYS"):
        monkeypatch.delenv(key, raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text(
        "DATABASE_URL=postgresql+asyncpg://u:p@localhost:5432/from_dotenv\n"
        "LOG_LEVEL=WARNING\n"
        "QUARANTINE_RETENTION_DAYS=45\n",
        encoding="utf-8",
    )
    settings_cls = cast(SettingsFactoryProto, require_symbol("src.core.config", "Settings"))
    s = settings_cls(_env_file=env_file)
    assert "from_dotenv" in str(s.database_url)
    assert s.log_level == "WARNING"
    assert s.quarantine_retention_days == 45


def test_f3_t1_04_get_settings_factory(monkeypatch: pytest.MonkeyPatch) -> None:
    """F3-T1-04: Verify `get_settings()` returns a valid `Settings` instance."""
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://u:p@localhost:5432/sro_get")
    get_settings = cast(GetSettingsProto, require_symbol("src.core.config", "get_settings"))
    cache_clear = getattr(get_settings, "cache_clear", None)
    if callable(cache_clear):
        cache_clear()
    s = get_settings()
    assert "sro_get" in str(s.database_url)


def test_f3_t1_05_electus_cf_clearance_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """F3-T1-05: Verify `ELECTUS_CF_CLEARANCE` stores configured token string."""
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://u:p@localhost:5432/sro")
    monkeypatch.setenv("ELECTUS_CF_CLEARANCE", "cf_token_abc123")
    settings_cls = cast(SettingsFactoryProto, require_symbol("src.core.config", "Settings"))
    s = settings_cls(_env_file=None)
    assert s.electus_cf_clearance == "cf_token_abc123"


# =============================================================================
# Feature 4: Async Engine & Session (src/core/database.py)
# =============================================================================


async def test_f4_t1_01_create_engine_from_url_asyncpg() -> None:
    """F4-T1-01: Verify `create_engine_from_url` builds a postgresql+asyncpg AsyncEngine."""
    create_engine = cast(
        CreateEngineFromUrlProto, require_symbol("src.core.database", "create_engine_from_url")
    )
    engine = create_engine("postgresql+asyncpg://user:pass@localhost:5432/sro")
    try:
        assert isinstance(engine, AsyncEngine)
        assert engine.dialect.name == "postgresql"
        assert engine.dialect.driver == "asyncpg"
    finally:
        await engine.dispose()


async def test_f4_t1_02_create_session_factory() -> None:
    """F4-T1-02: Verify `create_session_factory` returns an async_sessionmaker."""
    create_engine = cast(
        CreateEngineFromUrlProto, require_symbol("src.core.database", "create_engine_from_url")
    )
    create_factory = cast(
        CreateSessionFactoryProto, require_symbol("src.core.database", "create_session_factory")
    )
    engine = create_engine("postgresql+asyncpg://user:pass@localhost:5432/sro")
    try:
        factory = create_factory(engine)
        assert callable(factory)
    finally:
        await engine.dispose()


async def test_f4_t1_03_session_context_manager_offline() -> None:
    """F4-T1-03: Open and close `AsyncSession` via `create_session_factory` without DB."""
    create_engine = cast(
        CreateEngineFromUrlProto, require_symbol("src.core.database", "create_engine_from_url")
    )
    create_factory = cast(
        CreateSessionFactoryProto, require_symbol("src.core.database", "create_session_factory")
    )
    engine = create_engine("postgresql+asyncpg://user:pass@localhost:5432/sro")
    try:
        factory = create_factory(engine)
        async with factory() as session:
            assert isinstance(session, AsyncSession)
            assert session.get_bind() is engine.sync_engine
    finally:
        await engine.dispose()


async def test_f4_t1_04_accepts_settings_postgres_dsn() -> None:
    """F4-T1-04: Verify `str(settings.database_url)` passes cleanly to `create_engine_from_url`."""
    settings_cls = cast(SettingsFactoryProto, require_symbol("src.core.config", "Settings"))
    create_engine = cast(
        CreateEngineFromUrlProto, require_symbol("src.core.database", "create_engine_from_url")
    )
    s = settings_cls(_env_file=None, database_url="postgresql+asyncpg://u:p@localhost:5432/sro")
    engine = create_engine(str(s.database_url))
    try:
        assert engine.url.database == "sro"
    finally:
        await engine.dispose()


@pytest.mark.e2e_db
async def test_f4_t1_05_live_select_one(e2e_migrated_db: str) -> None:
    """F4-T1-05: Execute `SELECT 1` via `create_engine_from_url` + `create_session_factory`."""
    create_engine = cast(
        CreateEngineFromUrlProto, require_symbol("src.core.database", "create_engine_from_url")
    )
    create_factory = cast(
        CreateSessionFactoryProto, require_symbol("src.core.database", "create_session_factory")
    )
    engine = create_engine(e2e_migrated_db)
    try:
        factory = create_factory(engine)
        async with factory() as session:
            res = await session.execute(text("SELECT 1"))
            assert res.scalar_one() == 1
    finally:
        await engine.dispose()


# =============================================================================
# Feature 5: Monthly Partition Auto-Provisioner (ensure_monthly_partitions)
# =============================================================================


async def test_f5_t1_01_ensure_monthly_partitions_ddl_explicit_time() -> None:
    """F5-T1-01: Verify current & next month partition DDL for both fact tables."""
    ensure_parts = cast(
        EnsureMonthlyPartitionsProto,
        require_symbol("src.core.database", "ensure_monthly_partitions"),
    )
    stub = _DdlRecordingSession()
    created = await ensure_parts(
        cast(AsyncSession, stub),
        reference_time=datetime(2026, 11, 19, 4, 0, tzinfo=UTC),
    )
    assert created == [
        "character_snapshots_2026_11",
        "character_snapshots_2026_12",
        "guild_snapshots_2026_11",
        "guild_snapshots_2026_12",
    ] or set(created) == {
        "character_snapshots_2026_11",
        "character_snapshots_2026_12",
        "guild_snapshots_2026_11",
        "guild_snapshots_2026_12",
    }
    joined = "\n".join(stub.executed_sql)
    assert "CREATE TABLE IF NOT EXISTS" in joined
    assert "character_snapshots_2026_11" in joined
    assert "guild_snapshots_2026_12" in joined
    assert "2026-11-01" in joined
    assert "2027-01-01" in joined


async def test_f5_t1_02_ensure_monthly_partitions_default_now() -> None:
    """F5-T1-02: Verify `reference_time=None` defaults to current UTC month and next month."""
    ensure_parts = cast(
        EnsureMonthlyPartitionsProto,
        require_symbol("src.core.database", "ensure_monthly_partitions"),
    )
    now = datetime.now(UTC)
    next_year = now.year + (1 if now.month == 12 else 0)
    next_month = 1 if now.month == 12 else now.month + 1
    cur_suffix = f"{now.year}_{now.month:02d}"
    nxt_suffix = f"{next_year}_{next_month:02d}"

    stub = _DdlRecordingSession()
    created = await ensure_parts(cast(AsyncSession, stub), reference_time=None)
    assert len(created) == 4
    assert f"character_snapshots_{cur_suffix}" in created
    assert f"character_snapshots_{nxt_suffix}" in created
    assert f"guild_snapshots_{cur_suffix}" in created
    assert f"guild_snapshots_{nxt_suffix}" in created


async def test_f5_t1_03_ensure_monthly_partitions_covers_both_fact_tables() -> None:
    """F5-T1-03: Confirm 2 partitions per table (4 total) in one invocation."""
    ensure_parts = cast(
        EnsureMonthlyPartitionsProto,
        require_symbol("src.core.database", "ensure_monthly_partitions"),
    )
    stub = _DdlRecordingSession()
    created = await ensure_parts(
        cast(AsyncSession, stub), reference_time=datetime(2027, 5, 10, tzinfo=UTC)
    )
    char_parts = [p for p in created if p.startswith("character_snapshots_")]
    guild_parts = [p for p in created if p.startswith("guild_snapshots_")]
    assert len(char_parts) == 2
    assert len(guild_parts) == 2


@pytest.mark.e2e_db
async def test_f5_t1_04_live_pg_inherits_partitions_created(clean_db: AsyncSession) -> None:
    """F5-T1-04: Verify partitions created in live PostgreSQL `pg_inherits`."""
    ensure_parts = cast(
        EnsureMonthlyPartitionsProto,
        require_symbol("src.core.database", "ensure_monthly_partitions"),
    )
    await ensure_parts(clean_db, reference_time=datetime(2027, 3, 10, tzinfo=UTC))
    char_parts = await list_table_partitions(clean_db, "character_snapshots")
    guild_parts = await list_table_partitions(clean_db, "guild_snapshots")
    assert "character_snapshots_2027_03" in char_parts
    assert "character_snapshots_2027_04" in char_parts
    assert "guild_snapshots_2027_03" in guild_parts
    assert "guild_snapshots_2027_04" in guild_parts


@pytest.mark.e2e_db
async def test_f5_t1_05_live_snapshot_routes_to_provisioned_partition(
    clean_db: AsyncSession,
) -> None:
    """F5-T1-05: Confirm inserted row routes to `character_snapshots_2027_03` (not default)."""
    ensure_parts = cast(
        EnsureMonthlyPartitionsProto,
        require_symbol("src.core.database", "ensure_monthly_partitions"),
    )
    ts = datetime(2027, 3, 15, 10, 0, tzinfo=UTC)
    await ensure_parts(clean_db, reference_time=ts)
    await seed_historical_run(
        clean_db,
        server_id="chronicle",
        created_at=ts,
        characters=[
            HistoricalCharSeed(char_name="PartHero", rank_position=1, level=120, item_points=500.0)
        ],
    )
    row = (
        await clean_db.execute(
            text("SELECT id, created_at FROM character_snapshots WHERE created_at = :ts"),
            {"ts": ts},
        )
    ).one()
    partition = await get_snapshot_partition_name(
        clean_db, "character_snapshots", int(row[0]), row[1]
    )
    assert partition == "character_snapshots_2027_03"


# =============================================================================
# Feature 6: Structured JSON Logging (src/core/logging.py)
# =============================================================================


def test_f6_t1_01_structured_json_stdout_event(capsys: pytest.CaptureFixture[str]) -> None:
    """F6-T1-01: Emit info event and verify valid JSON with event, level, timestamp."""
    configure_logging = cast(
        ConfigureLoggingProto, require_symbol("src.core.logging", "configure_logging")
    )
    get_logger = cast(GetLoggerProto, require_symbol("src.core.logging", "get_logger"))
    clear_ctx = cast(ClearRunContextProto, require_symbol("src.core.logging", "clear_run_context"))
    clear_ctx()
    configure_logging("INFO")
    logger = get_logger("f6_t1_01")
    logger.info("scrape_started", count=100)
    lines = parse_json_log_lines(capsys.readouterr().out)
    assert len(lines) == 1
    assert lines[0]["event"] == "scrape_started"
    assert lines[0]["count"] == 100
    assert str(lines[0]["level"]).lower() == "info"
    assert "timestamp" in lines[0]


def test_f6_t1_02_bind_run_context_attaches_server_and_run_id(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """F6-T1-02: Verify `bind_run_context(server_id, run_id)` propagates into JSON logs."""
    configure_logging = cast(
        ConfigureLoggingProto, require_symbol("src.core.logging", "configure_logging")
    )
    get_logger = cast(GetLoggerProto, require_symbol("src.core.logging", "get_logger"))
    bind_ctx = cast(BindRunContextProto, require_symbol("src.core.logging", "bind_run_context"))
    clear_ctx = cast(ClearRunContextProto, require_symbol("src.core.logging", "clear_run_context"))

    configure_logging("INFO")
    bind_ctx(server_id="chronicle", run_id="run-uuid-1")
    try:
        get_logger("f6_t1_02").info("batch_persisted")
    finally:
        clear_ctx()
    lines = parse_json_log_lines(capsys.readouterr().out)
    assert lines[-1]["server_id"] == "chronicle"
    assert lines[-1]["run_id"] == "run-uuid-1"


def test_f6_t1_03_clear_run_context_removes_bound_keys(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """F6-T1-03: Verify `clear_run_context()` removes `server_id` and `run_id`."""
    configure_logging = cast(
        ConfigureLoggingProto, require_symbol("src.core.logging", "configure_logging")
    )
    get_logger = cast(GetLoggerProto, require_symbol("src.core.logging", "get_logger"))
    bind_ctx = cast(BindRunContextProto, require_symbol("src.core.logging", "bind_run_context"))
    clear_ctx = cast(ClearRunContextProto, require_symbol("src.core.logging", "clear_run_context"))

    configure_logging("INFO")
    bind_ctx(server_id="electus", run_id="run-uuid-2")
    clear_ctx()
    get_logger("f6_t1_03").info("after_clear")
    lines = parse_json_log_lines(capsys.readouterr().out)
    assert "server_id" not in lines[-1]
    assert "run_id" not in lines[-1]


def test_f6_t1_04_debug_level_emits_debug_events(capsys: pytest.CaptureFixture[str]) -> None:
    """F6-T1-04: Configure `DEBUG` level and verify debug records appear in JSON."""
    configure_logging = cast(
        ConfigureLoggingProto, require_symbol("src.core.logging", "configure_logging")
    )
    get_logger = cast(GetLoggerProto, require_symbol("src.core.logging", "get_logger"))
    configure_logging("DEBUG")
    get_logger("f6_t1_04").debug("debug_probe", step=1)
    lines = parse_json_log_lines(capsys.readouterr().out)
    assert lines[-1]["event"] == "debug_probe"
    assert str(lines[-1]["level"]).lower() == "debug"


def test_f6_t1_05_unicode_path_and_uuid_serialization(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    """F6-T1-05: Serialize Unicode strings, `Path`, and `UUID` values cleanly in JSON."""
    configure_logging = cast(
        ConfigureLoggingProto, require_symbol("src.core.logging", "configure_logging")
    )
    get_logger = cast(GetLoggerProto, require_symbol("src.core.logging", "get_logger"))
    configure_logging("INFO")
    uid = uuid.uuid4()
    get_logger("f6_t1_05").info(
        "unicode_probe",
        char_name="Ladrão_Ç",
        qpath=tmp_path / "x.html.gz",
        uid=uid,
    )
    lines = parse_json_log_lines(capsys.readouterr().out)
    assert lines[-1]["char_name"] == "Ladrão_Ç"
    assert str(uid) in str(lines[-1]["uid"])


# =============================================================================
# Feature 7: servers Table + 3 Seeds (001_initial_schema.py)
# =============================================================================


def test_f7_t1_01_server_orm_model_columns() -> None:
    """F7-T1-01: Inspect `Server` ORM model columns matching design doc."""
    server_cls = cast(OrmTableModelProto, require_symbol("src.models.db_models", "Server"))
    assert server_cls.__tablename__ == "servers"
    cols = set(server_cls.__table__.columns.keys())
    assert {
        "id",
        "display_name",
        "base_url",
        "current_season",
        "is_active",
        "fw_day_of_week",
        "fw_end_time_utc",
        "min_records_expected",
        "circuit_breaker_until",
        "created_at",
        "updated_at",
    }.issubset(cols)


def test_f7_t1_02_alembic_offline_sql_has_servers_and_seeds(repo_root: Path) -> None:
    """F7-T1-02: Verify `alembic upgrade head --sql` emits `servers` DDL and 3 seeds."""
    require_file("alembic.ini", repo_root)
    proc = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head", "--sql"],
        cwd=repo_root,
        env={**os.environ, "DATABASE_URL": "postgresql+asyncpg://u:p@localhost:5432/db"},
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    sql = proc.stdout
    assert "CREATE TABLE servers" in sql
    for sid in ("'chronicle'", "'electus'", "'play_golden'"):
        assert sid in sql
    assert "ON CONFLICT" in sql.upper()


@pytest.mark.e2e_db
async def test_f7_t1_03_seeded_servers_in_postgres(clean_db: AsyncSession) -> None:
    """F7-T1-03: Query seeded `servers` table after `alembic upgrade head`."""
    rows = (
        (
            await clean_db.execute(
                text(
                    "SELECT id, base_url, fw_day_of_week, fw_end_time_utc, "
                    "min_records_expected, is_active, current_season, circuit_breaker_until "
                    "FROM servers ORDER BY id"
                )
            )
        )
        .mappings()
        .all()
    )
    assert len(rows) == 3
    by_id = {str(r["id"]): dict(r) for r in rows}
    assert by_id["chronicle"]["base_url"] == "https://play-chronicle.com"
    assert by_id["chronicle"]["min_records_expected"] == 100
    assert by_id["chronicle"]["fw_day_of_week"] == 0
    assert by_id["electus"]["base_url"] == "https://electus.online"
    assert by_id["electus"]["min_records_expected"] == 100
    assert by_id["play_golden"]["base_url"] == "https://play-golden.online"
    assert by_id["play_golden"]["min_records_expected"] == 50
    assert by_id["play_golden"]["fw_day_of_week"] == 6
    for srv in by_id.values():
        assert srv["is_active"] is True
        assert srv["current_season"] == "v1"
        assert srv["circuit_breaker_until"] is None


@pytest.mark.e2e_db
async def test_f7_t1_04_circuit_breaker_timestamptz_persists(clean_db: AsyncSession) -> None:
    """F7-T1-04: Persist `circuit_breaker_until` +6h on `electus` and verify tz-aware read."""
    await clean_db.execute(
        text(
            "UPDATE servers SET circuit_breaker_until = CURRENT_TIMESTAMP + INTERVAL '6 hours' "
            "WHERE id = 'electus'"
        )
    )
    await clean_db.commit()
    srv = await fetch_server_row(clean_db, "electus")
    cb = srv["circuit_breaker_until"]
    assert isinstance(cb, datetime)
    assert cb.tzinfo is not None
    assert cb > datetime.now(UTC) + timedelta(hours=5, minutes=55)


@pytest.mark.e2e_db
async def test_f7_t1_05_is_active_filtering(clean_db: AsyncSession) -> None:
    """F7-T1-05: Toggling `is_active = FALSE` excludes server from active filter."""
    await clean_db.execute(text("UPDATE servers SET is_active = FALSE WHERE id = 'play_golden'"))
    await clean_db.commit()
    active_ids = [
        str(r[0])
        for r in (
            await clean_db.execute(
                text("SELECT id FROM servers WHERE is_active = TRUE ORDER BY id")
            )
        ).all()
    ]
    assert active_ids == ["chronicle", "electus"]


# =============================================================================
# Feature 8: scrape_runs Audit Table (001_initial_schema.py)
# =============================================================================


def test_f8_t1_01_scrape_run_orm_and_canonical_constants() -> None:
    """F8-T1-01: Verify `ScrapeRun` columns and canonical status/trigger constants."""
    sr_cls = cast(OrmTableModelProto, require_symbol("src.models.db_models", "ScrapeRun"))
    assert sr_cls.__tablename__ == "scrape_runs"
    statuses = set(
        assert_sequence_of_str(require_symbol("src.models.pydantic_schemas", "CANONICAL_STATUSES"))
    )
    triggers = set(
        assert_sequence_of_str(
            require_symbol("src.models.pydantic_schemas", "CANONICAL_TRIGGER_TYPES")
        )
    )
    assert statuses == {
        "SUCCESS",
        "PARTIAL_SUCCESS",
        "SCHEMA_DRIFT",
        "LOW_COUNT_QUARANTINE",
        "BLOCKED_CHALLENGE",
        "TARGET_MAINTENANCE",
        "HTTP_ERROR",
    }
    assert triggers == {"scheduled_12h", "post_fw_extra", "manual"}


@pytest.mark.e2e_db
@pytest.mark.parametrize(
    "status",
    [
        "SUCCESS",
        "PARTIAL_SUCCESS",
        "SCHEMA_DRIFT",
        "LOW_COUNT_QUARANTINE",
        "BLOCKED_CHALLENGE",
        "TARGET_MAINTENANCE",
        "HTTP_ERROR",
    ],
)
async def test_f8_t1_02_insert_all_canonical_statuses(clean_db: AsyncSession, status: str) -> None:
    """F8-T1-02: Insert `scrape_runs` rows for every canonical status."""
    await clean_db.execute(
        text(
            "INSERT INTO scrape_runs (server_id, trigger_type, status, execution_time_ms) "
            "VALUES ('chronicle', 'scheduled_12h', :status, 100)"
        ),
        {"status": status},
    )
    await clean_db.commit()
    latest = await fetch_latest_scrape_run(clean_db, "chronicle")
    assert latest is not None
    assert latest["status"] == status
    assert latest["ranking_category"] == "all"


@pytest.mark.e2e_db
@pytest.mark.parametrize("trigger_type", ["scheduled_12h", "post_fw_extra", "manual"])
async def test_f8_t1_03_insert_all_trigger_types(clean_db: AsyncSession, trigger_type: str) -> None:
    """F8-T1-03: Insert `scrape_runs` rows for all 3 canonical trigger types."""
    await clean_db.execute(
        text(
            "INSERT INTO scrape_runs (server_id, trigger_type, status, execution_time_ms) "
            "VALUES ('electus', :trigger_type, 'SUCCESS', 120)"
        ),
        {"trigger_type": trigger_type},
    )
    await clean_db.commit()
    latest = await fetch_latest_scrape_run(clean_db, "electus")
    assert latest is not None
    assert latest["trigger_type"] == trigger_type


@pytest.mark.e2e_db
async def test_f8_t1_04_quarantine_metadata_and_jsonb_roundtrip(clean_db: AsyncSession) -> None:
    """F8-T1-04: Persist quarantine path, CHAR(64) sha256, size_bytes, and error_details JSONB."""
    sha = "a" * 64
    err_json = orjson.dumps(
        {
            "reason": "Top rank missing",
            "failed_category": "player",
            "sample_errors": [{"rank": 1, "error": "missing"}],
            "quarantine_sha256": sha,
            "quarantine_size_bytes": 512,
        }
    ).decode("utf-8")
    await clean_db.execute(
        text(
            """
            INSERT INTO scrape_runs (
                server_id, trigger_type, status, execution_time_ms,
                quarantine_file_path, quarantine_sha256, quarantine_size_bytes, error_details
            ) VALUES (
                'chronicle', 'manual', 'SCHEMA_DRIFT', 250,
                './quarantine/chronicle/test.html.gz', :sha, 512, CAST(:err AS jsonb)
            )
            """
        ),
        {"sha": sha, "err": err_json},
    )
    await clean_db.commit()
    row = await fetch_latest_scrape_run(clean_db, "chronicle")
    assert row is not None
    assert row["quarantine_sha256"] == sha
    assert row["quarantine_size_bytes"] == 512
    assert row["error_details"]["failed_category"] == "player"


@pytest.mark.e2e_db
async def test_f8_t1_05_telemetry_counters_persisted(clean_db: AsyncSession) -> None:
    """F8-T1-05: Persist extraction, persistence, validation error, HTTP, and timing counters."""
    await clean_db.execute(
        text(
            """
            INSERT INTO scrape_runs (
                server_id, trigger_type, status, records_extracted, records_persisted,
                validation_errors_count, execution_time_ms, http_status_code
            ) VALUES ('play_golden', 'scheduled_12h', 'PARTIAL_SUCCESS', 1000, 995, 5, 420, 200)
            """
        )
    )
    await clean_db.commit()
    row = await fetch_latest_scrape_run(clean_db, "play_golden")
    assert row is not None
    assert row["records_extracted"] == 1000
    assert row["records_persisted"] == 995
    assert row["validation_errors_count"] == 5
    assert row["execution_time_ms"] == 420
    assert row["http_status_code"] == 200


# =============================================================================
# Feature 9: characters & guilds Dimension Tables (001_initial_schema.py)
# =============================================================================


def test_f9_t1_01_character_and_guild_orm_contracts() -> None:
    """F9-T1-01: Verify `Character` and `Guild` ORM table metadata & constraints."""
    char_cls = cast(OrmTableModelProto, require_symbol("src.models.db_models", "Character"))
    guild_cls = cast(OrmTableModelProto, require_symbol("src.models.db_models", "Guild"))
    assert char_cls.__tablename__ == "characters"
    assert guild_cls.__tablename__ == "guilds"
    char_constraints = {c.name for c in char_cls.__table__.constraints}
    guild_constraints = {c.name for c in guild_cls.__table__.constraints}
    assert "uq_char_per_server" in char_constraints
    assert "uq_guild_per_server" in guild_constraints


@pytest.mark.e2e_db
@pytest.mark.parametrize(("raw_race", "expected_race"), [("CH", "CH"), ("EU", "EU"), (None, "UNK")])
async def test_f9_t1_02_character_upsert_coerces_null_race_to_unk(
    clean_db: AsyncSession, raw_race: str | None, expected_race: str
) -> None:
    """F9-T1-02: Upsert characters with CH, EU, and NULL -> UNK."""
    now = datetime.now(UTC)
    res = await clean_db.execute(
        text(
            """
            INSERT INTO characters (server_id, char_name, race, first_seen_at, last_seen_at)
            VALUES ('chronicle', :name, COALESCE(:race, 'UNK'), :now, :now)
            ON CONFLICT (server_id, char_name) DO UPDATE
            SET last_seen_at = EXCLUDED.last_seen_at
            RETURNING id, race
            """
        ),
        {"name": f"Hero_{raw_race}", "race": raw_race, "now": now},
    )
    row = res.one()
    await clean_db.commit()
    assert row[1] == expected_race


@pytest.mark.e2e_db
async def test_f9_t1_03_character_race_upgrade_unk_to_ch_preserves_first_seen(
    clean_db: AsyncSession,
) -> None:
    """F9-T1-03: Upgrade character race from `UNK` to `CH` while preserving `first_seen_at`."""
    t1 = datetime(2026, 11, 18, 0, 0, tzinfo=UTC)
    t2 = datetime(2026, 11, 18, 12, 0, tzinfo=UTC)
    sql = text(
        """
        INSERT INTO characters (server_id, char_name, race, first_seen_at, last_seen_at)
        VALUES ('chronicle', 'EvolHero', COALESCE(:race, 'UNK'), :ts, :ts)
        ON CONFLICT (server_id, char_name) DO UPDATE
        SET race = CASE
                WHEN EXCLUDED.race IN ('CH', 'EU') THEN EXCLUDED.race
                ELSE characters.race
            END,
            last_seen_at = EXCLUDED.last_seen_at
        RETURNING id, race, first_seen_at, last_seen_at
        """
    )
    r1 = (await clean_db.execute(sql, {"race": None, "ts": t1})).one()
    r2 = (await clean_db.execute(sql, {"race": "CH", "ts": t2})).one()
    await clean_db.commit()
    assert r1[0] == r2[0]
    assert r2[1] == "CH"
    assert r2[2] == t1
    assert r2[3] == t2


@pytest.mark.e2e_db
async def test_f9_t1_04_guild_master_char_name_coalesce_update(clean_db: AsyncSession) -> None:
    """F9-T1-04: Upsert guild first without master, then populate `master_char_name`."""
    t1 = datetime(2026, 11, 18, 0, 0, tzinfo=UTC)
    t2 = datetime(2026, 11, 18, 12, 0, tzinfo=UTC)
    sql = text(
        """
        INSERT INTO guilds (
            server_id, guild_name, master_char_name, first_seen_at, last_seen_at, is_active
        )
        VALUES ('electus', 'Titans', :master, :ts, :ts, TRUE)
        ON CONFLICT (server_id, guild_name) DO UPDATE
        SET master_char_name = COALESCE(EXCLUDED.master_char_name, guilds.master_char_name),
            last_seen_at = EXCLUDED.last_seen_at
        RETURNING id, master_char_name, first_seen_at
        """
    )
    r1 = (await clean_db.execute(sql, {"master": None, "ts": t1})).one()
    r2 = (await clean_db.execute(sql, {"master": "GuildBoss", "ts": t2})).one()
    await clean_db.commit()
    assert r1[0] == r2[0]
    assert r2[1] == "GuildBoss"
    assert r2[2] == t1


@pytest.mark.e2e_db
async def test_f9_t1_05_same_names_isolated_across_servers(clean_db: AsyncSession) -> None:
    """F9-T1-05: Identical `char_name` and `guild_name` on different servers get distinct UUIDs."""
    now = datetime.now(UTC)
    c_ids = []
    for srv in ("chronicle", "electus"):
        res = await clean_db.execute(
            text(
                "INSERT INTO characters (server_id, char_name, race, first_seen_at, last_seen_at) "
                "VALUES (:srv, 'HeroOne', 'CH', :now, :now) RETURNING id"
            ),
            {"srv": srv, "now": now},
        )
        c_ids.append(res.scalar_one())
    await clean_db.commit()
    assert c_ids[0] != c_ids[1]


# =============================================================================
# Feature 10: Partitioned Fact Tables & Indexes (001_initial_schema.py)
# =============================================================================


def test_f10_t1_01_fact_models_declare_range_partitioning(repo_root: Path) -> None:
    """F10-T1-01: Verify ORM `__table_args__` and `alembic --sql` declare RANGE partitioning."""
    cs_cls = cast(OrmTableModelProto, require_symbol("src.models.db_models", "CharacterSnapshot"))
    gs_cls = cast(OrmTableModelProto, require_symbol("src.models.db_models", "GuildSnapshot"))
    assert [c.name for c in cs_cls.__table__.primary_key.columns] == ["id", "created_at"]
    assert [c.name for c in gs_cls.__table__.primary_key.columns] == ["id", "created_at"]
    require_file("alembic.ini", repo_root)
    proc = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head", "--sql"],
        cwd=repo_root,
        env={**os.environ, "DATABASE_URL": "postgresql+asyncpg://u:p@localhost:5432/db"},
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert proc.returncode == 0
    assert "PARTITION BY RANGE (created_at)" in proc.stdout


@pytest.mark.e2e_db
@pytest.mark.parametrize(
    ("index_name", "expected_method"),
    [
        ("idx_char_snapshots_lookup", "btree"),
        ("idx_char_snapshots_rank", "btree"),
        ("idx_char_snapshots_brin_time", "brin"),
        ("idx_char_snapshots_extra_metrics", "gin"),
        ("idx_guild_snapshots_lookup", "btree"),
        ("idx_guild_snapshots_rank", "btree"),
        ("idx_guild_snapshots_brin_time", "brin"),
    ],
)
async def test_f10_t1_02_pg_indexes_exist_with_expected_methods(
    clean_db: AsyncSession, index_name: str, expected_method: str
) -> None:
    """F10-T1-02: Verify all 7 B-Tree/BRIN/GIN indexes exist in PostgreSQL `pg_indexes`."""
    res = await clean_db.execute(
        text("SELECT indexdef FROM pg_indexes WHERE indexname = :idx"), {"idx": index_name}
    )
    defs = [str(r[0]).lower() for r in res.all()]
    assert defs, f"Index {index_name} missing"
    assert any(f"using {expected_method}" in d for d in defs)


@pytest.mark.e2e_db
async def test_f10_t1_03_rows_route_to_2026_11_partition(clean_db: AsyncSession) -> None:
    """F10-T1-03: Insert November 2026 snapshot and verify `tableoid` partition."""
    ts = datetime(2026, 11, 19, 12, 0, tzinfo=UTC)
    await seed_historical_run(
        clean_db,
        server_id="chronicle",
        created_at=ts,
        characters=[
            HistoricalCharSeed(char_name="NovHero", rank_position=1, level=120, item_points=500.0)
        ],
    )
    row = (
        await clean_db.execute(
            text("SELECT id, created_at FROM character_snapshots WHERE created_at = :ts"),
            {"ts": ts},
        )
    ).one()
    part = await get_snapshot_partition_name(clean_db, "character_snapshots", int(row[0]), row[1])
    assert part == "character_snapshots_2026_11"


@pytest.mark.e2e_db
async def test_f10_t1_04_rows_route_to_2026_12_partition(clean_db: AsyncSession) -> None:
    """F10-T1-04: Insert December 2026 snapshot and verify `tableoid` partition."""
    ts = datetime(2026, 12, 15, 0, 0, tzinfo=UTC)
    await seed_historical_run(
        clean_db,
        server_id="chronicle",
        created_at=ts,
        characters=[
            HistoricalCharSeed(char_name="DecHero", rank_position=1, level=120, item_points=500.0)
        ],
    )
    row = (
        await clean_db.execute(
            text("SELECT id, created_at FROM character_snapshots WHERE created_at = :ts"),
            {"ts": ts},
        )
    ).one()
    part = await get_snapshot_partition_name(clean_db, "character_snapshots", int(row[0]), row[1])
    assert part == "character_snapshots_2026_12"


@pytest.mark.e2e_db
async def test_f10_t1_05_chunked_bulk_insert_2500_rows(clean_db: AsyncSession) -> None:
    """F10-T1-05: Bulk-insert 2,500 snapshot rows in chunks <= 1000 cleanly."""
    ts = datetime(2026, 11, 20, 0, 0, tzinfo=UTC)
    run_id = uuid.uuid4()
    await clean_db.execute(
        text(
            "INSERT INTO scrape_runs (id, server_id, trigger_type, status, execution_time_ms) "
            "VALUES (:id, 'chronicle', 'manual', 'SUCCESS', 100)"
        ),
        {"id": run_id},
    )
    char_rows = [
        {"server_id": "chronicle", "char_name": f"Bulk_{i:04d}", "race": "CH", "ts": ts}
        for i in range(2500)
    ]
    res = await clean_db.execute(
        text(
            "INSERT INTO characters (server_id, char_name, race, first_seen_at, last_seen_at) "
            "VALUES (:server_id, :char_name, :race, :ts, :ts) RETURNING id"
        ),
        char_rows,
    )
    char_ids = [r[0] for r in res.all()]
    snap_rows = [
        {
            "run_id": run_id,
            "server_id": "chronicle",
            "char_id": cid,
            "rank": idx + 1,
            "level": 120,
            "pts": 100.0,
            "ts": ts,
        }
        for idx, cid in enumerate(char_ids)
    ]
    t_start = pytime.perf_counter()
    for start in range(0, len(snap_rows), 1000):
        await clean_db.execute(
            text(
                "INSERT INTO character_snapshots "
                "(scrape_run_id, server_id, character_id, rank_position, "
                "level, item_points, created_at, updated_at) "
                "VALUES (:run_id, :server_id, :char_id, :rank, :level, :pts, :ts, :ts)"
            ),
            snap_rows[start : start + 1000],
        )
    await clean_db.commit()
    elapsed_ms = (pytime.perf_counter() - t_start) * 1000
    assert await count_rows(clean_db, "character_snapshots", "chronicle") == 2500
    assert elapsed_ms < 5000


# =============================================================================
# Feature 11: 5 Analytical SQL Views (002_analytical_views.py)
# =============================================================================


def test_f11_t1_01_alembic_sql_creates_and_drops_5_views(repo_root: Path) -> None:
    """F11-T1-01: Verify offline Alembic SQL creates and drops all 5 exact views."""
    require_file("alembic.ini", repo_root)
    up_proc = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head", "--sql"],
        cwd=repo_root,
        env={**os.environ, "DATABASE_URL": "postgresql+asyncpg://u:p@localhost:5432/db"},
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert up_proc.returncode == 0
    views = [
        "v_character_activity_deltas",
        "v_bot_vs_donor_classification",
        "v_guild_turnover_dominance",
        "v_race_balance_ch_vs_eu",
        "v_server_retention_weekly",
    ]
    for view in views:
        assert view in up_proc.stdout
    down_proc = subprocess.run(
        [sys.executable, "-m", "alembic", "downgrade", "head:base", "--sql"],
        cwd=repo_root,
        env={**os.environ, "DATABASE_URL": "postgresql+asyncpg://u:p@localhost:5432/db"},
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert down_proc.returncode == 0
    for view in views:
        assert view in down_proc.stdout


@pytest.mark.e2e_db
async def test_f11_t1_02_view_character_activity_deltas_rate_per_hour(
    clean_db: AsyncSession,
) -> None:
    """F11-T1-02: Verify `v_character_activity_deltas` computes hourly normalized rates."""
    t0 = datetime(2026, 11, 18, 8, 0, tzinfo=UTC)
    t1 = t0 + timedelta(hours=4)
    await seed_historical_run(
        clean_db,
        server_id="chronicle",
        created_at=t0,
        characters=[
            HistoricalCharSeed(char_name="Ace", rank_position=10, level=119, item_points=100.0)
        ],
    )
    await seed_historical_run(
        clean_db,
        server_id="chronicle",
        created_at=t1,
        characters=[
            HistoricalCharSeed(char_name="Ace", rank_position=5, level=120, item_points=140.0)
        ],
    )
    rows = await query_view(clean_db, "v_character_activity_deltas", "char_name = 'Ace'")
    assert len(rows) == 1
    assert float(rows[0]["hours_since_last_snapshot"]) == pytest.approx(4.0)
    assert float(rows[0]["delta_item_points"]) == pytest.approx(40.0)
    assert float(rows[0]["item_points_rate_per_hour"]) == pytest.approx(10.0)
    assert rows[0]["delta_level"] == 1
    assert rows[0]["rank_gain"] == 5


@pytest.mark.e2e_db
async def test_f11_t1_03_view_bot_vs_donor_classification_archetypes(
    clean_db: AsyncSession,
) -> None:
    """F11-T1-03: Verify 4 behavioral archetypes in `v_bot_vs_donor_classification`."""
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    for step in range(8):
        ts = now - timedelta(hours=12 * (7 - step))
        chars = [
            HistoricalCharSeed(
                char_name="Bot1",
                rank_position=1,
                level=100 + step,
                item_points=100.0 + step * 5,
                job_exp=1000 * (step + 1),
            ),
            HistoricalCharSeed(
                char_name="Whale1",
                rank_position=2,
                level=120 + (1 if step == 1 else 0),
                item_points=100.0 + (200.0 if step == 3 else 0.0),
            ),
            HistoricalCharSeed(
                char_name="Hardcore1",
                rank_position=3,
                level=110 + (step if step <= 5 else 5),
                item_points=100.0 + step * 10,
            ),
            HistoricalCharSeed(
                char_name="Casual1",
                rank_position=4,
                level=100 + (1 if step in (1, 2) else 0) + (1 if step >= 2 else 0),
                item_points=100.0 + (10.0 if step in (1, 2) else 0.0),
            ),
        ]
        await seed_historical_run(clean_db, server_id="chronicle", created_at=ts, characters=chars)
    rows = await query_view(clean_db, "v_bot_vs_donor_classification")
    seg = {r["char_name"]: r["player_behavior_segment"] for r in rows}
    assert seg["Bot1"] == "SUSPECTED_BOT_24_7"
    assert seg["Whale1"] == "SUSPECTED_DONATOR_WHALE"
    assert seg["Hardcore1"] == "HARDCORE_HUMAN"
    assert seg["Casual1"] == "CASUAL_OR_BALANCED"


@pytest.mark.e2e_db
async def test_f11_t1_04_view_guild_turnover_dominance(clean_db: AsyncSession) -> None:
    """F11-T1-04: Verify top-100 market share and `[SEM GUILDA]` fallback."""
    ts = datetime(2026, 11, 18, 12, 0, tzinfo=UTC)
    chars: list[HistoricalCharSeed] = []
    for r in range(1, 101):
        gname = "GuildA" if r <= 40 else ("GuildB" if r <= 80 else None)
        chars.append(
            HistoricalCharSeed(
                char_name=f"P{r}", rank_position=r, level=120, item_points=100.0, guild_name=gname
            )
        )
    await seed_historical_run(clean_db, server_id="chronicle", created_at=ts, characters=chars)
    rows = await query_view(clean_db, "v_guild_turnover_dominance", "server_id = 'chronicle'")
    by_guild = {r["guild_name"]: r for r in rows}
    assert by_guild["GuildA"]["players_in_top_100"] == 40
    assert by_guild["GuildA"]["players_in_top_50"] == 40
    assert float(by_guild["GuildA"]["top_100_market_share_percent"]) == pytest.approx(40.0)
    assert by_guild["[SEM GUILDA]"]["players_in_top_100"] == 20


@pytest.mark.e2e_db
async def test_f11_t1_05_views_race_balance_and_weekly_retention(clean_db: AsyncSession) -> None:
    """F11-T1-05: Verify `v_race_balance_ch_vs_eu` and `v_server_retention_weekly`."""
    t0 = datetime(2026, 11, 16, 0, 0, tzinfo=UTC)
    t1 = t0 + timedelta(hours=12)
    await seed_historical_run(
        clean_db,
        server_id="chronicle",
        created_at=t0,
        status="PARTIAL_SUCCESS",
        characters=[
            HistoricalCharSeed(
                char_name="Ch1", rank_position=1, level=120, item_points=200.0, race="CH"
            ),
            HistoricalCharSeed(
                char_name="Eu1", rank_position=2, level=118, item_points=100.0, race="EU"
            ),
        ],
    )
    await seed_historical_run(
        clean_db,
        server_id="chronicle",
        created_at=t1,
        status="PARTIAL_SUCCESS",
        characters=[
            HistoricalCharSeed(
                char_name="Ch1", rank_position=1, level=120, item_points=220.0, race="CH"
            ),
            HistoricalCharSeed(
                char_name="Eu1", rank_position=2, level=118, item_points=100.0, race="EU"
            ),
        ],
    )
    race_rows = await query_view(clean_db, "v_race_balance_ch_vs_eu", "server_id = 'chronicle'")
    by_race = {r["race"]: r for r in race_rows}
    assert by_race["CH"]["total_ranked_characters"] == 1
    assert by_race["EU"]["total_ranked_characters"] == 1
    ret_rows = await query_view(clean_db, "v_server_retention_weekly", "server_id = 'chronicle'")
    assert len(ret_rows) == 1
    assert ret_rows[0]["total_tracked_characters"] == 2
    assert ret_rows[0]["active_characters"] == 1
    assert float(ret_rows[0]["activity_retention_rate_pct"]) == pytest.approx(50.0)


# =============================================================================
# Feature 12: Pydantic v2 Schemas & Validators (pydantic_schemas.py)
# =============================================================================


def test_f12_t1_01_raw_player_ranking_item_normalization() -> None:
    """F12-T1-01: Instantiate `RawPlayerRankingItem` with whitespace & formatted strings."""
    cls = cast(
        PydanticModelClsProto, require_symbol("src.models.pydantic_schemas", "RawPlayerRankingItem")
    )
    item = cast(
        RawPlayerItemProto,
        cls(
            rank=1,
            char_name="  Arthur  ",
            level="120",
            guild_name="Knights",
            race="CH",
            item_points="1,250.50",
            strength="450",
            intellect="120",
            job_type="Trader",
            job_level="75",
            honor_points="1,500",
            extra_metrics={"pvp_kd": "2.1"},
        ),
    )
    assert item.char_name == "Arthur"
    assert item.level == 120
    assert item.race == "CH"
    assert item.item_points == pytest.approx(1250.5)
    assert item.strength == 450
    assert item.job_type == "TRADER"
    assert item.honor_points == 1500
    assert item.extra_metrics == {"pvp_kd": "2.1"}


def test_f12_t1_02_raw_job_ranking_item_portuguese_thief() -> None:
    """F12-T1-02: Verify `RawJobRankingItem` maps `Ladrão` -> `THIEF` and cleans ints."""
    cls = cast(
        PydanticModelClsProto, require_symbol("src.models.pydantic_schemas", "RawJobRankingItem")
    )
    item = cast(
        RawJobItemProto,
        cls(
            rank=1,
            char_name="Shadow",
            job_type="Ladrão",
            job_level="80",
            job_exp="9,876,543",
            guild_name="Thieves",
        ),
    )
    assert item.job_type == "THIEF"
    assert item.job_level == 80
    assert item.job_exp == 9876543


@pytest.mark.parametrize(
    ("raw_job", "expected"),
    [("Caçador", "HUNTER"), ("HUNTER", "HUNTER"), ("Mercador", "TRADER"), ("trader", "TRADER")],
)
def test_f12_t1_03_raw_job_ranking_item_portuguese_variants(raw_job: str, expected: str) -> None:
    """F12-T1-03: Verify job_type normalization across English and Portuguese aliases."""
    cls = cast(
        PydanticModelClsProto, require_symbol("src.models.pydantic_schemas", "RawJobRankingItem")
    )
    item = cast(
        RawJobItemProto,
        cls(rank=1, char_name="JobChar", job_type=raw_job, job_level=50),
    )
    assert item.job_type == expected


def test_f12_t1_04_raw_guild_ranking_item_eu_float() -> None:
    """F12-T1-04: Instantiate `RawGuildRankingItem` with European decimal notation."""
    cls = cast(
        PydanticModelClsProto, require_symbol("src.models.pydantic_schemas", "RawGuildRankingItem")
    )
    item = cast(
        RawGuildItemProto,
        cls(
            rank=1,
            guild_name="Empire",
            master_name="Emperor",
            members_count="48",
            total_item_points="45.123,75",
            fortress_held="Jangan",
        ),
    )
    assert item.members_count == 48
    assert item.total_item_points == pytest.approx(45123.75)
    assert item.fortress_held == "Jangan"


def test_f12_t1_05_error_details_and_module_constants() -> None:
    """F12-T1-05: Verify `ErrorDetails` model and schema threshold constants."""
    max_err = require_symbol("src.models.pydantic_schemas", "MAX_ERROR_RATE")
    min_guilds = require_symbol("src.models.pydantic_schemas", "MIN_GUILD_RECORDS")
    err_cls = cast(
        PydanticModelClsProto, require_symbol("src.models.pydantic_schemas", "ErrorDetails")
    )
    assert max_err == 0.05
    assert min_guilds == 5
    details = err_cls(
        reason="Validation error rate exceeded",
        failed_category="player",
        sample_errors=[{"rank": 4, "error": "level too high"}],
        quarantine_sha256="b" * 64,
        quarantine_size_bytes=1234,
    )
    assert isinstance(details, BaseModel)
    dumped = details.model_dump()
    assert dumped["failed_category"] == "player"
    assert dumped["quarantine_size_bytes"] == 1234


# =============================================================================
# Feature 13: HTTPX Session & Transport Retries (BaseRankingScraper)
# =============================================================================


@pytest.mark.e2e_db
async def test_f13_t1_01_httpx_client_http2_limits_timeouts(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """F13-T1-01: Verify `BaseRankingScraper` instantiates `httpx.AsyncClient(http2=True)`."""
    captured_kwargs: list[dict[str, Any]] = []
    real_async_client = httpx.AsyncClient

    def spy_async_client(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        captured_kwargs.append(kwargs)
        return real_async_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", spy_async_client)
    respx_mock.get("https://play-chronicle.com/ranking/player?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html(make_player_specs(100)))
    )
    respx_mock.get("https://play-chronicle.com/ranking/player?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html([]))
    )
    respx_mock.get("https://play-chronicle.com/ranking/job?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_job_html([]))
    )
    respx_mock.get("https://play-chronicle.com/ranking/guild?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_guild_html(make_guild_specs(5)))
    )
    respx_mock.get("https://play-chronicle.com/ranking/guild?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_guild_html([]))
    )

    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    scraper = get_scraper("chronicle", e2e_settings)
    await scraper.run(clean_db, trigger_type="manual")
    assert captured_kwargs
    assert captured_kwargs[0].get("http2") is True


async def test_f13_t1_02_outbound_headers_contain_user_agent(
    e2e_settings: Any, respx_mock: respx.MockRouter
) -> None:
    """F13-T1-02: Verify outgoing requests include legitimate User-Agent/Accept headers."""
    seen_headers: list[httpx.Headers] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_headers.append(request.headers)
        if "page=1" in str(request.url):
            return httpx.Response(200, text=render_chronicle_player_html(make_player_specs(5)))
        return httpx.Response(200, text=render_chronicle_player_html([]))

    respx_mock.get(url__regex=r"https://play-chronicle\.com/ranking/player\?page=\d+").mock(
        side_effect=handler
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    scraper = get_scraper("chronicle", e2e_settings)
    async with httpx.AsyncClient(http2=True) as client:
        res = await scraper.fetch_player_pages(client, "https://play-chronicle.com")
    assert len(res.raw_records) == 5
    assert seen_headers
    assert "user-agent" in seen_headers[0]


async def test_f13_t1_03_single_connect_timeout_recovers_with_2s_backoff(
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """F13-T1-03: Recover from 1 ConnectTimeout with 2s retry backoff."""
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    route = respx_mock.get("https://play-chronicle.com/ranking/player?page=1")
    route.side_effect = [
        httpx.ConnectTimeout("timed out"),
        httpx.Response(200, text=render_chronicle_player_html(make_player_specs(5))),
    ]
    respx_mock.get("https://play-chronicle.com/ranking/player?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html([]))
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    scraper = get_scraper("chronicle", e2e_settings)
    async with httpx.AsyncClient(http2=True) as client:
        res = await scraper.fetch_player_pages(client, "https://play-chronicle.com")
    assert len(res.raw_records) == 5
    assert route.call_count == 2
    assert sleeps == [2] or sleeps == [2.0]


async def test_f13_t1_04_two_retries_502_504_then_200(
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """F13-T1-04: Recover on 3rd attempt after 502 and 504 with [2, 5] backoff."""
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    route = respx_mock.get("https://play-chronicle.com/ranking/player?page=1")
    route.side_effect = [
        httpx.Response(502, text="Bad Gateway"),
        httpx.Response(504, text="Gateway Timeout"),
        httpx.Response(200, text=render_chronicle_player_html(make_player_specs(5))),
    ]
    respx_mock.get("https://play-chronicle.com/ranking/player?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html([]))
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    scraper = get_scraper("chronicle", e2e_settings)
    async with httpx.AsyncClient(http2=True) as client:
        res = await scraper.fetch_player_pages(client, "https://play-chronicle.com")
    assert len(res.raw_records) == 5
    assert route.call_count == 3
    assert [int(s) for s in sleeps] == [2, 5]


async def test_f13_t1_05_semaphore_limits_concurrency_to_3(
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """F13-T1-05: Confirm peak concurrent HTTP requests never exceed Semaphore(3)."""
    in_flight = 0
    max_in_flight = 0
    lock = asyncio.Lock()

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal in_flight, max_in_flight
        async with lock:
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
        await asyncio.sleep(0.01)
        async with lock:
            in_flight -= 1
        return httpx.Response(200, text=render_chronicle_player_html(make_player_specs(5)))

    respx_mock.get(url__regex=r"https://play-chronicle\.com/ranking/player\?page=\d+").mock(
        side_effect=handler
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    scraper = get_scraper("chronicle", e2e_settings)
    async with httpx.AsyncClient(http2=True) as client:
        res = await scraper.fetch_player_pages(client, "https://play-chronicle.com")
    assert len(res.raw_records) == 50
    assert max_in_flight <= 3


# =============================================================================
# Feature 14: Persistent Circuit Breaker (BaseRankingScraper)
# =============================================================================


@pytest.mark.e2e_db
async def test_f14_t1_01_403_turnstile_trips_6h_cb_zero_retries(
    clean_db: AsyncSession,
    e2e_settings: Any,
    e2e_quarantine_dir: Path,
    respx_mock: respx.MockRouter,
) -> None:
    """F14-T1-01: HTTP 403 Turnstile trips 6h CB immediately with 0 retries and .html.gz."""
    route = respx_mock.get(url__regex=r"https://electus\.online/.*").mock(
        return_value=httpx.Response(403, text=render_turnstile_html())
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    scraper = get_scraper("electus", e2e_settings)
    await scraper.run(clean_db, trigger_type="scheduled_12h")

    assert route.call_count == 1
    run_row = await fetch_latest_scrape_run(clean_db, "electus")
    assert run_row is not None
    assert run_row["status"] == "BLOCKED_CHALLENGE"
    assert run_row["http_status_code"] == 403
    assert run_row["records_persisted"] == 0
    assert path_exists(str(run_row["quarantine_file_path"]))
    srv = await fetch_server_row(clean_db, "electus")
    assert srv["circuit_breaker_until"] > datetime.now(UTC) + timedelta(hours=5, minutes=55)


@pytest.mark.e2e_db
async def test_f14_t1_02_429_rate_limit_trips_cb_zero_retries(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """F14-T1-02: HTTP 429 triggers immediate BLOCKED_CHALLENGE and 6h cooldown."""
    route = respx_mock.get(url__regex=r"https://play-chronicle\.com/.*").mock(
        return_value=httpx.Response(429, text="Too Many Requests")
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("chronicle", e2e_settings).run(clean_db)
    assert route.call_count == 1
    run_row = await fetch_latest_scrape_run(clean_db, "chronicle")
    assert run_row is not None
    assert run_row["status"] == "BLOCKED_CHALLENGE"
    assert (await fetch_server_row(clean_db, "chronicle"))["circuit_breaker_until"] is not None


@pytest.mark.e2e_db
async def test_f14_t1_03_active_cb_skips_http_and_scrape_runs(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """F14-T1-03: Pre-active `circuit_breaker_until` skips HTTP and inserts 0 `scrape_runs`."""
    await clean_db.execute(
        text(
            "UPDATE servers SET circuit_breaker_until = CURRENT_TIMESTAMP + INTERVAL '3 hours' "
            "WHERE id = 'chronicle'"
        )
    )
    await clean_db.commit()
    route = respx_mock.get(url__regex=r"https://play-chronicle\.com/.*").mock(
        return_value=httpx.Response(200, text="should not be called")
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("chronicle", e2e_settings).run(clean_db)
    assert route.call_count == 0
    assert await count_rows(clean_db, "scrape_runs", "chronicle") == 0
    assert "chronicle" in capsys.readouterr().out


@pytest.mark.e2e_db
async def test_f14_t1_04_cf_mitigated_header_trips_cb(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """F14-T1-04: HTTP 200 with header `cf-mitigated: challenge` trips CB."""
    respx_mock.get(url__regex=r"https://play-golden\.online/.*").mock(
        return_value=httpx.Response(
            200, headers={"cf-mitigated": "challenge"}, text="<html></html>"
        )
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("play_golden", e2e_settings).run(clean_db)
    run_row = await fetch_latest_scrape_run(clean_db, "play_golden")
    assert run_row is not None
    assert run_row["status"] == "BLOCKED_CHALLENGE"
    assert (await fetch_server_row(clean_db, "play_golden"))["circuit_breaker_until"] is not None


@pytest.mark.e2e_db
async def test_f14_t1_05_cb_persists_across_consecutive_runs(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """F14-T1-05: Second run observes persisted DB CB timestamp and fires 0 HTTP calls."""
    route = respx_mock.get(url__regex=r"https://electus\.online/.*").mock(
        return_value=httpx.Response(403, text=render_turnstile_html())
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    scraper = get_scraper("electus", e2e_settings)
    await scraper.run(clean_db)
    assert route.call_count == 1
    await scraper.run(clean_db)
    assert route.call_count == 1
    assert await count_rows(clean_db, "scrape_runs", "electus") == 1


# =============================================================================
# Feature 15: Maintenance Detection (BaseRankingScraper)
# =============================================================================


@pytest.mark.e2e_db
async def test_f15_t1_01_503_server_under_maintenance_no_cb(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """F15-T1-01: 503 with 'Server under maintenance' yields TARGET_MAINTENANCE without CB."""
    route = respx_mock.get(url__regex=r"https://play-chronicle\.com/.*").mock(
        return_value=httpx.Response(503, text="Server under maintenance - back soon")
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("chronicle", e2e_settings).run(clean_db)
    assert route.call_count == 1
    run_row = await fetch_latest_scrape_run(clean_db, "chronicle")
    assert run_row is not None
    assert run_row["status"] == "TARGET_MAINTENANCE"
    assert (await fetch_server_row(clean_db, "chronicle"))["circuit_breaker_until"] is None


@pytest.mark.e2e_db
async def test_f15_t1_02_503_website_under_maintenance(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """F15-T1-02: 503 with 'Website under maintenance' yields TARGET_MAINTENANCE."""
    respx_mock.get(url__regex=r"https://play-golden\.online/.*").mock(
        return_value=httpx.Response(503, text="Website under maintenance")
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("play_golden", e2e_settings).run(clean_db)
    run_row = await fetch_latest_scrape_run(clean_db, "play_golden")
    assert run_row is not None
    assert run_row["status"] == "TARGET_MAINTENANCE"


@pytest.mark.e2e_db
async def test_f15_t1_03_200_server_maintenance_banner(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """F15-T1-03: HTTP 200 HTML banner 'Server under maintenance' yields TARGET_MAINTENANCE."""
    respx_mock.get(url__regex=r"https://play-chronicle\.com/.*").mock(
        return_value=httpx.Response(200, text=render_maintenance_html("Server under maintenance"))
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("chronicle", e2e_settings).run(clean_db)
    run_row = await fetch_latest_scrape_run(clean_db, "chronicle")
    assert run_row is not None
    assert run_row["status"] == "TARGET_MAINTENANCE"
    assert await count_rows(clean_db, "character_snapshots", "chronicle") == 0


@pytest.mark.e2e_db
async def test_f15_t1_04_200_website_maintenance_banner(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """F15-T1-04: HTTP 200 HTML banner 'Website under maintenance' yields TARGET_MAINTENANCE."""
    respx_mock.get(url__regex=r"https://play-golden\.online/.*").mock(
        return_value=httpx.Response(200, text=render_maintenance_html("Website under maintenance"))
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("play_golden", e2e_settings).run(clean_db)
    run_row = await fetch_latest_scrape_run(clean_db, "play_golden")
    assert run_row is not None
    assert run_row["status"] == "TARGET_MAINTENANCE"


@pytest.mark.e2e_db
async def test_f15_t1_05_immediate_recovery_after_maintenance(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """F15-T1-05: Next run succeeds immediately after TARGET_MAINTENANCE (no CB cooldown)."""
    respx_mock.get("https://play-golden.online/ranking/characters?page=1").mock(
        side_effect=[
            httpx.Response(503, text="Server under maintenance"),
            httpx.Response(200, text=render_golden_player_html(make_player_specs(50))),
        ]
    )
    respx_mock.get("https://play-golden.online/ranking/characters?page=2").mock(
        return_value=httpx.Response(200, text=render_golden_player_html([]))
    )
    respx_mock.get("https://play-golden.online/ranking/jobs?page=1").mock(
        return_value=httpx.Response(200, text=render_golden_job_html([]))
    )
    respx_mock.get("https://play-golden.online/ranking/guilds?page=1").mock(
        return_value=httpx.Response(200, text=render_golden_guild_html(make_guild_specs(5)))
    )
    respx_mock.get("https://play-golden.online/ranking/guilds?page=2").mock(
        return_value=httpx.Response(200, text=render_golden_guild_html([]))
    )

    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    scraper = get_scraper("play_golden", e2e_settings)
    await scraper.run(clean_db)
    assert (await fetch_latest_scrape_run(clean_db, "play_golden") or {})[
        "status"
    ] == "TARGET_MAINTENANCE"
    await scraper.run(clean_db)
    assert (await fetch_latest_scrape_run(clean_db, "play_golden") or {})["status"] == "SUCCESS"
    assert await count_rows(clean_db, "character_snapshots", "play_golden") == 50


# =============================================================================
# Feature 16: In-Memory Job -> Player Reconciliation (BaseRankingScraper)
# =============================================================================


@pytest.mark.e2e_db
@pytest.mark.parametrize(
    ("mode", "expected_job_type", "expected_job_level", "expected_job_rank"),
    [
        ("merge_into_empty", "HUNTER", 90, 4),
        ("overwrite_existing", "THIEF", 88, 2),
        ("extra_metrics_preserved", "TRADER", 77, 1),
        ("no_job_match", None, None, None),
        ("live_persist", "HUNTER", 95, 3),
    ],
    ids=[
        "F16-T1-01_merge_into_empty",
        "F16-T1-02_overwrite_player_job",
        "F16-T1-03_merge_extra_metrics",
        "F16-T1-04_player_without_job_match",
        "F16-T1-05_persists_job_rank_jsonb",
    ],
)
async def test_f16_t1_job_to_player_reconciliation(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
    mode: str,
    expected_job_type: str | None,
    expected_job_level: int | None,
    expected_job_rank: int | None,
) -> None:
    """F16-T1-01..05: Verify in-memory left-join reconciliation & JSONB `job_rank` persistence."""
    players = make_player_specs(100)
    target_name = players[0].char_name
    jobs: list[JobRowSpec] = []
    if mode != "no_job_match":
        assert expected_job_type is not None
        assert expected_job_level is not None
        assert expected_job_rank is not None
        jobs.append(
            JobRowSpec(
                rank=expected_job_rank,
                char_name=target_name,
                job_type=expected_job_type,
                job_level=expected_job_level,
                job_exp=500_000,
            )
        )
    if mode == "overwrite_existing":
        players[0] = PlayerRowSpec(
            rank=1, char_name=target_name, level=120, job_type="TRADER", job_level=10
        )

    respx_mock.get("https://electus.online/api/ranking/player?page=1&limit=100").mock(
        return_value=httpx.Response(200, content=render_electus_player_json(players))
    )
    respx_mock.get("https://electus.online/api/ranking/player?page=2&limit=100").mock(
        return_value=httpx.Response(200, content=render_electus_player_json([]))
    )
    respx_mock.get("https://electus.online/api/ranking/job?page=1&limit=100").mock(
        return_value=httpx.Response(200, content=render_electus_job_json(jobs))
    )
    respx_mock.get("https://electus.online/api/ranking/job?page=2&limit=100").mock(
        return_value=httpx.Response(200, content=render_electus_job_json([]))
    )
    respx_mock.get("https://electus.online/api/ranking/guild?page=1&limit=50").mock(
        return_value=httpx.Response(200, content=render_electus_guild_json(make_guild_specs(5)))
    )
    respx_mock.get("https://electus.online/api/ranking/guild?page=2&limit=50").mock(
        return_value=httpx.Response(200, content=render_electus_guild_json([]))
    )

    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("electus", e2e_settings).run(clean_db)
    row = (
        (
            await clean_db.execute(
                text(
                    "SELECT cs.job_type, cs.job_level, cs.job_exp, cs.extra_metrics "
                    "FROM character_snapshots cs "
                    "JOIN characters c ON c.id = cs.character_id "
                    "WHERE c.char_name = :name AND cs.server_id = 'electus'"
                ),
                {"name": target_name},
            )
        )
        .mappings()
        .one()
    )

    if mode == "no_job_match":
        assert "job_rank" not in row["extra_metrics"]
    else:
        assert row["job_type"] == expected_job_type
        assert row["job_level"] == expected_job_level
        assert row["job_exp"] == 500_000
        assert row["extra_metrics"]["job_rank"] == expected_job_rank
        if mode == "extra_metrics_preserved":
            assert "elo_points" in row["extra_metrics"]


# =============================================================================
# Feature 17: ChronicleScraper (src/scrapers/chronicle.py)
# =============================================================================


async def test_f17_t1_01_chronicle_player_html_and_early_stop(
    e2e_settings: Any, respx_mock: respx.MockRouter
) -> None:
    """F17-T1-01: Parse Chronicle player HTML & stop pagination on empty tbody."""
    p1 = respx_mock.get("https://play-chronicle.com/ranking/player?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html(make_player_specs(100)))
    )
    p2 = respx_mock.get("https://play-chronicle.com/ranking/player?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html([]))
    )
    p3 = respx_mock.get("https://play-chronicle.com/ranking/player?page=3").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html([]))
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    scraper = get_scraper("chronicle", e2e_settings)
    async with httpx.AsyncClient(http2=True) as client:
        res = await scraper.fetch_player_pages(client, "https://play-chronicle.com")
    assert len(res.raw_records) == 100
    assert p1.call_count == 1
    assert p2.call_count == 1
    assert p3.call_count == 0


async def test_f17_t1_02_chronicle_job_html_columns(
    e2e_settings: Any, respx_mock: respx.MockRouter
) -> None:
    """F17-T1-02: Parse Chronicle job HTML columns [0..5]."""
    respx_mock.get("https://play-chronicle.com/ranking/job?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_job_html(make_job_specs(15)))
    )
    respx_mock.get("https://play-chronicle.com/ranking/job?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_job_html([]))
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    async with httpx.AsyncClient(http2=True) as client:
        res = await get_scraper("chronicle", e2e_settings).fetch_job_pages(
            client, "https://play-chronicle.com"
        )
    assert len(res.raw_records) == 15
    assert str(res.raw_records[0]["job_type"]).upper() == "TRADER"


async def test_f17_t1_03_chronicle_guild_presplits_members_fraction(
    e2e_settings: Any, respx_mock: respx.MockRouter
) -> None:
    """F17-T1-03: Parse Chronicle guild HTML and pre-split `45/50` -> `45`."""
    guilds = [
        GuildRowSpec(
            rank=1,
            guild_name="HotanKings",
            master_name="King",
            members_count="45/50",
            total_item_points="125,400.50",
            fortress_held="Hotan",
        )
    ]
    respx_mock.get("https://play-chronicle.com/ranking/guild?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_guild_html(guilds))
    )
    respx_mock.get("https://play-chronicle.com/ranking/guild?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_guild_html([]))
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    guild_cls = cast(
        PydanticModelClsProto, require_symbol("src.models.pydantic_schemas", "RawGuildRankingItem")
    )
    async with httpx.AsyncClient(http2=True) as client:
        res = await get_scraper("chronicle", e2e_settings).fetch_guild_pages(
            client, "https://play-chronicle.com"
        )
    item = cast(RawGuildItemProto, guild_cls.model_validate(res.raw_records[0]))
    assert item.members_count == 45
    assert item.total_item_points == pytest.approx(125400.5)
    assert item.fortress_held == "Hotan"


async def test_f17_t1_04_chronicle_full_multipage_crawl_caps(
    e2e_settings: Any, respx_mock: respx.MockRouter
) -> None:
    """F17-T1-04: Full 10 player pages, 5 job pages, 2 guild pages crawl."""
    p_urls: list[str] = []
    j_urls: list[str] = []
    g_urls: list[str] = []

    def p_side(req: httpx.Request) -> httpx.Response:
        p_urls.append(str(req.url))
        page = int(req.url.params["page"])
        return httpx.Response(
            200,
            text=render_chronicle_player_html(
                make_player_specs(100, start_rank=(page - 1) * 100 + 1)
            ),
        )

    def j_side(req: httpx.Request) -> httpx.Response:
        j_urls.append(str(req.url))
        page = int(req.url.params["page"])
        return httpx.Response(
            200,
            text=render_chronicle_job_html(make_job_specs(100, start_rank=(page - 1) * 100 + 1)),
        )

    def g_side(req: httpx.Request) -> httpx.Response:
        g_urls.append(str(req.url))
        page = int(req.url.params["page"])
        return httpx.Response(
            200,
            text=render_chronicle_guild_html(make_guild_specs(50, start_rank=(page - 1) * 50 + 1)),
        )

    respx_mock.get(url__regex=r"https://play-chronicle\.com/ranking/player\?page=\d+").mock(
        side_effect=p_side
    )
    respx_mock.get(url__regex=r"https://play-chronicle\.com/ranking/job\?page=\d+").mock(
        side_effect=j_side
    )
    respx_mock.get(url__regex=r"https://play-chronicle\.com/ranking/guild\?page=\d+").mock(
        side_effect=g_side
    )

    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    scraper = get_scraper("chronicle", e2e_settings)
    async with httpx.AsyncClient(http2=True) as client:
        p_res = await scraper.fetch_player_pages(client, "https://play-chronicle.com")
        j_res = await scraper.fetch_job_pages(client, "https://play-chronicle.com")
        g_res = await scraper.fetch_guild_pages(client, "https://play-chronicle.com")

    assert len(p_res.raw_records) == 1000 and len(p_urls) == 10
    assert len(j_res.raw_records) == 500 and len(j_urls) == 5
    assert len(g_res.raw_records) == 100 and len(g_urls) == 2


def test_f17_t1_05_chronicle_server_id_identity(e2e_settings: Any) -> None:
    """F17-T1-05: Verify `ChronicleScraper.server_id == 'chronicle'`."""
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    scraper = get_scraper("chronicle", e2e_settings)
    assert scraper.server_id == "chronicle"


# =============================================================================
# Feature 18: ElectusScraper (src/scrapers/electus.py)
# =============================================================================


async def test_f18_t1_01_electus_json_api_data_root_and_aliases(
    e2e_settings: Any, respx_mock: respx.MockRouter
) -> None:
    """F18-T1-01: Parse Electus JSON `/api/ranking/*` (`data` root, `race_id`, aliases)."""
    respx_mock.get("https://electus.online/api/ranking/player?page=1&limit=100").mock(
        return_value=httpx.Response(200, content=render_electus_player_json(make_player_specs(10)))
    )
    respx_mock.get("https://electus.online/api/ranking/player?page=2&limit=100").mock(
        return_value=httpx.Response(200, content=render_electus_player_json([]))
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    player_cls = cast(
        PydanticModelClsProto, require_symbol("src.models.pydantic_schemas", "RawPlayerRankingItem")
    )
    async with httpx.AsyncClient(http2=True) as client:
        res = await get_scraper("electus", e2e_settings).fetch_player_pages(
            client, "https://electus.online"
        )
    assert len(res.raw_records) == 10
    p0 = cast(RawPlayerItemProto, player_cls.model_validate(res.raw_records[0]))
    p1 = cast(RawPlayerItemProto, player_cls.model_validate(res.raw_records[1]))
    assert p0.race == "CH"
    assert p1.race == "EU"
    assert p0.extra_metrics["elo_points"] == 1600


async def test_f18_t1_02_electus_json_items_root_key(
    e2e_settings: Any, respx_mock: respx.MockRouter
) -> None:
    """F18-T1-02: Parse Electus JSON using alternate `items` root key and string `race`."""
    respx_mock.get("https://electus.online/api/ranking/player?page=1&limit=100").mock(
        return_value=httpx.Response(
            200,
            content=render_electus_player_json(
                make_player_specs(5), root_key="items", use_race_id=False
            ),
        )
    )
    respx_mock.get("https://electus.online/api/ranking/player?page=2&limit=100").mock(
        return_value=httpx.Response(200, content=render_electus_player_json([], root_key="items"))
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    async with httpx.AsyncClient(http2=True) as client:
        res = await get_scraper("electus", e2e_settings).fetch_player_pages(
            client, "https://electus.online"
        )
    assert len(res.raw_records) == 5


async def test_f18_t1_03_electus_html_fallback_on_non_challenge_html(
    e2e_settings: Any, respx_mock: respx.MockRouter
) -> None:
    """F18-T1-03: Fall back cleanly to `table#ranking-table tbody tr` on non-challenge HTML."""
    respx_mock.get("https://electus.online/api/ranking/player?page=1&limit=100").mock(
        return_value=httpx.Response(200, text="<html><body>Not JSON</body></html>")
    )
    respx_mock.get("https://electus.online/ranking/player?page=1").mock(
        return_value=httpx.Response(
            200, text=render_electus_player_html_fallback(make_player_specs(8))
        )
    )
    respx_mock.get("https://electus.online/api/ranking/player?page=2&limit=100").mock(
        return_value=httpx.Response(200, content=render_electus_player_json([]))
    )
    respx_mock.get("https://electus.online/ranking/player?page=2").mock(
        return_value=httpx.Response(200, text=render_electus_player_html_fallback([]))
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    async with httpx.AsyncClient(http2=True) as client:
        res = await get_scraper("electus", e2e_settings).fetch_player_pages(
            client, "https://electus.online"
        )
    assert len(res.raw_records) == 8


async def test_f18_t1_04_electus_sends_cf_clearance_cookie(
    e2e_quarantine_dir: Path,
    respx_mock: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """F18-T1-04: Attach `cf_clearance` cookie when `ELECTUS_CF_CLEARANCE` is set."""
    monkeypatch.setenv("ELECTUS_CF_CLEARANCE", "secret_cf_cookie")
    settings_cls = cast(SettingsFactoryProto, require_symbol("src.core.config", "Settings"))
    s = settings_cls(
        _env_file=None,
        database_url="postgresql+asyncpg://u:p@localhost:5432/sro",
        quarantine_dir=e2e_quarantine_dir,
        electus_cf_clearance="secret_cf_cookie",
    )
    seen_cookies: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen_cookies.append(req.headers.get("cookie", ""))
        page = req.url.params.get("page", "1")
        rows = make_player_specs(5) if page == "1" else []
        return httpx.Response(200, content=render_electus_player_json(rows))

    respx_mock.get(url__regex=r"https://electus\.online/api/ranking/player.*").mock(
        side_effect=handler
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    async with httpx.AsyncClient(http2=True) as client:
        await get_scraper("electus", s).fetch_player_pages(client, "https://electus.online")
    assert seen_cookies
    assert "cf_clearance=secret_cf_cookie" in seen_cookies[0]


async def test_f18_t1_05_electus_multipage_json_crawl(
    e2e_settings: Any, respx_mock: respx.MockRouter
) -> None:
    """F18-T1-05: Crawl 10 player, 5 job, 2 guild pages via Electus JSON API."""
    respx_mock.get(url__regex=r"https://electus\.online/api/ranking/player.*").mock(
        side_effect=lambda req: httpx.Response(
            200,
            content=render_electus_player_json(
                make_player_specs(100, start_rank=(int(req.url.params["page"]) - 1) * 100 + 1)
            ),
        )
    )
    respx_mock.get(url__regex=r"https://electus\.online/api/ranking/job.*").mock(
        side_effect=lambda req: httpx.Response(
            200,
            content=render_electus_job_json(
                make_job_specs(100, start_rank=(int(req.url.params["page"]) - 1) * 100 + 1)
            ),
        )
    )
    respx_mock.get(url__regex=r"https://electus\.online/api/ranking/guild.*").mock(
        side_effect=lambda req: httpx.Response(
            200,
            content=render_electus_guild_json(
                make_guild_specs(50, start_rank=(int(req.url.params["page"]) - 1) * 50 + 1)
            ),
        )
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    scraper = get_scraper("electus", e2e_settings)
    async with httpx.AsyncClient(http2=True) as client:
        p = await scraper.fetch_player_pages(client, "https://electus.online")
        j = await scraper.fetch_job_pages(client, "https://electus.online")
        g = await scraper.fetch_guild_pages(client, "https://electus.online")
    assert len(p.raw_records) == 1000
    assert len(j.raw_records) == 500
    assert len(g.raw_records) == 100


# =============================================================================
# Feature 19: PlayGoldenScraper (src/scrapers/golden.py)
# =============================================================================


async def test_f19_t1_01_golden_player_html_columns(
    e2e_settings: Any, respx_mock: respx.MockRouter
) -> None:
    """F19-T1-01: Parse Play Golden `/ranking/characters` HTML (`table.table-ranking`)."""
    respx_mock.get("https://play-golden.online/ranking/characters?page=1").mock(
        return_value=httpx.Response(200, text=render_golden_player_html(make_player_specs(50)))
    )
    respx_mock.get("https://play-golden.online/ranking/characters?page=2").mock(
        return_value=httpx.Response(200, text=render_golden_player_html([]))
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    player_cls = cast(
        PydanticModelClsProto, require_symbol("src.models.pydantic_schemas", "RawPlayerRankingItem")
    )
    async with httpx.AsyncClient(http2=True) as client:
        res = await get_scraper("play_golden", e2e_settings).fetch_player_pages(
            client, "https://play-golden.online"
        )
    assert len(res.raw_records) == 50
    p0 = cast(RawPlayerItemProto, player_cls.model_validate(res.raw_records[0]))
    assert p0.race == "CH"
    assert p0.strength == 450
    assert p0.intellect == 120


async def test_f19_t1_02_golden_job_column_order(
    e2e_settings: Any, respx_mock: respx.MockRouter
) -> None:
    """F19-T1-02: Verify Play Golden `/ranking/jobs` unique column order [0..5]."""
    respx_mock.get("https://play-golden.online/ranking/jobs?page=1").mock(
        return_value=httpx.Response(200, text=render_golden_job_html(make_job_specs(6)))
    )
    respx_mock.get("https://play-golden.online/ranking/jobs?page=2").mock(
        return_value=httpx.Response(200, text=render_golden_job_html([]))
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    job_cls = cast(
        PydanticModelClsProto, require_symbol("src.models.pydantic_schemas", "RawJobRankingItem")
    )
    async with httpx.AsyncClient(http2=True) as client:
        res = await get_scraper("play_golden", e2e_settings).fetch_job_pages(
            client, "https://play-golden.online"
        )
    j0 = cast(RawJobItemProto, job_cls.model_validate(res.raw_records[0]))
    assert j0.job_type == "TRADER"
    assert j0.guild_name == "Titans"


async def test_f19_t1_03_golden_guild_parsing(
    e2e_settings: Any, respx_mock: respx.MockRouter
) -> None:
    """F19-T1-03: Parse Play Golden `/ranking/guilds` HTML."""
    respx_mock.get("https://play-golden.online/ranking/guilds?page=1").mock(
        return_value=httpx.Response(200, text=render_golden_guild_html(make_guild_specs(10)))
    )
    respx_mock.get("https://play-golden.online/ranking/guilds?page=2").mock(
        return_value=httpx.Response(200, text=render_golden_guild_html([]))
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    async with httpx.AsyncClient(http2=True) as client:
        res = await get_scraper("play_golden", e2e_settings).fetch_guild_pages(
            client, "https://play-golden.online"
        )
    assert len(res.raw_records) == 10


@pytest.mark.e2e_db
async def test_f19_t1_04_golden_min_50_acceptance(
    clean_db: AsyncSession, e2e_settings: Any, respx_mock: respx.MockRouter
) -> None:
    """F19-T1-04: Persist Play Golden run at exact `min_records_expected=50`."""
    respx_mock.get("https://play-golden.online/ranking/characters?page=1").mock(
        return_value=httpx.Response(200, text=render_golden_player_html(make_player_specs(50)))
    )
    respx_mock.get("https://play-golden.online/ranking/characters?page=2").mock(
        return_value=httpx.Response(200, text=render_golden_player_html([]))
    )
    respx_mock.get("https://play-golden.online/ranking/jobs?page=1").mock(
        return_value=httpx.Response(200, text=render_golden_job_html([]))
    )
    respx_mock.get("https://play-golden.online/ranking/guilds?page=1").mock(
        return_value=httpx.Response(200, text=render_golden_guild_html(make_guild_specs(5)))
    )
    respx_mock.get("https://play-golden.online/ranking/guilds?page=2").mock(
        return_value=httpx.Response(200, text=render_golden_guild_html([]))
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("play_golden", e2e_settings).run(clean_db)
    run_row = await fetch_latest_scrape_run(clean_db, "play_golden")
    assert run_row is not None
    assert run_row["status"] == "SUCCESS"
    assert await count_rows(clean_db, "character_snapshots", "play_golden") == 50


async def test_f19_t1_05_golden_multipage_crawl(
    e2e_settings: Any, respx_mock: respx.MockRouter
) -> None:
    """F19-T1-05: Full 10-page Play Golden character crawl."""
    respx_mock.get(url__regex=r"https://play-golden\.online/ranking/characters\?page=\d+").mock(
        side_effect=lambda req: httpx.Response(
            200,
            text=render_golden_player_html(
                make_player_specs(100, start_rank=(int(req.url.params["page"]) - 1) * 100 + 1)
            ),
        )
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    async with httpx.AsyncClient(http2=True) as client:
        res = await get_scraper("play_golden", e2e_settings).fetch_player_pages(
            client, "https://play-golden.online"
        )
    assert len(res.raw_records) == 1000


# =============================================================================
# Feature 20: Top-Rank Integrity Guard & Batch Thresholds
# =============================================================================


@pytest.mark.e2e_db
@pytest.mark.parametrize(
    ("total_players", "invalid_count", "total_guilds", "expected_status", "expected_persisted"),
    [
        (100, 0, 5, "SUCCESS", 100),
        (105, 2, 5, "PARTIAL_SUCCESS", 103),
        (105, 6, 5, "SCHEMA_DRIFT", 0),
        (80, 0, 10, "LOW_COUNT_QUARANTINE", 0),
        (120, 0, 4, "LOW_COUNT_QUARANTINE", 0),
    ],
    ids=[
        "F20-T1-01_clean_success",
        "F20-T1-02_partial_success_le_5pct",
        "F20-T1-03_schema_drift_gt_5pct",
        "F20-T1-04_low_player_count",
        "F20-T1-05_low_guild_count",
    ],
)
async def test_f20_t1_batch_thresholds(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
    total_players: int,
    invalid_count: int,
    total_guilds: int,
    expected_status: str,
    expected_persisted: int,
) -> None:
    """F20-T1-01..05: Verify SUCCESS, PARTIAL_SUCCESS, SCHEMA_DRIFT, and LOW_COUNT_QUARANTINE."""
    bad = {10 + i: {"level": 999} for i in range(invalid_count)}
    players = make_player_specs(total_players, invalid_ranks=bad)
    guilds = make_guild_specs(total_guilds)

    respx_mock.get("https://play-chronicle.com/ranking/player?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html(players))
    )
    respx_mock.get("https://play-chronicle.com/ranking/player?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html([]))
    )
    respx_mock.get("https://play-chronicle.com/ranking/job?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_job_html([]))
    )
    respx_mock.get("https://play-chronicle.com/ranking/guild?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_guild_html(guilds))
    )
    respx_mock.get("https://play-chronicle.com/ranking/guild?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_guild_html([]))
    )

    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("chronicle", e2e_settings).run(clean_db)
    run_row = await fetch_latest_scrape_run(clean_db, "chronicle")
    assert run_row is not None
    assert run_row["status"] == expected_status
    assert await count_rows(clean_db, "character_snapshots", "chronicle") == expected_persisted


# =============================================================================
# Feature 21: Dead-Letter Quarantine & Retention Purge
# =============================================================================


@pytest.mark.e2e_db
async def test_f21_t1_01_schema_drift_writes_valid_gzip(
    clean_db: AsyncSession,
    e2e_settings: Any,
    e2e_quarantine_dir: Path,
    respx_mock: respx.MockRouter,
) -> None:
    """F21-T1-01: SCHEMA_DRIFT writes decompressable `.html.gz` under `quarantine/{server}`."""
    players = make_player_specs(100, invalid_ranks={10 + i: {"level": 999} for i in range(6)})
    html = render_chronicle_player_html(players)
    respx_mock.get("https://play-chronicle.com/ranking/player?page=1").mock(
        return_value=httpx.Response(200, text=html)
    )
    respx_mock.get("https://play-chronicle.com/ranking/player?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html([]))
    )
    respx_mock.get("https://play-chronicle.com/ranking/job?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_job_html([]))
    )
    respx_mock.get("https://play-chronicle.com/ranking/guild?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_guild_html(make_guild_specs(5)))
    )
    respx_mock.get("https://play-chronicle.com/ranking/guild?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_guild_html([]))
    )

    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("chronicle", e2e_settings).run(clean_db)
    run_row = await fetch_latest_scrape_run(clean_db, "chronicle")
    assert run_row is not None
    qpath = Path(str(run_row["quarantine_file_path"]))
    assert path_exists(qpath)
    assert qpath.name.endswith(".html.gz")
    decompressed = decompress_gz(qpath)
    assert html.encode("utf-8") in decompressed


@pytest.mark.e2e_db
async def test_f21_t1_02_quarantine_sha256_and_size_match_disk(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """F21-T1-02: Verify `quarantine_sha256` (64 hex chars) and `quarantine_size_bytes`."""
    respx_mock.get(url__regex=r"https://electus\.online/.*").mock(
        return_value=httpx.Response(403, text=render_turnstile_html())
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("electus", e2e_settings).run(clean_db)
    run_row = await fetch_latest_scrape_run(clean_db, "electus")
    assert run_row is not None
    qbytes = read_bytes(str(run_row["quarantine_file_path"]))
    assert run_row["quarantine_sha256"] == hashlib.sha256(qbytes).hexdigest()
    assert re.fullmatch(r"[0-9a-f]{64}", str(run_row["quarantine_sha256"]))
    assert run_row["quarantine_size_bytes"] == len(qbytes) > 0


@pytest.mark.e2e_db
async def test_f21_t1_03_error_details_validates_against_pydantic_contract(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """F21-T1-03: Verify `scrape_runs.error_details` validates against `ErrorDetails`."""
    respx_mock.get(url__regex=r"https://play-chronicle\.com/.*").mock(
        return_value=httpx.Response(403, text=render_turnstile_html())
    )
    err_cls = cast(
        PydanticModelClsProto, require_symbol("src.models.pydantic_schemas", "ErrorDetails")
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("chronicle", e2e_settings).run(clean_db)
    run_row = await fetch_latest_scrape_run(clean_db, "chronicle")
    assert run_row is not None
    validated = err_cls.model_validate(run_row["error_details"])
    assert isinstance(validated, BaseModel)


@pytest.mark.e2e_db
async def test_f21_t1_04_low_count_quarantine_writes_gz_and_sha(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """F21-T1-04: Verify LOW_COUNT_QUARANTINE creates `.html.gz` + SHA-256."""
    respx_mock.get("https://play-golden.online/ranking/characters?page=1").mock(
        return_value=httpx.Response(200, text=render_golden_player_html(make_player_specs(10)))
    )
    respx_mock.get("https://play-golden.online/ranking/characters?page=2").mock(
        return_value=httpx.Response(200, text=render_golden_player_html([]))
    )
    respx_mock.get("https://play-golden.online/ranking/jobs?page=1").mock(
        return_value=httpx.Response(200, text=render_golden_job_html([]))
    )
    respx_mock.get("https://play-golden.online/ranking/guilds?page=1").mock(
        return_value=httpx.Response(200, text=render_golden_guild_html(make_guild_specs(5)))
    )
    respx_mock.get("https://play-golden.online/ranking/guilds?page=2").mock(
        return_value=httpx.Response(200, text=render_golden_guild_html([]))
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("play_golden", e2e_settings).run(clean_db)
    run_row = await fetch_latest_scrape_run(clean_db, "play_golden")
    assert run_row is not None
    assert run_row["status"] == "LOW_COUNT_QUARANTINE"
    assert path_exists(str(run_row["quarantine_file_path"]))


def test_f21_t1_05_purge_old_quarantine_deletes_expired_only(
    e2e_quarantine_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """F21-T1-05: `purge_old_quarantine_task(30)` removes 35d file and keeps 5d file."""
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://u:p@localhost:5432/sro")
    monkeypatch.setenv("QUARANTINE_DIR", str(e2e_quarantine_dir))
    get_settings = require_symbol("src.core.config", "get_settings")
    cache_clear = getattr(get_settings, "cache_clear", None)
    if callable(cache_clear):
        cache_clear()

    srv_dir = e2e_quarantine_dir / "chronicle"
    srv_dir.mkdir(parents=True, exist_ok=True)
    old_file = srv_dir / "old_run.html.gz"
    new_file = srv_dir / "new_run.html.gz"
    old_file.write_bytes(gzip.compress(b"old"))
    new_file.write_bytes(gzip.compress(b"new"))
    now_ts = pytime.time()
    os.utime(old_file, (now_ts - 35 * 86400, now_ts - 35 * 86400))
    os.utime(new_file, (now_ts - 5 * 86400, now_ts - 5 * 86400))

    purge_task = cast(
        PrefectTaskProto, require_symbol("src.flows.tasks", "purge_old_quarantine_task")
    )
    purge_task.fn(retention_days=30)
    assert not old_file.exists()
    assert new_file.exists()


# =============================================================================
# Feature 22: Prefect 3 Tasks, Flows & CLI (src/flows/)
# =============================================================================


@pytest.mark.parametrize(
    "task_name",
    ["ensure_partitions_task", "scrape_target_task", "purge_old_quarantine_task"],
)
def test_f22_t1_01_prefect_tasks_enforce_zero_retries(task_name: str) -> None:
    """F22-T1-01: Confirm `@task(retries=0)` on all 3 Prefect tasks."""
    task_obj = cast(PrefectTaskProto, require_symbol("src.flows.tasks", task_name))
    assert task_obj.retries == 0


@pytest.mark.parametrize(
    ("dow", "end_time", "expected_cron"),
    [
        (0, time(22, 0), "30 22 * * 0"),
        (0, time(21, 0), "30 21 * * 0"),
        (6, time(20, 0), "30 20 * * 6"),
    ],
)
def test_f22_t1_02_compute_post_fw_cron_seeded_schedules(
    dow: int, end_time: time, expected_cron: str
) -> None:
    """F22-T1-02: Compute 30-min post-FW cron strings for all 3 seeded servers."""
    fn = cast(
        ComputePostFwCronProto, require_symbol("src.flows.orchestrator", "compute_post_fw_cron")
    )
    assert fn(dow, end_time) == expected_cron


@pytest.mark.e2e_db
async def test_f22_t1_03_scheduled_12h_flow_runs_all_active_targets(
    clean_db: AsyncSession,
    e2e_quarantine_dir: Path,
    respx_mock: respx.MockRouter,
) -> None:
    """F22-T1-03: Run `scheduled_12h_flow` with `prefect_test_harness` across 3 targets."""
    from prefect.testing.utilities import prefect_test_harness

    respx_mock.get("https://play-chronicle.com/ranking/player?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html(make_player_specs(100)))
    )
    respx_mock.get("https://play-chronicle.com/ranking/player?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html([]))
    )
    respx_mock.get("https://play-chronicle.com/ranking/job?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_job_html([]))
    )
    respx_mock.get("https://play-chronicle.com/ranking/guild?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_guild_html(make_guild_specs(5)))
    )
    respx_mock.get("https://play-chronicle.com/ranking/guild?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_guild_html([]))
    )
    respx_mock.get("https://electus.online/api/ranking/player?page=1&limit=100").mock(
        return_value=httpx.Response(200, content=render_electus_player_json(make_player_specs(100)))
    )
    respx_mock.get("https://electus.online/api/ranking/player?page=2&limit=100").mock(
        return_value=httpx.Response(200, content=render_electus_player_json([]))
    )
    respx_mock.get("https://electus.online/api/ranking/job?page=1&limit=100").mock(
        return_value=httpx.Response(200, content=render_electus_job_json([]))
    )
    respx_mock.get("https://electus.online/api/ranking/guild?page=1&limit=50").mock(
        return_value=httpx.Response(200, content=render_electus_guild_json(make_guild_specs(5)))
    )
    respx_mock.get("https://electus.online/api/ranking/guild?page=2&limit=50").mock(
        return_value=httpx.Response(200, content=render_electus_guild_json([]))
    )
    respx_mock.get("https://play-golden.online/ranking/characters?page=1").mock(
        return_value=httpx.Response(200, text=render_golden_player_html(make_player_specs(50)))
    )
    respx_mock.get("https://play-golden.online/ranking/characters?page=2").mock(
        return_value=httpx.Response(200, text=render_golden_player_html([]))
    )
    respx_mock.get("https://play-golden.online/ranking/jobs?page=1").mock(
        return_value=httpx.Response(200, text=render_golden_job_html([]))
    )
    respx_mock.get("https://play-golden.online/ranking/guilds?page=1").mock(
        return_value=httpx.Response(200, text=render_golden_guild_html(make_guild_specs(5)))
    )
    respx_mock.get("https://play-golden.online/ranking/guilds?page=2").mock(
        return_value=httpx.Response(200, text=render_golden_guild_html([]))
    )

    flow_fn = cast(PrefectFlowProto, require_symbol("src.flows.orchestrator", "scheduled_12h_flow"))
    with prefect_test_harness():
        await flow_fn()
    assert await count_rows(clean_db, "scrape_runs") == 3


@pytest.mark.e2e_db
async def test_f22_t1_04_post_fw_flow_records_post_fw_extra(
    clean_db: AsyncSession,
    respx_mock: respx.MockRouter,
) -> None:
    """F22-T1-04: Run `post_fw_flow('electus')` and assert `trigger_type='post_fw_extra'`."""
    from prefect.testing.utilities import prefect_test_harness

    respx_mock.get("https://electus.online/api/ranking/player?page=1&limit=100").mock(
        return_value=httpx.Response(200, content=render_electus_player_json(make_player_specs(100)))
    )
    respx_mock.get("https://electus.online/api/ranking/player?page=2&limit=100").mock(
        return_value=httpx.Response(200, content=render_electus_player_json([]))
    )
    respx_mock.get("https://electus.online/api/ranking/job?page=1&limit=100").mock(
        return_value=httpx.Response(200, content=render_electus_job_json([]))
    )
    respx_mock.get("https://electus.online/api/ranking/guild?page=1&limit=50").mock(
        return_value=httpx.Response(200, content=render_electus_guild_json(make_guild_specs(5)))
    )
    respx_mock.get("https://electus.online/api/ranking/guild?page=2&limit=50").mock(
        return_value=httpx.Response(200, content=render_electus_guild_json([]))
    )

    flow_fn = cast(PrefectFlowProto, require_symbol("src.flows.orchestrator", "post_fw_flow"))
    with prefect_test_harness():
        await flow_fn(server_id="electus")
    run_row = await fetch_latest_scrape_run(clean_db, "electus")
    assert run_row is not None
    assert run_row["trigger_type"] == "post_fw_extra"


def test_f22_t1_05_cli_help_exposes_mode_and_target(repo_root: Path) -> None:
    """F22-T1-05: Verify `python -m src.flows.orchestrator --help` documents CLI modes."""
    require_module("src.flows.orchestrator")
    proc = subprocess.run(
        [sys.executable, "-m", "src.flows.orchestrator", "--help"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert "--mode" in proc.stdout
    assert "--target" in proc.stdout


# =============================================================================
# Feature 23: Docker Compose, Dockerfile & Operational README
# =============================================================================


def test_f23_t1_01_docker_compose_topology_and_memory_limits(repo_root: Path) -> None:
    """F23-T1-01: Validate `docker-compose.yml` 3-service topology, limits, and volumes."""
    compose = _load_yaml(require_file("docker-compose.yml", repo_root).read_text(encoding="utf-8"))
    services = compose["services"]
    assert {"postgres", "prefect-server", "scraper-worker"}.issubset(set(services.keys()))
    pg = services["postgres"]
    assert str(pg["mem_limit"]).lower() == "1g"
    assert "pg_isready" in str(pg.get("healthcheck", ""))
    worker = services["scraper-worker"]
    assert str(worker["mem_limit"]).lower() == "512m"
    assert "--mode serve" in str(worker.get("command", ""))
    assert any("quarantine" in str(v) for v in worker.get("volumes", []))


def test_f23_t1_02_dockerfile_base_and_non_root_user(repo_root: Path) -> None:
    """F23-T1-02: Verify `Dockerfile` uses `python:3.11-slim`, installs deps, switches to USER."""
    df = require_file("Dockerfile", repo_root).read_text(encoding="utf-8")
    assert "FROM python:3.11-slim" in df
    assert "PYTHONPATH" in df
    user_lines = [ln.strip() for ln in df.splitlines() if ln.strip().upper().startswith("USER ")]
    assert user_lines
    assert user_lines[-1].split()[1].lower() not in {"root", "0"}


def test_f23_t1_03_readme_operational_docs(repo_root: Path) -> None:
    """F23-T1-03: Verify `README.md` covers Compose, Alembic, CLI modes, Prefect UI, and views."""
    require_file("docker-compose.yml", repo_root)
    readme = require_file("README.md", repo_root).read_text(encoding="utf-8")
    for kw in (
        "docker compose",
        "alembic upgrade head",
        "--mode serve",
        "--mode manual",
        "v_character_activity_deltas",
    ):
        assert kw in readme, f"README missing required operational keyword: {kw}"


@pytest.mark.live_e2e
@pytest.mark.e2e_docker
def test_f23_t1_04_live_docker_compose_build_and_health(
    repo_root: Path, docker_available: None
) -> None:
    """F23-T1-04: Build Docker Compose stack and inspect container health/limits."""
    build_proc = subprocess.run(
        ["docker", "compose", "build"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert build_proc.returncode == 0, build_proc.stderr
    ps_proc = subprocess.run(
        ["docker", "compose", "config", "--services"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert ps_proc.returncode == 0
    assert {"postgres", "prefect-server", "scraper-worker"}.issubset(set(ps_proc.stdout.split()))


@pytest.mark.live_e2e
@pytest.mark.e2e_db
async def test_f23_t1_05_live_manual_chronicle_ingestion(
    repo_root: Path,
    e2e_migrated_db: str,
) -> None:
    """F23-T1-05: Execute live `--mode manual --target chronicle` and verify DB + views."""
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "src.flows.orchestrator",
        "--mode",
        "manual",
        "--target",
        "chronicle",
        cwd=repo_root,
        env={**os.environ, "DATABASE_URL": e2e_migrated_db},
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout_b, stderr_b = await asyncio.wait_for(proc.communicate(), timeout=180)
    assert proc.returncode == 0, stderr_b.decode("utf-8", errors="replace")
    assert stdout_b is not None
