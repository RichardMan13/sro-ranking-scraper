"""Shared pytest fixtures and environment gates for opaque-box E2E tests."""

from __future__ import annotations

import functools
import inspect
import os
import socket
import subprocess
import sys
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import cast

import psycopg
import pytest
import pytest_asyncio
import respx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from tests.e2e._contracts import SettingsFactoryProto, SettingsProto
from tests.e2e._loader import get_repo_root, require_file, require_module, require_symbol

DEFAULT_E2E_DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/sro_e2e_test"


def pytest_configure(config: pytest.Config) -> None:
    """Register E2E markers programmatically so pyproject.toml stays untouched."""
    markers = [
        "e2e: Opaque-box end-to-end test",
        "e2e_tier1: Tier 1 isolated feature tests",
        "e2e_tier2: Tier 2 boundary, corner, and negative tests",
        "e2e_tier3: Tier 3 pairwise cross-feature combination tests",
        "e2e_tier4: Tier 4 real-world application scenario tests",
        "e2e_db: Requires live PostgreSQL 16 database",
        "e2e_docker: Requires Docker daemon availability",
        "live_e2e: Requires SRO_RUN_LIVE_E2E=1 (live stack/network/full suite)",
    ]
    for marker in markers:
        config.addinivalue_line("markers", marker)


@functools.lru_cache(maxsize=1)
def _loopback_sockets_allowed() -> tuple[bool, str]:
    """Probe whether `127.0.0.1` loopback TCP sockets work (<1ms).

    On Python 3.11 Windows without AF_UNIX, `asyncio.new_event_loop()` calls
    `socket.socketpair()` -> `_fallback_socketpair()` -> `lsock.accept()`, which
    hangs indefinitely if the execution sandbox blocks `127.0.0.1` (WinError 10013).
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        addr = sock.getsockname()
        conn.settimeout(0.5)
        conn.connect(addr)
        accepted, _ = sock.accept()
        accepted.close()
        return True, ""
    except OSError as exc:
        return False, str(exc)
    finally:
        conn.close()
        sock.close()


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item: pytest.Item) -> None:
    """Gate `live_e2e` early and prevent `asyncio.new_event_loop()` sandbox deadlock."""
    if "tests\\e2e" not in str(item.path) and "tests/e2e" not in str(item.path):
        return
    if item.get_closest_marker("live_e2e") is not None and os.getenv("SRO_RUN_LIVE_E2E") != "1":
        pytest.skip("Live E2E test skipped (set SRO_RUN_LIVE_E2E=1 to execute).")
    obj = getattr(item, "obj", None)
    is_async = inspect.iscoroutinefunction(obj) or item.get_closest_marker("asyncio") is not None
    if not is_async:
        return
    allowed, reason = _loopback_sockets_allowed()
    if not allowed:
        if os.getenv("SRO_RUN_LIVE_E2E") == "1":
            pytest.fail(f"Loopback sockets blocked under SRO_RUN_LIVE_E2E=1: {reason}")
        pytest.skip(
            "Sandbox blocks 127.0.0.1 loopback sockets required by Windows asyncio "
            f"socketpair(): {reason}"
        )


def _to_psycopg_dsn(async_url: str) -> str:
    """Convert `postgresql+asyncpg://` URL to `postgresql://` for psycopg probe."""
    if async_url.startswith("postgresql+asyncpg://"):
        return "postgresql://" + async_url.removeprefix("postgresql+asyncpg://")
    return async_url


def _clear_settings_cache_if_present() -> None:
    """Clear `get_settings` lru_cache if implemented by `src.core.config`."""
    try:
        mod = require_module("src.core.config")
    except pytest.skip.Exception:
        return
    get_settings = getattr(mod, "get_settings", None)
    cache_clear = getattr(get_settings, "cache_clear", None)
    if callable(cache_clear):
        cache_clear()


@pytest.fixture(autouse=True)
def _gate_live_e2e_marker(request: pytest.FixtureRequest) -> None:
    """Automatically skip `@pytest.mark.live_e2e` tests unless `SRO_RUN_LIVE_E2E=1`."""
    if (
        request.node.get_closest_marker("live_e2e") is not None
        and os.getenv("SRO_RUN_LIVE_E2E") != "1"
    ):
        pytest.skip("Live E2E test skipped (set SRO_RUN_LIVE_E2E=1 to execute).")


@pytest.fixture
def repo_root() -> Path:
    """Return repository root Path."""
    return get_repo_root()


@pytest.fixture
def e2e_quarantine_dir(tmp_path: Path) -> Path:
    """Return isolated temporary quarantine directory."""
    qdir = tmp_path / "quarantine"
    qdir.mkdir(parents=True, exist_ok=True)
    return qdir


@pytest.fixture
def e2e_settings(
    e2e_quarantine_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> SettingsProto:
    """Build isolated `Settings` pointing at `e2e_quarantine_dir` and E2E DB URL."""
    db_url = os.getenv("E2E_DATABASE_URL", DEFAULT_E2E_DATABASE_URL)
    monkeypatch.setenv("DATABASE_URL", db_url)
    monkeypatch.setenv("QUARANTINE_DIR", str(e2e_quarantine_dir))
    monkeypatch.delenv("ELECTUS_CF_CLEARANCE", raising=False)
    _clear_settings_cache_if_present()

    settings_cls = cast(
        SettingsFactoryProto,
        require_symbol("src.core.config", "Settings"),
    )
    settings = settings_cls(
        _env_file=None,
        database_url=db_url,
        quarantine_dir=e2e_quarantine_dir,
    )
    return settings


@pytest.fixture
def respx_mock() -> Iterator[respx.MockRouter]:
    """Provide strict `respx` HTTPX transport mock allowing local Prefect API pass-through."""
    with respx.mock(assert_all_called=False, assert_all_mocked=True) as router:
        router.route(host="127.0.0.1").pass_through()
        router.route(host="localhost").pass_through()
        yield router


@pytest.fixture
def docker_available() -> None:
    """Probe Docker daemon reachability; skip gracefully unless `SRO_RUN_LIVE_E2E=1`."""
    try:
        proc = subprocess.run(
            ["docker", "info", "--format", "{{.ServerVersion}}"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (FileNotFoundError, PermissionError, subprocess.TimeoutExpired) as exc:
        if os.getenv("SRO_RUN_LIVE_E2E") == "1":
            pytest.fail(f"Docker daemon probe failed under SRO_RUN_LIVE_E2E=1: {exc}")
        pytest.skip(f"Docker daemon unavailable: {exc}")
        return
    if proc.returncode != 0:
        msg = proc.stderr.strip() or proc.stdout.strip() or f"exit {proc.returncode}"
        if os.getenv("SRO_RUN_LIVE_E2E") == "1":
            pytest.fail(f"Docker daemon unavailable under SRO_RUN_LIVE_E2E=1: {msg}")
        pytest.skip(f"Docker daemon unavailable: {msg}")


@pytest.fixture(scope="session")
def e2e_migrated_db() -> str:
    """Probe PG16 reachability and run `alembic upgrade head` once per test session."""
    root = get_repo_root()
    require_file("alembic.ini", root)
    require_file("alembic/env.py", root)
    require_module("src.models.db_models")

    db_url = os.getenv("E2E_DATABASE_URL", DEFAULT_E2E_DATABASE_URL)
    sync_dsn = _to_psycopg_dsn(db_url)
    try:
        with psycopg.connect(sync_dsn, connect_timeout=2) as conn:
            conn.execute("SELECT 1")
    except Exception as exc:
        if os.getenv("E2E_REQUIRE_DB") == "1":
            pytest.fail(f"Required E2E PostgreSQL unreachable at {db_url}: {exc}")
        pytest.skip(f"E2E PostgreSQL unreachable (set E2E_REQUIRE_DB=1 to require): {exc}")

    env = dict(os.environ)
    env["DATABASE_URL"] = db_url
    proc = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    if proc.returncode != 0:
        pytest.fail(
            f"`alembic upgrade head` failed (exit {proc.returncode}):\n"
            f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )
    return db_url


async def _reset_database_state(engine: AsyncEngine) -> None:
    """Truncate fact/dimension/run tables, restore 3 server seeds, drop custom partitions."""
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "TRUNCATE TABLE character_snapshots, guild_snapshots, "
                "scrape_runs, characters, guilds RESTART IDENTITY CASCADE"
            )
        )
        await conn.execute(
            text(
                """
                INSERT INTO servers (
                    id, display_name, base_url, current_season, is_active,
                    fw_day_of_week, fw_end_time_utc, min_records_expected,
                    circuit_breaker_until
                ) VALUES
                    ('chronicle', 'Chronicle SRO', 'https://play-chronicle.com', 'v1', TRUE,
                     0, '22:00:00', 100, NULL),
                    ('electus', 'Electus Online', 'https://electus.online', 'v1', TRUE,
                     0, '21:00:00', 100, NULL),
                    ('play_golden', 'Play Golden', 'https://play-golden.online', 'v1', TRUE,
                     6, '20:00:00', 50, NULL)
                ON CONFLICT (id) DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    base_url = EXCLUDED.base_url,
                    current_season = EXCLUDED.current_season,
                    is_active = EXCLUDED.is_active,
                    fw_day_of_week = EXCLUDED.fw_day_of_week,
                    fw_end_time_utc = EXCLUDED.fw_end_time_utc,
                    min_records_expected = EXCLUDED.min_records_expected,
                    circuit_breaker_until = NULL,
                    updated_at = CURRENT_TIMESTAMP
                """
            )
        )
        await conn.execute(
            text("DELETE FROM servers WHERE id NOT IN ('chronicle', 'electus', 'play_golden')")
        )

        # Drop dynamically created partitions beyond seed partitions (2026_11, 2026_12, default)
        res = await conn.execute(
            text(
                """
                SELECT child.relname
                FROM pg_inherits
                JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
                JOIN pg_class child ON pg_inherits.inhrelid = child.oid
                WHERE parent.relname IN ('character_snapshots', 'guild_snapshots')
                """
            )
        )
        keep_suffixes = ("_2026_11", "_2026_12", "_default")
        for (relname,) in res.all():
            name = str(relname)
            if not name.endswith(keep_suffixes) and name.replace("_", "").isalnum():
                await conn.execute(text(f"DROP TABLE IF EXISTS {name}"))


@pytest_asyncio.fixture(loop_scope="function")
async def clean_db(
    e2e_migrated_db: str,
    e2e_quarantine_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[AsyncSession]:
    """Provide a clean committed PostgreSQL 16 AsyncSession per test function."""
    monkeypatch.setenv("DATABASE_URL", e2e_migrated_db)
    monkeypatch.setenv("QUARANTINE_DIR", str(e2e_quarantine_dir))
    _clear_settings_cache_if_present()

    engine = create_async_engine(e2e_migrated_db, pool_pre_ping=True)
    await _reset_database_state(engine)
    session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
        engine, expire_on_commit=False
    )
    try:
        async with session_factory() as session:
            yield session
    finally:
        await _reset_database_state(engine)
        await engine.dispose()
        _clear_settings_cache_if_present()
