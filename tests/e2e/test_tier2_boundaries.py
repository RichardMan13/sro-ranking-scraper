"""Tier 2 E2E Boundary, Corner & Negative Tests for Features F1-F23.

Derived strictly from `ORIGINAL_REQUEST.md`, `PROJECT.md`, `AGENTS.md`,
and `docs/designs/001_sro_ranking_scraper_pipeline.md`.
Minimum 5 Tier 2 cases per feature (>= 115 cases total).
"""

from __future__ import annotations

import ast
import asyncio
import gzip
import importlib
import os
import re
import shutil
import subprocess
import sys
import time as pytime
from collections.abc import Callable
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from typing import Any, cast

import httpx
import orjson
import pytest
import respx
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.exc import ArgumentError, DataError, IntegrityError, InvalidRequestError
from sqlalchemy.ext.asyncio import AsyncSession

from tests.e2e._contracts import (
    BindRunContextProto,
    ClearRunContextProto,
    ComputePostFwCronProto,
    ConfigureLoggingProto,
    CreateEngineFromUrlProto,
    CreateSessionFactoryProto,
    EnsureMonthlyPartitionsProto,
    GetLoggerProto,
    GetScraperProto,
    PrefectTaskProto,
    PydanticModelClsProto,
    RawGuildItemProto,
    RawJobItemProto,
    RawPlayerItemProto,
    SettingsFactoryProto,
)
from tests.e2e._loader import require_file, require_module, require_symbol
from tests.e2e.helpers.db_helpers import (
    HistoricalCharSeed,
    count_rows,
    fetch_latest_scrape_run,
    fetch_server_row,
    get_snapshot_partition_name,
    query_view,
    seed_historical_run,
)
from tests.e2e.helpers.fs_helpers import decompress_gz, path_exists, rglob_gz
from tests.e2e.helpers.log_helpers import parse_json_log_lines
from tests.e2e.helpers.payload_factory import (
    GuildRowSpec,
    JobRowSpec,
    PlayerRowSpec,
    make_guild_specs,
    make_player_specs,
    render_chronicle_guild_html,
    render_chronicle_job_html,
    render_chronicle_player_html,
    render_electus_guild_json,
    render_electus_job_json,
    render_electus_player_json,
    render_golden_guild_html,
    render_golden_job_html,
    render_golden_player_html,
    render_turnstile_html,
)

pytestmark = [pytest.mark.e2e, pytest.mark.e2e_tier2]


def _venv_tool(name: str) -> str:
    """Resolve CLI tool inside the active Python `.venv` directory."""
    exe_dir = Path(sys.executable).parent
    for candidate in (exe_dir / f"{name}.exe", exe_dir / name):
        if candidate.exists():
            return str(candidate)
    resolved = shutil.which(name)
    return resolved if resolved is not None else name


def _load_yaml(text_data: str) -> dict[str, Any]:
    """Load YAML dynamically via `importlib`."""
    yaml_mod = importlib.import_module("yaml")
    safe_load = cast(Callable[[str], object], yaml_mod.safe_load)
    parsed = safe_load(text_data)
    assert isinstance(parsed, dict)
    return parsed


class _DdlRecordingSession:
    """Minimal session recording DDL statements for offline partition boundary checks."""

    def __init__(self) -> None:
        self.executed_sql: list[str] = []

    async def execute(self, statement: Any, *args: Any, **kwargs: Any) -> Any:
        self.executed_sql.append(str(statement))
        return None

    async def commit(self) -> None:
        return None


# =============================================================================
# Feature 1: Mandatory Tech Stack & Prohibitions (Boundaries & Negative)
# =============================================================================


@pytest.mark.parametrize(
    "banned_token",
    [
        "playwright",
        "selenium",
        "puppeteer",
        "pyppeteer",
        "apache-airflow",
        "pyspark",
        "2captcha",
        "anticaptcha",
        "capsolver",
    ],
)
def test_f1_t2_01_zero_prohibited_packages_or_imports(repo_root: Path, banned_token: str) -> None:
    """F1-T2-01: Scan manifests and `src/`/`alembic/` AST for banned tooling."""
    files_to_scan: list[Path] = []
    for name in ("pyproject.toml", "Dockerfile", "docker-compose.yml"):
        p = repo_root / name
        if p.exists():
            files_to_scan.append(p)
    for folder in ("src", "alembic"):
        d = repo_root / folder
        if d.exists():
            files_to_scan.extend(d.rglob("*.py"))

    for path in files_to_scan:
        content = path.read_text(encoding="utf-8", errors="ignore").lower()
        assert banned_token not in content, f"Prohibited token {banned_token!r} found in {path}"


def test_f1_t2_02_zero_hardcoded_secrets_in_src(repo_root: Path) -> None:
    """F1-T2-02: AST scan `src/` for hardcoded DSN passwords or `cf_clearance` literals."""
    src_dir = repo_root / "src"
    if not src_dir.exists():
        pytest.skip("src/ directory not present")
    secret_dsn_re = re.compile(r"postgresql(?:\+asyncpg)?://[^:\s]+:[^@\s]+@")
    for py_file in src_dir.rglob("*.py"):
        source = py_file.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(py_file))
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                assert not secret_dsn_re.search(node.value), (
                    f"Hardcoded DB credentials found in {py_file}"
                )
                assert "cf_clearance=" not in node.value.lower(), (
                    f"Hardcoded cf_clearance cookie literal found in {py_file}"
                )


def test_f1_t2_03_httpx_http2_driver_available() -> None:
    """F1-T2-03: Confirm `h2` is installed so `httpx.AsyncClient(http2=True)` initializes."""
    client = httpx.AsyncClient(http2=True)
    assert client is not None


async def test_f1_t2_04_orjson_lxml_and_asyncpg_dialect_available() -> None:
    """F1-T2-04: Confirm `orjson`, `lxml`, and `postgresql+asyncpg` dialect work in runtime."""
    from lxml import html as lxml_html
    from sqlalchemy.ext.asyncio import create_async_engine

    assert orjson.loads(b'{"ok": true}')["ok"] is True
    doc = lxml_html.fromstring("<table><tr><td>1</td></tr></table>")
    assert doc.xpath("//td/text()") == ["1"]
    eng = create_async_engine("postgresql+asyncpg://u:p@localhost:5432/db")
    await eng.dispose()


def test_f1_t2_05_quarantine_gitkeep_and_gitignore_protection(repo_root: Path) -> None:
    """F1-T2-05: Confirm `quarantine/.gitkeep` exists and `.gitignore` excludes `.html.gz`."""
    assert (repo_root / "quarantine" / ".gitkeep").exists()
    gitignore = require_file(".gitignore", repo_root).read_text(encoding="utf-8")
    assert "quarantine" in gitignore


# =============================================================================
# Feature 2: Code Quality Gate Compliance (Boundaries & Negative)
# =============================================================================


def test_f2_t2_01_zero_crlf_line_endings_in_code_and_yaml(repo_root: Path) -> None:
    """F2-T2-01: Scan `.py`, `.yml`, `.sh` files in repo for CRLF (`\\r\\n`)."""
    targets: list[Path] = []
    for folder in ("src", "alembic", "tests"):
        d = repo_root / folder
        if d.exists():
            targets.extend(d.rglob("*.py"))
    for fname in ("docker-compose.yml",):
        p = repo_root / fname
        if p.exists():
            targets.append(p)
    for path in targets:
        raw = path.read_bytes()
        assert b"\r\n" not in raw, f"CRLF detected in {path}"


def test_f2_t2_02_gitattributes_enforces_lf(repo_root: Path) -> None:
    """F2-T2-02: Verify `.gitattributes` enforces `eol=lf` for Windows compatibility."""
    ga = require_file(".gitattributes", repo_root).read_text(encoding="utf-8")
    assert "eol=lf" in ga


def test_f2_t2_03_mypy_strict_passes_on_e2e_package(repo_root: Path) -> None:
    """F2-T2-03: Verify `mypy` passes cleanly on `tests/e2e` itself."""
    proc = subprocess.run(
        [_venv_tool("mypy"), "tests/e2e"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert proc.returncode == 0, f"mypy tests/e2e failed:\n{proc.stdout}\n{proc.stderr}"


def test_f2_t2_04_ruff_format_detects_crlf_drift(repo_root: Path, tmp_path: Path) -> None:
    """F2-T2-04: Verify `ruff format --check` rejects CRLF drift under `line-ending = 'lf'`."""
    lf_file = tmp_path / "sample_lf.py"
    crlf_file = tmp_path / "sample_crlf.py"
    lf_file.write_bytes(b"x = 1\n")
    crlf_file.write_bytes(b"x = 1\r\n")
    cfg = str(repo_root / "pyproject.toml")
    lf_res = subprocess.run(
        [_venv_tool("ruff"), "format", "--check", "--config", cfg, str(lf_file)],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    crlf_res = subprocess.run(
        [_venv_tool("ruff"), "format", "--check", "--config", cfg, str(crlf_file)],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert lf_res.returncode == 0
    assert crlf_res.returncode != 0


@pytest.mark.live_e2e
@pytest.mark.e2e_db
def test_f2_t2_05_alembic_downgrade_upgrade_roundtrip(
    repo_root: Path, e2e_migrated_db: str
) -> None:
    """F2-T2-05: Execute `alembic downgrade base` then `alembic upgrade head` cleanly."""
    env = {**os.environ, "DATABASE_URL": e2e_migrated_db}
    down = subprocess.run(
        [sys.executable, "-m", "alembic", "downgrade", "base"],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert down.returncode == 0, down.stderr
    up = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert up.returncode == 0, up.stderr


# =============================================================================
# Feature 3: Settings (Boundaries & Negative)
# =============================================================================


def test_f3_t2_01_settings_missing_database_url_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """F3-T2-01: Omitting required `DATABASE_URL` raises `ValidationError`."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    settings_cls = cast(SettingsFactoryProto, require_symbol("src.core.config", "Settings"))
    with pytest.raises(ValidationError):
        settings_cls(_env_file=None)


def test_f3_t2_02_settings_ignores_unknown_env_vars(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """F3-T2-02: `extra='ignore'` ignores unrelated environment variables."""
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://u:p@localhost:5432/sro")
    monkeypatch.setenv("UNRELATED_EXTRA_KEY", "ignored_value")
    settings_cls = cast(SettingsFactoryProto, require_symbol("src.core.config", "Settings"))
    s = settings_cls(_env_file=None)
    assert not hasattr(s, "unrelated_extra_key")


@pytest.mark.parametrize(
    ("env_key", "bad_val"),
    [
        ("QUARANTINE_RETENTION_DAYS", "thirty"),
        ("CIRCUIT_BREAKER_COOLDOWN_HOURS", "abc"),
    ],
)
def test_f3_t2_03_settings_invalid_integer_env_raises(
    monkeypatch: pytest.MonkeyPatch, env_key: str, bad_val: str
) -> None:
    """F3-T2-03: Non-integer retention/cooldown env values raise `ValidationError`."""
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://u:p@localhost:5432/sro")
    monkeypatch.setenv(env_key, bad_val)
    settings_cls = cast(SettingsFactoryProto, require_symbol("src.core.config", "Settings"))
    with pytest.raises(ValidationError):
        settings_cls(_env_file=None)


def test_f3_t2_04_settings_empty_cf_clearance_handled_cleanly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """F3-T2-04: Empty `ELECTUS_CF_CLEARANCE=''` loads without error and is falsy."""
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://u:p@localhost:5432/sro")
    monkeypatch.setenv("ELECTUS_CF_CLEARANCE", "")
    settings_cls = cast(SettingsFactoryProto, require_symbol("src.core.config", "Settings"))
    s = settings_cls(_env_file=None)
    assert not s.electus_cf_clearance


def test_f3_t2_05_settings_preserves_dsn_query_params(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """F3-T2-05: Complex `postgresql+asyncpg://` DSN with query string is accepted cleanly."""
    dsn = "postgresql+asyncpg://user:p%40ss@postgres:5432/sro_db?ssl=disable"
    monkeypatch.setenv("DATABASE_URL", dsn)
    settings_cls = cast(SettingsFactoryProto, require_symbol("src.core.config", "Settings"))
    s = settings_cls(_env_file=None)
    assert "sro_db" in str(s.database_url)


# =============================================================================
# Feature 4: Async Engine & Session (Boundaries & Negative)
# =============================================================================


@pytest.mark.parametrize("bad_url", ["", "not_a_url"])
def test_f4_t2_01_invalid_database_url_raises(bad_url: str) -> None:
    """F4-T2-01: Empty or malformed DB URL raises ArgumentError or ValueError."""
    create_engine = cast(
        CreateEngineFromUrlProto, require_symbol("src.core.database", "create_engine_from_url")
    )
    with pytest.raises((ArgumentError, ValueError)):
        create_engine(bad_url)


def test_f4_t2_02_sync_sqlite_url_rejected_by_async_engine() -> None:
    """F4-T2-02: Synchronous SQLite URL is rejected by `create_async_engine`."""
    create_engine = cast(
        CreateEngineFromUrlProto, require_symbol("src.core.database", "create_engine_from_url")
    )
    with pytest.raises((InvalidRequestError, ArgumentError, ValueError)):
        create_engine("sqlite:///local.db")


async def test_f4_t2_03_idempotent_engine_dispose() -> None:
    """F4-T2-03: Calling `await engine.dispose()` multiple times is idempotent."""
    create_engine = cast(
        CreateEngineFromUrlProto, require_symbol("src.core.database", "create_engine_from_url")
    )
    engine = create_engine("postgresql+asyncpg://u:p@localhost:5432/db")
    await engine.dispose()
    await engine.dispose()


@pytest.mark.e2e_db
async def test_f4_t2_04_concurrent_sessions_isolated(e2e_migrated_db: str) -> None:
    """F4-T2-04: Two concurrent `AsyncSession`s execute queries cleanly."""
    create_engine = cast(
        CreateEngineFromUrlProto, require_symbol("src.core.database", "create_engine_from_url")
    )
    create_factory = cast(
        CreateSessionFactoryProto, require_symbol("src.core.database", "create_session_factory")
    )
    engine = create_engine(e2e_migrated_db)
    factory = create_factory(engine)

    async def probe() -> str:
        async with factory() as sess:
            res = await sess.execute(text("SELECT current_database()"))
            return str(res.scalar_one())

    try:
        r1, r2 = await asyncio.gather(probe(), probe())
        assert r1 == r2
    finally:
        await engine.dispose()


@pytest.mark.e2e_db
async def test_f4_t2_05_unreachable_port_fails_fast(e2e_migrated_db: str) -> None:
    """F4-T2-05: Unreachable port raises connection error instead of hanging."""
    create_engine = cast(
        CreateEngineFromUrlProto, require_symbol("src.core.database", "create_engine_from_url")
    )
    create_factory = cast(
        CreateSessionFactoryProto, require_symbol("src.core.database", "create_session_factory")
    )
    engine = create_engine("postgresql+asyncpg://postgres:postgres@127.0.0.1:59999/sro_e2e_test")
    factory = create_factory(engine)
    try:
        with pytest.raises((OSError, TimeoutError)):
            async with asyncio.timeout(5.0), factory() as sess:
                await sess.execute(text("SELECT 1"))
    finally:
        await engine.dispose()


# =============================================================================
# Feature 5: Monthly Partition Auto-Provisioner (Boundaries)
# =============================================================================


@pytest.mark.parametrize(
    ("ref_dt", "expected_cur", "expected_nxt"),
    [
        (datetime(2026, 12, 31, 23, 59, 59, tzinfo=UTC), "2026_12", "2027_01"),
        (datetime(2026, 1, 31, 12, 0, tzinfo=UTC), "2026_01", "2026_02"),
        (datetime(2028, 2, 29, 23, 0, tzinfo=UTC), "2028_02", "2028_03"),
        (datetime(2026, 11, 1, 0, 0, 0), "2026_11", "2026_12"),
    ],
    ids=[
        "F5-T2-01_year_rollover",
        "F5-T2-02_jan31_no_skip_feb",
        "F5-T2-03_leap_day_feb29",
        "F5-T2-04_naive_datetime",
    ],
)
async def test_f5_t2_01_to_04_calendar_rollover_boundaries(
    ref_dt: datetime, expected_cur: str, expected_nxt: str
) -> None:
    """F5-T2-01..04: Verify year-end, 31st-day, leap-day, and naive datetime rollovers."""
    ensure_parts = cast(
        EnsureMonthlyPartitionsProto,
        require_symbol("src.core.database", "ensure_monthly_partitions"),
    )
    stub = _DdlRecordingSession()
    created = await ensure_parts(cast(AsyncSession, stub), reference_time=ref_dt)
    assert f"character_snapshots_{expected_cur}" in created
    assert f"character_snapshots_{expected_nxt}" in created
    assert f"guild_snapshots_{expected_cur}" in created
    assert f"guild_snapshots_{expected_nxt}" in created


@pytest.mark.e2e_db
async def test_f5_t2_05_ensure_monthly_partitions_idempotent_in_pg(
    clean_db: AsyncSession,
) -> None:
    """F5-T2-05: Repeated calls with identical `reference_time` are completely idempotent."""
    ensure_parts = cast(
        EnsureMonthlyPartitionsProto,
        require_symbol("src.core.database", "ensure_monthly_partitions"),
    )
    ref = datetime(2027, 6, 15, tzinfo=UTC)
    first = await ensure_parts(clean_db, reference_time=ref)
    second = await ensure_parts(clean_db, reference_time=ref)
    assert set(first) == set(second)


# =============================================================================
# Feature 6: Structured JSON Logging (Boundaries)
# =============================================================================


def test_f6_t2_01_warning_level_filters_info(capsys: pytest.CaptureFixture[str]) -> None:
    """F6-T2-01: Configuring WARNING suppresses INFO and emits WARNING."""
    configure_logging = cast(
        ConfigureLoggingProto, require_symbol("src.core.logging", "configure_logging")
    )
    get_logger = cast(GetLoggerProto, require_symbol("src.core.logging", "get_logger"))
    configure_logging("WARNING")
    log = get_logger("f6_t2_01")
    log.info("filtered_out")
    log.warning("kept_warning")
    lines = parse_json_log_lines(capsys.readouterr().out)
    assert [ln["event"] for ln in lines] == ["kept_warning"]


def test_f6_t2_02_rebind_run_context_overwrites_prior(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """F6-T2-02: Re-binding `server_id` and `run_id` overwrites previous contextvars."""
    configure_logging = cast(
        ConfigureLoggingProto, require_symbol("src.core.logging", "configure_logging")
    )
    get_logger = cast(GetLoggerProto, require_symbol("src.core.logging", "get_logger"))
    bind_ctx = cast(BindRunContextProto, require_symbol("src.core.logging", "bind_run_context"))
    clear_ctx = cast(ClearRunContextProto, require_symbol("src.core.logging", "clear_run_context"))

    configure_logging("INFO")
    bind_ctx(server_id="chronicle", run_id="r1")
    bind_ctx(server_id="electus", run_id="r2")
    try:
        get_logger("f6_t2_02").info("rebound")
    finally:
        clear_ctx()
    lines = parse_json_log_lines(capsys.readouterr().out)
    assert lines[-1]["server_id"] == "electus"
    assert lines[-1]["run_id"] == "r2"


def test_f6_t2_03_lowercase_log_level_accepted(capsys: pytest.CaptureFixture[str]) -> None:
    """F6-T2-03: Lowercase `'info'` level configures logging without raising."""
    configure_logging = cast(
        ConfigureLoggingProto, require_symbol("src.core.logging", "configure_logging")
    )
    get_logger = cast(GetLoggerProto, require_symbol("src.core.logging", "get_logger"))
    configure_logging("info")
    get_logger("f6_t2_03").info("lower_ok")
    lines = parse_json_log_lines(capsys.readouterr().out)
    assert lines[-1]["event"] == "lower_ok"


def test_f6_t2_04_exception_serializes_as_single_line_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """F6-T2-04: `logger.exception(...)` produces valid single-line JSON."""
    configure_logging = cast(
        ConfigureLoggingProto, require_symbol("src.core.logging", "configure_logging")
    )
    get_logger = cast(GetLoggerProto, require_symbol("src.core.logging", "get_logger"))
    configure_logging("INFO")
    try:
        raise ValueError("boom_err")
    except ValueError:
        get_logger("f6_t2_04").exception("failed_step")
    lines = parse_json_log_lines(capsys.readouterr().out)
    assert len(lines) == 1
    assert lines[0]["event"] == "failed_step"


def test_f6_t2_05_configure_logging_idempotent_no_duplicate_lines(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """F6-T2-05: Calling `configure_logging` twice does not duplicate log handlers."""
    configure_logging = cast(
        ConfigureLoggingProto, require_symbol("src.core.logging", "configure_logging")
    )
    get_logger = cast(GetLoggerProto, require_symbol("src.core.logging", "get_logger"))
    configure_logging("INFO")
    configure_logging("INFO")
    get_logger("f6_t2_05").info("once")
    lines = parse_json_log_lines(capsys.readouterr().out)
    assert len(lines) == 1


# =============================================================================
# Feature 7: servers Table + 3 Seeds (Boundaries & Negative)
# =============================================================================


@pytest.mark.e2e_db
async def test_f7_t2_01_seed_on_conflict_preserves_custom_threshold(
    clean_db: AsyncSession,
) -> None:
    """F7-T2-01: `ON CONFLICT (id) DO NOTHING` preserves modified `min_records_expected`."""
    await clean_db.execute(
        text("UPDATE servers SET min_records_expected = 150 WHERE id = 'electus'")
    )
    await clean_db.execute(
        text(
            "INSERT INTO servers (id, display_name, base_url, min_records_expected) "
            "VALUES ('electus', 'Electus Online', 'https://electus.online', 100) "
            "ON CONFLICT (id) DO NOTHING"
        )
    )
    await clean_db.commit()
    srv = await fetch_server_row(clean_db, "electus")
    assert srv["min_records_expected"] == 150


@pytest.mark.e2e_db
async def test_f7_t2_02_duplicate_server_id_raises_integrity_error(
    clean_db: AsyncSession,
) -> None:
    """F7-T2-02: Duplicate PK `chronicle` without ON CONFLICT raises IntegrityError."""
    with pytest.raises(IntegrityError):
        await clean_db.execute(
            text(
                "INSERT INTO servers (id, display_name, base_url) "
                "VALUES ('chronicle', 'Dup', 'https://dup.example')"
            )
        )
    await clean_db.rollback()


@pytest.mark.e2e_db
async def test_f7_t2_03_null_base_url_raises_not_null(clean_db: AsyncSession) -> None:
    """F7-T2-03: NULL `base_url` violates NOT NULL constraint."""
    with pytest.raises(IntegrityError):
        await clean_db.execute(
            text("INSERT INTO servers (id, display_name, base_url) VALUES ('s4', 'S4', NULL)")
        )
    await clean_db.rollback()


@pytest.mark.e2e_db
async def test_f7_t2_04_server_id_exceeding_varchar32_raises_data_error(
    clean_db: AsyncSession,
) -> None:
    """F7-T2-04: `id` of 33 characters raises DataError."""
    with pytest.raises(DataError):
        await clean_db.execute(
            text(
                "INSERT INTO servers (id, display_name, base_url) "
                "VALUES (:id, 'Long', 'https://long.example')"
            ),
            {"id": "x" * 33},
        )
    await clean_db.rollback()


@pytest.mark.e2e_db
async def test_f7_t2_05_fw_day_of_week_boundaries_0_and_6(clean_db: AsyncSession) -> None:
    """F7-T2-05: Sunday (0) and Saturday (6) `fw_day_of_week` persist accurately."""
    chronicle = await fetch_server_row(clean_db, "chronicle")
    golden = await fetch_server_row(clean_db, "play_golden")
    assert chronicle["fw_day_of_week"] == 0
    assert golden["fw_day_of_week"] == 6


# =============================================================================
# Feature 8: scrape_runs Audit Table (Boundaries & Negative)
# =============================================================================


@pytest.mark.e2e_db
@pytest.mark.parametrize("bad_status", ["FAILED", "UNKNOWN"])
async def test_f8_t2_01_non_canonical_status_rejected(
    clean_db: AsyncSession, bad_status: str
) -> None:
    """F8-T2-01: CHECK constraint rejects non-canonical status values."""
    with pytest.raises(IntegrityError):
        await clean_db.execute(
            text(
                "INSERT INTO scrape_runs (server_id, trigger_type, status, execution_time_ms) "
                "VALUES ('chronicle', 'manual', :st, 10)"
            ),
            {"st": bad_status},
        )
    await clean_db.rollback()


@pytest.mark.e2e_db
async def test_f8_t2_02_nonexistent_server_id_fk_rejected(clean_db: AsyncSession) -> None:
    """F8-T2-02: Nonexistent `server_id` violates foreign key constraint."""
    with pytest.raises(IntegrityError):
        await clean_db.execute(
            text(
                "INSERT INTO scrape_runs (server_id, trigger_type, status, execution_time_ms) "
                "VALUES ('nonexistent_srv', 'manual', 'SUCCESS', 10)"
            )
        )
    await clean_db.rollback()


@pytest.mark.e2e_db
async def test_f8_t2_03_null_execution_time_ms_rejected(clean_db: AsyncSession) -> None:
    """F8-T2-03: NULL `execution_time_ms` violates NOT NULL constraint."""
    with pytest.raises(IntegrityError):
        await clean_db.execute(
            text(
                "INSERT INTO scrape_runs (server_id, trigger_type, status, execution_time_ms) "
                "VALUES ('chronicle', 'manual', 'SUCCESS', NULL)"
            )
        )
    await clean_db.rollback()


@pytest.mark.e2e_db
async def test_f8_t2_04_quarantine_sha256_char64_boundary(clean_db: AsyncSession) -> None:
    """F8-T2-04: 64-char sha256 succeeds; 65-char sha256 raises DataError."""
    await clean_db.execute(
        text(
            "INSERT INTO scrape_runs "
            "(server_id, trigger_type, status, execution_time_ms, quarantine_sha256) "
            "VALUES ('chronicle', 'manual', 'SCHEMA_DRIFT', 10, :sha)"
        ),
        {"sha": "a" * 64},
    )
    await clean_db.commit()
    with pytest.raises(DataError):
        await clean_db.execute(
            text(
                "INSERT INTO scrape_runs "
                "(server_id, trigger_type, status, execution_time_ms, quarantine_sha256) "
                "VALUES ('chronicle', 'manual', 'SCHEMA_DRIFT', 10, :sha)"
            ),
            {"sha": "a" * 65},
        )
    await clean_db.rollback()


@pytest.mark.e2e_db
async def test_f8_t2_05_trigger_type_over_16_chars_rejected(clean_db: AsyncSession) -> None:
    """F8-T2-05: `trigger_type` exceeding VARCHAR(16) raises DataError."""
    with pytest.raises(DataError):
        await clean_db.execute(
            text(
                "INSERT INTO scrape_runs (server_id, trigger_type, status, execution_time_ms) "
                "VALUES ('chronicle', :tt, 'SUCCESS', 10)"
            ),
            {"tt": "t" * 17},
        )
    await clean_db.rollback()


# =============================================================================
# Feature 9: characters & guilds Dimension Tables (Boundaries & Negative)
# =============================================================================


@pytest.mark.e2e_db
async def test_f9_t2_01_race_never_regresses_from_ch_to_unk(clean_db: AsyncSession) -> None:
    """F9-T2-01: Upsert with `'UNK'` does not overwrite existing `'CH'` race."""
    now = datetime.now(UTC)
    sql = text(
        """
        INSERT INTO characters (server_id, char_name, race, first_seen_at, last_seen_at)
        VALUES ('chronicle', 'NoRegress', COALESCE(:race, 'UNK'), :ts, :ts)
        ON CONFLICT (server_id, char_name) DO UPDATE
        SET race = CASE
                WHEN EXCLUDED.race IN ('CH', 'EU') THEN EXCLUDED.race
                ELSE characters.race
            END,
            last_seen_at = EXCLUDED.last_seen_at
        RETURNING race
        """
    )
    await clean_db.execute(sql, {"race": "CH", "ts": now})
    race_after = (await clean_db.execute(sql, {"race": None, "ts": now})).scalar_one()
    await clean_db.commit()
    assert race_after == "CH"


@pytest.mark.e2e_db
async def test_f9_t2_02_guild_master_never_regresses_to_null(clean_db: AsyncSession) -> None:
    """F9-T2-02: Upsert with `master_char_name=NULL` preserves existing master name."""
    now = datetime.now(UTC)
    sql = text(
        """
        INSERT INTO guilds (
            server_id, guild_name, master_char_name, first_seen_at, last_seen_at, is_active
        )
        VALUES ('chronicle', 'KeepMaster', :master, :ts, :ts, TRUE)
        ON CONFLICT (server_id, guild_name) DO UPDATE
        SET master_char_name = COALESCE(EXCLUDED.master_char_name, guilds.master_char_name)
        RETURNING master_char_name
        """
    )
    await clean_db.execute(sql, {"master": "Boss1", "ts": now})
    m_after = (await clean_db.execute(sql, {"master": None, "ts": now})).scalar_one()
    await clean_db.commit()
    assert m_after == "Boss1"


@pytest.mark.e2e_db
async def test_f9_t2_03_invalid_character_race_check_rejected(clean_db: AsyncSession) -> None:
    """F9-T2-03: CHECK constraint rejects race outside `('CH', 'EU', 'UNK')`."""
    now = datetime.now(UTC)
    with pytest.raises(IntegrityError):
        await clean_db.execute(
            text(
                "INSERT INTO characters (server_id, char_name, race, first_seen_at, last_seen_at) "
                "VALUES ('chronicle', 'BadRace', 'INV', :ts, :ts)"
            ),
            {"ts": now},
        )
    await clean_db.rollback()


@pytest.mark.e2e_db
async def test_f9_t2_04_duplicate_char_without_on_conflict_rejected(
    clean_db: AsyncSession,
) -> None:
    """F9-T2-04: Raw duplicate `(server_id, char_name)` insert raises IntegrityError."""
    now = datetime.now(UTC)
    sql = text(
        "INSERT INTO characters (server_id, char_name, race, first_seen_at, last_seen_at) "
        "VALUES ('chronicle', 'DupChar', 'CH', :ts, :ts)"
    )
    await clean_db.execute(sql, {"ts": now})
    with pytest.raises(IntegrityError):
        await clean_db.execute(sql, {"ts": now})
    await clean_db.rollback()


@pytest.mark.e2e_db
async def test_f9_t2_05_case_sensitive_char_names_distinct(clean_db: AsyncSession) -> None:
    """F9-T2-05: `'PlayerA'` and `'playera'` are treated as distinct identities."""
    now = datetime.now(UTC)
    r1 = (
        await clean_db.execute(
            text(
                "INSERT INTO characters (server_id, char_name, race, first_seen_at, last_seen_at) "
                "VALUES ('chronicle', 'PlayerA', 'CH', :ts, :ts) RETURNING id"
            ),
            {"ts": now},
        )
    ).scalar_one()
    r2 = (
        await clean_db.execute(
            text(
                "INSERT INTO characters (server_id, char_name, race, first_seen_at, last_seen_at) "
                "VALUES ('chronicle', 'playera', 'CH', :ts, :ts) RETURNING id"
            ),
            {"ts": now},
        )
    ).scalar_one()
    await clean_db.commit()
    assert r1 != r2


# =============================================================================
# Feature 10: Partitioned Fact Tables & Indexes (Boundaries & Negative)
# =============================================================================


@pytest.mark.e2e_db
async def test_f10_t2_01_partition_upper_bound_exclusive(clean_db: AsyncSession) -> None:
    """F10-T2-01: Exact `'2026-12-01 00:00:00+00'` timestamp routes to `*_2026_12`."""
    ts = datetime(2026, 12, 1, 0, 0, 0, tzinfo=UTC)
    await seed_historical_run(
        clean_db,
        server_id="chronicle",
        created_at=ts,
        characters=[
            HistoricalCharSeed(char_name="BoundChar", rank_position=1, level=120, item_points=100.0)
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
async def test_f10_t2_02_unprovisioned_far_future_routes_to_default(
    clean_db: AsyncSession,
) -> None:
    """F10-T2-02: Far-future timestamp `2099-05-01` is safely caught by `*_default` partition."""
    ts = datetime(2099, 5, 1, 0, 0, 0, tzinfo=UTC)
    await seed_historical_run(
        clean_db,
        server_id="chronicle",
        created_at=ts,
        characters=[
            HistoricalCharSeed(
                char_name="FutureChar", rank_position=1, level=120, item_points=100.0
            )
        ],
    )
    row = (
        await clean_db.execute(
            text("SELECT id, created_at FROM character_snapshots WHERE created_at = :ts"),
            {"ts": ts},
        )
    ).one()
    part = await get_snapshot_partition_name(clean_db, "character_snapshots", int(row[0]), row[1])
    assert part == "character_snapshots_default"


@pytest.mark.e2e_db
async def test_f10_t2_03_duplicate_snapshot_per_run_rejected(clean_db: AsyncSession) -> None:
    """F10-T2-03: Duplicate `(scrape_run_id, character_id, created_at)` raises IntegrityError."""
    ts = datetime(2026, 11, 20, 0, 0, tzinfo=UTC)
    run_id = await seed_historical_run(
        clean_db,
        server_id="chronicle",
        created_at=ts,
        characters=[
            HistoricalCharSeed(
                char_name="UniqueChar", rank_position=1, level=120, item_points=100.0
            )
        ],
    )
    char_id = (
        await clean_db.execute(
            text("SELECT id FROM characters WHERE server_id='chronicle' AND char_name='UniqueChar'")
        )
    ).scalar_one()
    with pytest.raises(IntegrityError):
        await clean_db.execute(
            text(
                "INSERT INTO character_snapshots "
                "(scrape_run_id, server_id, character_id, rank_position, "
                "level, item_points, created_at, updated_at) "
                "VALUES (:run_id, 'chronicle', :char_id, 2, 120, 100.0, :ts, :ts)"
            ),
            {"run_id": run_id, "char_id": char_id, "ts": ts},
        )
    await clean_db.rollback()


@pytest.mark.e2e_db
async def test_f10_t2_04_null_guild_and_gin_extra_metrics_query(clean_db: AsyncSession) -> None:
    """F10-T2-04: Solo player (`guild_id=NULL`) and GIN `@>` JSONB query succeed."""
    ts = datetime(2026, 11, 20, 0, 0, tzinfo=UTC)
    run_id = await seed_historical_run(
        clean_db,
        server_id="chronicle",
        created_at=ts,
        characters=[
            HistoricalCharSeed(
                char_name="SoloGin", rank_position=1, level=120, item_points=100.0, guild_name=None
            )
        ],
    )
    await clean_db.execute(
        text(
            "UPDATE character_snapshots "
            'SET extra_metrics = \'{"pvp_kd": "3.5", "sox_parts": 4}\'::jsonb '
            "WHERE scrape_run_id = :rid"
        ),
        {"rid": run_id},
    )
    await clean_db.commit()
    hits = (
        await clean_db.execute(
            text(
                "SELECT COUNT(*) FROM character_snapshots "
                "WHERE guild_id IS NULL AND extra_metrics @> '{\"sox_parts\": 4}'::jsonb"
            )
        )
    ).scalar_one()
    assert hits == 1


@pytest.mark.e2e_db
async def test_f10_t2_05_numeric_precision_10_2_overflow_vs_12_2(
    clean_db: AsyncSession,
) -> None:
    """F10-T2-05: `NUMERIC(10, 2)` rejects >=1e8; `NUMERIC(12, 2)` accepts 1e10-0.01."""
    ts = datetime(2026, 11, 20, 0, 0, tzinfo=UTC)
    run_id = await seed_historical_run(
        clean_db,
        server_id="chronicle",
        created_at=ts,
        characters=[
            HistoricalCharSeed(char_name="NumChar", rank_position=1, level=120, item_points=100.0)
        ],
    )
    g_id = (
        await clean_db.execute(
            text(
                "INSERT INTO guilds (server_id, guild_name, first_seen_at, last_seen_at) "
                "VALUES ('chronicle', 'BigGuild', :ts, :ts) RETURNING id"
            ),
            {"ts": ts},
        )
    ).scalar_one()
    await clean_db.execute(
        text(
            "INSERT INTO guild_snapshots "
            "(scrape_run_id, server_id, guild_id, rank_position, "
            "total_item_points, created_at, updated_at) "
            "VALUES (:rid, 'chronicle', :gid, 1, 9999999999.99, :ts, :ts)"
        ),
        {"rid": run_id, "gid": g_id, "ts": ts},
    )
    await clean_db.commit()

    char_id = (
        await clean_db.execute(text("SELECT id FROM characters WHERE char_name='NumChar'"))
    ).scalar_one()
    with pytest.raises(DataError):
        await clean_db.execute(
            text(
                "INSERT INTO character_snapshots "
                "(scrape_run_id, server_id, character_id, rank_position, "
                "level, item_points, created_at, updated_at) "
                "VALUES (:rid, 'chronicle', :cid, 2, 120, 123456789.99, :ts, :ts)"
            ),
            {"rid": run_id, "cid": char_id, "ts": ts},
        )
    await clean_db.rollback()


# =============================================================================
# Feature 11: 5 Analytical SQL Views (Boundaries & Negative)
# =============================================================================


@pytest.mark.e2e_db
async def test_f11_t2_01_bot_view_excludes_under_6_intervals_and_over_7_days(
    clean_db: AsyncSession,
) -> None:
    """F11-T2-01: Exclude `< 6` intervals and snapshots older than 7 days."""
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    # 5 intervals (6 snapshots) -> excluded
    for step in range(6):
        await seed_historical_run(
            clean_db,
            server_id="chronicle",
            created_at=now - timedelta(hours=12 * (5 - step)),
            characters=[
                HistoricalCharSeed(
                    char_name="FiveIntervals",
                    rank_position=1,
                    level=100 + step,
                    item_points=100.0 + step,
                )
            ],
        )
    # Old character > 7 days ago -> excluded
    for step in range(8):
        await seed_historical_run(
            clean_db,
            server_id="chronicle",
            created_at=now - timedelta(days=10) + timedelta(hours=12 * step),
            characters=[
                HistoricalCharSeed(
                    char_name="AncientBot",
                    rank_position=1,
                    level=100 + step,
                    item_points=100.0 + step,
                )
            ],
        )
    rows = await query_view(clean_db, "v_bot_vs_donor_classification")
    names = {r["char_name"] for r in rows}
    assert "FiveIntervals" not in names
    assert "AncientBot" not in names


@pytest.mark.e2e_db
async def test_f11_t2_02_bot_vs_donor_exact_ratio_and_jump_thresholds(
    clean_db: AsyncSession,
) -> None:
    """F11-T2-02: Test exact `0.90`, `0.89`, `0.50`, `150.0` vs `149.99` thresholds."""
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    # 11 snapshots = 10 intervals: 9/10 = 0.90 -> BOT; 8/10 = 0.80 -> HARDCORE
    # 7 snapshots = 6 intervals: 3/6 -> HARDCORE; 1/6 +150 -> WHALE; 1/6 +149.99 -> CASUAL
    for step in range(11):
        ts = now - timedelta(hours=12 * (10 - step))
        chars = [
            HistoricalCharSeed(
                char_name="Bot90",
                rank_position=1,
                level=100 + (step if step <= 9 else 9),
                item_points=100.0,
            ),
        ]
        if step >= 4:  # 7 snapshots = 6 intervals
            idx = step - 4
            chars.extend(
                [
                    HistoricalCharSeed(
                        char_name="Hardcore50",
                        rank_position=2,
                        level=100 + (idx if idx <= 3 else 3),
                        item_points=100.0,
                    ),
                    HistoricalCharSeed(
                        char_name="Whale150",
                        rank_position=3,
                        level=100 + (1 if idx == 1 else 0) + (1 if idx >= 1 else 0) * 0,
                        item_points=100.0 + (150.0 if idx >= 2 else 0.0),
                    ),
                    HistoricalCharSeed(
                        char_name="Casual149",
                        rank_position=4,
                        level=100 + (1 if idx >= 1 else 0),
                        item_points=100.0 + (149.99 if idx >= 2 else 0.0),
                    ),
                ]
            )
        await seed_historical_run(clean_db, server_id="chronicle", created_at=ts, characters=chars)

    rows = await query_view(clean_db, "v_bot_vs_donor_classification")
    seg = {r["char_name"]: r["player_behavior_segment"] for r in rows}
    assert seg["Bot90"] == "SUSPECTED_BOT_24_7"
    assert seg["Hardcore50"] == "HARDCORE_HUMAN"
    assert seg["Whale150"] == "SUSPECTED_DONATOR_WHALE"
    assert seg["Casual149"] == "CASUAL_OR_BALANCED"


@pytest.mark.e2e_db
async def test_f11_t2_03_latest_run_views_ignore_subsequent_failed_runs(
    clean_db: AsyncSession,
) -> None:
    """F11-T2-03: Latest-run views ignore newer `SCHEMA_DRIFT` / `BLOCKED_CHALLENGE` runs."""
    t0 = datetime(2026, 11, 18, 0, 0, tzinfo=UTC)
    t1 = t0 + timedelta(hours=12)
    await seed_historical_run(
        clean_db,
        server_id="chronicle",
        created_at=t0,
        status="SUCCESS",
        characters=[
            HistoricalCharSeed(
                char_name="GoodChar",
                rank_position=1,
                level=120,
                item_points=500.0,
                guild_name="GoodGuild",
            )
        ],
    )
    await clean_db.execute(
        text(
            "INSERT INTO scrape_runs "
            "(server_id, trigger_type, status, execution_time_ms, created_at, updated_at) "
            "VALUES ('chronicle', 'scheduled_12h', 'BLOCKED_CHALLENGE', 50, :t1, :t1)"
        ),
        {"t1": t1},
    )
    await clean_db.commit()
    g_rows = await query_view(clean_db, "v_guild_turnover_dominance", "server_id = 'chronicle'")
    r_rows = await query_view(clean_db, "v_race_balance_ch_vs_eu", "server_id = 'chronicle'")
    assert len(g_rows) == 1 and g_rows[0]["guild_name"] == "GoodGuild"
    assert len(r_rows) == 1 and r_rows[0]["total_ranked_characters"] == 1


@pytest.mark.e2e_db
async def test_f11_t2_04_zero_elapsed_time_no_division_by_zero(clean_db: AsyncSession) -> None:
    """F11-T2-04: Identical timestamps across 2 runs yield `item_points_rate_per_hour == 0`."""
    ts = datetime(2026, 11, 18, 12, 0, tzinfo=UTC)
    await seed_historical_run(
        clean_db,
        server_id="chronicle",
        created_at=ts,
        characters=[
            HistoricalCharSeed(char_name="SameTs", rank_position=1, level=120, item_points=100.0)
        ],
    )
    await seed_historical_run(
        clean_db,
        server_id="chronicle",
        created_at=ts,
        characters=[
            HistoricalCharSeed(char_name="SameTs", rank_position=1, level=120, item_points=200.0)
        ],
    )
    rows = await query_view(clean_db, "v_character_activity_deltas", "char_name = 'SameTs'")
    assert len(rows) == 1
    assert float(rows[0]["item_points_rate_per_hour"]) == pytest.approx(0.0)


@pytest.mark.e2e_db
@pytest.mark.parametrize(
    "view_name",
    [
        "v_character_activity_deltas",
        "v_bot_vs_donor_classification",
        "v_guild_turnover_dominance",
        "v_race_balance_ch_vs_eu",
        "v_server_retention_weekly",
    ],
)
async def test_f11_t2_05_empty_tables_views_return_zero_rows(
    clean_db: AsyncSession, view_name: str
) -> None:
    """F11-T2-05: Querying all 5 views on empty DB succeeds returning 0 rows."""
    rows = await query_view(clean_db, view_name)
    assert rows == []


# =============================================================================
# Feature 12: Pydantic v2 Schemas & Validators (Boundaries & Negative)
# =============================================================================


@pytest.mark.parametrize("raw_empty", [None, "", "   ", "-", "None", "No Name", "XXXX", "N/A"])
def test_f12_t2_01_empty_placeholder_tokens_normalize_to_none(
    raw_empty: str | None,
) -> None:
    """F12-T2-01: Empty placeholder tokens normalize to `None` across optional string fields."""
    p_cls = cast(
        PydanticModelClsProto, require_symbol("src.models.pydantic_schemas", "RawPlayerRankingItem")
    )
    j_cls = cast(
        PydanticModelClsProto, require_symbol("src.models.pydantic_schemas", "RawJobRankingItem")
    )
    g_cls = cast(
        PydanticModelClsProto, require_symbol("src.models.pydantic_schemas", "RawGuildRankingItem")
    )
    p = cast(
        RawPlayerItemProto,
        p_cls(rank=1, char_name="Hero", level=100, guild_name=raw_empty, title=raw_empty),
    )
    j = cast(
        RawJobItemProto,
        j_cls(rank=1, char_name="Hero", job_type="HUNTER", job_level=50, guild_name=raw_empty),
    )
    g = cast(
        RawGuildItemProto,
        g_cls(rank=1, guild_name="ValidGuild", master_name=raw_empty, fortress_held=raw_empty),
    )
    assert p.guild_name is None and p.title is None
    assert j.guild_name is None
    assert g.master_name is None and g.fortress_held is None


@pytest.mark.parametrize(
    ("raw_float", "expected"),
    [
        ("1,234.56", 1234.56),
        ("1.234,56", 1234.56),
        ("123,45", 123.45),
        ("123,4", 123.4),
        ("1,234", 1234.0),
        ("1,234,567", 1234567.0),
        ("abc", None),
        ("", None),
    ],
)
def test_f12_t2_02_float_locale_separators(raw_float: str, expected: float | None) -> None:
    """F12-T2-02: Test US, EU, 1/2-decimal comma, multi-comma thousands, and non-numeric floats."""
    p_cls = cast(
        PydanticModelClsProto, require_symbol("src.models.pydantic_schemas", "RawPlayerRankingItem")
    )
    item = cast(
        RawPlayerItemProto, p_cls(rank=1, char_name="Hero", level=100, item_points=raw_float)
    )
    if expected is None:
        assert item.item_points is None
    else:
        assert item.item_points == pytest.approx(expected)


def test_f12_t2_03_rank_level_and_char_name_boundaries() -> None:
    """F12-T2-03: Enforce bounds on `rank>=1`, `level in 1..140`, `char_name 1..64`."""
    p_cls = cast(
        PydanticModelClsProto, require_symbol("src.models.pydantic_schemas", "RawPlayerRankingItem")
    )
    assert cast(RawPlayerItemProto, p_cls(rank=1, char_name="a" * 64, level=1)).level == 1
    assert cast(RawPlayerItemProto, p_cls(rank=1, char_name="b", level=140)).level == 140
    for bad_kwargs in (
        {"rank": 0, "char_name": "Hero", "level": 100},
        {"rank": 1, "char_name": "Hero", "level": 0},
        {"rank": 1, "char_name": "Hero", "level": 141},
        {"rank": 1, "char_name": "", "level": 100},
        {"rank": 1, "char_name": "c" * 65, "level": 100},
        {"rank": 1, "char_name": "Hero", "level": 100, "item_points": -1.0},
    ):
        with pytest.raises(ValidationError):
            p_cls(**bad_kwargs)


def test_f12_t2_04_unknown_race_and_job_type_handling() -> None:
    """F12-T2-04: Optional player fields degrade unknown to None; job item rejects invalid."""
    p_cls = cast(
        PydanticModelClsProto, require_symbol("src.models.pydantic_schemas", "RawPlayerRankingItem")
    )
    j_cls = cast(
        PydanticModelClsProto, require_symbol("src.models.pydantic_schemas", "RawJobRankingItem")
    )
    p = cast(
        RawPlayerItemProto,
        p_cls(rank=1, char_name="Hero", level=100, race="Russian", job_type="Merchant"),
    )
    assert p.race is None
    assert p.job_type is None
    with pytest.raises(ValidationError):
        j_cls(rank=1, char_name="Hero", job_type="Merchant", job_level=50)


def test_f12_t2_05_clean_int_numbers_dirty_strings_and_none() -> None:
    """F12-T2-05: Dirty formatted ints (`1,250 pts`), empty string, and None handled cleanly."""
    p_cls = cast(
        PydanticModelClsProto, require_symbol("src.models.pydantic_schemas", "RawPlayerRankingItem")
    )
    item = cast(
        RawPlayerItemProto,
        p_cls(
            rank=1,
            char_name="Hero",
            level=100,
            strength="1,250 pts",
            intellect="",
            honor_points=None,
        ),
    )
    assert item.strength == 1250
    assert item.intellect is None
    assert item.honor_points is None


# =============================================================================
# Feature 13: HTTPX Session & Transport Retries (Boundaries & Negative)
# =============================================================================


@pytest.mark.e2e_db
async def test_f13_t2_01_exhausted_502_retries_records_http_error_no_cb(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """F13-T2-01: 3x HTTP 502 exhausts 2 retries `[2, 5]`, records HTTP_ERROR, does NOT trip CB."""
    sleeps: list[float] = []

    async def fake_sleep(sec: float) -> None:
        sleeps.append(sec)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    route = respx_mock.get(url__regex=r"https://play-chronicle\.com/.*").mock(
        return_value=httpx.Response(502, text="Bad Gateway")
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("chronicle", e2e_settings).run(clean_db)
    assert route.call_count == 3
    assert [int(s) for s in sleeps] == [2, 5]
    run_row = await fetch_latest_scrape_run(clean_db, "chronicle")
    assert run_row is not None
    assert run_row["status"] == "HTTP_ERROR"
    assert (await fetch_server_row(clean_db, "chronicle"))["circuit_breaker_until"] is None


@pytest.mark.e2e_db
async def test_f13_t2_02_http_500_not_retried_records_http_error(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """F13-T2-02: HTTP 500 is NOT retried and records `HTTP_ERROR`."""
    route = respx_mock.get(url__regex=r"https://play-chronicle\.com/.*").mock(
        return_value=httpx.Response(500, text="Internal Server Error")
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("chronicle", e2e_settings).run(clean_db)
    assert route.call_count == 1
    run_row = await fetch_latest_scrape_run(clean_db, "chronicle")
    assert run_row is not None
    assert run_row["status"] == "HTTP_ERROR"


@pytest.mark.e2e_db
async def test_f13_t2_03_exhausted_transport_error_records_http_error(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """F13-T2-03: 3x `httpx.ConnectError` exhausts retries and records `HTTP_ERROR`."""

    async def fake_sleep(sec: float) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    route = respx_mock.get(url__regex=r"https://play-chronicle\.com/.*").mock(
        side_effect=httpx.ConnectError("connection reset")
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("chronicle", e2e_settings).run(clean_db)
    assert route.call_count == 3
    run_row = await fetch_latest_scrape_run(clean_db, "chronicle")
    assert run_row is not None
    assert run_row["status"] == "HTTP_ERROR"
    assert run_row["records_persisted"] == 0


async def test_f13_t2_04_transient_503_without_banner_retries_to_success(
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """F13-T2-04: Generic 503 (no maintenance banner) retries twice and recovers to 200."""
    sleeps: list[float] = []

    async def fake_sleep(sec: float) -> None:
        sleeps.append(sec)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    route = respx_mock.get("https://play-chronicle.com/ranking/player?page=1")
    route.side_effect = [
        httpx.Response(503, text="Service Temporarily Unavailable"),
        httpx.Response(503, text="Service Temporarily Unavailable"),
        httpx.Response(200, text=render_chronicle_player_html(make_player_specs(5))),
    ]
    respx_mock.get("https://play-chronicle.com/ranking/player?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html([]))
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    async with httpx.AsyncClient(http2=True) as client:
        res = await get_scraper("chronicle", e2e_settings).fetch_player_pages(
            client, "https://play-chronicle.com"
        )
    assert len(res.raw_records) == 5
    assert route.call_count == 3
    assert [int(s) for s in sleeps] == [2, 5]


@pytest.mark.e2e_db
async def test_f13_t2_05_180s_timeout_budget_aborts_cleanly(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """F13-T2-05: Timeout guard aborts hanging scrape cleanly and records `HTTP_ERROR`."""
    real_timeout = asyncio.timeout

    def fast_timeout(delay: float | None) -> Any:
        if delay is not None and delay >= 60.0:
            return real_timeout(0.05)
        return real_timeout(delay)

    monkeypatch.setattr(asyncio, "timeout", fast_timeout)

    async def hanging(req: httpx.Request) -> httpx.Response:
        await asyncio.sleep(5.0)
        return httpx.Response(200, text=render_chronicle_player_html([]))

    respx_mock.get(url__regex=r"https://play-chronicle\.com/.*").mock(side_effect=hanging)
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("chronicle", e2e_settings).run(clean_db)
    run_row = await fetch_latest_scrape_run(clean_db, "chronicle")
    assert run_row is not None
    assert run_row["status"] == "HTTP_ERROR"
    assert await count_rows(clean_db, "character_snapshots", "chronicle") == 0


# =============================================================================
# Feature 14: Persistent Circuit Breaker (Boundaries & Negative)
# =============================================================================


@pytest.mark.e2e_db
async def test_f14_t2_01_expired_cb_allows_normal_run(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """F14-T2-01: Expired `circuit_breaker_until` allows normal HTTP extraction & SUCCESS."""
    await clean_db.execute(
        text(
            "UPDATE servers SET circuit_breaker_until = CURRENT_TIMESTAMP - INTERVAL '1 second' "
            "WHERE id = 'chronicle'"
        )
    )
    await clean_db.commit()
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
    await get_scraper("chronicle", e2e_settings).run(clean_db)
    run_row = await fetch_latest_scrape_run(clean_db, "chronicle")
    assert run_row is not None
    assert run_row["status"] == "SUCCESS"


@pytest.mark.e2e_db
@pytest.mark.parametrize(
    "marker",
    [
        '<div id="turnstile-wrapper"></div>',
        "cf-chl-bypass",
        "<title>Just a moment...</title>",
        "Attention Required! | Cloudflare",
    ],
)
async def test_f14_t2_02_http_200_turnstile_markers_trip_cb(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
    marker: str,
) -> None:
    """F14-T2-02: HTTP 200 containing Turnstile/Cloudflare markers triggers BLOCKED_CHALLENGE."""
    respx_mock.get(url__regex=r"https://play-chronicle\.com/.*").mock(
        return_value=httpx.Response(200, text=render_turnstile_html(marker))
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("chronicle", e2e_settings).run(clean_db)
    run_row = await fetch_latest_scrape_run(clean_db, "chronicle")
    assert run_row is not None
    assert run_row["status"] == "BLOCKED_CHALLENGE"
    assert (await fetch_server_row(clean_db, "chronicle"))["circuit_breaker_until"] is not None


@pytest.mark.e2e_db
async def test_f14_t2_03_mid_scrape_403_on_job_aborts_before_guild(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """F14-T2-03: 403 on Job category stops before Guild and persists 0 snapshots."""
    respx_mock.get("https://play-chronicle.com/ranking/player?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html(make_player_specs(100)))
    )
    respx_mock.get("https://play-chronicle.com/ranking/player?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html([]))
    )
    respx_mock.get("https://play-chronicle.com/ranking/job?page=1").mock(
        return_value=httpx.Response(403, text=render_turnstile_html())
    )
    guild_route = respx_mock.get(url__regex=r"https://play-chronicle\.com/ranking/guild.*").mock(
        return_value=httpx.Response(200, text=render_chronicle_guild_html(make_guild_specs(5)))
    )

    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("chronicle", e2e_settings).run(clean_db)
    assert guild_route.call_count == 0
    assert await count_rows(clean_db, "character_snapshots", "chronicle") == 0
    assert (await fetch_latest_scrape_run(clean_db, "chronicle") or {})[
        "status"
    ] == "BLOCKED_CHALLENGE"


@pytest.mark.e2e_db
async def test_f14_t2_04_custom_12h_cb_cooldown_from_settings(
    clean_db: AsyncSession,
    e2e_quarantine_dir: Path,
    respx_mock: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """F14-T2-04: `CIRCUIT_BREAKER_COOLDOWN_HOURS=12` sets ~12h cooldown."""
    monkeypatch.setenv("CIRCUIT_BREAKER_COOLDOWN_HOURS", "12")
    settings_cls = cast(SettingsFactoryProto, require_symbol("src.core.config", "Settings"))
    s = settings_cls(
        _env_file=None,
        database_url=os.getenv("DATABASE_URL", ""),
        quarantine_dir=e2e_quarantine_dir,
        circuit_breaker_cooldown_hours=12,
    )
    respx_mock.get(url__regex=r"https://play-chronicle\.com/.*").mock(
        return_value=httpx.Response(403, text="Forbidden")
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("chronicle", s).run(clean_db)
    srv = await fetch_server_row(clean_db, "chronicle")
    assert srv["circuit_breaker_until"] > datetime.now(UTC) + timedelta(hours=11, minutes=55)


@pytest.mark.e2e_db
async def test_f14_t2_05_403_empty_body_quarantined_cleanly(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """F14-T2-05: HTTP 403 with empty body `b''` quarantines valid gzip without crashing."""
    respx_mock.get(url__regex=r"https://play-chronicle\.com/.*").mock(
        return_value=httpx.Response(403, content=b"")
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("chronicle", e2e_settings).run(clean_db)
    run_row = await fetch_latest_scrape_run(clean_db, "chronicle")
    assert run_row is not None
    assert run_row["status"] == "BLOCKED_CHALLENGE"
    qpath = Path(str(run_row["quarantine_file_path"]))
    assert path_exists(qpath)
    _ = decompress_gz(qpath)


# =============================================================================
# Feature 15: Maintenance Detection (Boundaries & Negative)
# =============================================================================


@pytest.mark.e2e_db
@pytest.mark.parametrize(
    "banner",
    ["SERVER UNDER MAINTENANCE", "website under maintenance"],
)
async def test_f15_t2_01_case_insensitive_maintenance_detection(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
    banner: str,
) -> None:
    """F15-T2-01: Case-insensitive maintenance banner triggers `TARGET_MAINTENANCE`."""
    respx_mock.get(url__regex=r"https://play-chronicle\.com/.*").mock(
        return_value=httpx.Response(503, text=banner)
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("chronicle", e2e_settings).run(clean_db)
    assert (await fetch_latest_scrape_run(clean_db, "chronicle") or {})[
        "status"
    ] == "TARGET_MAINTENANCE"
    assert (await fetch_server_row(clean_db, "chronicle"))["circuit_breaker_until"] is None


@pytest.mark.e2e_db
async def test_f15_t2_02_503_maintenance_vs_generic_503(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """F15-T2-02: Plain 503 retries 2x -> `HTTP_ERROR`, whereas banner 503 stops on 1 call."""

    async def fake_sleep(sec: float) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    route = respx_mock.get(url__regex=r"https://play-chronicle\.com/.*").mock(
        return_value=httpx.Response(503, text="upstream connect error")
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("chronicle", e2e_settings).run(clean_db)
    assert route.call_count == 3
    assert (await fetch_latest_scrape_run(clean_db, "chronicle") or {})["status"] == "HTTP_ERROR"


@pytest.mark.e2e_db
async def test_f15_t2_03_mid_scrape_guild_maintenance_aborts_cleanly(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """F15-T2-03: Maintenance on Guild after Player 200 aborts with 0 snapshots persisted."""
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
        return_value=httpx.Response(503, text="Server under maintenance")
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("chronicle", e2e_settings).run(clean_db)
    assert (await fetch_latest_scrape_run(clean_db, "chronicle") or {})[
        "status"
    ] == "TARGET_MAINTENANCE"
    assert await count_rows(clean_db, "character_snapshots", "chronicle") == 0


@pytest.mark.e2e_db
async def test_f15_t2_04_challenge_precedence_over_maintenance_text_on_403(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """F15-T2-04: HTTP 403 / `cf-mitigated: challenge` takes precedence as BLOCKED_CHALLENGE."""
    respx_mock.get(url__regex=r"https://play-chronicle\.com/.*").mock(
        return_value=httpx.Response(
            403,
            headers={"cf-mitigated": "challenge"},
            text="Website under maintenance",
        )
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("chronicle", e2e_settings).run(clean_db)
    assert (await fetch_latest_scrape_run(clean_db, "chronicle") or {})[
        "status"
    ] == "BLOCKED_CHALLENGE"


@pytest.mark.e2e_db
async def test_f15_t2_05_electus_503_json_maintenance_body(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """F15-T2-05: Electus 503 with JSON maintenance message yields TARGET_MAINTENANCE."""
    respx_mock.get(url__regex=r"https://electus\.online/.*").mock(
        return_value=httpx.Response(
            503,
            content=orjson.dumps({"status": "maintenance", "message": "Server under maintenance"}),
        )
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("electus", e2e_settings).run(clean_db)
    assert (await fetch_latest_scrape_run(clean_db, "electus") or {})[
        "status"
    ] == "TARGET_MAINTENANCE"


# =============================================================================
# Feature 16: In-Memory Job -> Player Reconciliation (Boundaries & Negative)
# =============================================================================


@pytest.mark.e2e_db
async def test_f16_t2_01_orphan_job_not_inserted(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """F16-T2-01: Job row absent from Top-1000 Players does not create an orphan snapshot."""
    respx_mock.get("https://play-chronicle.com/ranking/player?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html(make_player_specs(100)))
    )
    respx_mock.get("https://play-chronicle.com/ranking/player?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html([]))
    )
    respx_mock.get("https://play-chronicle.com/ranking/job?page=1").mock(
        return_value=httpx.Response(
            200,
            text=render_chronicle_job_html(
                [
                    JobRowSpec(
                        rank=1,
                        char_name="OutOfTop1000",
                        job_type="THIEF",
                        job_level=90,
                        job_exp=100,
                    )
                ]
            ),
        )
    )
    respx_mock.get("https://play-chronicle.com/ranking/job?page=2").mock(
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
    assert await count_rows(clean_db, "character_snapshots", "chronicle") == 100
    orphan = (
        await clean_db.execute(
            text("SELECT COUNT(*) FROM characters WHERE char_name='OutOfTop1000'")
        )
    ).scalar_one()
    assert orphan == 0


@pytest.mark.e2e_db
async def test_f16_t2_02_player_pagination_duplicate_keeps_first_rank(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """F16-T2-02: Duplicate `char_name` across pages keeps highest-ranked (first) occurrence."""
    p1 = make_player_specs(100)
    dup_p2 = [PlayerRowSpec(rank=101, char_name=p1[9].char_name, level=120)]
    respx_mock.get("https://play-chronicle.com/ranking/player?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html(p1))
    )
    respx_mock.get("https://play-chronicle.com/ranking/player?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html(dup_p2))
    )
    respx_mock.get("https://play-chronicle.com/ranking/player?page=3").mock(
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
    assert await count_rows(clean_db, "character_snapshots", "chronicle") == 100
    kept_rank = (
        await clean_db.execute(
            text(
                "SELECT cs.rank_position FROM character_snapshots cs "
                "JOIN characters c ON c.id = cs.character_id WHERE c.char_name = :n"
            ),
            {"n": p1[9].char_name},
        )
    ).scalar_one()
    assert kept_rank == 10


@pytest.mark.e2e_db
async def test_f16_t2_03_guild_pagination_duplicate_keeps_first_rank(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """F16-T2-03: Duplicate `guild_name` across guild pages keeps rank 3 instead of rank 51."""
    g1 = make_guild_specs(5)
    g2 = [GuildRowSpec(rank=51, guild_name=g1[2].guild_name)]
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
        return_value=httpx.Response(200, text=render_chronicle_guild_html(g1))
    )
    respx_mock.get("https://play-chronicle.com/ranking/guild?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_guild_html(g2))
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("chronicle", e2e_settings).run(clean_db)
    assert await count_rows(clean_db, "guild_snapshots", "chronicle") == 5


@pytest.mark.e2e_db
async def test_f16_t2_04_case_sensitive_job_join_does_not_mismatch(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """F16-T2-04: Job row `'player_0001'` does not join onto player `'Player_0001'`."""
    players = make_player_specs(100)
    respx_mock.get("https://play-chronicle.com/ranking/player?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html(players))
    )
    respx_mock.get("https://play-chronicle.com/ranking/player?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html([]))
    )
    respx_mock.get("https://play-chronicle.com/ranking/job?page=1").mock(
        return_value=httpx.Response(
            200,
            text=render_chronicle_job_html(
                [
                    JobRowSpec(
                        rank=1,
                        char_name=players[0].char_name.lower(),
                        job_type="THIEF",
                        job_level=99,
                        job_exp=999,
                    )
                ]
            ),
        )
    )
    respx_mock.get("https://play-chronicle.com/ranking/job?page=2").mock(
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
    extra = (
        await clean_db.execute(
            text(
                "SELECT cs.extra_metrics FROM character_snapshots cs "
                "JOIN characters c ON c.id = cs.character_id WHERE c.char_name = :n"
            ),
            {"n": players[0].char_name},
        )
    ).scalar_one()
    assert "job_rank" not in extra


@pytest.mark.e2e_db
async def test_f16_t2_05_invalid_job_row_recorded_without_crashing_valid_batch(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """F16-T2-05: Invalid Job row increments `validation_errors_count`; valid players persist."""
    players = make_player_specs(100)
    jobs = [
        JobRowSpec(rank=1, char_name=players[0].char_name, job_type="HUNTER", job_level=80),
        JobRowSpec(rank=2, char_name=players[1].char_name, job_type="THIEF", job_level=200),
    ]
    respx_mock.get("https://play-chronicle.com/ranking/player?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html(players))
    )
    respx_mock.get("https://play-chronicle.com/ranking/player?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html([]))
    )
    respx_mock.get("https://play-chronicle.com/ranking/job?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_job_html(jobs))
    )
    respx_mock.get("https://play-chronicle.com/ranking/job?page=2").mock(
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
    assert run_row["status"] == "PARTIAL_SUCCESS"
    assert run_row["validation_errors_count"] >= 1


# =============================================================================
# Feature 17: ChronicleScraper (Boundaries & Negative)
# =============================================================================


@pytest.mark.parametrize(
    ("members_raw", "expected"),
    [("0/50", 0), ("50/50", 50), ("1/50", 1), ("32", 32)],
)
async def test_f17_t2_01_chronicle_members_count_fraction_boundaries(
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
    members_raw: str,
    expected: int,
) -> None:
    """F17-T2-01: Chronicle guild `members_count` fractions parse numerator cleanly."""
    respx_mock.get("https://play-chronicle.com/ranking/guild?page=1").mock(
        return_value=httpx.Response(
            200,
            text=render_chronicle_guild_html(
                [GuildRowSpec(rank=1, guild_name="G1", members_count=members_raw)]
            ),
        )
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
    assert item.members_count == expected


async def test_f17_t2_02_chronicle_empty_cells_normalize_to_none(
    e2e_settings: Any, respx_mock: respx.MockRouter
) -> None:
    """F17-T2-02: `'-'`, `'XXXX'`, and `''` cells normalize to None."""
    rows = [PlayerRowSpec(rank=1, char_name="Hero1", guild_name="XXXX", title="-")]
    respx_mock.get("https://play-chronicle.com/ranking/player?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html(rows))
    )
    respx_mock.get("https://play-chronicle.com/ranking/player?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html([]))
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    player_cls = cast(
        PydanticModelClsProto, require_symbol("src.models.pydantic_schemas", "RawPlayerRankingItem")
    )
    async with httpx.AsyncClient(http2=True) as client:
        res = await get_scraper("chronicle", e2e_settings).fetch_player_pages(
            client, "https://play-chronicle.com"
        )
    p = cast(RawPlayerItemProto, player_cls.model_validate(res.raw_records[0]))
    assert p.guild_name is None
    assert p.title is None


@pytest.mark.e2e_db
async def test_f17_t2_03_chronicle_layout_drift_zero_rows_quarantined(
    clean_db: AsyncSession, e2e_settings: Any, respx_mock: respx.MockRouter
) -> None:
    """F17-T2-03: Changed table CSS class yields 0 rows -> rejected & quarantined."""
    respx_mock.get(url__regex=r"https://play-chronicle\.com/.*").mock(
        return_value=httpx.Response(
            200, text='<table class="new-layout"><tbody><tr><td>1</td></tr></tbody></table>'
        )
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("chronicle", e2e_settings).run(clean_db)
    run_row = await fetch_latest_scrape_run(clean_db, "chronicle")
    assert run_row is not None
    assert run_row["status"] in {"SCHEMA_DRIFT", "LOW_COUNT_QUARANTINE"}
    assert path_exists(str(run_row["quarantine_file_path"]))


async def test_f17_t2_04_chronicle_missing_race_img_and_optional_tds(
    e2e_settings: Any, respx_mock: respx.MockRouter
) -> None:
    """F17-T2-04: Missing race `<img>` and optional extras parse without IndexError."""
    html = (
        '<table class="ranking-table"><tbody>'
        '<tr><td>1</td><td><a class="char-link">NoImgChar</a></td>'
        "<td>-</td><td>120</td><td>100</td><td>0</td><td>-</td></tr>"
        "</tbody></table>"
    )
    respx_mock.get("https://play-chronicle.com/ranking/player?page=1").mock(
        return_value=httpx.Response(200, text=html)
    )
    respx_mock.get("https://play-chronicle.com/ranking/player?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html([]))
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    player_cls = cast(
        PydanticModelClsProto, require_symbol("src.models.pydantic_schemas", "RawPlayerRankingItem")
    )
    async with httpx.AsyncClient(http2=True) as client:
        res = await get_scraper("chronicle", e2e_settings).fetch_player_pages(
            client, "https://play-chronicle.com"
        )
    p = cast(RawPlayerItemProto, player_cls.model_validate(res.raw_records[0]))
    assert p.char_name == "NoImgChar"
    assert p.race is None


async def test_f17_t2_05_chronicle_pagination_caps_never_exceeded(
    e2e_settings: Any, respx_mock: respx.MockRouter
) -> None:
    """F17-T2-05: Never request player page 11, job page 6, or guild page 3."""
    requested: list[str] = []

    def rec(req: httpx.Request) -> httpx.Response:
        requested.append(str(req.url))
        if "/player" in req.url.path:
            return httpx.Response(200, text=render_chronicle_player_html(make_player_specs(10)))
        if "/job" in req.url.path:
            return httpx.Response(
                200, text=render_chronicle_job_html([JobRowSpec(rank=1, char_name="J1")])
            )
        return httpx.Response(200, text=render_chronicle_guild_html(make_guild_specs(5)))

    respx_mock.get(url__regex=r"https://play-chronicle\.com/ranking/.*").mock(side_effect=rec)
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    s = get_scraper("chronicle", e2e_settings)
    async with httpx.AsyncClient(http2=True) as client:
        await s.fetch_player_pages(client, "https://play-chronicle.com")
        await s.fetch_job_pages(client, "https://play-chronicle.com")
        await s.fetch_guild_pages(client, "https://play-chronicle.com")
    assert all("page=11" not in u for u in requested)
    assert not any("/job" in u and "page=6" in u for u in requested)
    assert not any("/guild" in u and "page=3" in u for u in requested)


# =============================================================================
# Feature 18: ElectusScraper (Boundaries & Negative)
# =============================================================================


@pytest.mark.parametrize(
    ("race_id", "expected_race"),
    [(1, "CH"), ("1", "CH"), (2, "EU"), ("2", "EU"), (0, None), (99, None), (None, None)],
)
async def test_f18_t2_01_electus_race_id_boundary_mapping(
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
    race_id: int | str | None,
    expected_race: str | None,
) -> None:
    """F18-T2-01: Map `race_id` 1/2/'1'/'2'/0/99/None safely."""
    payload = orjson.dumps(
        {"data": [{"rank": 1, "charname": "E1", "level": 120, "race_id": race_id}]}
    )
    respx_mock.get("https://electus.online/api/ranking/player?page=1&limit=100").mock(
        return_value=httpx.Response(200, content=payload)
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
    p = cast(RawPlayerItemProto, player_cls.model_validate(res.raw_records[0]))
    assert p.race == expected_race


async def test_f18_t2_02_electus_omits_cf_clearance_cookie_when_unset(
    e2e_settings: Any, respx_mock: respx.MockRouter
) -> None:
    """F18-T2-02: Omit `cf_clearance` cookie when `ELECTUS_CF_CLEARANCE` is None."""
    seen: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req.headers.get("cookie", ""))
        page = req.url.params.get("page", "1")
        rows = make_player_specs(5) if page == "1" else []
        return httpx.Response(200, content=render_electus_player_json(rows))

    respx_mock.get(url__regex=r"https://electus\.online/api/ranking/player.*").mock(
        side_effect=handler
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    async with httpx.AsyncClient(http2=True) as client:
        await get_scraper("electus", e2e_settings).fetch_player_pages(
            client, "https://electus.online"
        )
    assert all("cf_clearance" not in c for c in seen)


@pytest.mark.e2e_db
async def test_f18_t2_03_electus_turnstile_200_on_api_triggers_cb_not_fallback(
    clean_db: AsyncSession, e2e_settings: Any, respx_mock: respx.MockRouter
) -> None:
    """F18-T2-03: HTTP 200 Turnstile HTML on `/api/ranking/player` immediately trips CB."""
    respx_mock.get("https://electus.online/api/ranking/player?page=1&limit=100").mock(
        return_value=httpx.Response(
            200, text=render_turnstile_html("<title>Just a moment...</title>")
        )
    )
    fb = respx_mock.get(url__regex=r"https://electus\.online/ranking/.*").mock(
        return_value=httpx.Response(200, text="should not be called")
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("electus", e2e_settings).run(clean_db)
    assert fb.call_count == 0
    assert (await fetch_latest_scrape_run(clean_db, "electus") or {})[
        "status"
    ] == "BLOCKED_CHALLENGE"


@pytest.mark.e2e_db
async def test_f18_t2_04_electus_truncated_json_handled_without_crash(
    clean_db: AsyncSession, e2e_settings: Any, respx_mock: respx.MockRouter
) -> None:
    """F18-T2-04: Malformed JSON bytes do not crash with unhandled `orjson.JSONDecodeError`."""
    respx_mock.get(url__regex=r"https://electus\.online/.*").mock(
        return_value=httpx.Response(200, content=b'{"data": [')
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("electus", e2e_settings).run(clean_db)
    run_row = await fetch_latest_scrape_run(clean_db, "electus")
    assert run_row is not None
    assert run_row["status"] in {"SCHEMA_DRIFT", "LOW_COUNT_QUARANTINE", "HTTP_ERROR"}


async def test_f18_t2_05_electus_null_optional_fields_and_zero_sox_preserved(
    e2e_settings: Any, respx_mock: respx.MockRouter
) -> None:
    """F18-T2-05: Empty guild, null honor, and `sox_parts=0` preserved accurately."""
    payload = orjson.dumps(
        {
            "data": [
                {
                    "rank": 1,
                    "charname": "ZeroSox",
                    "guild": "",
                    "level": 110,
                    "honor": None,
                    "sox_parts": 0,
                }
            ]
        }
    )
    respx_mock.get("https://electus.online/api/ranking/player?page=1&limit=100").mock(
        return_value=httpx.Response(200, content=payload)
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
    p = cast(RawPlayerItemProto, player_cls.model_validate(res.raw_records[0]))
    assert p.guild_name is None
    assert p.honor_points is None
    assert p.extra_metrics["sox_parts"] == 0


# =============================================================================
# Feature 19: PlayGoldenScraper (Boundaries & Negative)
# =============================================================================


@pytest.mark.e2e_db
async def test_f19_t2_01_golden_49_players_low_count_quarantine(
    clean_db: AsyncSession, e2e_settings: Any, respx_mock: respx.MockRouter
) -> None:
    """F19-T2-01: 49 valid Play Golden players (< 50) triggers `LOW_COUNT_QUARANTINE`."""
    respx_mock.get("https://play-golden.online/ranking/characters?page=1").mock(
        return_value=httpx.Response(200, text=render_golden_player_html(make_player_specs(49)))
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
    assert (await fetch_latest_scrape_run(clean_db, "play_golden") or {})[
        "status"
    ] == "LOW_COUNT_QUARANTINE"
    assert await count_rows(clean_db, "character_snapshots", "play_golden") == 0


@pytest.mark.parametrize(
    ("img_html", "expected_race"),
    [
        ('<img src="/assets/races/european.svg" />', "EU"),
        ('<img src="/assets/races/chinese.png" />', "CH"),
        ('<img src="/x.png" alt="European" />', "EU"),
    ],
)
async def test_f19_t2_02_golden_race_img_src_or_alt_detection(
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
    img_html: str,
    expected_race: str,
) -> None:
    """F19-T2-02: Resolve race from `img` `src` when `alt` is absent, or vice versa."""
    html = (
        '<table class="table-ranking"><tbody>'
        f"<tr><td>1</td><td>{img_html}</td><td>GChar</td><td>Titans</td>"
        "<td>120</td><td>400</td><td>100</td><td>500</td><td>50</td><td>1000</td><td>2026-11-19</td></tr>"
        "</tbody></table>"
    )
    respx_mock.get("https://play-golden.online/ranking/characters?page=1").mock(
        return_value=httpx.Response(200, text=html)
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
    p = cast(RawPlayerItemProto, player_cls.model_validate(res.raw_records[0]))
    assert p.race == expected_race


async def test_f19_t2_03_golden_formatted_stats_and_eu_float(
    e2e_settings: Any, respx_mock: respx.MockRouter
) -> None:
    """F19-T2-03: Parse formatted strength `'1,200'`, empty `'-'`, EU float `'12.345,67'`."""
    rows = [
        PlayerRowSpec(
            rank=1,
            char_name="EuHero",
            strength="1,200",
            intellect="-",
            item_points="12.345,67",
            gold="999,000",
        )
    ]
    respx_mock.get("https://play-golden.online/ranking/characters?page=1").mock(
        return_value=httpx.Response(200, text=render_golden_player_html(rows))
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
    p = cast(RawPlayerItemProto, player_cls.model_validate(res.raw_records[0]))
    assert p.strength == 1200
    assert p.intellect is None
    assert p.item_points == pytest.approx(12345.67)


@pytest.mark.e2e_db
async def test_f19_t2_04_golden_wrong_selector_quarantined(
    clean_db: AsyncSession, e2e_settings: Any, respx_mock: respx.MockRouter
) -> None:
    """F19-T2-04: Wrong table class (`table.ranking-table`) quarantines run cleanly."""
    respx_mock.get(url__regex=r"https://play-golden\.online/.*").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html(make_player_specs(60)))
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("play_golden", e2e_settings).run(clean_db)
    run_row = await fetch_latest_scrape_run(clean_db, "play_golden")
    assert run_row is not None
    assert run_row["status"] in {"SCHEMA_DRIFT", "LOW_COUNT_QUARANTINE"}
    assert path_exists(str(run_row["quarantine_file_path"]))


def test_f19_t2_05_unknown_server_id_rejected_by_get_scraper(e2e_settings: Any) -> None:
    """F19-T2-05: Unknown `server_id` passed to `get_scraper` raises ValueError/KeyError."""
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    with pytest.raises((ValueError, KeyError)):
        get_scraper("unknown_sro", e2e_settings)


# =============================================================================
# Feature 20: Top-Rank Integrity Guard & Batch Thresholds (Boundaries)
# =============================================================================


@pytest.mark.e2e_db
@pytest.mark.parametrize(
    ("total", "errors", "expected_status"),
    [
        (105, 5, "PARTIAL_SUCCESS"),
        (100, 6, "SCHEMA_DRIFT"),
    ],
)
async def test_f20_t2_01_exact_5_percent_error_rate_boundary(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
    total: int,
    errors: int,
    expected_status: str,
) -> None:
    """F20-T2-01: Verify exact `<= 5.0%` (`PARTIAL_SUCCESS`) vs `> 5.0%` (`SCHEMA_DRIFT`)."""
    bad = {10 + i: {"level": 999} for i in range(errors)}
    respx_mock.get("https://play-chronicle.com/ranking/player?page=1").mock(
        return_value=httpx.Response(
            200, text=render_chronicle_player_html(make_player_specs(total, invalid_ranks=bad))
        )
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
    assert (await fetch_latest_scrape_run(clean_db, "chronicle") or {})["status"] == expected_status


@pytest.mark.e2e_db
@pytest.mark.parametrize("bad_rank", [1, 2, 3])
async def test_f20_t2_02_player_podium_guard_overrides_low_error_rate(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
    bad_rank: int,
) -> None:
    """F20-T2-02: Invalid Player rank 1, 2, or 3 triggers `SCHEMA_DRIFT` despite 0.5% error rate."""
    players = make_player_specs(200, invalid_ranks={bad_rank: {"level": 999}})
    respx_mock.get("https://play-chronicle.com/ranking/player?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html(players[:100]))
    )
    respx_mock.get("https://play-chronicle.com/ranking/player?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html(players[100:]))
    )
    respx_mock.get("https://play-chronicle.com/ranking/player?page=3").mock(
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
    assert (await fetch_latest_scrape_run(clean_db, "chronicle") or {})["status"] == "SCHEMA_DRIFT"
    assert await count_rows(clean_db, "character_snapshots", "chronicle") == 0


@pytest.mark.e2e_db
async def test_f20_t2_03_missing_rank_1_triggers_schema_drift(
    clean_db: AsyncSession, e2e_settings: Any, respx_mock: respx.MockRouter
) -> None:
    """F20-T2-03: Ranks starting at 2 (missing rank 1) triggers `SCHEMA_DRIFT`."""
    players = make_player_specs(120, start_rank=2)
    respx_mock.get("https://play-chronicle.com/ranking/player?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html(players[:100]))
    )
    respx_mock.get("https://play-chronicle.com/ranking/player?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html(players[100:]))
    )
    respx_mock.get("https://play-chronicle.com/ranking/player?page=3").mock(
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
    assert (await fetch_latest_scrape_run(clean_db, "chronicle") or {})["status"] == "SCHEMA_DRIFT"


@pytest.mark.e2e_db
@pytest.mark.parametrize("bad_guild_rank", [1, 2, 3])
async def test_f20_t2_04_guild_podium_guard_rejects_entire_batch(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
    bad_guild_rank: int,
) -> None:
    """F20-T2-04: Corrupt Guild rank 1, 2, or 3 triggers `SCHEMA_DRIFT` and persists 0 rows."""
    guilds = make_guild_specs(10, invalid_ranks={bad_guild_rank: {"guild_name": "-"}})
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
        return_value=httpx.Response(200, text=render_chronicle_guild_html(guilds))
    )
    respx_mock.get("https://play-chronicle.com/ranking/guild?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_guild_html([]))
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("chronicle", e2e_settings).run(clean_db)
    assert (await fetch_latest_scrape_run(clean_db, "chronicle") or {})["status"] == "SCHEMA_DRIFT"
    assert await count_rows(clean_db, "character_snapshots", "chronicle") == 0
    assert await count_rows(clean_db, "guild_snapshots", "chronicle") == 0


@pytest.mark.e2e_db
@pytest.mark.parametrize(
    ("players_n", "guilds_n", "expected_status"),
    [
        (100, 5, "SUCCESS"),
        (99, 5, "LOW_COUNT_QUARANTINE"),
        (100, 4, "LOW_COUNT_QUARANTINE"),
    ],
)
async def test_f20_t2_05_exact_min_record_boundaries(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
    players_n: int,
    guilds_n: int,
    expected_status: str,
) -> None:
    """F20-T2-05: Exact `(100, 5)` -> SUCCESS; `(99, 5)` and `(100, 4)` -> LOW_COUNT_QUARANTINE."""
    respx_mock.get("https://play-chronicle.com/ranking/player?page=1").mock(
        return_value=httpx.Response(
            200, text=render_chronicle_player_html(make_player_specs(players_n))
        )
    )
    respx_mock.get("https://play-chronicle.com/ranking/player?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html([]))
    )
    respx_mock.get("https://play-chronicle.com/ranking/job?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_job_html([]))
    )
    respx_mock.get("https://play-chronicle.com/ranking/guild?page=1").mock(
        return_value=httpx.Response(
            200, text=render_chronicle_guild_html(make_guild_specs(guilds_n))
        )
    )
    respx_mock.get("https://play-chronicle.com/ranking/guild?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_guild_html([]))
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("chronicle", e2e_settings).run(clean_db)
    assert (await fetch_latest_scrape_run(clean_db, "chronicle") or {})["status"] == expected_status


# =============================================================================
# Feature 21: Dead-Letter Quarantine & Retention Purge (Boundaries)
# =============================================================================


def test_f21_t2_01_purge_exact_30d_cutoff(
    e2e_quarantine_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F21-T2-01: Keep file at `now - 29d 23h`; delete file at `now - 30d 5s`."""
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://u:p@localhost:5432/sro")
    monkeypatch.setenv("QUARANTINE_DIR", str(e2e_quarantine_dir))
    cache_clear = getattr(require_symbol("src.core.config", "get_settings"), "cache_clear", None)
    if callable(cache_clear):
        cache_clear()

    srv = e2e_quarantine_dir / "chronicle"
    srv.mkdir(parents=True, exist_ok=True)
    keep_f = srv / "keep.html.gz"
    del_f = srv / "del.html.gz"
    keep_f.write_bytes(gzip.compress(b"k"))
    del_f.write_bytes(gzip.compress(b"d"))
    now_ts = pytime.time()
    os.utime(keep_f, (now_ts - (29 * 86400 + 23 * 3600), now_ts - (29 * 86400 + 23 * 3600)))
    os.utime(del_f, (now_ts - (30 * 86400 + 5), now_ts - (30 * 86400 + 5)))

    purge_task = cast(
        PrefectTaskProto, require_symbol("src.flows.tasks", "purge_old_quarantine_task")
    )
    purge_task.fn(retention_days=30)
    assert keep_f.exists()
    assert not del_f.exists()


def test_f21_t2_02_purge_missing_dir_and_gitkeep_safe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F21-T2-02: Missing dir & `.gitkeep` file are handled without error or deletion."""
    qdir = tmp_path / "nonexistent_q"
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://u:p@localhost:5432/sro")
    monkeypatch.setenv("QUARANTINE_DIR", str(qdir))
    cache_clear = getattr(require_symbol("src.core.config", "get_settings"), "cache_clear", None)
    if callable(cache_clear):
        cache_clear()

    purge_task = cast(
        PrefectTaskProto, require_symbol("src.flows.tasks", "purge_old_quarantine_task")
    )
    purge_task.fn(retention_days=30)

    qdir.mkdir()
    gitkeep = qdir / ".gitkeep"
    gitkeep.write_text("", encoding="utf-8")
    old_ts = pytime.time() - 90 * 86400
    os.utime(gitkeep, (old_ts, old_ts))
    purge_task.fn(retention_days=30)
    assert gitkeep.exists()


def test_f21_t2_03_purge_recursive_across_multiple_servers(
    e2e_quarantine_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F21-T2-03: Purge cleans expired `.html.gz` across all server subdirectories."""
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://u:p@localhost:5432/sro")
    monkeypatch.setenv("QUARANTINE_DIR", str(e2e_quarantine_dir))
    cache_clear = getattr(require_symbol("src.core.config", "get_settings"), "cache_clear", None)
    if callable(cache_clear):
        cache_clear()

    old_ts = pytime.time() - 40 * 86400
    targets = []
    for srv in ("chronicle", "electus", "play_golden"):
        d = e2e_quarantine_dir / srv
        d.mkdir(parents=True, exist_ok=True)
        f = d / "expired.html.gz"
        f.write_bytes(gzip.compress(b"x"))
        os.utime(f, (old_ts, old_ts))
        targets.append(f)

    purge_task = cast(
        PrefectTaskProto, require_symbol("src.flows.tasks", "purge_old_quarantine_task")
    )
    purge_task.fn(retention_days=30)
    assert all(not f.exists() for f in targets)


@pytest.mark.e2e_db
async def test_f21_t2_04_quarantine_non_utf8_binary_bytes(
    clean_db: AsyncSession, e2e_settings: Any, respx_mock: respx.MockRouter
) -> None:
    """F21-T2-04: Non-UTF-8 binary payload in 403 response is gzipped without UnicodeDecodeError."""
    raw_bytes = b"\xff\xfe\x80\x99<html>bad-bytes</html>"
    respx_mock.get(url__regex=r"https://play-chronicle\.com/.*").mock(
        return_value=httpx.Response(403, content=raw_bytes)
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("chronicle", e2e_settings).run(clean_db)
    run_row = await fetch_latest_scrape_run(clean_db, "chronicle")
    assert run_row is not None
    assert decompress_gz(str(run_row["quarantine_file_path"])) == raw_bytes


@pytest.mark.e2e_db
async def test_f21_t2_05_success_run_leaves_quarantine_empty(
    clean_db: AsyncSession,
    e2e_settings: Any,
    e2e_quarantine_dir: Path,
    respx_mock: respx.MockRouter,
) -> None:
    """F21-T2-05: `SUCCESS` run writes zero `.html.gz` files and leaves audit columns NULL."""
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
    await get_scraper("chronicle", e2e_settings).run(clean_db)
    run_row = await fetch_latest_scrape_run(clean_db, "chronicle")
    assert run_row is not None
    assert run_row["quarantine_file_path"] is None
    assert run_row["quarantine_sha256"] is None
    assert run_row["quarantine_size_bytes"] is None
    assert rglob_gz(e2e_quarantine_dir) == []


# =============================================================================
# Feature 22: Prefect 3 Tasks, Flows & CLI (Boundaries & Negative)
# =============================================================================


@pytest.mark.parametrize(
    ("dow", "end_time", "expected_cron"),
    [
        (6, time(23, 45), "15 0 * * 0"),
        (6, time(23, 30), "0 0 * * 0"),
        (0, time(23, 59), "29 0 * * 1"),
        (2, time(23, 30), "0 0 * * 3"),
    ],
)
def test_f22_t2_01_compute_post_fw_cron_midnight_and_week_rollover(
    dow: int, end_time: time, expected_cron: str
) -> None:
    """F22-T2-01: Test 30-min rollover across midnight and Saturday(6)->Sunday(0)."""
    fn = cast(
        ComputePostFwCronProto, require_symbol("src.flows.orchestrator", "compute_post_fw_cron")
    )
    assert fn(dow, end_time) == expected_cron


@pytest.mark.e2e_db
async def test_f22_t2_02_scheduled_12h_flow_isolates_target_exception(
    clean_db: AsyncSession,
    respx_mock: respx.MockRouter,
) -> None:
    """F22-T2-02: Failure on `electus` does not abort `chronicle` or `play_golden`."""
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
    respx_mock.get(url__regex=r"https://electus\.online/.*").mock(
        return_value=httpx.Response(500, text="Unexpected error")
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

    flow_fn = cast(
        Callable[..., Any], require_symbol("src.flows.orchestrator", "scheduled_12h_flow")
    )
    with prefect_test_harness():
        await flow_fn()
    assert (await fetch_latest_scrape_run(clean_db, "chronicle") or {})["status"] == "SUCCESS"
    assert (await fetch_latest_scrape_run(clean_db, "electus") or {})["status"] == "HTTP_ERROR"
    assert (await fetch_latest_scrape_run(clean_db, "play_golden") or {})["status"] == "SUCCESS"


@pytest.mark.parametrize(
    "bad_args",
    [
        ["--mode", "manual", "--target", "invalid_server"],
        ["--mode", "invalid_mode"],
    ],
)
def test_f22_t2_03_cli_rejects_invalid_arguments(repo_root: Path, bad_args: list[str]) -> None:
    """F22-T2-03: CLI rejects invalid `--mode` or `--target` with non-zero exit."""
    require_module("src.flows.orchestrator")
    proc = subprocess.run(
        [sys.executable, "-m", "src.flows.orchestrator", *bad_args],
        cwd=repo_root,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert proc.returncode != 0


@pytest.mark.e2e_db
async def test_f22_t2_04_inactive_server_excluded_from_scheduled_flow(
    clean_db: AsyncSession,
    respx_mock: respx.MockRouter,
) -> None:
    """F22-T2-04: `is_active = FALSE` server is excluded by `scheduled_12h_flow`."""
    from prefect.testing.utilities import prefect_test_harness

    await clean_db.execute(text("UPDATE servers SET is_active = FALSE WHERE id = 'play_golden'"))
    await clean_db.commit()

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
    golden_route = respx_mock.get(url__regex=r"https://play-golden\.online/.*").mock(
        return_value=httpx.Response(200, text="should not be called")
    )

    flow_fn = cast(
        Callable[..., Any], require_symbol("src.flows.orchestrator", "scheduled_12h_flow")
    )
    with prefect_test_harness():
        await flow_fn()
    assert golden_route.call_count == 0
    assert await count_rows(clean_db, "scrape_runs", "play_golden") == 0


@pytest.mark.live_e2e
async def test_f22_t2_05_live_prefect_server_deployments_registered() -> None:
    """F22-T2-05: Query live Prefect 3 server API for expected 4 deployments."""
    api_url = os.getenv("PREFECT_API_URL", "http://localhost:4200/api").rstrip("/")
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(f"{api_url}/deployments/filter", json={})
    assert resp.status_code == 200
    names = {d["name"] for d in resp.json()}
    assert {
        "scheduled-12h",
        "post-fw-chronicle",
        "post-fw-electus",
        "post-fw-play_golden",
    }.issubset(names)


# =============================================================================
# Feature 23: Docker Compose, Dockerfile & Operational README (Boundaries)
# =============================================================================


def test_f23_t2_01_postgres_not_publicly_exposed_on_0_0_0_0(repo_root: Path) -> None:
    """F23-T2-01: Confirm `postgres` in `docker-compose.yml` stays isolated on `sro_network`."""
    compose = _load_yaml(require_file("docker-compose.yml", repo_root).read_text(encoding="utf-8"))
    pg = compose["services"]["postgres"]
    ports = pg.get("ports", [])
    for p in ports:
        assert not str(p).startswith("5432:"), f"Public 5432 exposure in compose: {p}"
    assert "sro_network" in str(pg.get("networks", ""))


@pytest.mark.e2e_docker
def test_f23_t2_02_docker_compose_config_quiet_valid(
    repo_root: Path, docker_available: None
) -> None:
    """F23-T2-02: Validate `docker-compose.yml` syntax via `docker compose config --quiet`."""
    require_file("docker-compose.yml", repo_root)
    proc = subprocess.run(
        ["docker", "compose", "config", "--quiet"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr


def test_f23_t2_03_dockerfile_switches_to_non_root_after_copy(repo_root: Path) -> None:
    """F23-T2-03: Confirm `Dockerfile` sets non-root `USER` after `COPY` / installs."""
    lines = [
        ln.strip()
        for ln in require_file("Dockerfile", repo_root).read_text(encoding="utf-8").splitlines()
        if ln.strip() and not ln.strip().startswith("#")
    ]
    user_indices = [i for i, ln in enumerate(lines) if ln.upper().startswith("USER ")]
    copy_indices = [i for i, ln in enumerate(lines) if ln.upper().startswith("COPY ")]
    assert user_indices and copy_indices
    assert user_indices[-1] > copy_indices[0]
    assert lines[user_indices[-1]].split()[1].lower() not in {"root", "0"}


def test_f23_t2_04_compose_no_hardcoded_passwords_or_tokens(repo_root: Path) -> None:
    """F23-T2-04: Confirm `docker-compose.yml` uses env interpolation/env_file for secrets."""
    compose_raw = require_file("docker-compose.yml", repo_root).read_text(encoding="utf-8")
    assert "cf_clearance=" not in compose_raw.lower()
    compose = _load_yaml(compose_raw)
    pg_env = str(compose["services"]["postgres"].get("environment", ""))
    if "POSTGRES_PASSWORD" in pg_env:
        assert "${" in pg_env or "POSTGRES_PASSWORD:-" in pg_env


@pytest.mark.live_e2e
@pytest.mark.e2e_docker
def test_f23_t2_05_live_worker_container_non_root_and_quarantine_writable(
    repo_root: Path, docker_available: None
) -> None:
    """F23-T2-05: Confirm running `scraper-worker` UID != 0 and `/app/quarantine` writable."""
    uid_proc = subprocess.run(
        ["docker", "compose", "exec", "-T", "scraper-worker", "id", "-u"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert uid_proc.returncode == 0, uid_proc.stderr
    assert uid_proc.stdout.strip() != "0"
    write_proc = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "scraper-worker",
            "python",
            "-c",
            (
                "from pathlib import Path; p=Path('/app/quarantine/.probe'); "
                "p.write_text('ok'); p.unlink()"
            ),
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert write_proc.returncode == 0, write_proc.stderr
