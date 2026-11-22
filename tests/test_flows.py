from __future__ import annotations

import gzip
import inspect
import os
import tempfile
import time as pytime
import uuid
from collections.abc import Sequence
from datetime import time
from pathlib import Path
from typing import Any, cast

_PREFECT_TEST_HOME = Path(tempfile.gettempdir()) / "sro_ranking_scraper_prefect_home"
_PREFECT_TEST_HOME.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("PREFECT_HOME", str(_PREFECT_TEST_HOME))

import asyncio  # noqa: E402

import pytest  # noqa: E402
from prefect.client.schemas.schedules import CronSchedule  # noqa: E402
from prefect.states import Completed, Failed  # noqa: E402
from src.core.config import Settings  # noqa: E402
from src.flows import orchestrator, tasks  # noqa: E402
from src.models.db_models import Server  # noqa: E402
from src.scrapers.base import ScrapeRunSummary  # noqa: E402

from tests.test_validation import _run_async  # noqa: E402


def _make_test_settings(tmp_path: Path | None = None) -> Settings:
    qdir = (tmp_path / "quarantine") if tmp_path is not None else Path("./quarantine")
    return Settings(
        _env_file=None,
        database_url="postgresql+asyncpg://u:p@localhost:5432/sro_test",
        prefect_api_url="http://localhost:4200/api",
        log_level="INFO",
        quarantine_dir=qdir,
        quarantine_retention_days=30,
        circuit_breaker_cooldown_hours=6,
    )


def _make_server(
    server_id: str,
    *,
    fw_day_of_week: int = 0,
    fw_end_time_utc: time = time(22, 0),
    is_active: bool = True,
) -> Server:
    return Server(
        id=server_id,
        display_name=server_id.title(),
        base_url=f"https://{server_id}.example",
        is_active=is_active,
        fw_day_of_week=fw_day_of_week,
        fw_end_time_utc=fw_end_time_utc,
        min_records_expected=100,
    )


def _make_summary(
    server_id: str,
    *,
    status: str = "SUCCESS",
    trigger_type: str = "scheduled_12h",
) -> ScrapeRunSummary:
    return ScrapeRunSummary(
        run_id=uuid.uuid4(),
        server_id=server_id,
        status=status,
        trigger_type=trigger_type,
        records_extracted=100,
        records_persisted=100,
        execution_time_ms=42,
    )


class _DummySession:
    def __init__(self, scalar_items: Sequence[Any] = ()) -> None:
        self.committed = False
        self.rolled_back = False
        self.scalar_items = list(scalar_items)
        self.executed_stmts: list[Any] = []

    async def __aenter__(self) -> _DummySession:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    async def commit(self) -> None:
        self.committed = True

    async def rollback(self) -> None:
        self.rolled_back = True

    async def scalars(self, stmt: Any) -> _DummyScalarsResult:
        self.executed_stmts.append(stmt)
        return _DummyScalarsResult(self.scalar_items)


class _DummyScalarsResult:
    def __init__(self, items: list[Any]) -> None:
        self._items = items

    def all(self) -> list[Any]:
        return list(self._items)


class _DummyEngine:
    def __init__(self) -> None:
        self.disposed = False

    async def dispose(self) -> None:
        self.disposed = True


class _DummySessionFactory:
    def __init__(self, session: _DummySession) -> None:
        self._session = session

    def __call__(self) -> _DummySession:
        return self._session


# ---------------------------------------------------------------------------
# Task metadata & execution tests
# ---------------------------------------------------------------------------


def test_prefect_tasks_enforce_zero_retries_and_expected_sync_async_signatures() -> None:
    assert tasks.ensure_partitions_task.retries == 0
    assert tasks.scrape_target_task.retries == 0
    assert tasks.purge_old_quarantine_task.retries == 0
    assert inspect.iscoroutinefunction(tasks.ensure_partitions_task.fn)
    assert inspect.iscoroutinefunction(tasks.scrape_target_task.fn)
    assert not inspect.iscoroutinefunction(tasks.purge_old_quarantine_task.fn)


def test_ensure_partitions_task_commits_and_disposes_engine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _make_test_settings()
    session = _DummySession()
    engine = _DummyEngine()
    expected = [
        "character_snapshots_2026_11",
        "guild_snapshots_2026_11",
        "character_snapshots_2026_12",
        "guild_snapshots_2026_12",
    ]

    async def fake_ensure_monthly_partitions(sess: Any, reference_time: Any = None) -> list[str]:
        assert sess is session
        return expected

    monkeypatch.setattr(tasks, "get_settings", lambda: settings)
    monkeypatch.setattr(tasks, "create_engine_from_url", lambda url: engine)
    monkeypatch.setattr(tasks, "create_session_factory", lambda eng: _DummySessionFactory(session))
    monkeypatch.setattr(tasks, "ensure_monthly_partitions", fake_ensure_monthly_partitions)

    result = _run_async(tasks.ensure_partitions_task.fn())

    assert result == expected
    assert session.committed is True
    assert session.rolled_back is False
    assert engine.disposed is True


def test_ensure_partitions_task_rolls_back_and_disposes_on_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _make_test_settings()
    session = _DummySession()
    engine = _DummyEngine()

    async def failing_partitions(sess: Any, reference_time: Any = None) -> list[str]:
        raise RuntimeError("ddl failed")

    monkeypatch.setattr(tasks, "get_settings", lambda: settings)
    monkeypatch.setattr(tasks, "create_engine_from_url", lambda url: engine)
    monkeypatch.setattr(tasks, "create_session_factory", lambda eng: _DummySessionFactory(session))
    monkeypatch.setattr(tasks, "ensure_monthly_partitions", failing_partitions)

    with pytest.raises(RuntimeError, match="ddl failed"):
        _run_async(tasks.ensure_partitions_task.fn())

    assert session.committed is False
    assert session.rolled_back is True
    assert engine.disposed is True


def test_scrape_target_task_runs_scraper_and_disposes_engine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _make_test_settings()
    session = _DummySession()
    engine = _DummyEngine()
    expected = _make_summary("chronicle", trigger_type="manual")
    recorded_calls: list[tuple[str, str]] = []

    class _FakeScraper:
        async def run(self, sess: Any, trigger_type: str = "scheduled_12h") -> ScrapeRunSummary:
            assert sess is session
            recorded_calls.append(("chronicle", trigger_type))
            return expected

    def fake_get_scraper(server_id: str, settings: Settings) -> Any:
        assert server_id == "chronicle"
        return _FakeScraper()

    monkeypatch.setattr(tasks, "get_settings", lambda: settings)
    monkeypatch.setattr(tasks, "get_scraper", fake_get_scraper)
    monkeypatch.setattr(tasks, "create_engine_from_url", lambda url: engine)
    monkeypatch.setattr(tasks, "create_session_factory", lambda eng: _DummySessionFactory(session))

    summary = _run_async(tasks.scrape_target_task.fn(server_id="chronicle", trigger_type="manual"))

    assert summary == expected
    assert recorded_calls == [("chronicle", "manual")]
    assert engine.disposed is True


def test_scrape_target_task_disposes_engine_on_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _make_test_settings()
    session = _DummySession()
    engine = _DummyEngine()

    class _FailingScraper:
        async def run(self, sess: Any, trigger_type: str = "scheduled_12h") -> ScrapeRunSummary:
            raise RuntimeError("scraper boom")

    monkeypatch.setattr(tasks, "get_settings", lambda: settings)
    monkeypatch.setattr(tasks, "get_scraper", lambda sid, settings: _FailingScraper())
    monkeypatch.setattr(tasks, "create_engine_from_url", lambda url: engine)
    monkeypatch.setattr(tasks, "create_session_factory", lambda eng: _DummySessionFactory(session))

    with pytest.raises(RuntimeError, match="scraper boom"):
        _run_async(tasks.scrape_target_task.fn(server_id="chronicle", trigger_type="manual"))

    assert engine.disposed is True


def test_purge_old_quarantine_task_retention_edges_and_gitkeep_safety(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(ValueError, match="non-negative"):
        tasks.purge_old_quarantine_task.fn(retention_days=-1, quarantine_dir=tmp_path)

    # Explicit quarantine_dir must not call get_settings()
    monkeypatch.setattr(
        tasks,
        "get_settings",
        lambda: (_ for _ in ()).throw(AssertionError("get_settings should not be called")),
    )
    missing_dir = tmp_path / "does_not_exist"
    assert tasks.purge_old_quarantine_task.fn(retention_days=30, quarantine_dir=missing_dir) == 0

    qdir = tmp_path / "quarantine"
    qdir.mkdir()
    gitkeep = qdir / ".gitkeep"
    gitkeep.write_text("", encoding="utf-8")
    notes_txt = qdir / "chronicle" / "notes.txt"
    notes_txt.parent.mkdir(parents=True, exist_ok=True)
    notes_txt.write_text("keep me", encoding="utf-8")
    dir_named_gz = qdir / "chronicle" / "nested.html.gz"
    dir_named_gz.mkdir(parents=True, exist_ok=True)

    old_files: list[Path] = []
    fresh_files: list[Path] = []
    now_ts = pytime.time()

    for srv in ("chronicle", "electus", "play_golden"):
        srv_dir = qdir / srv
        srv_dir.mkdir(parents=True, exist_ok=True)
        expired = srv_dir / "expired.html.gz"
        fresh = srv_dir / "fresh.html.gz"
        expired.write_bytes(gzip.compress(b"old"))
        fresh.write_bytes(gzip.compress(b"new"))
        os.utime(expired, (now_ts - (30 * 86400 + 10), now_ts - (30 * 86400 + 10)))
        os.utime(fresh, (now_ts - (29 * 86400 + 23 * 3600), now_ts - (29 * 86400 + 23 * 3600)))
        old_files.append(expired)
        fresh_files.append(fresh)

    os.utime(gitkeep, (now_ts - 90 * 86400, now_ts - 90 * 86400))
    os.utime(notes_txt, (now_ts - 90 * 86400, now_ts - 90 * 86400))

    deleted = tasks.purge_old_quarantine_task.fn(retention_days=30, quarantine_dir=qdir)

    assert deleted == 3
    assert all(not p.exists() for p in old_files)
    assert all(p.exists() for p in fresh_files)
    assert gitkeep.exists()
    assert notes_txt.exists()
    assert dir_named_gz.is_dir()


def test_purge_old_quarantine_task_resolves_dir_from_settings_when_none(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    qdir = tmp_path / "env_q"
    srv_dir = qdir / "chronicle"
    srv_dir.mkdir(parents=True)
    expired = srv_dir / "old.html.gz"
    expired.write_bytes(gzip.compress(b"old"))
    old_ts = pytime.time() - 35 * 86400
    os.utime(expired, (old_ts, old_ts))

    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://u:p@localhost:5432/sro_test")
    monkeypatch.setenv("QUARANTINE_DIR", str(qdir))

    deleted = tasks.purge_old_quarantine_task.fn(retention_days=30)

    assert deleted == 1
    assert not expired.exists()


# ---------------------------------------------------------------------------
# Cron computation, server lookup & flow isolation tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("dow", "end_time", "expected"),
    [
        (0, time(22, 0), "30 22 * * 0"),
        (0, time(21, 0), "30 21 * * 0"),
        (6, time(20, 0), "30 20 * * 6"),
        (0, time(22, 45), "15 23 * * 0"),
        (6, time(23, 30), "0 0 * * 0"),
        (6, time(23, 45), "15 0 * * 0"),
        (0, time(23, 59), "29 0 * * 1"),
        (2, time(23, 30), "0 0 * * 3"),
    ],
)
def test_compute_post_fw_cron_schedules_and_rollovers(
    dow: int, end_time: time, expected: str
) -> None:
    assert orchestrator.compute_post_fw_cron(dow, end_time) == expected


@pytest.mark.parametrize("bad_dow", [-1, 7, 99])
def test_compute_post_fw_cron_rejects_invalid_weekday(bad_dow: int) -> None:
    with pytest.raises(ValueError, match="fw_day_of_week"):
        orchestrator.compute_post_fw_cron(bad_dow, time(22, 0))


def test_get_active_servers_queries_and_disposes_engine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    servers = [_make_server("chronicle"), _make_server("electus")]
    session = _DummySession(scalar_items=servers)
    engine = _DummyEngine()

    monkeypatch.setattr(orchestrator, "create_engine_from_url", lambda url: engine)
    monkeypatch.setattr(
        orchestrator, "create_session_factory", lambda eng: _DummySessionFactory(session)
    )

    result = _run_async(orchestrator.get_active_servers(settings=_make_test_settings()))

    assert [s.id for s in result] == ["chronicle", "electus"]
    assert len(session.executed_stmts) == 1
    assert engine.disposed is True


def test_scheduled_12h_flow_isolates_target_failures_and_runs_purge(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _make_test_settings(tmp_path)
    servers = [
        _make_server("chronicle"),
        _make_server("electus"),
        _make_server("play_golden"),
    ]
    call_order: list[str] = []
    purge_calls: list[tuple[int, Path | None]] = []

    async def fake_ensure_partitions() -> list[str]:
        call_order.append("ensure_partitions")
        return ["character_snapshots_2026_11"]

    async def fake_get_active_servers(settings: Settings | None = None) -> list[Server]:
        call_order.append("get_active_servers")
        return servers

    async def fake_scrape_target_task(
        *,
        server_id: str,
        trigger_type: str = "scheduled_12h",
        return_state: bool = False,
    ) -> Any:
        assert trigger_type == "scheduled_12h"
        assert return_state is True
        call_order.append(f"scrape:{server_id}")
        if server_id == "electus":
            return Failed(message="HTTP 500 from electus")
        return Completed(data=_make_summary(server_id))

    def fake_purge(*, retention_days: int = 30, quarantine_dir: Path | None = None) -> int:
        call_order.append("purge")
        purge_calls.append((retention_days, quarantine_dir))
        return 2

    monkeypatch.setattr(orchestrator, "get_settings", lambda: settings)
    monkeypatch.setattr(orchestrator, "ensure_partitions_task", fake_ensure_partitions)
    monkeypatch.setattr(orchestrator, "get_active_servers", fake_get_active_servers)
    monkeypatch.setattr(orchestrator, "scrape_target_task", fake_scrape_target_task)
    monkeypatch.setattr(orchestrator, "purge_old_quarantine_task", fake_purge)

    outcomes = _run_async(orchestrator.scheduled_12h_flow.fn())

    assert call_order == [
        "ensure_partitions",
        "get_active_servers",
        "scrape:chronicle",
        "scrape:electus",
        "scrape:play_golden",
        "purge",
    ]
    assert isinstance(outcomes["chronicle"], ScrapeRunSummary)
    assert outcomes["electus"] == "FAILED:Failed"
    assert isinstance(outcomes["play_golden"], ScrapeRunSummary)
    assert purge_calls == [(30, settings.quarantine_dir)]


def test_post_fw_flow_uses_post_fw_extra_trigger(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _make_test_settings()
    expected = _make_summary("electus", trigger_type="post_fw_extra")
    seen_calls: list[tuple[str, str]] = []

    async def fake_ensure_partitions() -> list[str]:
        seen_calls.append(("partitions", ""))
        return []

    async def fake_scrape_target_task(*, server_id: str, trigger_type: str) -> ScrapeRunSummary:
        seen_calls.append((server_id, trigger_type))
        return expected

    monkeypatch.setattr(orchestrator, "get_settings", lambda: settings)
    monkeypatch.setattr(orchestrator, "ensure_partitions_task", fake_ensure_partitions)
    monkeypatch.setattr(orchestrator, "scrape_target_task", fake_scrape_target_task)

    summary = _run_async(orchestrator.post_fw_flow.fn(server_id="electus"))

    assert summary == expected
    assert seen_calls == [("partitions", ""), ("electus", "post_fw_extra")]


def test_manual_scrape_flow_single_and_all_targets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _make_test_settings()
    servers = [_make_server("chronicle"), _make_server("electus")]

    async def fake_ensure_partitions() -> list[str]:
        return []

    async def fake_get_active_servers(settings: Settings | None = None) -> list[Server]:
        return servers

    async def fake_scrape_target_task(
        *,
        server_id: str,
        trigger_type: str,
        return_state: bool = False,
    ) -> Any:
        summary = _make_summary(server_id, trigger_type=trigger_type)
        if return_state:
            if server_id == "electus":
                return Failed(message="boom")
            return Completed(data=summary)
        return summary

    monkeypatch.setattr(orchestrator, "get_settings", lambda: settings)
    monkeypatch.setattr(orchestrator, "ensure_partitions_task", fake_ensure_partitions)
    monkeypatch.setattr(orchestrator, "get_active_servers", fake_get_active_servers)
    monkeypatch.setattr(orchestrator, "scrape_target_task", fake_scrape_target_task)

    single = _run_async(orchestrator.manual_scrape_flow.fn(target="chronicle"))
    assert isinstance(single["chronicle"], ScrapeRunSummary)
    assert single["chronicle"].trigger_type == "manual"

    all_res = _run_async(orchestrator.manual_scrape_flow.fn(target="all"))
    assert isinstance(all_res["chronicle"], ScrapeRunSummary)
    assert all_res["electus"] == "FAILED:Failed"


# ---------------------------------------------------------------------------
# Deployments & CLI tests
# ---------------------------------------------------------------------------


def test_build_deployments_creates_scheduled_12h_and_post_fw_deployments() -> None:
    servers = [
        _make_server("chronicle", fw_day_of_week=0, fw_end_time_utc=time(22, 0)),
        _make_server("electus", fw_day_of_week=0, fw_end_time_utc=time(21, 0)),
        _make_server("play_golden", fw_day_of_week=6, fw_end_time_utc=time(20, 0)),
    ]

    deployments = orchestrator.build_deployments(servers)

    by_name = {d.name: d for d in deployments}
    assert set(by_name) == {
        "scheduled-12h",
        "post-fw-chronicle",
        "post-fw-electus",
        "post-fw-play_golden",
    }
    assert by_name["post-fw-chronicle"].parameters == {"server_id": "chronicle"}
    assert by_name["post-fw-electus"].parameters == {"server_id": "electus"}
    assert by_name["post-fw-play_golden"].parameters == {"server_id": "play_golden"}

    # Verify cron expressions in schedules
    def _extract_cron(deployment_name: str) -> str:
        schedules = by_name[deployment_name].schedules
        assert schedules is not None and len(schedules) == 1
        sched = schedules[0].schedule
        assert isinstance(sched, CronSchedule)
        return sched.cron

    assert _extract_cron("scheduled-12h") == "0 0,12 * * *"
    assert _extract_cron("post-fw-chronicle") == "30 22 * * 0"
    assert _extract_cron("post-fw-electus") == "30 21 * * 0"
    assert _extract_cron("post-fw-play_golden") == "30 20 * * 6"


def test_cli_main_serve_and_manual_and_invalid_args(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _make_test_settings()
    servers = [
        _make_server("chronicle", fw_day_of_week=0, fw_end_time_utc=time(22, 0)),
        _make_server("electus", fw_day_of_week=0, fw_end_time_utc=time(21, 0)),
        _make_server("play_golden", fw_day_of_week=6, fw_end_time_utc=time(20, 0)),
    ]
    served_deployments: list[Any] = []
    manual_targets: list[str] = []

    async def fake_get_active_servers(settings: Settings | None = None) -> list[Server]:
        return servers

    def fake_serve(*deployments: Any, **kwargs: Any) -> None:
        served_deployments.extend(deployments)

    async def fake_manual_flow(target: str = "all") -> dict[str, ScrapeRunSummary | str]:
        manual_targets.append(target)
        return {target: _make_summary(target, trigger_type="manual")}

    monkeypatch.setattr(orchestrator, "get_settings", lambda: settings)
    monkeypatch.setattr(orchestrator, "get_active_servers", fake_get_active_servers)
    monkeypatch.setattr(orchestrator, "serve", fake_serve)
    monkeypatch.setattr(orchestrator, "manual_scrape_flow", fake_manual_flow)
    monkeypatch.setattr(asyncio, "run", _run_async)

    assert orchestrator.main(["--mode", "serve"]) == 0
    assert len(served_deployments) == 4
    assert {d.name for d in served_deployments} == {
        "scheduled-12h",
        "post-fw-chronicle",
        "post-fw-electus",
        "post-fw-play_golden",
    }

    assert orchestrator.main(["--mode", "manual", "--target", "chronicle"]) == 0
    assert manual_targets == ["chronicle"]

    with pytest.raises(SystemExit) as exc_mode:
        orchestrator.main(["--mode", "invalid_mode"])
    assert cast(int, exc_mode.value.code) != 0

    with pytest.raises(SystemExit) as exc_target:
        orchestrator.main(["--mode", "manual", "--target", "invalid_server"])
    assert cast(int, exc_target.value.code) != 0


def test_ensure_accessible_prefect_home_fallback_on_permission_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PREFECT_HOME", raising=False)
    blocked_home = tmp_path / "blocked_home"
    fallback_temp = tmp_path / "fallback_temp"
    fallback_temp.mkdir()

    monkeypatch.setattr(Path, "home", classmethod(lambda cls: blocked_home))

    real_exists = Path.exists

    def fake_exists(self: Path) -> bool:
        if self == blocked_home / ".prefect" / "profiles.toml":
            raise PermissionError("Access denied")
        return real_exists(self)

    monkeypatch.setattr(Path, "exists", fake_exists)
    monkeypatch.setattr(tempfile, "gettempdir", lambda: str(fallback_temp))

    tasks._ensure_accessible_prefect_home()

    expected = str(fallback_temp / "sro_ranking_scraper_prefect_home")
    assert os.environ["PREFECT_HOME"] == expected
    assert Path(expected).is_dir()
