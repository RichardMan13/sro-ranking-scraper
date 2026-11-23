from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx
import orjson
import pytest
import respx
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from src.core.config import Settings
from src.core.database import create_engine_from_url, create_session_factory
from src.core.logging import clear_run_context, configure_logging
from src.core.notifications import (
    OPS_ALERT_STATUSES,
    GuildRankEntry,
    PostFWComparison,
    build_ops_alert_payload,
    build_post_fw_payload,
    load_post_fw_comparison,
    notify_operational_failure,
    notify_post_fw_summary,
    send_discord_webhook,
)
from src.models.db_models import Guild, GuildSnapshot, ScrapeRun, Server
from src.models.pydantic_schemas import ErrorDetails
from src.scrapers.base import ScrapeRunSummary

from tests.test_validation import _run_async

_TEST_TOKEN = "super_secret_discord_token_987654321"
_TEST_WEBHOOK_URL = f"https://discord.com/api/webhooks/1234567890/{_TEST_TOKEN}"


def _make_settings(
    *,
    ops_url: str | None = None,
    community_url: str | None = None,
) -> Settings:
    return Settings(
        _env_file=None,
        database_url="sqlite+aiosqlite:///:memory:",
        discord_ops_webhook_url=SecretStr(ops_url) if ops_url is not None else None,
        discord_community_webhook_url=(
            SecretStr(community_url) if community_url is not None else None
        ),
    )


def _make_summary(
    *,
    run_id: uuid.UUID | None = None,
    server_id: str = "chronicle",
    status: str = "SCHEMA_DRIFT",
    trigger_type: str = "scheduled_12h",
    records_extracted: int = 120,
    records_persisted: int = 0,
    validation_errors_count: int = 12,
    execution_time_ms: int = 1450,
    http_status_code: int | None = 200,
    quarantine_file_path: str | None = "./quarantine/chronicle/run.html.gz",
    circuit_breaker_until: datetime | None = None,
    error_details: ErrorDetails | None = None,
) -> ScrapeRunSummary:
    return ScrapeRunSummary(
        run_id=run_id or uuid.uuid4(),
        server_id=server_id,
        status=status,
        trigger_type=trigger_type,
        records_extracted=records_extracted,
        records_persisted=records_persisted,
        validation_errors_count=validation_errors_count,
        execution_time_ms=execution_time_ms,
        http_status_code=http_status_code,
        quarantine_file_path=quarantine_file_path,
        circuit_breaker_until=circuit_breaker_until,
        error_details=error_details,
    )


async def _init_sqlite_schema(engine: AsyncEngine) -> None:
    """Create minimal SQLite-compatible DDL for the 4 tables read by post-FW queries."""
    ddl_statements = (
        """
        CREATE TABLE servers (
            id VARCHAR(32) PRIMARY KEY,
            display_name VARCHAR(64) NOT NULL,
            base_url VARCHAR(255) NOT NULL,
            current_season VARCHAR(32) NOT NULL DEFAULT 'v1',
            is_active BOOLEAN NOT NULL DEFAULT 1,
            fw_day_of_week SMALLINT NOT NULL DEFAULT 0,
            fw_end_time_utc TIME NOT NULL DEFAULT '22:00:00',
            min_records_expected INTEGER NOT NULL DEFAULT 50,
            circuit_breaker_until TIMESTAMP NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE scrape_runs (
            id CHAR(32) PRIMARY KEY,
            server_id VARCHAR(32) NOT NULL REFERENCES servers(id),
            ranking_category VARCHAR(32) NOT NULL DEFAULT 'all',
            trigger_type VARCHAR(16) NOT NULL,
            status VARCHAR(24) NOT NULL,
            records_extracted INTEGER NOT NULL DEFAULT 0,
            records_persisted INTEGER NOT NULL DEFAULT 0,
            validation_errors_count INTEGER NOT NULL DEFAULT 0,
            execution_time_ms INTEGER NOT NULL,
            http_status_code INTEGER NULL,
            quarantine_file_path VARCHAR(255) NULL,
            quarantine_sha256 CHAR(64) NULL,
            quarantine_size_bytes BIGINT NULL,
            error_details TEXT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE guilds (
            id CHAR(32) PRIMARY KEY,
            server_id VARCHAR(32) NOT NULL REFERENCES servers(id),
            guild_name VARCHAR(64) NOT NULL,
            master_char_name VARCHAR(64) NULL,
            first_seen_at TIMESTAMP NOT NULL,
            last_seen_at TIMESTAMP NOT NULL,
            is_active BOOLEAN NOT NULL DEFAULT 1
        )
        """,
        """
        CREATE TABLE guild_snapshots (
            id INTEGER PRIMARY KEY,
            scrape_run_id CHAR(32) NOT NULL REFERENCES scrape_runs(id),
            server_id VARCHAR(32) NOT NULL REFERENCES servers(id),
            guild_id CHAR(32) NOT NULL REFERENCES guilds(id),
            rank_position INTEGER NOT NULL,
            members_count INTEGER NOT NULL DEFAULT 0,
            total_item_points NUMERIC(12, 2) NOT NULL DEFAULT 0.0,
            fortress_held VARCHAR(32) NULL,
            extra_metrics TEXT NOT NULL DEFAULT '{}',
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """,
    )
    async with engine.begin() as conn:
        for ddl in ddl_statements:
            await conn.execute(text(ddl))


def _fields_by_name(payload: dict[str, Any]) -> dict[str, str]:
    embed = payload["embeds"][0]
    return {field["name"]: field["value"] for field in embed["fields"]}


# ---------------------------------------------------------------------------
# 1. Operational Alert Payload (`build_ops_alert_payload`)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("status", "expected_color"),
    [
        ("SCHEMA_DRIFT", 0xE74C3C),
        ("LOW_COUNT_QUARANTINE", 0xE74C3C),
        ("BLOCKED_CHALLENGE", 0xE67E22),
        ("HTTP_ERROR", 0xF1C40F),
    ],
)
def test_build_ops_alert_payload_covers_critical_statuses_and_invariants(
    status: str,
    expected_color: int,
) -> None:
    cb_until = datetime(2026, 11, 23, 4, 30, tzinfo=UTC)
    summary = _make_summary(
        server_id="electus",
        status=status,
        trigger_type="manual",
        http_status_code=403 if status == "BLOCKED_CHALLENGE" else None,
        quarantine_file_path=None if status == "HTTP_ERROR" else "./quarantine/electus/x.html.gz",
        circuit_breaker_until=cb_until if status == "BLOCKED_CHALLENGE" else None,
        error_details=ErrorDetails(
            reason="Link [phishing](https://evil.test)\n*boom* _err_ `cmd`",
            failed_category="player",
        ),
    )

    payload = build_ops_alert_payload(summary)
    assert payload["allowed_mentions"] == {"parse": []}

    embed = payload["embeds"][0]
    assert embed["color"] == expected_color
    assert embed["title"] == f"[SRO Alert] electus — {status}"
    assert embed["footer"]["text"].startswith("SRO Ranking Scraper • ")
    assert embed["footer"]["text"].endswith(" UTC")

    fields = _fields_by_name(payload)
    assert fields["Servidor"] == "electus"
    assert fields["Status"] == status
    assert fields["Gatilho"] == "manual"
    assert fields["Run ID"] == f"`{summary.run_id}`"
    assert fields["Extração / Erros"] == "120 extraídos / 12 erros"
    assert fields["Duração"] == "1450 ms"

    if status == "HTTP_ERROR":
        assert fields["HTTP Status"] == "N/A"
        assert fields["Quarentena"] == "N/A"
    else:
        assert fields["Quarentena"] == "`./quarantine/electus/x.html.gz`"

    if status == "BLOCKED_CHALLENGE":
        assert fields["HTTP Status"] == "403"
        assert fields["Circuit Breaker"] == "2026-11-23 04:30 UTC"
    else:
        assert "Circuit Breaker" not in fields

    assert "\n" not in fields["Detalhes"]
    assert r"\[phishing\]\(https://evil.test\)" in fields["Detalhes"]
    assert r"\*boom\*" in fields["Detalhes"]
    assert r"\_err\_" in fields["Detalhes"]
    assert r"\`cmd\`" in fields["Detalhes"]


def test_build_ops_alert_payload_truncates_long_details() -> None:
    summary = _make_summary(
        status="SCHEMA_DRIFT",
        error_details=ErrorDetails(reason="X" * 2000, failed_category="guild"),
    )
    payload = build_ops_alert_payload(summary)
    details = _fields_by_name(payload)["Detalhes"]
    assert len(details) == 1024
    assert details.endswith("...")


# ---------------------------------------------------------------------------
# 2. Webhook Transport & Security (`send_discord_webhook`)
# ---------------------------------------------------------------------------


def test_send_discord_webhook_noop_when_url_is_none() -> None:
    async def _run() -> bool:
        with respx.mock(assert_all_called=False) as router:
            result = await send_discord_webhook(
                None,
                {"allowed_mentions": {"parse": []}, "embeds": []},
                channel="ops",
                server_id="chronicle",
                run_id=uuid.uuid4(),
            )
            assert len(router.calls) == 0
            return result

    assert _run_async(_run()) is False


def test_send_discord_webhook_success_204_and_no_secret_leak(
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging("INFO")
    clear_run_context()
    run_id = uuid.uuid4()

    async def _run() -> bool:
        with respx.mock() as router:
            route = router.post(_TEST_WEBHOOK_URL).mock(return_value=httpx.Response(204))
            result = await send_discord_webhook(
                SecretStr(_TEST_WEBHOOK_URL),
                {"allowed_mentions": {"parse": []}, "embeds": [{"title": "Test"}]},
                channel="ops",
                server_id="chronicle",
                run_id=run_id,
            )
            assert route.call_count == 1
            return result

    with caplog.at_level(logging.INFO):
        sent = _run_async(_run())

    assert sent is True
    captured_out = capsys.readouterr().out
    assert _TEST_TOKEN not in captured_out
    assert _TEST_WEBHOOK_URL not in captured_out
    assert _TEST_TOKEN not in caplog.text
    assert _TEST_WEBHOOK_URL not in caplog.text

    log_lines = [line for line in captured_out.splitlines() if line.strip()]
    assert len(log_lines) == 1
    event = orjson.loads(log_lines[0])
    assert event["event"] == "discord_webhook_sent"
    assert event["channel"] == "ops"
    assert event["server_id"] == "chronicle"
    assert event["run_id"] == str(run_id)
    assert event["http_status_code"] == 204


@pytest.mark.parametrize("status_code", [302, 429, 500])
def test_send_discord_webhook_non_2xx_and_no_redirects_or_retries(
    status_code: int,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging("INFO")
    clear_run_context()
    run_id = uuid.uuid4()

    async def _run() -> bool:
        with respx.mock() as router:
            headers = {"Location": "https://evil.example/steal"} if status_code == 302 else {}
            route = router.post(_TEST_WEBHOOK_URL).mock(
                return_value=httpx.Response(status_code, headers=headers)
            )
            result = await send_discord_webhook(
                SecretStr(_TEST_WEBHOOK_URL),
                {"allowed_mentions": {"parse": []}, "embeds": []},
                channel="community",
                server_id="electus",
                run_id=run_id,
            )
            assert route.call_count == 1
            assert len(router.calls) == 1
            return result

    with caplog.at_level(logging.INFO):
        sent = _run_async(_run())

    assert sent is False
    captured_out = capsys.readouterr().out
    assert _TEST_TOKEN not in captured_out
    assert _TEST_TOKEN not in caplog.text
    event = orjson.loads(captured_out.strip().splitlines()[-1])
    assert event["event"] == "discord_webhook_http_error"
    assert event["http_status_code"] == status_code
    assert event["run_id"] == str(run_id)


def test_send_discord_webhook_connect_timeout_isolated_and_sanitized(
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging("INFO")
    clear_run_context()
    run_id = uuid.uuid4()

    async def _run() -> bool:
        with respx.mock() as router:
            route = router.post(_TEST_WEBHOOK_URL).mock(
                side_effect=httpx.ConnectTimeout(f"timeout reaching {_TEST_WEBHOOK_URL}")
            )
            result = await send_discord_webhook(
                SecretStr(_TEST_WEBHOOK_URL),
                {"allowed_mentions": {"parse": []}, "embeds": []},
                channel="ops",
                server_id="play_golden",
                run_id=run_id,
            )
            assert route.call_count == 1
            return result

    with caplog.at_level(logging.INFO):
        sent = _run_async(_run())

    assert sent is False
    captured_out = capsys.readouterr().out
    assert _TEST_TOKEN not in captured_out
    assert _TEST_TOKEN not in caplog.text
    event = orjson.loads(captured_out.strip().splitlines()[-1])
    assert event["event"] == "discord_webhook_failed"
    assert event["error_type"] == "ConnectTimeout"
    assert event["run_id"] == str(run_id)


# ---------------------------------------------------------------------------
# 3. Operational Facade (`notify_operational_failure`)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("status", sorted(OPS_ALERT_STATUSES))
def test_notify_operational_failure_fires_for_critical_statuses(status: str) -> None:
    settings = _make_settings(ops_url=_TEST_WEBHOOK_URL)
    summary = _make_summary(status=status)

    async def _run() -> bool:
        with respx.mock() as router:
            route = router.post(_TEST_WEBHOOK_URL).mock(return_value=httpx.Response(204))
            sent = await notify_operational_failure(summary, settings=settings)
            assert route.call_count == 1
            return sent

    assert _run_async(_run()) is True


@pytest.mark.parametrize(
    "status",
    ["SUCCESS", "PARTIAL_SUCCESS", "TARGET_MAINTENANCE", "SKIPPED_CIRCUIT_BREAKER"],
)
def test_notify_operational_failure_silent_for_non_critical_statuses(status: str) -> None:
    settings = _make_settings(ops_url=_TEST_WEBHOOK_URL)
    summary = _make_summary(status=status)

    async def _run() -> bool:
        with respx.mock(assert_all_called=False) as router:
            sent = await notify_operational_failure(summary, settings=settings)
            assert len(router.calls) == 0
            return sent

    assert _run_async(_run()) is False


def test_notify_operational_failure_isolates_formatter_exceptions(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging("INFO")
    clear_run_context()
    settings = _make_settings(ops_url=_TEST_WEBHOOK_URL)
    summary = _make_summary(status="SCHEMA_DRIFT")

    def boom(_summary: ScrapeRunSummary) -> dict[str, Any]:
        raise RuntimeError("formatter bug")

    monkeypatch.setattr("src.core.notifications.build_ops_alert_payload", boom)
    sent = _run_async(notify_operational_failure(summary, settings=settings))

    assert sent is False
    event = orjson.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert event["event"] == "discord_ops_notification_error"
    assert event["error_type"] == "RuntimeError"
    assert event["run_id"] == str(summary.run_id)


# ---------------------------------------------------------------------------
# 4. Post-FW Comparison Query (`load_post_fw_comparison`)
# ---------------------------------------------------------------------------


def test_load_post_fw_comparison_primary_baseline_rank_filter_and_fortress_guards() -> None:
    async def _run() -> PostFWComparison | None:
        engine = create_engine_from_url("sqlite+aiosqlite:///:memory:")
        try:
            await _init_sqlite_schema(engine)
            session_factory = create_session_factory(engine)

            base_time = datetime(2026, 11, 22, 12, 0, 0)  # naive to test UTC normalization
            fw_prev_time = datetime(2026, 11, 22, 18, 0, 0)
            current_time = datetime(2026, 11, 22, 22, 30, 0)

            scheduled_run_id = uuid.uuid4()
            earlier_fw_run_id = uuid.uuid4()
            current_run_id = uuid.uuid4()

            guild_ids = [uuid.uuid4() for _ in range(6)]
            guild_names = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta"]

            async with session_factory() as session:
                session.add(
                    Server(
                        id="chronicle",
                        display_name="Chronicle SRO",
                        base_url="https://play-chronicle.com",
                    )
                )
                session.add_all(
                    [
                        ScrapeRun(
                            id=scheduled_run_id,
                            server_id="chronicle",
                            trigger_type="scheduled_12h",
                            status="SUCCESS",
                            execution_time_ms=100,
                            created_at=base_time,
                            updated_at=base_time,
                        ),
                        ScrapeRun(
                            id=earlier_fw_run_id,
                            server_id="chronicle",
                            trigger_type="post_fw_extra",
                            status="SUCCESS",
                            execution_time_ms=100,
                            created_at=fw_prev_time,
                            updated_at=fw_prev_time,
                        ),
                        ScrapeRun(
                            id=current_run_id,
                            server_id="chronicle",
                            trigger_type="post_fw_extra",
                            status="SUCCESS",
                            execution_time_ms=120,
                            created_at=current_time,
                            updated_at=current_time,
                        ),
                    ]
                )
                for gid, gname in zip(guild_ids, guild_names, strict=True):
                    session.add(
                        Guild(
                            id=gid,
                            server_id="chronicle",
                            guild_name=gname,
                            first_seen_at=base_time,
                            last_seen_at=current_time,
                        )
                    )

                # Baseline (scheduled_12h) snapshots
                session.add_all(
                    [
                        GuildSnapshot(
                            id=1,
                            scrape_run_id=scheduled_run_id,
                            server_id="chronicle",
                            guild_id=guild_ids[0],
                            rank_position=2,
                            members_count=48,
                            total_item_points=Decimal("10000.00"),
                            fortress_held="Jangan",
                            created_at=base_time,
                            updated_at=base_time,
                        ),
                        GuildSnapshot(
                            id=2,
                            scrape_run_id=scheduled_run_id,
                            server_id="chronicle",
                            guild_id=guild_ids[1],
                            rank_position=1,
                            members_count=50,
                            total_item_points=Decimal("11000.00"),
                            fortress_held="Hotan",
                            created_at=base_time,
                            updated_at=base_time,
                        ),
                        GuildSnapshot(
                            id=3,
                            scrape_run_id=scheduled_run_id,
                            server_id="chronicle",
                            guild_id=guild_ids[2],
                            rank_position=8,
                            members_count=40,
                            total_item_points=Decimal("8000.00"),
                            fortress_held="Bandit",
                            created_at=base_time,
                            updated_at=base_time,
                        ),
                    ]
                )

                # Current post-FW snapshots (rank 6 holds a fortress; rank 4 has whitespace)
                session.add_all(
                    [
                        GuildSnapshot(
                            id=10,
                            scrape_run_id=current_run_id,
                            server_id="chronicle",
                            guild_id=guild_ids[0],
                            rank_position=1,
                            members_count=50,
                            total_item_points=Decimal("12345.67"),
                            fortress_held="Hotan",
                            created_at=current_time,
                            updated_at=current_time,
                        ),
                        GuildSnapshot(
                            id=11,
                            scrape_run_id=current_run_id,
                            server_id="chronicle",
                            guild_id=guild_ids[1],
                            rank_position=2,
                            members_count=49,
                            total_item_points=Decimal("11500.20"),
                            fortress_held="Hotan",  # duplicate fortress: rank 1 must win
                            created_at=current_time,
                            updated_at=current_time,
                        ),
                        GuildSnapshot(
                            id=12,
                            scrape_run_id=current_run_id,
                            server_id="chronicle",
                            guild_id=guild_ids[2],
                            rank_position=3,
                            members_count=45,
                            total_item_points=Decimal("9800.00"),
                            fortress_held=None,
                            created_at=current_time,
                            updated_at=current_time,
                        ),
                        GuildSnapshot(
                            id=13,
                            scrape_run_id=current_run_id,
                            server_id="chronicle",
                            guild_id=guild_ids[3],
                            rank_position=4,
                            members_count=42,
                            total_item_points=Decimal("9100.00"),
                            fortress_held="   ",  # whitespace-only must be ignored
                            created_at=current_time,
                            updated_at=current_time,
                        ),
                        GuildSnapshot(
                            id=14,
                            scrape_run_id=current_run_id,
                            server_id="chronicle",
                            guild_id=guild_ids[4],
                            rank_position=5,
                            members_count=38,
                            total_item_points=Decimal("8500.00"),
                            fortress_held=None,
                            created_at=current_time,
                            updated_at=current_time,
                        ),
                        GuildSnapshot(
                            id=15,
                            scrape_run_id=current_run_id,
                            server_id="chronicle",
                            guild_id=guild_ids[5],
                            rank_position=6,
                            members_count=30,
                            total_item_points=Decimal("7000.00"),
                            fortress_held="Jangan",  # rank 6: in fortresses, excluded from Top 5
                            created_at=current_time,
                            updated_at=current_time,
                        ),
                    ]
                )
                await session.commit()

            summary = _make_summary(
                run_id=current_run_id,
                server_id="chronicle",
                status="SUCCESS",
                trigger_type="post_fw_extra",
            )
            async with session_factory() as read_session:
                comparison = await load_post_fw_comparison(read_session, summary)

            assert comparison is not None
            assert comparison.baseline_run_id == scheduled_run_id
            assert comparison.current_run_at.tzinfo is UTC
            assert comparison.baseline_run_at is not None
            assert comparison.baseline_run_at.tzinfo is UTC
            assert comparison.server_display_name == "Chronicle SRO"
            assert [g.guild_name for g in comparison.current_top_guilds] == [
                "Alpha",
                "Beta",
                "Gamma",
                "Delta",
                "Epsilon",
            ]
            assert isinstance(comparison.current_top_guilds[0].total_item_points, Decimal)
            assert comparison.current_top_guilds[0].total_item_points == Decimal("12345.67")
            assert comparison.current_fortresses == {"Hotan": "Alpha", "Jangan": "Zeta"}
            assert comparison.baseline_fortresses == {
                "Jangan": "Alpha",
                "Hotan": "Beta",
                "Bandit": "Gamma",
            }
            assert comparison.baseline_rank_by_guild["Gamma"] == 8
            return comparison
        finally:
            await engine.dispose()

    assert _run_async(_run()) is not None


def test_load_post_fw_comparison_fallback_and_no_baseline_and_missing_run() -> None:
    async def _run() -> None:
        engine = create_engine_from_url("sqlite+aiosqlite:///:memory:")
        try:
            await _init_sqlite_schema(engine)
            session_factory = create_session_factory(engine)

            t0 = datetime(2026, 11, 20, 12, 0, 0, tzinfo=UTC)  # >24h before t2
            t1 = datetime(2026, 11, 22, 21, 0, 0, tzinfo=UTC)  # post_fw_extra within 24h
            t2 = datetime(2026, 11, 22, 22, 30, 0, tzinfo=UTC)  # current

            old_run_id = uuid.uuid4()
            prev_fw_run_id = uuid.uuid4()
            curr_run_id = uuid.uuid4()
            solo_run_id = uuid.uuid4()

            async with session_factory() as session:
                session.add_all(
                    [
                        Server(
                            id="electus",
                            display_name="Electus Online",
                            base_url="https://electus.online",
                        ),
                        Server(
                            id="play_golden",
                            display_name="Play Golden",
                            base_url="https://play-golden.online",
                        ),
                    ]
                )
                session.add_all(
                    [
                        ScrapeRun(
                            id=old_run_id,
                            server_id="electus",
                            trigger_type="scheduled_12h",
                            status="SUCCESS",
                            execution_time_ms=100,
                            created_at=t0,
                            updated_at=t0,
                        ),
                        ScrapeRun(
                            id=prev_fw_run_id,
                            server_id="electus",
                            trigger_type="post_fw_extra",
                            status="PARTIAL_SUCCESS",
                            execution_time_ms=100,
                            created_at=t1,
                            updated_at=t1,
                        ),
                        ScrapeRun(
                            id=curr_run_id,
                            server_id="electus",
                            trigger_type="post_fw_extra",
                            status="SUCCESS",
                            execution_time_ms=100,
                            created_at=t2,
                            updated_at=t2,
                        ),
                        ScrapeRun(
                            id=solo_run_id,
                            server_id="play_golden",
                            trigger_type="post_fw_extra",
                            status="SUCCESS",
                            execution_time_ms=100,
                            created_at=t2,
                            updated_at=t2,
                        ),
                    ]
                )
                await session.commit()

            async with session_factory() as read_session:
                # Fallback to earlier post_fw_extra within 24h
                fallback_cmp = await load_post_fw_comparison(
                    read_session,
                    _make_summary(run_id=curr_run_id, server_id="electus", status="SUCCESS"),
                )
                assert fallback_cmp is not None
                assert fallback_cmp.baseline_run_id == prev_fw_run_id

                # No baseline within 24h
                no_base_cmp = await load_post_fw_comparison(
                    read_session,
                    _make_summary(run_id=solo_run_id, server_id="play_golden", status="SUCCESS"),
                )
                assert no_base_cmp is not None
                assert no_base_cmp.baseline_run_id is None
                assert no_base_cmp.baseline_run_at is None

                # Missing run_id
                missing_cmp = await load_post_fw_comparison(
                    read_session,
                    _make_summary(run_id=uuid.uuid4(), server_id="electus", status="SUCCESS"),
                )
                assert missing_cmp is None
        finally:
            await engine.dispose()

    _run_async(_run())


# ---------------------------------------------------------------------------
# 5. Post-FW Payload & Community Facade (`build_post_fw_payload`, `notify_post_fw_summary`)
# ---------------------------------------------------------------------------


def test_build_post_fw_payload_with_baseline_all_diffs_rounding_and_sanitization() -> None:
    curr_id = uuid.uuid4()
    base_id = uuid.uuid4()
    comparison = PostFWComparison(
        server_id="chronicle",
        server_display_name="Chronicle SRO",
        current_run_id=curr_id,
        current_run_at=datetime(2026, 11, 22, 22, 30, tzinfo=UTC),
        current_status="PARTIAL_SUCCESS",
        baseline_run_id=base_id,
        baseline_run_at=datetime(2026, 11, 22, 12, 0, tzinfo=UTC),
        current_fortresses={
            "Constantinople": "NewConquerors",
            "Hotan": "[phishing](https://evil.test)",
            "Jangan": "Defenders",
        },
        baseline_fortresses={
            "Bandit": "OldOwners",
            "Hotan": "@everyone_guild",
            "Jangan": "Defenders",
        },
        current_top_guilds=(
            GuildRankEntry(1, "[phishing](https://evil.test)", Decimal("12345.67"), 50, "Hotan"),
            GuildRankEntry(2, "Defenders", Decimal("10000.00"), 48, "Jangan"),
            GuildRankEntry(3, "Climbers", Decimal("9500.40"), 45, None),
            GuildRankEntry(4, "Droppers", Decimal("8900.50"), 40, None),
            GuildRankEntry(5, "Newcomers", Decimal("7200.00"), 35, None),
        ),
        baseline_rank_by_guild={
            "[phishing](https://evil.test)": 1,
            "Defenders": 2,
            "Climbers": 8,
            "Droppers": 2,
        },
    )

    payload = build_post_fw_payload(comparison)
    assert payload["allowed_mentions"] == {"parse": []}

    embed = payload["embeds"][0]
    assert embed["color"] == 0xF39C12
    assert embed["title"] == "Resumo Pós-Fortress War — Chronicle SRO"
    assert "2026-11-22 22:30 UTC" in embed["description"]
    assert "2026-11-22 12:00 UTC" in embed["description"]
    assert "Coleta parcial com <= 5% de erros de validação." in embed["description"]
    assert embed["footer"]["text"] == f"Run {curr_id} • 2026-11-22 22:30 UTC"

    fields = _fields_by_name(payload)
    fortresses = fields["Fortalezas"].splitlines()
    assert fortresses == [
        "• **Bandit:** OldOwners → Sem dono",
        "• **Constantinople:** Sem dono → NewConquerors",
        r"• **Hotan:** @everyone\_guild → \[phishing\]\(https://evil.test\) (nova posse)",
        "• **Jangan:** Defenders (mantida)",
    ]

    top_guilds = fields["Top 5 Guildas"].splitlines()
    assert top_guilds == [
        r"#1 **\[phishing\]\(https://evil.test\)** (=) — 12.346 IP (50 membros)",
        "#2 **Defenders** (=) — 10.000 IP (48 membros)",
        "#3 **Climbers** (+5) — 9.500 IP (45 membros)",
        "#4 **Droppers** (-2) — 8.901 IP (40 membros)",
        "#5 **Newcomers** (nova) — 7.200 IP (35 membros)",
    ]


def test_build_post_fw_payload_without_baseline_and_empty_sections() -> None:
    comparison = PostFWComparison(
        server_id="play_golden",
        server_display_name="Play Golden",
        current_run_id=uuid.uuid4(),
        current_run_at=datetime(2026, 11, 22, 20, 30, tzinfo=UTC),
        current_status="SUCCESS",
        baseline_run_id=None,
        baseline_run_at=None,
        current_fortresses={"Hotan": "Kings"},
        baseline_fortresses={},
        current_top_guilds=(GuildRankEntry(1, "Kings", Decimal("5000.00"), 40, "Hotan"),),
        baseline_rank_by_guild={},
    )
    payload = build_post_fw_payload(comparison)
    embed = payload["embeds"][0]
    assert embed["color"] == 0x3498DB
    assert "Sem baseline pré-FW nas últimas 24h" in embed["description"]

    fields = _fields_by_name(payload)
    assert fields["Fortalezas"] == "• **Hotan:** Kings"
    assert fields["Top 5 Guildas"] == "#1 **Kings** — 5.000 IP (40 membros)"

    empty_cmp = PostFWComparison(
        server_id="play_golden",
        server_display_name="Play Golden",
        current_run_id=uuid.uuid4(),
        current_run_at=datetime(2026, 11, 22, 20, 30, tzinfo=UTC),
        current_status="SUCCESS",
        baseline_run_id=None,
        baseline_run_at=None,
        current_fortresses={},
        baseline_fortresses={},
        current_top_guilds=(),
        baseline_rank_by_guild={},
    )
    empty_fields = _fields_by_name(build_post_fw_payload(empty_cmp))
    assert empty_fields["Fortalezas"] == "Nenhuma fortaleza registrada neste snapshot."
    assert empty_fields["Top 5 Guildas"] == "Ranking de guildas indisponível neste snapshot."


def test_notify_post_fw_summary_sends_and_isolates_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    comparison = PostFWComparison(
        server_id="chronicle",
        server_display_name="Chronicle SRO",
        current_run_id=uuid.uuid4(),
        current_run_at=datetime(2026, 11, 22, 22, 30, tzinfo=UTC),
        current_status="SUCCESS",
        baseline_run_id=None,
        baseline_run_at=None,
        current_fortresses={},
        baseline_fortresses={},
        current_top_guilds=(),
        baseline_rank_by_guild={},
    )

    assert (
        _run_async(notify_post_fw_summary(None, _make_settings(community_url=_TEST_WEBHOOK_URL)))
        is False
    )
    assert _run_async(notify_post_fw_summary(comparison, _make_settings())) is False

    async def _send_ok() -> bool:
        with respx.mock() as router:
            route = router.post(_TEST_WEBHOOK_URL).mock(return_value=httpx.Response(204))
            ok = await notify_post_fw_summary(
                comparison,
                _make_settings(community_url=_TEST_WEBHOOK_URL),
            )
            assert route.call_count == 1
            return ok

    assert _run_async(_send_ok()) is True

    monkeypatch.setattr(
        "src.core.notifications.build_post_fw_payload",
        lambda _cmp: (_ for _ in ()).throw(ValueError("bad format")),
    )
    assert (
        _run_async(
            notify_post_fw_summary(
                comparison,
                _make_settings(community_url=_TEST_WEBHOOK_URL),
            )
        )
        is False
    )
