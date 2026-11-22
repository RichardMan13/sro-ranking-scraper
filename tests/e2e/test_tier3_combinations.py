"""Tier 3 E2E Cross-Feature Combination Tests (28 Pairwise Scenarios T3-01..T3-28).

All Tier 3 tests execute against real PostgreSQL 16 (migrated via `alembic upgrade head`),
`respx` HTTPX transport mocks, isolated `tmp_path` quarantine, and Prefect test harness.
"""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import os
import runpy
import sys
import time as pytime
from collections.abc import Callable
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
import respx
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from tests.e2e._contracts import (
    ComputePostFwCronProto,
    EnsureMonthlyPartitionsProto,
    GetScraperProto,
    PrefectFlowProto,
    PrefectTaskProto,
    PydanticModelClsProto,
    SettingsFactoryProto,
)
from tests.e2e._loader import require_module, require_symbol
from tests.e2e.helpers.db_helpers import (
    HistoricalCharSeed,
    count_rows,
    fetch_latest_scrape_run,
    fetch_server_row,
    list_table_partitions,
    query_view,
    seed_historical_run,
)
from tests.e2e.helpers.fs_helpers import path_exists, read_bytes, write_gz
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

pytestmark = [pytest.mark.e2e, pytest.mark.e2e_tier3, pytest.mark.e2e_db]


def _mock_chronicle_ok(
    router: respx.MockRouter,
    *,
    players: list[PlayerRowSpec] | None = None,
    jobs: list[JobRowSpec] | None = None,
    guilds: list[GuildRowSpec] | None = None,
) -> None:
    p = players if players is not None else make_player_specs(100)
    j = jobs if jobs is not None else []
    g = guilds if guilds is not None else make_guild_specs(5)
    router.get("https://play-chronicle.com/ranking/player?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html(p))
    )
    router.get("https://play-chronicle.com/ranking/player?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html([]))
    )
    router.get("https://play-chronicle.com/ranking/job?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_job_html(j))
    )
    router.get("https://play-chronicle.com/ranking/job?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_job_html([]))
    )
    router.get("https://play-chronicle.com/ranking/guild?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_guild_html(g))
    )
    router.get("https://play-chronicle.com/ranking/guild?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_guild_html([]))
    )


async def test_t3_01_turnstile_403_quarantine_sha256_and_cb(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """T3-01: 403 Turnstile x Quarantine .html.gz + SHA-256 x 6h CB."""
    body_bytes = render_turnstile_html().encode("utf-8")
    route = respx_mock.get(url__regex=r"https://electus\.online/.*").mock(
        return_value=httpx.Response(403, content=body_bytes)
    )
    err_cls = cast(
        PydanticModelClsProto, require_symbol("src.models.pydantic_schemas", "ErrorDetails")
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("electus", e2e_settings).run(clean_db, trigger_type="scheduled_12h")

    assert route.call_count == 1
    run_row = await fetch_latest_scrape_run(clean_db, "electus")
    assert run_row is not None
    assert run_row["status"] == "BLOCKED_CHALLENGE"
    assert run_row["records_persisted"] == 0
    assert run_row["http_status_code"] == 403
    qpath = Path(str(run_row["quarantine_file_path"]))
    assert qpath.name.endswith(".html.gz")
    qbytes = read_bytes(qpath)
    assert run_row["quarantine_sha256"] == hashlib.sha256(qbytes).hexdigest()
    assert run_row["quarantine_size_bytes"] == len(qbytes)
    assert gzip.decompress(qbytes) == body_bytes
    assert isinstance(err_cls.model_validate(run_row["error_details"]), BaseModel)
    srv = await fetch_server_row(clean_db, "electus")
    now = datetime.now(UTC)
    assert (
        now + timedelta(hours=5, minutes=59)
        < srv["circuit_breaker_until"]
        < now + timedelta(hours=6, minutes=1)
    )
    assert await count_rows(clean_db, "character_snapshots", "electus") == 0


async def test_t3_02_active_cb_pre_http_abort_and_zero_scrape_runs(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """T3-02: Active CB x Pre-HTTP abort x zero scrape_runs & structured log."""
    await clean_db.execute(
        text(
            "UPDATE servers SET circuit_breaker_until = CURRENT_TIMESTAMP + INTERVAL '3 hours' "
            "WHERE id = 'chronicle'"
        )
    )
    await clean_db.commit()
    route = respx_mock.get(url__regex=r"https://play-chronicle\.com/.*").mock(
        return_value=httpx.Response(200, text="forbidden call")
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("chronicle", e2e_settings).run(clean_db)
    assert route.call_count == 0
    assert await count_rows(clean_db, "scrape_runs", "chronicle") == 0
    logs = parse_json_log_lines(capsys.readouterr().out)
    assert any(ln.get("server_id") == "chronicle" for ln in logs)


async def test_t3_03_expired_cb_recovery_and_snapshot_persistence(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """T3-03: Expired CB x normal HTTP fetch x snapshot persistence."""
    await clean_db.execute(
        text(
            "UPDATE servers SET circuit_breaker_until = CURRENT_TIMESTAMP - INTERVAL '1 minute' "
            "WHERE id = 'chronicle'"
        )
    )
    await clean_db.commit()
    _mock_chronicle_ok(
        respx_mock,
        players=make_player_specs(105),
        jobs=make_job_specs(20),
        guilds=make_guild_specs(6),
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("chronicle", e2e_settings).run(clean_db)
    run_row = await fetch_latest_scrape_run(clean_db, "chronicle")
    assert run_row is not None
    assert run_row["status"] == "SUCCESS"
    assert await count_rows(clean_db, "character_snapshots", "chronicle") == 105
    assert await count_rows(clean_db, "guild_snapshots", "chronicle") == 6


async def test_t3_04_mid_pagination_429_cf_mitigated_aborts_atomically(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """T3-04: Mid-pagination 429 cf-mitigated x atomic abort x quarantine + 6h CB."""
    respx_mock.get("https://play-golden.online/ranking/characters?page=1").mock(
        return_value=httpx.Response(200, text=render_golden_player_html(make_player_specs(100)))
    )
    p2 = respx_mock.get("https://play-golden.online/ranking/characters?page=2").mock(
        return_value=httpx.Response(429, headers={"cf-mitigated": "challenge"}, text="Rate limited")
    )
    guild_route = respx_mock.get(url__regex=r"https://play-golden\.online/ranking/guilds.*").mock(
        return_value=httpx.Response(200, text=render_golden_guild_html(make_guild_specs(5)))
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("play_golden", e2e_settings).run(clean_db)
    assert p2.call_count == 1
    assert guild_route.call_count == 0
    assert await count_rows(clean_db, "character_snapshots", "play_golden") == 0
    run_row = await fetch_latest_scrape_run(clean_db, "play_golden")
    assert run_row is not None
    assert run_row["status"] == "BLOCKED_CHALLENGE"
    assert run_row["http_status_code"] == 429
    assert (await fetch_server_row(clean_db, "play_golden"))["circuit_breaker_until"] is not None
    assert path_exists(str(run_row["quarantine_file_path"]))


async def test_t3_05_job_left_join_partial_success_and_orphan_filtering(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """T3-05: Job->Player left-join x PARTIAL_SUCCESS (<=5%) x orphan job filtering."""
    players = make_player_specs(
        120, invalid_ranks={50: {"level": 999}, 80: {"level": 999}, 110: {"level": 999}}
    )
    jobs = [
        *make_job_specs(10),
        JobRowSpec(rank=11, char_name=players[49].char_name, job_type="THIEF", job_level=90),
        JobRowSpec(rank=12, char_name="NotInTop1000", job_type="HUNTER", job_level=85),
    ]
    _mock_chronicle_ok(respx_mock, players=players, jobs=jobs, guilds=make_guild_specs(6))
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("chronicle", e2e_settings).run(clean_db)

    run_row = await fetch_latest_scrape_run(clean_db, "chronicle")
    assert run_row is not None
    assert run_row["status"] == "PARTIAL_SUCCESS"
    assert run_row["validation_errors_count"] == 3
    assert len(run_row["error_details"]["sample_errors"]) >= 3
    assert run_row["quarantine_file_path"] is None
    assert await count_rows(clean_db, "character_snapshots", "chronicle") == 117
    assert await count_rows(clean_db, "guild_snapshots", "chronicle") == 6
    orphan_cnt = (
        await clean_db.execute(
            text("SELECT COUNT(*) FROM characters WHERE char_name='NotInTop1000'")
        )
    ).scalar_one()
    assert orphan_cnt == 0


async def test_t3_06_podium_rank2_override_under_5pct_quarantine_no_cb(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """T3-06: Player rank 2 invalid (0.5%) overrides <=5% rule -> SCHEMA_DRIFT, no CB."""
    players = make_player_specs(200, invalid_ranks={2: {"char_name": ""}})
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
        return_value=httpx.Response(200, text=render_chronicle_guild_html(make_guild_specs(6)))
    )
    respx_mock.get("https://play-chronicle.com/ranking/guild?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_guild_html([]))
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("chronicle", e2e_settings).run(clean_db)
    run_row = await fetch_latest_scrape_run(clean_db, "chronicle")
    assert run_row is not None
    assert run_row["status"] == "SCHEMA_DRIFT"
    assert await count_rows(clean_db, "character_snapshots", "chronicle") == 0
    assert await count_rows(clean_db, "guild_snapshots", "chronicle") == 0
    assert path_exists(str(run_row["quarantine_file_path"]))
    assert (await fetch_server_row(clean_db, "chronicle"))["circuit_breaker_until"] is None


async def test_t3_07_guild_podium_invalid_rejects_valid_players_too(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """T3-07: Guild rank 1 invalid (`'-'`) rejects entire batch including valid players."""
    respx_mock.get("https://play-golden.online/ranking/characters?page=1").mock(
        return_value=httpx.Response(200, text=render_golden_player_html(make_player_specs(60)))
    )
    respx_mock.get("https://play-golden.online/ranking/characters?page=2").mock(
        return_value=httpx.Response(200, text=render_golden_player_html([]))
    )
    respx_mock.get("https://play-golden.online/ranking/jobs?page=1").mock(
        return_value=httpx.Response(200, text=render_golden_job_html([]))
    )
    respx_mock.get("https://play-golden.online/ranking/guilds?page=1").mock(
        return_value=httpx.Response(
            200,
            text=render_golden_guild_html(
                make_guild_specs(6, invalid_ranks={1: {"guild_name": "-"}})
            ),
        )
    )
    respx_mock.get("https://play-golden.online/ranking/guilds?page=2").mock(
        return_value=httpx.Response(200, text=render_golden_guild_html([]))
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("play_golden", e2e_settings).run(clean_db)
    run_row = await fetch_latest_scrape_run(clean_db, "play_golden")
    assert run_row is not None
    assert run_row["status"] == "SCHEMA_DRIFT"
    assert run_row["error_details"]["failed_category"] == "guild"
    assert await count_rows(clean_db, "character_snapshots", "play_golden") == 0
    assert await count_rows(clean_db, "guild_snapshots", "play_golden") == 0


async def test_t3_08_error_rate_boundary_5pct_vs_6pct(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """T3-08: Exact 5.00% -> PARTIAL_SUCCESS (95 rows) vs 6.00% -> SCHEMA_DRIFT (0 rows)."""
    bad5 = {10 + i: {"level": 999} for i in range(5)}
    respx_mock.get("https://play-golden.online/ranking/characters?page=1").mock(
        return_value=httpx.Response(
            200, text=render_golden_player_html(make_player_specs(100, invalid_ranks=bad5))
        )
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
    r1 = await fetch_latest_scrape_run(clean_db, "play_golden")
    assert r1 is not None and r1["status"] == "PARTIAL_SUCCESS"
    assert r1["quarantine_file_path"] is None
    assert await count_rows(clean_db, "character_snapshots", "play_golden") == 95

    bad6 = {10 + i: {"level": 999} for i in range(6)}
    respx_mock.get("https://play-golden.online/ranking/characters?page=1").mock(
        return_value=httpx.Response(
            200, text=render_golden_player_html(make_player_specs(100, invalid_ranks=bad6))
        )
    )
    await get_scraper("play_golden", e2e_settings).run(clean_db)
    r2 = await fetch_latest_scrape_run(clean_db, "play_golden")
    assert r2 is not None and r2["status"] == "SCHEMA_DRIFT"
    assert path_exists(str(r2["quarantine_file_path"]))
    assert await count_rows(clean_db, "character_snapshots", "play_golden") == 95


async def test_t3_09_low_count_quarantine_players_and_guilds_no_cb(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """T3-09: Players < min_records OR Guilds < 5 -> LOW_COUNT_QUARANTINE, CB stays NULL."""
    _mock_chronicle_ok(respx_mock, players=make_player_specs(99), guilds=make_guild_specs(6))
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("chronicle", e2e_settings).run(clean_db)
    r_chr = await fetch_latest_scrape_run(clean_db, "chronicle")
    assert r_chr is not None and r_chr["status"] == "LOW_COUNT_QUARANTINE"
    assert (await fetch_server_row(clean_db, "chronicle"))["circuit_breaker_until"] is None

    respx_mock.get("https://play-golden.online/ranking/characters?page=1").mock(
        return_value=httpx.Response(200, text=render_golden_player_html(make_player_specs(60)))
    )
    respx_mock.get("https://play-golden.online/ranking/characters?page=2").mock(
        return_value=httpx.Response(200, text=render_golden_player_html([]))
    )
    respx_mock.get("https://play-golden.online/ranking/jobs?page=1").mock(
        return_value=httpx.Response(200, text=render_golden_job_html([]))
    )
    respx_mock.get("https://play-golden.online/ranking/guilds?page=1").mock(
        return_value=httpx.Response(200, text=render_golden_guild_html(make_guild_specs(4)))
    )
    respx_mock.get("https://play-golden.online/ranking/guilds?page=2").mock(
        return_value=httpx.Response(200, text=render_golden_guild_html([]))
    )
    await get_scraper("play_golden", e2e_settings).run(clean_db)
    r_pg = await fetch_latest_scrape_run(clean_db, "play_golden")
    assert r_pg is not None and r_pg["status"] == "LOW_COUNT_QUARANTINE"
    assert (await fetch_server_row(clean_db, "play_golden"))["circuit_breaker_until"] is None


async def test_t3_10_dimension_upsert_race_and_master_evolution(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """T3-10: Dimension evolution `UNK -> CH -> CH` and `master=NULL -> King -> King`."""
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    scraper = get_scraper("chronicle", e2e_settings)

    # Run 1: Alpha has race=None, Guild "Titans" seen only via player
    p_run1 = make_player_specs(100, race_cycle=[None])
    p_run1[0] = PlayerRowSpec(rank=1, char_name="Alpha", guild_name="Titans", race=None)
    _mock_chronicle_ok(respx_mock, players=p_run1, guilds=make_guild_specs(5, prefix="OtherG"))
    await scraper.run(clean_db)
    c1 = (
        await clean_db.execute(
            text("SELECT id, race, first_seen_at FROM characters WHERE char_name='Alpha'")
        )
    ).one()
    g1 = (
        await clean_db.execute(
            text("SELECT id, master_char_name FROM guilds WHERE guild_name='Titans'")
        )
    ).one()
    assert c1[1] == "UNK" and g1[1] is None

    # Run 2: Alpha has race=CH, Guild "Titans" appears with master="King"
    p_run2 = make_player_specs(100)
    p_run2[0] = PlayerRowSpec(rank=1, char_name="Alpha", guild_name="Titans", race="CH")
    guilds2 = [
        GuildRowSpec(rank=1, guild_name="Titans", master_name="King"),
        *make_guild_specs(5, start_rank=2, prefix="OtherG"),
    ]
    _mock_chronicle_ok(respx_mock, players=p_run2, guilds=guilds2)
    await scraper.run(clean_db)
    c2 = (
        await clean_db.execute(
            text(
                "SELECT id, race, first_seen_at, last_seen_at "
                "FROM characters WHERE char_name='Alpha'"
            )
        )
    ).one()
    g2 = (
        await clean_db.execute(
            text("SELECT id, master_char_name FROM guilds WHERE guild_name='Titans'")
        )
    ).one()
    assert c2[0] == c1[0] and c2[1] == "CH" and c2[2] == c1[2] and c2[3] > c1[2]
    assert g2[0] == g1[0] and g2[1] == "King"

    # Run 3: Alpha race=None, Titans absent from guild table -> no regression
    _mock_chronicle_ok(respx_mock, players=p_run1, guilds=make_guild_specs(5, prefix="OtherG"))
    await scraper.run(clean_db)
    c3_race = (
        await clean_db.execute(text("SELECT race FROM characters WHERE char_name='Alpha'"))
    ).scalar_one()
    g3_master = (
        await clean_db.execute(
            text("SELECT master_char_name FROM guilds WHERE guild_name='Titans'")
        )
    ).scalar_one()
    assert c3_race == "CH"
    assert g3_master == "King"


async def test_t3_11_dedup_reconciliation_and_chunked_bulk_insert_over_1000(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """T3-11: 1,050 rows with 10 dups -> 1,040 unique chunked snapshots + job enrichment."""
    unique_1040 = make_player_specs(1040)
    dups_10 = [
        PlayerRowSpec(rank=1041 + i, char_name=unique_1040[i].char_name, level=120)
        for i in range(10)
    ]
    all_players = (
        unique_1040 + dups_10
    )  # 1050 across 11 pages? Cap is 10 pages -> 105 per page on JSON!

    def p_side(req: httpx.Request) -> httpx.Response:
        page = int(req.url.params["page"])
        if page <= 10:
            chunk = all_players[(page - 1) * 105 : page * 105]
            return httpx.Response(200, content=render_electus_player_json(chunk))
        return httpx.Response(200, content=render_electus_player_json([]))

    respx_mock.get(url__regex=r"https://electus\.online/api/ranking/player.*").mock(
        side_effect=p_side
    )
    respx_mock.get("https://electus.online/api/ranking/job?page=1&limit=100").mock(
        return_value=httpx.Response(
            200,
            content=render_electus_job_json(
                [
                    JobRowSpec(
                        rank=1,
                        char_name=unique_1040[0].char_name,
                        job_type="THIEF",
                        job_level=99,
                        job_exp=777,
                    )
                ]
            ),
        )
    )
    respx_mock.get("https://electus.online/api/ranking/job?page=2&limit=100").mock(
        return_value=httpx.Response(200, content=render_electus_job_json([]))
    )
    g5 = make_guild_specs(5)
    g6 = [*g5, GuildRowSpec(rank=6, guild_name=g5[0].guild_name)]
    respx_mock.get("https://electus.online/api/ranking/guild?page=1&limit=50").mock(
        return_value=httpx.Response(200, content=render_electus_guild_json(g6))
    )
    respx_mock.get("https://electus.online/api/ranking/guild?page=2&limit=50").mock(
        return_value=httpx.Response(200, content=render_electus_guild_json([]))
    )

    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("electus", e2e_settings).run(clean_db)
    assert await count_rows(clean_db, "character_snapshots", "electus") == 1040
    assert await count_rows(clean_db, "guild_snapshots", "electus") == 5
    distinct_ts = (
        await clean_db.execute(
            text(
                "SELECT COUNT(DISTINCT created_at) "
                "FROM character_snapshots WHERE server_id='electus'"
            )
        )
    ).scalar_one()
    assert distinct_ts == 1
    first_rank = (
        await clean_db.execute(
            text(
                "SELECT cs.rank_position, cs.job_type FROM character_snapshots cs "
                "JOIN characters c ON c.id = cs.character_id WHERE c.char_name = :n"
            ),
            {"n": unique_1040[0].char_name},
        )
    ).one()
    assert first_rank[0] == 1 and first_rank[1] == "THIEF"


async def test_t3_12_cross_month_year_partition_rollover_and_lag_view(
    clean_db: AsyncSession,
) -> None:
    """T3-12: `2026_11` -> `2026_12` -> `2027_01` partitions & continuous `LAG()` deltas."""
    ensure_parts = cast(
        EnsureMonthlyPartitionsProto,
        require_symbol("src.core.database", "ensure_monthly_partitions"),
    )
    await ensure_parts(clean_db, reference_time=datetime(2026, 12, 31, 23, 55, tzinfo=UTC))
    parts = await list_table_partitions(clean_db, "character_snapshots")
    assert "character_snapshots_2027_01" in parts

    t_nov = datetime(2026, 11, 30, 23, 55, tzinfo=UTC)
    t_dec = datetime(2026, 12, 15, 12, 0, tzinfo=UTC)
    t_jan = datetime(2027, 1, 1, 0, 5, tzinfo=UTC)
    for idx, ts in enumerate((t_nov, t_dec, t_jan)):
        await seed_historical_run(
            clean_db,
            server_id="chronicle",
            created_at=ts,
            characters=[
                HistoricalCharSeed(
                    char_name="YearHero", rank_position=1, level=120, item_points=100.0 + idx * 50.0
                )
            ],
        )
    def_cnt = (
        await clean_db.execute(text("SELECT COUNT(*) FROM character_snapshots_default"))
    ).scalar_one()
    assert def_cnt == 0
    deltas = await query_view(clean_db, "v_character_activity_deltas", "char_name='YearHero'")
    assert len(deltas) == 2


async def test_t3_13_ensure_partitions_task_idempotent_in_prefect_harness(
    clean_db: AsyncSession,
) -> None:
    """T3-13: `ensure_partitions_task` executes idempotently twice in Prefect harness."""
    from prefect.testing.utilities import prefect_test_harness

    task_obj = cast(PrefectTaskProto, require_symbol("src.flows.tasks", "ensure_partitions_task"))
    with prefect_test_harness():
        r1 = await task_obj.fn()
        r2 = await task_obj.fn()
    assert set(r1) == set(r2)
    assert len(r1) == 4


async def test_t3_14_scheduled_12h_flow_isolation_and_quarantine_purge(
    clean_db: AsyncSession,
    e2e_quarantine_dir: Path,
    respx_mock: respx.MockRouter,
) -> None:
    """T3-14: `scheduled_12h_flow` isolates 403/503/200 and purges >30d `.html.gz`."""
    from prefect.testing.utilities import prefect_test_harness

    qchr = e2e_quarantine_dir / "chronicle"
    old_f = qchr / "old.html.gz"
    keep_f = qchr / "keep.html.gz"
    write_gz(old_f, b"old")
    write_gz(keep_f, b"keep")
    now_ts = pytime.time()
    os.utime(old_f, (now_ts - 31 * 86400, now_ts - 31 * 86400))
    os.utime(keep_f, (now_ts - 5 * 86400, now_ts - 5 * 86400))

    respx_mock.get(url__regex=r"https://play-chronicle\.com/.*").mock(
        return_value=httpx.Response(403, text=render_turnstile_html())
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
    respx_mock.get(url__regex=r"https://play-golden\.online/.*").mock(
        return_value=httpx.Response(503, text="Server under maintenance")
    )

    flow_fn = cast(PrefectFlowProto, require_symbol("src.flows.orchestrator", "scheduled_12h_flow"))
    with prefect_test_harness():
        await flow_fn()
    assert (await fetch_latest_scrape_run(clean_db, "chronicle") or {})[
        "status"
    ] == "BLOCKED_CHALLENGE"
    assert (await fetch_latest_scrape_run(clean_db, "electus") or {})["status"] == "SUCCESS"
    assert (await fetch_latest_scrape_run(clean_db, "play_golden") or {})[
        "status"
    ] == "TARGET_MAINTENANCE"
    assert (await fetch_server_row(clean_db, "play_golden"))["circuit_breaker_until"] is None
    assert not path_exists(old_f)
    assert path_exists(keep_f)


async def test_t3_15_electus_json_items_fallback_html_and_cf_cookie(
    clean_db: AsyncSession,
    e2e_quarantine_dir: Path,
    respx_mock: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T3-15: Electus JSON `items` + HTML fallback + `cf_clearance` cookie."""
    monkeypatch.setenv("ELECTUS_CF_CLEARANCE", "cf_abc_123")
    settings_cls = cast(SettingsFactoryProto, require_symbol("src.core.config", "Settings"))
    s = settings_cls(
        _env_file=None,
        database_url=os.getenv("DATABASE_URL", ""),
        quarantine_dir=e2e_quarantine_dir,
        electus_cf_clearance="cf_abc_123",
    )
    seen_cookies: list[str] = []

    def json_p(req: httpx.Request) -> httpx.Response:
        seen_cookies.append(req.headers.get("cookie", ""))
        page = req.url.params.get("page", "1")
        rows = make_player_specs(100) if page == "1" else []
        return httpx.Response(200, content=render_electus_player_json(rows, root_key="items"))

    respx_mock.get(url__regex=r"https://electus\.online/api/ranking/player.*").mock(
        side_effect=json_p
    )
    respx_mock.get("https://electus.online/api/ranking/job?page=1&limit=100").mock(
        return_value=httpx.Response(200, content=render_electus_job_json([], root_key="items"))
    )
    respx_mock.get("https://electus.online/api/ranking/guild?page=1&limit=50").mock(
        return_value=httpx.Response(
            200, content=render_electus_guild_json(make_guild_specs(5), root_key="items")
        )
    )
    respx_mock.get("https://electus.online/api/ranking/guild?page=2&limit=50").mock(
        return_value=httpx.Response(200, content=render_electus_guild_json([], root_key="items"))
    )

    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("electus", s).run(clean_db)
    assert any("cf_clearance=cf_abc_123" in c for c in seen_cookies)
    assert (await fetch_latest_scrape_run(clean_db, "electus") or {})["status"] == "SUCCESS"

    # Subcase B: API returns non-challenge HTML -> HTML fallback succeeds
    respx_mock.get(url__regex=r"https://electus\.online/api/ranking/player.*").mock(
        return_value=httpx.Response(200, text="<html><body>HTML only</body></html>")
    )
    respx_mock.get("https://electus.online/ranking/player?page=1").mock(
        return_value=httpx.Response(
            200, text=render_electus_player_html_fallback(make_player_specs(100))
        )
    )
    respx_mock.get("https://electus.online/ranking/player?page=2").mock(
        return_value=httpx.Response(200, text=render_electus_player_html_fallback([]))
    )
    await get_scraper("electus", s).run(clean_db)
    assert (await fetch_latest_scrape_run(clean_db, "electus") or {})["status"] == "SUCCESS"


async def test_t3_16_electus_turnstile_on_api_never_falls_back(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """T3-16: Turnstile HTML on Electus API trips CB immediately without HTML fallback."""
    respx_mock.get(url__regex=r"https://electus\.online/api/ranking/player.*").mock(
        return_value=httpx.Response(
            200, text=render_turnstile_html("Attention Required! | Cloudflare")
        )
    )
    fb = respx_mock.get(url__regex=r"https://electus\.online/ranking/.*").mock(
        return_value=httpx.Response(200, text="never called")
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("electus", e2e_settings).run(clean_db)
    assert fb.call_count == 0
    assert (await fetch_latest_scrape_run(clean_db, "electus") or {})[
        "status"
    ] == "BLOCKED_CHALLENGE"
    assert (await fetch_server_row(clean_db, "electus"))["circuit_breaker_until"] is not None


async def test_t3_17_503_maintenance_vs_transient_503_502_504_retries(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T3-17: Maintenance 503 (0 retries) vs transient 503/502 vs Timeout/504/502."""
    sleeps: list[float] = []

    async def fake_sleep(sec: float) -> None:
        sleeps.append(sec)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    scraper = get_scraper("chronicle", e2e_settings)

    # A: Maintenance 503
    r_a = respx_mock.get(url__regex=r"https://play-chronicle\.com/.*").mock(
        return_value=httpx.Response(503, text=render_maintenance_html("Website under maintenance"))
    )
    await scraper.run(clean_db)
    assert r_a.call_count == 1 and sleeps == []
    assert (await fetch_latest_scrape_run(clean_db, "chronicle") or {})[
        "status"
    ] == "TARGET_MAINTENANCE"

    # B: Transient 503 -> 502 -> 200 recovery
    _mock_chronicle_ok(respx_mock)
    respx_mock.get("https://play-chronicle.com/ranking/player?page=1").mock(
        side_effect=[
            httpx.Response(503, text="Service Unavailable"),
            httpx.Response(502, text="Bad Gateway"),
            httpx.Response(200, text=render_chronicle_player_html(make_player_specs(100))),
        ]
    )
    await scraper.run(clean_db)
    assert [int(x) for x in sleeps] == [2, 5]
    assert (await fetch_latest_scrape_run(clean_db, "chronicle") or {})["status"] == "SUCCESS"

    # C: Timeout -> 504 -> 502 -> HTTP_ERROR
    sleeps.clear()
    respx_mock.get("https://play-chronicle.com/ranking/player?page=1").mock(
        side_effect=[
            httpx.ReadTimeout("timeout"),
            httpx.Response(504, text="GW timeout"),
            httpx.Response(502, text="Bad GW"),
        ]
    )
    await scraper.run(clean_db)
    assert [int(x) for x in sleeps] == [2, 5]
    assert (await fetch_latest_scrape_run(clean_db, "chronicle") or {})["status"] == "HTTP_ERROR"


async def test_t3_18_semaphore_3_concurrency_and_180s_budget(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """T3-18: Full 10+5+2 Chronicle crawl stays `max_concurrent <= 3` and finishes << 180s."""
    in_flight = 0
    max_in_flight = 0
    lock = asyncio.Lock()

    async def p_side(req: httpx.Request) -> httpx.Response:
        nonlocal in_flight, max_in_flight
        async with lock:
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
        await asyncio.sleep(0.01)
        async with lock:
            in_flight -= 1
        page = int(req.url.params["page"])
        return httpx.Response(
            200,
            text=render_chronicle_player_html(
                make_player_specs(100, start_rank=(page - 1) * 100 + 1)
            ),
        )

    respx_mock.get(url__regex=r"https://play-chronicle\.com/ranking/player\?page=\d+").mock(
        side_effect=p_side
    )
    respx_mock.get(url__regex=r"https://play-chronicle\.com/ranking/job\?page=\d+").mock(
        side_effect=lambda req: httpx.Response(
            200,
            text=render_chronicle_job_html(
                make_job_specs(100, start_rank=(int(req.url.params["page"]) - 1) * 100 + 1)
            ),
        )
    )
    respx_mock.get(url__regex=r"https://play-chronicle\.com/ranking/guild\?page=\d+").mock(
        side_effect=lambda req: httpx.Response(
            200,
            text=render_chronicle_guild_html(
                make_guild_specs(50, start_rank=(int(req.url.params["page"]) - 1) * 50 + 1)
            ),
        )
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    t0 = pytime.perf_counter()
    await get_scraper("chronicle", e2e_settings).run(clean_db)
    elapsed = pytime.perf_counter() - t0
    assert max_in_flight <= 3
    assert elapsed < 30.0
    assert await count_rows(clean_db, "character_snapshots", "chronicle") == 1000


async def test_t3_19_post_fw_flow_cron_rollover_and_trigger_type(
    clean_db: AsyncSession,
    respx_mock: respx.MockRouter,
) -> None:
    """T3-19: `compute_post_fw_cron` rollover x `post_fw_flow('chronicle')`."""
    from prefect.testing.utilities import prefect_test_harness

    cron_fn = cast(
        ComputePostFwCronProto, require_symbol("src.flows.orchestrator", "compute_post_fw_cron")
    )
    assert cron_fn(0, time(22, 0)) == "30 22 * * 0"
    assert cron_fn(0, time(22, 45)) == "15 23 * * 0"
    assert cron_fn(6, time(23, 45)) == "15 0 * * 0"

    _mock_chronicle_ok(respx_mock)
    flow_fn = cast(PrefectFlowProto, require_symbol("src.flows.orchestrator", "post_fw_flow"))
    with prefect_test_harness():
        await flow_fn(server_id="chronicle")
    run_row = await fetch_latest_scrape_run(clean_db, "chronicle")
    assert run_row is not None
    assert run_row["trigger_type"] == "post_fw_extra"


async def test_t3_20_cli_manual_target_and_inactive_server_skip(
    clean_db: AsyncSession,
    respx_mock: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T3-20: `scheduled_12h_flow` skips inactive server & `--mode manual --target chronicle`."""
    from prefect.testing.utilities import prefect_test_harness

    await clean_db.execute(text("UPDATE servers SET is_active = FALSE WHERE id = 'play_golden'"))
    await clean_db.commit()

    _mock_chronicle_ok(respx_mock)
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
        return_value=httpx.Response(200, text="skip")
    )

    scheduled_flow = cast(
        PrefectFlowProto, require_symbol("src.flows.orchestrator", "scheduled_12h_flow")
    )

    def _run_cli_manual() -> None:
        try:
            runpy.run_module("src.flows.orchestrator", run_name="__main__")
        except SystemExit as exc:
            if exc.code not in (None, 0):
                raise

    with prefect_test_harness():
        # Path A: scheduled_12h_flow skips inactive play_golden while running chronicle & electus
        await scheduled_flow()
        assert await count_rows(clean_db, "scrape_runs", "chronicle") == 1
        assert await count_rows(clean_db, "scrape_runs", "electus") == 1
        assert await count_rows(clean_db, "scrape_runs", "play_golden") == 0
        assert golden_route.call_count == 0
        await clean_db.commit()

        # Path B: CLI `--mode manual --target chronicle` targets only chronicle
        monkeypatch.setattr(
            sys, "argv", ["orchestrator", "--mode", "manual", "--target", "chronicle"]
        )
        await asyncio.to_thread(_run_cli_manual)

    assert await count_rows(clean_db, "scrape_runs", "chronicle") == 2
    assert await count_rows(clean_db, "scrape_runs", "electus") == 1
    assert await count_rows(clean_db, "scrape_runs", "play_golden") == 0
    assert golden_route.call_count == 0

    r_chr = await fetch_latest_scrape_run(clean_db, "chronicle")
    assert r_chr is not None and r_chr["trigger_type"] == "manual"
    trig_rows = (
        await clean_db.execute(
            text(
                "SELECT trigger_type, COUNT(*) FROM scrape_runs "
                "WHERE server_id = 'chronicle' GROUP BY trigger_type"
            )
        )
    ).all()
    by_trig = {str(r[0]): int(r[1]) for r in trig_rows}
    assert by_trig == {"scheduled_12h": 1, "manual": 1}


def test_t3_21_cli_serve_builds_4_deployments_from_db_seeds(
    e2e_migrated_db: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T3-21: `--mode serve` registers `scheduled-12h` + 3 `post-fw-*` deployments."""
    import prefect

    captured_deployments: list[Any] = []

    def fake_serve(*deployments: Any, **kwargs: Any) -> None:
        captured_deployments.extend(deployments)

    monkeypatch.setattr(prefect, "serve", fake_serve)
    orch = require_module("src.flows.orchestrator")
    if hasattr(orch, "serve"):
        monkeypatch.setattr(orch, "serve", fake_serve)

    monkeypatch.setenv("DATABASE_URL", e2e_migrated_db)
    monkeypatch.setattr(sys, "argv", ["orchestrator", "--mode", "serve"])
    runpy.run_module("src.flows.orchestrator", run_name="__main__")

    names = {getattr(d, "name", None) for d in captured_deployments}
    assert {
        "scheduled-12h",
        "post-fw-chronicle",
        "post-fw-electus",
        "post-fw-play_golden",
    }.issubset(names)


async def test_t3_22_structlog_contextvars_isolation_across_targets(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """T3-22: `structlog` binds `server_id`/`run_id` per target without leaking between runs."""
    _mock_chronicle_ok(respx_mock)
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

    configure_logging = cast(
        Callable[[str], None], require_symbol("src.core.logging", "configure_logging")
    )
    get_logger = cast(Callable[[str], Any], require_symbol("src.core.logging", "get_logger"))
    configure_logging("INFO")
    _ = capsys.readouterr()

    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("chronicle", e2e_settings).run(clean_db)
    await get_scraper("electus", e2e_settings).run(clean_db)
    get_logger("after").info("post_runs_probe")

    logs = parse_json_log_lines(capsys.readouterr().out)
    chr_logs = [ln for ln in logs if ln.get("server_id") == "chronicle"]
    el_logs = [ln for ln in logs if ln.get("server_id") == "electus"]
    assert chr_logs and el_logs
    post_logs = [ln for ln in logs if ln.get("event") == "post_runs_probe"]
    assert post_logs and "server_id" not in post_logs[0]


async def test_t3_23_chronicle_45_50_early_stop_and_gin_extra_metrics(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """T3-23: Chronicle `'45/50'` pre-split x early empty `<tr>` stop x GIN `@>` query."""
    guilds = [
        GuildRowSpec(rank=1, guild_name="SplitGuild", members_count="45/50"),
        *make_guild_specs(5, start_rank=2),
    ]
    p3 = respx_mock.get("https://play-chronicle.com/ranking/player?page=3").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html([]))
    )
    _mock_chronicle_ok(
        respx_mock, players=make_player_specs(100), jobs=make_job_specs(5), guilds=guilds
    )
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("chronicle", e2e_settings).run(clean_db)

    assert p3.call_count == 0
    m_cnt = (
        await clean_db.execute(
            text(
                "SELECT gs.members_count FROM guild_snapshots gs "
                "JOIN guilds g ON g.id = gs.guild_id WHERE g.guild_name = 'SplitGuild'"
            )
        )
    ).scalar_one()
    assert m_cnt == 45
    gin_hits = (
        await clean_db.execute(
            text(
                "SELECT COUNT(*) FROM character_snapshots "
                "WHERE extra_metrics @> '{\"job_rank\": 1}'::jsonb"
            )
        )
    ).scalar_one()
    assert gin_hits == 1


async def test_t3_24_play_golden_race_img_and_v_race_balance(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """T3-24: `PlayGoldenScraper` race img/stats x `v_race_balance_ch_vs_eu`."""
    players = make_player_specs(50, race_cycle=("CH", "EU"))
    respx_mock.get("https://play-golden.online/ranking/characters?page=1").mock(
        return_value=httpx.Response(200, text=render_golden_player_html(players))
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
    rows = await query_view(clean_db, "v_race_balance_ch_vs_eu", "server_id = 'play_golden'")
    by_race = {r["race"]: r for r in rows}
    assert by_race["CH"]["total_ranked_characters"] == 25
    assert by_race["EU"]["total_ranked_characters"] == 25


async def test_t3_25_partial_success_vs_failed_runs_in_point_in_time_views(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """T3-25: Views reflect `PARTIAL_SUCCESS` and ignore subsequent `SCHEMA_DRIFT` failure."""
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    scraper = get_scraper("chronicle", e2e_settings)

    # T0: SUCCESS
    _mock_chronicle_ok(respx_mock, players=make_player_specs(100))
    await scraper.run(clean_db)

    # T1: PARTIAL_SUCCESS (103 players, 2 invalid low-rank -> 101 valid)
    p_t1 = make_player_specs(103, invalid_ranks={10: {"level": 999}, 11: {"level": 999}})
    _mock_chronicle_ok(respx_mock, players=p_t1)
    await scraper.run(clean_db)
    assert (await fetch_latest_scrape_run(clean_db, "chronicle") or {})[
        "status"
    ] == "PARTIAL_SUCCESS"

    # T2: SCHEMA_DRIFT (rank 1 missing)
    _mock_chronicle_ok(
        respx_mock, players=make_player_specs(100, invalid_ranks={1: {"level": 999}})
    )
    await scraper.run(clean_db)
    assert (await fetch_latest_scrape_run(clean_db, "chronicle") or {})["status"] == "SCHEMA_DRIFT"

    race_rows = await query_view(clean_db, "v_race_balance_ch_vs_eu", "server_id = 'chronicle'")
    assert sum(int(r["total_ranked_characters"]) for r in race_rows) == 101


async def test_t3_26_locale_floats_empty_guild_and_sem_guilda_view(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """T3-26: Locale floats + empty guild tokens -> `guild_id=NULL` -> `[SEM GUILDA]` in view."""
    players = make_player_specs(100, guild_cycle=("Titans", "Titans", "Titans", "-"))
    players[3] = PlayerRowSpec(
        rank=4, char_name="SoloEU", guild_name="XXXX", item_points="1.234,56"
    )
    players[7] = PlayerRowSpec(rank=8, char_name="SoloUS", guild_name="N/A", item_points="2,345.67")
    _mock_chronicle_ok(respx_mock, players=players)
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("chronicle", e2e_settings).run(clean_db)

    snap_res = await clean_db.execute(
        text(
            "SELECT c.char_name, cs.item_points, cs.guild_id "
            "FROM character_snapshots cs "
            "JOIN characters c ON c.id = cs.character_id "
            "WHERE cs.server_id = 'chronicle' AND c.char_name IN ('SoloEU', 'SoloUS')"
        )
    )
    by_char = {str(r[0]): (float(r[1]), r[2]) for r in snap_res.all()}
    assert by_char["SoloEU"][0] == pytest.approx(1234.56)
    assert by_char["SoloEU"][1] is None
    assert by_char["SoloUS"][0] == pytest.approx(2345.67)
    assert by_char["SoloUS"][1] is None

    rows = await query_view(clean_db, "v_guild_turnover_dominance", "server_id = 'chronicle'")
    by_g = {r["guild_name"]: r for r in rows}
    assert by_g["[SEM GUILDA]"]["players_in_top_100"] == 25
    assert float(by_g["[SEM GUILDA]"]["total_top_item_points"]) == pytest.approx(116050.23)


async def test_t3_27_prefect_task_retries_zero_single_scrape_run_invariant(
    clean_db: AsyncSession,
    respx_mock: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T3-27: `scrape_target_task` (`retries=0`) produces 1 `scrape_runs` row per failure."""
    from prefect.testing.utilities import prefect_test_harness

    async def fake_sleep(sec: float) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    respx_mock.get(url__regex=r"https://play-chronicle\.com/.*").mock(
        return_value=httpx.Response(502, text="Bad GW")
    )
    scrape_task = cast(PrefectTaskProto, require_symbol("src.flows.tasks", "scrape_target_task"))
    with prefect_test_harness():
        await scrape_task.fn(server_id="chronicle", trigger_type="manual")
    assert await count_rows(clean_db, "scrape_runs", "chronicle") == 1


async def test_t3_28_settings_env_overrides_drive_quarantine_cb_and_purge(
    clean_db: AsyncSession,
    tmp_path: Path,
    respx_mock: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T3-28: Custom `QUARANTINE_DIR`, `RETENTION=7`, `COOLDOWN=12`, `ELECTUS_CF_CLEARANCE`."""
    custom_q = tmp_path / "custom_q"
    monkeypatch.setenv("QUARANTINE_DIR", str(custom_q))
    monkeypatch.setenv("QUARANTINE_RETENTION_DAYS", "7")
    monkeypatch.setenv("CIRCUIT_BREAKER_COOLDOWN_HOURS", "12")
    monkeypatch.setenv("ELECTUS_CF_CLEARANCE", "tok2")
    cache_clear = getattr(require_symbol("src.core.config", "get_settings"), "cache_clear", None)
    if callable(cache_clear):
        cache_clear()

    settings_cls = cast(SettingsFactoryProto, require_symbol("src.core.config", "Settings"))
    s = settings_cls(_env_file=None)
    seen_cookies: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen_cookies.append(req.headers.get("cookie", ""))
        return httpx.Response(403, text=render_turnstile_html())

    respx_mock.get(url__regex=r"https://electus\.online/.*").mock(side_effect=handler)
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    await get_scraper("electus", s).run(clean_db)

    assert any("cf_clearance=tok2" in c for c in seen_cookies)
    run_row = await fetch_latest_scrape_run(clean_db, "electus")
    assert run_row is not None
    assert str(custom_q) in str(run_row["quarantine_file_path"])
    srv = await fetch_server_row(clean_db, "electus")
    assert srv["circuit_breaker_until"] > datetime.now(UTC) + timedelta(hours=11, minutes=55)

    old_f = custom_q / "electus" / "aged_8d.html.gz"
    write_gz(old_f, b"aged")
    old_ts = pytime.time() - 8 * 86400
    os.utime(old_f, (old_ts, old_ts))
    purge_task = cast(
        PrefectTaskProto, require_symbol("src.flows.tasks", "purge_old_quarantine_task")
    )
    purge_task.fn(retention_days=s.quarantine_retention_days)
    assert not path_exists(old_f)
