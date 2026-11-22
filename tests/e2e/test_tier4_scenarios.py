"""Tier 4 E2E Real-World Application Scenarios (14 End-to-End Scenarios T4-01..T4-14).

Scenarios T4-01..T4-09 and T4-13..T4-14 run with mocked HTTP (`respx`) + real PG16.
Scenarios T4-10..T4-12 are gated behind `@pytest.mark.live_e2e` (`SRO_RUN_LIVE_E2E=1`).
"""

from __future__ import annotations

import ast
import asyncio
import importlib
import json
import os
import re
import shutil
import sys
import time as pytime
import tomllib
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
import respx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from tests.e2e._contracts import (
    EnsureMonthlyPartitionsProto,
    GetScraperProto,
    PrefectFlowProto,
    SettingsFactoryProto,
)
from tests.e2e._loader import require_file, require_symbol
from tests.e2e.helpers.db_helpers import (
    HistoricalCharSeed,
    count_rows,
    fetch_latest_scrape_run,
    fetch_server_row,
    list_table_partitions,
    query_view,
    seed_historical_run,
)
from tests.e2e.helpers.fs_helpers import (
    path_exists,
    read_text,
    rglob_gz,
    rglob_py,
    write_text,
)
from tests.e2e.helpers.log_helpers import parse_json_log_lines
from tests.e2e.helpers.payload_factory import (
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
    render_turnstile_html,
)

pytestmark = [pytest.mark.e2e, pytest.mark.e2e_tier4]


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


@pytest.mark.e2e_db
async def test_t4_01_multi_target_12h_cycle_and_next_cycle_cb_skip(
    clean_db: AsyncSession,
    respx_mock: respx.MockRouter,
) -> None:
    """T4-01: Multi-target 12h cycle (Chronicle OK, Electus 403 CB, Golden PARTIAL) + Cycle 2."""
    from prefect.testing.utilities import prefect_test_harness

    # Cycle 1 mocks
    respx_mock.get("https://play-chronicle.com/ranking/player?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html(make_player_specs(100)))
    )
    respx_mock.get("https://play-chronicle.com/ranking/player?page=2").mock(
        return_value=httpx.Response(
            200, text=render_chronicle_player_html(make_player_specs(20, start_rank=101))
        )
    )
    respx_mock.get("https://play-chronicle.com/ranking/player?page=3").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html([]))
    )
    respx_mock.get("https://play-chronicle.com/ranking/job?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_job_html(make_job_specs(30)))
    )
    respx_mock.get("https://play-chronicle.com/ranking/job?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_job_html([]))
    )
    respx_mock.get("https://play-chronicle.com/ranking/guild?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_guild_html(make_guild_specs(8)))
    )
    respx_mock.get("https://play-chronicle.com/ranking/guild?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_guild_html([]))
    )

    el_route = respx_mock.get(url__regex=r"https://electus\.online/.*").mock(
        return_value=httpx.Response(403, text=render_turnstile_html())
    )

    golden_bad = {10: {"level": 999}, 20: {"level": 999}, 30: {"level": 999}}
    respx_mock.get("https://play-golden.online/ranking/characters?page=1").mock(
        return_value=httpx.Response(
            200, text=render_golden_player_html(make_player_specs(100, invalid_ranks=golden_bad))
        )
    )
    respx_mock.get("https://play-golden.online/ranking/characters?page=2").mock(
        return_value=httpx.Response(200, text=render_golden_player_html([]))
    )
    respx_mock.get("https://play-golden.online/ranking/jobs?page=1").mock(
        return_value=httpx.Response(200, text=render_golden_job_html([]))
    )
    respx_mock.get("https://play-golden.online/ranking/guilds?page=1").mock(
        return_value=httpx.Response(200, text=render_golden_guild_html(make_guild_specs(6)))
    )
    respx_mock.get("https://play-golden.online/ranking/guilds?page=2").mock(
        return_value=httpx.Response(200, text=render_golden_guild_html([]))
    )

    flow_fn = cast(PrefectFlowProto, require_symbol("src.flows.orchestrator", "scheduled_12h_flow"))
    with prefect_test_harness():
        await flow_fn()

    assert (await fetch_latest_scrape_run(clean_db, "chronicle") or {})["status"] == "SUCCESS"
    r_el = await fetch_latest_scrape_run(clean_db, "electus")
    assert r_el is not None and r_el["status"] == "BLOCKED_CHALLENGE"
    assert path_exists(str(r_el["quarantine_file_path"]))
    r_pg = await fetch_latest_scrape_run(clean_db, "play_golden")
    assert r_pg is not None and r_pg["status"] == "PARTIAL_SUCCESS"
    assert len(r_pg["error_details"]["sample_errors"]) == 3
    assert await count_rows(clean_db, "character_snapshots", "chronicle") == 120
    assert await count_rows(clean_db, "character_snapshots", "play_golden") == 97
    assert el_route.call_count == 1

    # Cycle 2: Electus must be skipped via persisted CB; Chronicle & Golden append-only
    with prefect_test_harness():
        await flow_fn()

    assert el_route.call_count == 1
    assert await count_rows(clean_db, "scrape_runs", "electus") == 1
    assert await count_rows(clean_db, "scrape_runs", "chronicle") == 2
    assert await count_rows(clean_db, "scrape_runs", "play_golden") == 2
    assert await count_rows(clean_db, "character_snapshots", "chronicle") == 240
    assert await count_rows(clean_db, "character_snapshots", "play_golden") == 194


@pytest.mark.e2e_db
async def test_t4_02_three_day_progression_and_behavioral_classification(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """T4-02: 72h (6 intervals) progression across 5 archetypes + live ChronicleScraper run."""
    now = datetime.now(UTC)
    for step in range(6):
        ts = now - timedelta(hours=12 * (6 - step))
        chars = [
            HistoricalCharSeed(
                char_name="BotFarmer",
                rank_position=1,
                level=100 + step,
                item_points=100.0 + step * 5.0,
                job_exp=1000 * (step + 1),
            ),
            HistoricalCharSeed(
                char_name="WhaleDonor",
                rank_position=2,
                level=120 + (1 if step == 1 else 0),
                item_points=100.0 + (200.0 if step >= 2 else 0.0),
            ),
            HistoricalCharSeed(
                char_name="HardcoreAce",
                rank_position=3,
                level=110 + (step if step <= 4 else 4),
                item_points=100.0 + (min(step, 4) * 10.0),
            ),
            HistoricalCharSeed(
                char_name="CasualPlayer",
                rank_position=4,
                level=100 + (1 if step >= 1 else 0) + (1 if step >= 2 else 0),
                item_points=100.0 + (15.0 if step >= 2 else 0.0),
            ),
        ]
        if step >= 4:
            chars.append(
                HistoricalCharSeed(
                    char_name="Newcomer", rank_position=5, level=90, item_points=50.0
                )
            )
        await seed_historical_run(clean_db, server_id="chronicle", created_at=ts, characters=chars)

    # T_now (step 6 = +72h): live ChronicleScraper run with 100 players
    live_players = [
        PlayerRowSpec(rank=1, char_name="BotFarmer", level=106, item_points="130.00"),
        PlayerRowSpec(rank=2, char_name="WhaleDonor", level=121, item_points="300.00"),
        PlayerRowSpec(rank=3, char_name="HardcoreAce", level=114, item_points="140.00"),
        PlayerRowSpec(rank=4, char_name="CasualPlayer", level=102, item_points="115.00"),
        PlayerRowSpec(rank=5, char_name="Newcomer", level=91, item_points="60.00"),
        *make_player_specs(95, start_rank=6, prefix="Filler"),
    ]
    respx_mock.get("https://play-chronicle.com/ranking/player?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html(live_players))
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
                        char_name="BotFarmer",
                        job_type="HUNTER",
                        job_level=90,
                        job_exp=999999,
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

    deltas = await query_view(clean_db, "v_character_activity_deltas", "char_name = 'BotFarmer'")
    assert len(deltas) == 6
    assert float(deltas[-1]["hours_since_last_snapshot"]) == pytest.approx(12.0, abs=0.1)

    classes = await query_view(clean_db, "v_bot_vs_donor_classification")
    seg = {r["char_name"]: r["player_behavior_segment"] for r in classes}
    assert seg["BotFarmer"] == "SUSPECTED_BOT_24_7"
    assert seg["WhaleDonor"] == "SUSPECTED_DONATOR_WHALE"
    assert seg["HardcoreAce"] == "HARDCORE_HUMAN"
    assert seg["CasualPlayer"] == "CASUAL_OR_BALANCED"
    assert "Newcomer" not in seg


@pytest.mark.e2e_db
async def test_t4_03_guild_power_race_balance_and_multi_week_retention(
    clean_db: AsyncSession,
) -> None:
    """T4-03: 120 players across 2 weeks querying dominance, race balance, and retention."""
    w1_t0 = datetime(2026, 11, 16, 0, 0, tzinfo=UTC)
    w1_t1 = datetime(2026, 11, 16, 12, 0, tzinfo=UTC)
    w2_t0 = datetime(2026, 11, 23, 0, 0, tzinfo=UTC)
    w2_t1 = datetime(2026, 11, 23, 12, 0, tzinfo=UTC)

    def build_chars(week_gain: float, active_half: bool) -> list[HistoricalCharSeed]:
        out: list[HistoricalCharSeed] = []
        for r in range(1, 121):
            race = "CH" if r % 3 == 0 else ("EU" if r % 3 == 1 else "UNK")
            gname = "Apex" if r <= 30 else ("Vanguard" if r <= 70 else None)
            pts = 500.0 + (week_gain if (not active_half or r <= 60) else 0.0)
            out.append(
                HistoricalCharSeed(
                    char_name=f"WChar_{r:03d}",
                    rank_position=r,
                    level=120,
                    item_points=pts,
                    race=race,
                    guild_name=gname,
                )
            )
        return out

    await seed_historical_run(
        clean_db, server_id="chronicle", created_at=w1_t0, characters=build_chars(0.0, False)
    )
    await seed_historical_run(
        clean_db, server_id="chronicle", created_at=w1_t1, characters=build_chars(20.0, False)
    )
    await seed_historical_run(
        clean_db, server_id="chronicle", created_at=w2_t0, characters=build_chars(20.0, True)
    )
    await seed_historical_run(
        clean_db, server_id="chronicle", created_at=w2_t1, characters=build_chars(40.0, True)
    )

    dom = await query_view(clean_db, "v_guild_turnover_dominance", "server_id = 'chronicle'")
    by_g = {r["guild_name"]: r for r in dom}
    assert by_g["Apex"]["players_in_top_100"] == 30
    assert by_g["Apex"]["players_in_top_50"] == 30
    assert by_g["[SEM GUILDA]"]["players_in_top_100"] == 30

    races = await query_view(clean_db, "v_race_balance_ch_vs_eu", "server_id = 'chronicle'")
    assert {r["race"] for r in races} == {"CH", "EU", "UNK"}
    assert sum(int(r["total_ranked_characters"]) for r in races) == 120

    ret = await query_view(clean_db, "v_server_retention_weekly", "server_id = 'chronicle'")
    assert len(ret) == 2
    assert float(ret[0]["activity_retention_rate_pct"]) == pytest.approx(50.0)
    assert float(ret[1]["activity_retention_rate_pct"]) == pytest.approx(100.0)


@pytest.mark.e2e_db
async def test_t4_04_post_fw_flow_and_rate_per_hour_normalization(
    clean_db: AsyncSession,
    respx_mock: respx.MockRouter,
) -> None:
    """T4-04: Irregular 3.5h post-FW interval produces normalized 10.00/hr rate."""
    from prefect.testing.utilities import prefect_test_harness

    now = datetime.now(UTC)
    t_prior = now - timedelta(hours=3, minutes=30)
    await seed_historical_run(
        clean_db,
        server_id="chronicle",
        created_at=t_prior,
        characters=[
            HistoricalCharSeed(char_name="FwHero", rank_position=1, level=120, item_points=100.0)
        ],
    )

    players = [
        PlayerRowSpec(rank=1, char_name="FwHero", level=120, item_points="135.00"),
        *make_player_specs(99, start_rank=2),
    ]
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
        return_value=httpx.Response(200, text=render_chronicle_guild_html(make_guild_specs(5)))
    )
    respx_mock.get("https://play-chronicle.com/ranking/guild?page=2").mock(
        return_value=httpx.Response(200, text=render_chronicle_guild_html([]))
    )

    flow_fn = cast(PrefectFlowProto, require_symbol("src.flows.orchestrator", "post_fw_flow"))
    with prefect_test_harness():
        await flow_fn(server_id="chronicle")

    run_row = await fetch_latest_scrape_run(clean_db, "chronicle")
    assert run_row is not None and run_row["trigger_type"] == "post_fw_extra"
    deltas = await query_view(clean_db, "v_character_activity_deltas", "char_name = 'FwHero'")
    assert len(deltas) == 1
    assert float(deltas[0]["hours_since_last_snapshot"]) == pytest.approx(3.5, abs=0.1)
    assert float(deltas[0]["item_points_rate_per_hour"]) == pytest.approx(10.0, abs=0.2)


@pytest.mark.e2e_db
async def test_t4_05_end_to_end_quarantine_lifecycle_and_retention_purge(
    clean_db: AsyncSession,
    e2e_settings: Any,
    e2e_quarantine_dir: Path,
    respx_mock: respx.MockRouter,
) -> None:
    """T4-05: Generate SCHEMA_DRIFT, LOW_COUNT, BLOCKED_CHALLENGE, age >30d, purge via flow."""
    from prefect.testing.utilities import prefect_test_harness

    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))

    # 1. SCHEMA_DRIFT on chronicle
    respx_mock.get("https://play-chronicle.com/ranking/player?page=1").mock(
        return_value=httpx.Response(
            200,
            text=render_chronicle_player_html(
                make_player_specs(100, invalid_ranks={1: {"level": 999}})
            ),
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
    await get_scraper("chronicle", e2e_settings).run(clean_db)

    # 2. LOW_COUNT_QUARANTINE on play_golden
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
    await get_scraper("play_golden", e2e_settings).run(clean_db)

    # 3. BLOCKED_CHALLENGE on electus
    respx_mock.get(url__regex=r"https://electus\.online/.*").mock(
        return_value=httpx.Response(403, text=render_turnstile_html())
    )
    await get_scraper("electus", e2e_settings).run(clean_db)

    gz_files = rglob_gz(e2e_quarantine_dir)
    assert len(gz_files) == 3
    now_ts = pytime.time()
    for old_f in gz_files[:2]:
        os.utime(old_f, (now_ts - 31 * 86400, now_ts - 31 * 86400))
    os.utime(gz_files[2], (now_ts - 10 * 86400, now_ts - 10 * 86400))
    gitkeep = e2e_quarantine_dir / ".gitkeep"
    write_text(gitkeep, "")

    # Run scheduled_12h_flow (with clean responses for chronicle/golden; electus is CB-skipped)
    respx_mock.get("https://play-chronicle.com/ranking/player?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html(make_player_specs(100)))
    )
    respx_mock.get("https://play-golden.online/ranking/characters?page=1").mock(
        return_value=httpx.Response(200, text=render_golden_player_html(make_player_specs(50)))
    )
    flow_fn = cast(PrefectFlowProto, require_symbol("src.flows.orchestrator", "scheduled_12h_flow"))
    with prefect_test_harness():
        await flow_fn()

    assert not path_exists(gz_files[0])
    assert not path_exists(gz_files[1])
    assert path_exists(gz_files[2])
    assert path_exists(gitkeep)
    assert await count_rows(clean_db, "scrape_runs") >= 5


@pytest.mark.e2e_db
async def test_t4_06_month_and_year_end_partition_rollover_continuity(
    clean_db: AsyncSession,
) -> None:
    """T4-06: Nov 2026 -> Dec 2026 -> Jan 2027 rollover with 0 rows in default partition."""
    ensure_parts = cast(
        EnsureMonthlyPartitionsProto,
        require_symbol("src.core.database", "ensure_monthly_partitions"),
    )
    await ensure_parts(clean_db, reference_time=datetime(2026, 12, 31, 23, 55, tzinfo=UTC))
    assert "character_snapshots_2027_01" in await list_table_partitions(
        clean_db, "character_snapshots"
    )
    assert "guild_snapshots_2027_01" in await list_table_partitions(clean_db, "guild_snapshots")

    timestamps = [
        datetime(2026, 11, 30, 12, 0, tzinfo=UTC),
        datetime(2026, 12, 15, 12, 0, tzinfo=UTC),
        datetime(2027, 1, 1, 0, 5, tzinfo=UTC),
    ]
    for i, ts in enumerate(timestamps):
        await seed_historical_run(
            clean_db,
            server_id="chronicle",
            created_at=ts,
            characters=[
                HistoricalCharSeed(
                    char_name="RollHero", rank_position=1, level=120, item_points=100.0 + i * 25.0
                )
            ],
        )
    assert (
        await clean_db.execute(text("SELECT COUNT(*) FROM character_snapshots_default"))
    ).scalar_one() == 0
    deltas = await query_view(clean_db, "v_character_activity_deltas", "char_name = 'RollHero'")
    assert len(deltas) == 2


@pytest.mark.e2e_db
async def test_t4_07_high_scale_full_pagination_and_chunked_inserts(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """T4-07: 1,200 players + 500 jobs + 100 guilds within Semaphore(3) and chunked inserts."""
    in_flight = 0
    max_in_flight = 0
    lock = asyncio.Lock()

    async def p_side(req: httpx.Request) -> httpx.Response:
        nonlocal in_flight, max_in_flight
        async with lock:
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
        await asyncio.sleep(0.005)
        async with lock:
            in_flight -= 1
        page = int(req.url.params["page"])
        rows = make_player_specs(120, start_rank=(page - 1) * 120 + 1)
        return httpx.Response(200, text=render_chronicle_player_html(rows))

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
    assert await count_rows(clean_db, "character_snapshots", "chronicle") == 1200
    assert await count_rows(clean_db, "guild_snapshots", "chronicle") == 100


@pytest.mark.e2e_db
async def test_t4_08_electus_multi_phase_resilience(
    clean_db: AsyncSession,
    e2e_settings: Any,
    e2e_quarantine_dir: Path,
    respx_mock: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T4-08: Electus 4 phases: JSON -> HTML fallback -> 403 Turnstile CB -> cookie recovery."""
    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    scraper = get_scraper("electus", e2e_settings)
    players = make_player_specs(100)

    # Phase 1: JSON API
    respx_mock.get("https://electus.online/api/ranking/player?page=1&limit=100").mock(
        return_value=httpx.Response(200, content=render_electus_player_json(players))
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
    await scraper.run(clean_db)
    assert (await fetch_latest_scrape_run(clean_db, "electus") or {})["status"] == "SUCCESS"

    # Phase 2: HTML fallback
    respx_mock.get("https://electus.online/api/ranking/player?page=1&limit=100").mock(
        return_value=httpx.Response(200, text="<html><body>Fallback</body></html>")
    )
    respx_mock.get("https://electus.online/ranking/player?page=1").mock(
        return_value=httpx.Response(200, text=render_electus_player_html_fallback(players))
    )
    respx_mock.get("https://electus.online/ranking/player?page=2").mock(
        return_value=httpx.Response(200, text=render_electus_player_html_fallback([]))
    )
    await scraper.run(clean_db)
    assert (await fetch_latest_scrape_run(clean_db, "electus") or {})["status"] == "SUCCESS"

    # Phase 3: Turnstile 403 -> CB + quarantine
    respx_mock.get("https://electus.online/api/ranking/player?page=1&limit=100").mock(
        return_value=httpx.Response(403, text=render_turnstile_html())
    )
    await scraper.run(clean_db)
    assert (await fetch_latest_scrape_run(clean_db, "electus") or {})[
        "status"
    ] == "BLOCKED_CHALLENGE"

    # Phase 4: Expire CB + set refreshed ELECTUS_CF_CLEARANCE -> JSON recovery
    await clean_db.execute(
        text(
            "UPDATE servers SET circuit_breaker_until = CURRENT_TIMESTAMP - INTERVAL '1 minute' "
            "WHERE id = 'electus'"
        )
    )
    await clean_db.commit()
    monkeypatch.setenv("ELECTUS_CF_CLEARANCE", "refreshed_tok")
    settings_cls = cast(SettingsFactoryProto, require_symbol("src.core.config", "Settings"))
    s2 = settings_cls(
        _env_file=None,
        database_url=os.getenv("DATABASE_URL", ""),
        quarantine_dir=e2e_quarantine_dir,
        electus_cf_clearance="refreshed_tok",
    )
    seen_cookies: list[str] = []

    def p4(req: httpx.Request) -> httpx.Response:
        seen_cookies.append(req.headers.get("cookie", ""))
        return httpx.Response(200, content=render_electus_player_json(players))

    respx_mock.get("https://electus.online/api/ranking/player?page=1&limit=100").mock(
        side_effect=p4
    )
    await get_scraper("electus", s2).run(clean_db)
    assert any("cf_clearance=refreshed_tok" in c for c in seen_cookies)
    assert (await fetch_latest_scrape_run(clean_db, "electus") or {})["status"] == "SUCCESS"
    assert await count_rows(clean_db, "characters", "electus") == 100


@pytest.mark.e2e_db
async def test_t4_09_network_flap_recovery_vs_maintenance_vs_resumption(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T4-09: Flap (502/Timeout -> 200) -> 503 maintenance (no CB) -> immediate 200 recovery."""

    async def fake_sleep(sec: float) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
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

    p1 = respx_mock.get("https://play-golden.online/ranking/characters?page=1")
    p1.side_effect = [
        httpx.Response(502, text="Bad Gateway"),
        httpx.ReadTimeout("timeout"),
        httpx.Response(200, text=render_golden_player_html(make_player_specs(50))),
        httpx.Response(503, text="Server under maintenance"),
        httpx.Response(200, text=render_golden_player_html(make_player_specs(50))),
    ]

    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    scraper = get_scraper("play_golden", e2e_settings)

    await scraper.run(clean_db)
    assert (await fetch_latest_scrape_run(clean_db, "play_golden") or {})["status"] == "SUCCESS"
    await scraper.run(clean_db)
    assert (await fetch_latest_scrape_run(clean_db, "play_golden") or {})[
        "status"
    ] == "TARGET_MAINTENANCE"
    assert (await fetch_server_row(clean_db, "play_golden"))["circuit_breaker_until"] is None
    await scraper.run(clean_db)
    assert (await fetch_latest_scrape_run(clean_db, "play_golden") or {})["status"] == "SUCCESS"


@pytest.mark.live_e2e
@pytest.mark.e2e_docker
@pytest.mark.e2e_db
async def test_t4_10_live_docker_compose_build_up_and_alembic_idempotency(
    repo_root: Path,
    e2e_migrated_db: str,
    docker_available: None,
) -> None:
    """T4-10: Live `docker compose build`, `up -d --wait`, healthchecks, and idempotent Alembic."""
    for cmd in (
        ["docker", "compose", "build"],
        ["docker", "compose", "up", "-d", "--wait", "--wait-timeout", "180"],
    ):
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=repo_root,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr_b = await asyncio.wait_for(proc.communicate(), timeout=300)
        assert proc.returncode == 0, stderr_b.decode("utf-8", errors="replace")

    ps_proc = await asyncio.create_subprocess_exec(
        "docker",
        "compose",
        "ps",
        "--format",
        "json",
        cwd=repo_root,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    ps_out_b, ps_err_b = await asyncio.wait_for(ps_proc.communicate(), timeout=30)
    assert ps_proc.returncode == 0, ps_err_b.decode("utf-8", errors="replace")
    ps_raw = ps_out_b.decode("utf-8", errors="replace").strip()
    assert ps_raw, "Empty `docker compose ps --format json` output"
    if ps_raw.startswith("["):
        services_info = cast(list[dict[str, Any]], json.loads(ps_raw))
    else:
        services_info = [
            cast(dict[str, Any], json.loads(line)) for line in ps_raw.splitlines() if line.strip()
        ]
    by_service = {
        str(item.get("Service") or item.get("Name") or ""): item for item in services_info
    }
    for svc in ("postgres", "prefect-server", "scraper-worker"):
        assert svc in by_service, f"Missing service {svc} in compose ps: {list(by_service)}"
        state = str(by_service[svc].get("State", "")).lower()
        assert state == "running", f"Service {svc} not running: {by_service[svc]}"
        health = str(by_service[svc].get("Health") or "").strip().lower()
        if health:
            assert health == "healthy", f"Service {svc} unhealthy: {by_service[svc]}"

    for _ in range(2):
        mig = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "alembic",
            "upgrade",
            "head",
            cwd=repo_root,
            env={**os.environ, "DATABASE_URL": e2e_migrated_db},
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, mig_err = await asyncio.wait_for(mig.communicate(), timeout=60)
        assert mig.returncode == 0, mig_err.decode("utf-8", errors="replace")


@pytest.mark.live_e2e
@pytest.mark.e2e_db
async def test_t4_11_live_cli_manual_chronicle_ingestion_and_view_query(
    repo_root: Path,
    clean_db: AsyncSession,
    e2e_migrated_db: str,
) -> None:
    """T4-11: Live `--mode manual --target chronicle` populates DB & queries activity deltas."""
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
    logs = parse_json_log_lines(stdout_b.decode("utf-8", errors="replace"))
    assert any(ln.get("server_id") == "chronicle" for ln in logs)

    run_row = await fetch_latest_scrape_run(clean_db, "chronicle")
    assert run_row is not None
    assert run_row["trigger_type"] == "manual"
    assert run_row["status"] in {"SUCCESS", "PARTIAL_SUCCESS"}
    assert await count_rows(clean_db, "character_snapshots", "chronicle") > 0
    assert await count_rows(clean_db, "guild_snapshots", "chronicle") > 0
    _ = await query_view(clean_db, "v_character_activity_deltas")


@pytest.mark.live_e2e
@pytest.mark.e2e_docker
async def test_t4_12_live_prefect_deployments_and_worker_memory_under_512mib(
    repo_root: Path,
    docker_available: None,
) -> None:
    """T4-12: Verify live Prefect 3 deployments & `scraper-worker` RSS < 512MiB."""
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

    proc = await asyncio.create_subprocess_exec(
        "docker",
        "stats",
        "--no-stream",
        "--format",
        "{{.Name}} {{.MemUsage}}",
        cwd=repo_root,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out_b, _ = await asyncio.wait_for(proc.communicate(), timeout=30)
    assert proc.returncode == 0
    stats_out = out_b.decode("utf-8", errors="replace")
    assert "scraper-worker" in stats_out
    worker_line = next(
        (ln for ln in stats_out.splitlines() if "scraper-worker" in ln),
        "",
    )
    mem_usage_part = (
        worker_line.split(maxsplit=1)[1].split("/")[0] if " " in worker_line else worker_line
    )
    mem_match = re.search(r"(\d+(?:\.\d+)?)\s*(KiB|MiB|GiB|kB|MB|GB|B)\b", mem_usage_part)
    assert mem_match is not None, f"Could not parse memory usage from: {worker_line!r}"
    val = float(mem_match.group(1))
    unit = mem_match.group(2)
    mib_map = {
        "B": 1.0 / (1024.0 * 1024.0),
        "KiB": 1 / 1024,
        "kB": 1 / 1024,
        "MiB": 1.0,
        "MB": 1.0,
        "GiB": 1024.0,
        "GB": 1024.0,
    }
    assert val * mib_map[unit] < 512.0, f"scraper-worker exceeded 512MiB: {worker_line!r}"


@pytest.mark.e2e_db
async def test_t4_13_non_negotiable_architecture_security_and_quality_audit(
    repo_root: Path,
    clean_db: AsyncSession,
) -> None:
    """T4-13: Full audit of banned deps, HTML-in-PG columns, secrets, ruff, and mypy strict."""
    pyproject = tomllib.loads(read_text(require_file("pyproject.toml", repo_root)))
    deps_text = "\n".join(str(d).lower() for d in pyproject["project"]["dependencies"])
    for banned in (
        "playwright",
        "selenium",
        "puppeteer",
        "airflow",
        "pyspark",
        "2captcha",
        "anticaptcha",
    ):
        assert banned not in deps_text

    # Verify PostgreSQL information_schema has no raw HTML columns
    cols_res = await clean_db.execute(
        text(
            "SELECT table_name, column_name FROM information_schema.columns "
            "WHERE table_schema = 'public'"
        )
    )
    col_pairs = {(str(r[0]), str(r[1])) for r in cols_res.all()}
    for tbl, col in col_pairs:
        assert "raw_html" not in col and "html_body" not in col, (
            f"Unexpected raw HTML col {tbl}.{col}"
        )

    # Verify compose network isolation
    compose = _load_yaml(read_text(require_file("docker-compose.yml", repo_root)))
    assert "sro_network" in str(compose["services"]["postgres"].get("networks", ""))

    # Verify no hardcoded credentials in src/
    secret_re = re.compile(r"postgresql(?:\+asyncpg)?://[^:\s]+:[^@\s]+@")
    for py_file in rglob_py(repo_root / "src"):
        tree = ast.parse(read_text(py_file), filename=str(py_file))
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                assert not secret_re.search(node.value), f"Hardcoded credential in {py_file}"

    for cmd in (
        [_venv_tool("ruff"), "check", "."],
        [_venv_tool("ruff"), "format", "--check", "."],
        [_venv_tool("mypy"), "src", "tests"],
    ):
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=repo_root,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out_b, err_b = await asyncio.wait_for(proc.communicate(), timeout=60)
        assert proc.returncode == 0, (
            f"{cmd} failed:\n{out_b.decode('utf-8', 'replace')}\n{err_b.decode('utf-8', 'replace')}"
        )


@pytest.mark.e2e_db
async def test_t4_14_schema_drift_quarantine_and_clean_lag_recovery(
    clean_db: AsyncSession,
    e2e_settings: Any,
    respx_mock: respx.MockRouter,
) -> None:
    """T4-14: T0 SUCCESS -> T1 SCHEMA_DRIFT quarantined -> T2 (+24h) clean LAG() delta."""
    now = datetime.now(UTC)
    t0 = now - timedelta(hours=24)
    await seed_historical_run(
        clean_db,
        server_id="chronicle",
        created_at=t0,
        characters=[
            HistoricalCharSeed(char_name="GapHero", rank_position=1, level=120, item_points=100.0)
        ],
    )

    get_scraper = cast(GetScraperProto, require_symbol("src.scrapers", "get_scraper"))
    scraper = get_scraper("chronicle", e2e_settings)

    # T1: SCHEMA_DRIFT (rank 1 missing)
    respx_mock.get("https://play-chronicle.com/ranking/player?page=1").mock(
        return_value=httpx.Response(
            200, text=render_chronicle_player_html(make_player_specs(100, start_rank=2))
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
    await scraper.run(clean_db)
    r_t1 = await fetch_latest_scrape_run(clean_db, "chronicle")
    assert r_t1 is not None and r_t1["status"] == "SCHEMA_DRIFT"
    assert path_exists(str(r_t1["quarantine_file_path"]))
    assert await count_rows(clean_db, "character_snapshots", "chronicle") == 1

    # T2: Recovery (+120 item_points over 24h = 5.00/hr)
    rec_players = [
        PlayerRowSpec(rank=1, char_name="GapHero", level=120, item_points="220.00"),
        *make_player_specs(99, start_rank=2),
    ]
    respx_mock.get("https://play-chronicle.com/ranking/player?page=1").mock(
        return_value=httpx.Response(200, text=render_chronicle_player_html(rec_players))
    )
    await scraper.run(clean_db)

    deltas = await query_view(clean_db, "v_character_activity_deltas", "char_name = 'GapHero'")
    assert len(deltas) == 1
    assert float(deltas[0]["hours_since_last_snapshot"]) == pytest.approx(24.0, abs=0.1)
    assert float(deltas[0]["item_points_rate_per_hour"]) == pytest.approx(5.0, abs=0.1)
