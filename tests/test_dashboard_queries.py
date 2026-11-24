"""Unit and opt-in PostgreSQL 16 integration tests for `src.dashboard.queries`."""

from __future__ import annotations

import os
import re
import uuid
from collections.abc import Generator
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from src.dashboard.queries import (
    BOT_VS_DONOR_SQL,
    GUILD_DOMINANCE_SQL,
    LATEST_VALID_RUN_SQL,
    LIST_ACTIVE_SERVERS_SQL,
    RACE_BALANCE_SQL,
    TOP_CHARACTERS_DRILLDOWN_SQL,
    TOP_N_ITEM_POINTS_EVOLUTION_SQL,
    WEEKLY_RETENTION_SQL,
    get_bot_vs_donor_classification,
    get_guild_dominance,
    get_race_balance,
    get_server_freshness,
    get_top_characters_drilldown,
    get_top_n_item_points_evolution,
    get_weekly_retention,
    list_active_servers,
)
from src.dashboard.schemas import (
    BehaviorClassificationRow,
    GuildDominanceRow,
    RaceBalanceRow,
    ServerFreshness,
    ServerOption,
    TopCharacterTrendPoint,
    TopNAggregatePoint,
    WeeklyRetentionRow,
)

# ---------------------------------------------------------------------------
# Helpers for mocked SQLAlchemy Session
# ---------------------------------------------------------------------------


def _make_mock_session(results: list[list[dict[str, Any]]]) -> MagicMock:
    """Build a mock Session returning sequential mapping results for each execute()."""
    session = MagicMock(spec=Session)
    side_effects: list[MagicMock] = []
    for rows in results:
        mapping_result = MagicMock()
        mapping_result.all.return_value = rows
        mapping_result.first.return_value = rows[0] if rows else None
        exec_result = MagicMock()
        exec_result.mappings.return_value = mapping_result
        side_effects.append(exec_result)
    session.execute.side_effect = side_effects
    return session


# ---------------------------------------------------------------------------
# 1. SQL Partition Pruning & Read-Only Contract Tests
# ---------------------------------------------------------------------------


def test_sql_constants_are_strictly_read_only_and_parameterized() -> None:
    all_sqls = (
        LIST_ACTIVE_SERVERS_SQL,
        LATEST_VALID_RUN_SQL,
        TOP_N_ITEM_POINTS_EVOLUTION_SQL,
        TOP_CHARACTERS_DRILLDOWN_SQL,
        GUILD_DOMINANCE_SQL,
        RACE_BALANCE_SQL,
        WEEKLY_RETENTION_SQL,
        BOT_VS_DONOR_SQL,
    )
    forbidden = re.compile(r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE)\b", re.IGNORECASE)
    for clause in all_sqls:
        sql_text = clause.text
        assert not forbidden.search(sql_text), f"Write keyword found in SQL: {sql_text[:80]}"
        assert "%s" not in sql_text
        assert "{" not in sql_text


def test_sql_partition_pruning_contracts() -> None:
    # Active servers only
    assert "is_active = TRUE" in LIST_ACTIVE_SERVERS_SQL.text

    # View 1: temporal bound + rank <= 100
    assert "created_at >= :window_start" in TOP_N_ITEM_POINTS_EVOLUTION_SQL.text
    assert "rank_position <= 100" in TOP_N_ITEM_POINTS_EVOLUTION_SQL.text

    # View 1 drilldown: 2-step scalar bindings + temporal bound
    assert "scrape_run_id = :run_id" in TOP_CHARACTERS_DRILLDOWN_SQL.text
    assert "created_at BETWEEN :win_start AND :win_end" in TOP_CHARACTERS_DRILLDOWN_SQL.text
    assert "created_at >= :window_start" in TOP_CHARACTERS_DRILLDOWN_SQL.text

    # View 2: 2-step scalar bindings + rank <= 100 + [SEM GUILDA] + NULLIF
    assert "scrape_run_id = :run_id" in GUILD_DOMINANCE_SQL.text
    assert "created_at BETWEEN :win_start AND :win_end" in GUILD_DOMINANCE_SQL.text
    assert "rank_position <= 100" in GUILD_DOMINANCE_SQL.text
    assert "[SEM GUILDA]" in GUILD_DOMINANCE_SQL.text
    assert "NULLIF(st.sample_total_players, 0)" in GUILD_DOMINANCE_SQL.text

    # View 3: 2-step scalar bindings, NO rank_position <= 100 in WHERE
    race_sql = RACE_BALANCE_SQL.text
    assert "scrape_run_id = :run_id" in race_sql
    assert "created_at BETWEEN :win_start AND :win_end" in race_sql
    where_section = race_sql.rsplit("WHERE", maxsplit=1)[1]
    assert "rank_position <= 100" not in where_section

    # View 4 & View 5: lookback_start filter MUST precede LAG()
    for sql_clause in (WEEKLY_RETENTION_SQL, BOT_VS_DONOR_SQL):
        sql_text = sql_clause.text
        lookback_idx = sql_text.index("created_at >= :lookback_start")
        lag_idx = sql_text.index("LAG(")
        assert lookback_idx < lag_idx


# ---------------------------------------------------------------------------
# 2. Parameter Validation Tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad_days", [0, -1, 186, 365])
def test_view1_rejects_invalid_window_days(bad_days: int) -> None:
    session = _make_mock_session([])
    with pytest.raises(ValueError, match="window_days"):
        get_top_n_item_points_evolution(session, "chronicle", window_days=bad_days)
    with pytest.raises(ValueError, match="window_days"):
        get_top_characters_drilldown(session, "chronicle", window_days=bad_days, top_n=5)
    session.execute.assert_not_called()


@pytest.mark.parametrize("bad_top_n", [0, -3, 11, 50])
def test_view1_drilldown_rejects_invalid_top_n(bad_top_n: int) -> None:
    session = _make_mock_session([])
    with pytest.raises(ValueError, match="top_n"):
        get_top_characters_drilldown(session, "chronicle", window_days=30, top_n=bad_top_n)
    session.execute.assert_not_called()


@pytest.mark.parametrize("bad_weeks", [0, 1, -5, 27, 52])
def test_view4_rejects_invalid_window_weeks(bad_weeks: int) -> None:
    session = _make_mock_session([])
    with pytest.raises(ValueError, match="window_weeks"):
        get_weekly_retention(session, "chronicle", window_weeks=bad_weeks)
    session.execute.assert_not_called()


# ---------------------------------------------------------------------------
# 3. Mocked Session Unit Tests (DTO Mapping & Edge Cases)
# ---------------------------------------------------------------------------


def test_list_active_servers_maps_dtos() -> None:
    session = _make_mock_session(
        [
            [
                {"id": "chronicle", "display_name": "Chronicle Online", "min_records_expected": 50},
                {"id": "play_golden", "display_name": "Play Golden", "min_records_expected": 30},
            ]
        ]
    )
    result = list_active_servers(session)
    assert result == [
        ServerOption(id="chronicle", display_name="Chronicle Online", min_records_expected=50),
        ServerOption(id="play_golden", display_name="Play Golden", min_records_expected=30),
    ]


def test_get_server_freshness_present_and_none() -> None:
    ts = datetime(2026, 11, 22, 18, 0, tzinfo=UTC)
    session_with_run = _make_mock_session([[{"id": uuid.uuid4(), "created_at": ts}]])
    fresh = get_server_freshness(session_with_run, "chronicle")
    assert fresh == ServerFreshness(server_id="chronicle", last_snapshot_at=ts)

    session_empty = _make_mock_session([[]])
    empty = get_server_freshness(session_empty, "new_srv")
    assert empty == ServerFreshness(server_id="new_srv", last_snapshot_at=None)


def test_view1_evolution_and_drilldown_edge_cases() -> None:
    ref = datetime(2026, 11, 23, 12, 0, tzinfo=UTC)
    session = _make_mock_session(
        [
            [
                {
                    "snapshot_day": date(2026, 11, 22),
                    "tier": "Top 10",
                    "avg_item_points": Decimal("520.50"),
                    "median_item_points": Decimal("515.00"),
                    "p90_item_points": Decimal("590.25"),
                    "rank1_item_points": Decimal("620.00"),
                    "sample_size": 10,
                },
                {
                    "snapshot_day": date(2026, 11, 22),
                    "tier": "Top 100",
                    "avg_item_points": Decimal("380.00"),
                    "median_item_points": Decimal("370.00"),
                    "p90_item_points": Decimal("510.00"),
                    "rank1_item_points": Decimal("620.00"),
                    "sample_size": 100,
                },
            ]
        ]
    )
    points = get_top_n_item_points_evolution(
        session, "chronicle", window_days=30, reference_time=ref
    )
    assert len(points) == 2
    assert points[0] == TopNAggregatePoint(
        snapshot_day=date(2026, 11, 22),
        tier="Top 10",
        avg_item_points=520.5,
        median_item_points=515.0,
        p90_item_points=590.25,
        rank1_item_points=620.0,
        sample_size=10,
    )
    params = session.execute.call_args[0][1]
    assert params["window_start"] == ref - timedelta(days=30)

    # Drill-down empty when no valid run exists
    empty_session = _make_mock_session([[]])
    assert get_top_characters_drilldown(empty_session, "chronicle", window_days=30, top_n=5) == []
    assert empty_session.execute.call_count == 1

    # Drill-down populated
    run_id = uuid.uuid4()
    run_ts = datetime(2026, 11, 22, 18, 0, tzinfo=UTC)
    dd_session = _make_mock_session(
        [
            [{"id": run_id, "created_at": run_ts}],
            [
                {
                    "snapshot_day": date(2026, 11, 22),
                    "char_name": "HeroOne",
                    "rank_position": 1,
                    "item_points": Decimal("620.00"),
                }
            ],
        ]
    )
    dd_points = get_top_characters_drilldown(
        dd_session, "chronicle", window_days=30, top_n=5, reference_time=ref
    )
    assert dd_points == [
        TopCharacterTrendPoint(
            snapshot_day=date(2026, 11, 22),
            char_name="HeroOne",
            rank_position=1,
            item_points=620.0,
        )
    ]
    dd_params = dd_session.execute.call_args_list[1][0][1]
    assert dd_params["run_id"] == run_id
    assert dd_params["win_start"] == run_ts - timedelta(hours=1)
    assert dd_params["win_end"] == run_ts + timedelta(hours=1)
    assert dd_params["top_n"] == 5


def test_view2_guild_dominance_small_server_and_no_guild() -> None:
    run_id = uuid.uuid4()
    run_ts = datetime(2026, 11, 22, 12, 0, tzinfo=UTC)
    session = _make_mock_session(
        [
            [{"id": run_id, "created_at": run_ts}],
            [
                {
                    "guild_name": "GoldenKnights",
                    "players_in_top_100": 18,
                    "players_in_top_50": 12,
                    "total_top_item_points": Decimal("4500.50"),
                    "nominal_top_100_pct": Decimal("18.00"),
                    "sample_share_pct": Decimal("45.00"),
                    "sample_total_players": 40,
                },
                {
                    "guild_name": "[SEM GUILDA]",
                    "players_in_top_100": 22,
                    "players_in_top_50": 10,
                    "total_top_item_points": Decimal("3900.00"),
                    "nominal_top_100_pct": Decimal("22.00"),
                    "sample_share_pct": Decimal("55.00"),
                    "sample_total_players": 40,
                },
            ],
        ]
    )
    rows = get_guild_dominance(session, "play_golden")
    assert rows == [
        GuildDominanceRow(
            guild_name="GoldenKnights",
            players_in_top_100=18,
            players_in_top_50=12,
            total_top_item_points=4500.5,
            nominal_top_100_pct=18.0,
            sample_share_pct=45.0,
            sample_total_players=40,
        ),
        GuildDominanceRow(
            guild_name="[SEM GUILDA]",
            players_in_top_100=22,
            players_in_top_50=10,
            total_top_item_points=3900.0,
            nominal_top_100_pct=22.0,
            sample_share_pct=55.0,
            sample_total_players=40,
        ),
    ]
    assert get_guild_dominance(_make_mock_session([[]]), "play_golden") == []


def test_view3_race_balance_ch_only_and_unk() -> None:
    run_id = uuid.uuid4()
    run_ts = datetime(2026, 11, 22, 12, 0, tzinfo=UTC)
    session = _make_mock_session(
        [
            [{"id": run_id, "created_at": run_ts}],
            [
                {
                    "race": "CH",
                    "total_ranked_characters": 140,
                    "top_100_count": 98,
                    "avg_level": Decimal("109.5"),
                    "avg_item_points": Decimal("410.25"),
                    "max_item_points": Decimal("650.00"),
                },
                {
                    "race": "UNK",
                    "total_ranked_characters": 2,
                    "top_100_count": 2,
                    "avg_level": Decimal("110.0"),
                    "avg_item_points": Decimal("390.00"),
                    "max_item_points": Decimal("400.00"),
                },
            ],
        ]
    )
    rows = get_race_balance(session, "chronicle")
    assert rows == [
        RaceBalanceRow(
            race="CH",
            total_ranked_characters=140,
            top_100_count=98,
            avg_level=109.5,
            avg_item_points=410.25,
            max_item_points=650.0,
        ),
        RaceBalanceRow(
            race="UNK",
            total_ranked_characters=2,
            top_100_count=2,
            avg_level=110.0,
            avg_item_points=390.0,
            max_item_points=400.0,
        ),
    ]
    assert get_race_balance(_make_mock_session([[]]), "chronicle") == []


def test_view4_weekly_retention_iso_monday_alignment_and_current_week_flag() -> None:
    # 2026-11-25 is Wednesday; ISO week Monday is 2026-11-23
    ref = datetime(2026, 11, 25, 15, 30, tzinfo=UTC)
    session = _make_mock_session(
        [
            [
                {
                    "week_start": date(2026, 11, 16),
                    "total_tracked_characters": 100,
                    "active_characters": 82,
                    "activity_retention_rate_pct": Decimal("82.00"),
                },
                {
                    "week_start": date(2026, 11, 23),
                    "total_tracked_characters": 95,
                    "active_characters": 60,
                    "activity_retention_rate_pct": Decimal("63.16"),
                },
            ]
        ]
    )
    rows = get_weekly_retention(session, "chronicle", window_weeks=12, reference_time=ref)
    assert rows == [
        WeeklyRetentionRow(
            week_start=date(2026, 11, 16),
            total_tracked_characters=100,
            active_characters=82,
            activity_retention_rate_pct=82.0,
            is_current_week=False,
        ),
        WeeklyRetentionRow(
            week_start=date(2026, 11, 23),
            total_tracked_characters=95,
            active_characters=60,
            activity_retention_rate_pct=63.16,
            is_current_week=True,
        ),
    ]
    params = session.execute.call_args[0][1]
    expected_current_monday = datetime(2026, 11, 23, 0, 0, tzinfo=UTC)
    expected_window_start = expected_current_monday - timedelta(weeks=11)
    assert params["window_start"] == expected_window_start
    assert params["lookback_start"] == expected_window_start - timedelta(days=7)

    # Empty when < 2 snapshots exist
    assert get_weekly_retention(_make_mock_session([[]]), "new_srv", window_weeks=12) == []


def test_view5_bot_vs_donor_four_segments_and_empty() -> None:
    ref = datetime(2026, 11, 23, 12, 0, tzinfo=UTC)
    session = _make_mock_session(
        [
            [
                {
                    "char_name": "AutoFarm01",
                    "race": "CH",
                    "evaluated_intervals": 10,
                    "total_item_points_gain": Decimal("40.00"),
                    "max_single_jump_item_points": Decimal("10.00"),
                    "grind_persistence_ratio": Decimal("0.95"),
                    "player_behavior_segment": "SUSPECTED_BOT_24_7",
                },
                {
                    "char_name": "CreditCardKing",
                    "race": "EU",
                    "evaluated_intervals": 8,
                    "total_item_points_gain": Decimal("260.00"),
                    "max_single_jump_item_points": Decimal("210.00"),
                    "grind_persistence_ratio": Decimal("0.25"),
                    "player_behavior_segment": "SUSPECTED_DONATOR_WHALE",
                },
                {
                    "char_name": "DedicatedPlayer",
                    "race": "CH",
                    "evaluated_intervals": 10,
                    "total_item_points_gain": Decimal("75.00"),
                    "max_single_jump_item_points": Decimal("25.00"),
                    "grind_persistence_ratio": Decimal("0.70"),
                    "player_behavior_segment": "HARDCORE_HUMAN",
                },
                {
                    "char_name": "WeekendWarrior",
                    "race": "UNK",
                    "evaluated_intervals": 6,
                    "total_item_points_gain": Decimal("15.00"),
                    "max_single_jump_item_points": Decimal("10.00"),
                    "grind_persistence_ratio": Decimal("0.33"),
                    "player_behavior_segment": "CASUAL_OR_BALANCED",
                },
            ]
        ]
    )
    rows = get_bot_vs_donor_classification(session, "chronicle", reference_time=ref)
    assert [r.player_behavior_segment for r in rows] == [
        "SUSPECTED_BOT_24_7",
        "SUSPECTED_DONATOR_WHALE",
        "HARDCORE_HUMAN",
        "CASUAL_OR_BALANCED",
    ]
    assert rows[0] == BehaviorClassificationRow(
        char_name="AutoFarm01",
        race="CH",
        evaluated_intervals=10,
        total_item_points_gain=40.0,
        max_single_jump_item_points=10.0,
        grind_persistence_ratio=0.95,
        player_behavior_segment="SUSPECTED_BOT_24_7",
    )
    params = session.execute.call_args[0][1]
    assert params["window_start"] == ref - timedelta(days=7)
    assert params["lookback_start"] == ref - timedelta(days=9)

    assert get_bot_vs_donor_classification(_make_mock_session([[]]), "new_srv") == []


# ---------------------------------------------------------------------------
# 4. Opt-In Live PostgreSQL 16 Integration & Partition Pruning Suite
# ---------------------------------------------------------------------------


def _collect_relation_names(plan_node: dict[str, Any]) -> set[str]:
    names: set[str] = set()
    rel = plan_node.get("Relation Name")
    if isinstance(rel, str):
        names.add(rel)
    for child in plan_node.get("Plans", ()):
        if isinstance(child, dict):
            names.update(_collect_relation_names(child))
    return names


@pytest.fixture
def pg_integration_session() -> Generator[Session, None, None]:
    url = os.environ.get("TEST_POSTGRES_URL", "").strip()
    if not url:
        pytest.skip("TEST_POSTGRES_URL not set")

    engine = create_engine(url, pool_pre_ping=True)
    connection = engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection)
    try:
        yield session
    finally:
        session.close()
        transaction.rollback()
        connection.close()
        engine.dispose()


@pytest.mark.postgres
def test_postgres_queries_and_partition_pruning(pg_integration_session: Session) -> None:
    """Execute all 8 dashboard queries against live PostgreSQL 16 and verify pruning."""
    session = pg_integration_session
    server_id = f"dash_it_{uuid.uuid4().hex[:8]}"
    ref = datetime(2026, 12, 15, 12, 0, tzinfo=UTC)

    # Ensure monthly partitions exist for Oct, Nov, Dec 2026
    for year, month, next_year, next_month in (
        (2026, 10, 2026, 11),
        (2026, 11, 2026, 12),
        (2026, 12, 2027, 1),
    ):
        part_name = f"character_snapshots_{year}_{month:02d}"
        session.execute(
            text(
                f"CREATE TABLE IF NOT EXISTS {part_name} "
                f"PARTITION OF character_snapshots "
                f"FOR VALUES FROM ('{year}-{month:02d}-01 00:00:00+00') "
                f"TO ('{next_year}-{next_month:02d}-01 00:00:00+00')"
            )
        )

    session.execute(
        text(
            "INSERT INTO servers (id, display_name, base_url, is_active, min_records_expected) "
            "VALUES (:id, 'Dashboard IT Server', 'https://example.invalid', TRUE, 10)"
        ),
        {"id": server_id},
    )

    guild_id = uuid.uuid4()
    session.execute(
        text(
            "INSERT INTO guilds (id, server_id, guild_name, first_seen_at, last_seen_at) "
            "VALUES (:id, :sid, 'AlphaGuild', :ts, :ts)"
        ),
        {"id": guild_id, "sid": server_id, "ts": ref - timedelta(days=50)},
    )

    char_ids = [uuid.uuid4(), uuid.uuid4()]
    for cid, cname, race in (
        (char_ids[0], "BotChar", "CH"),
        (char_ids[1], "WhaleChar", "EU"),
    ):
        session.execute(
            text(
                "INSERT INTO characters "
                "(id, server_id, char_name, race, first_seen_at, last_seen_at) "
                "VALUES (:id, :sid, :name, :race, :ts, :ts)"
            ),
            {
                "id": cid,
                "sid": server_id,
                "name": cname,
                "race": race,
                "ts": ref - timedelta(days=50),
            },
        )

    # Insert 1 old October snapshot (should be pruned in Dec queries) + 8 recent December snapshots
    timestamps = [datetime(2026, 10, 5, 12, 0, tzinfo=UTC)] + [
        ref - timedelta(days=7 - i) for i in range(8)
    ]
    latest_run_id: uuid.UUID | None = None
    latest_run_ts: datetime | None = None
    for step, ts in enumerate(timestamps):
        run_id = uuid.uuid4()
        latest_run_id = run_id
        latest_run_ts = ts
        session.execute(
            text(
                "INSERT INTO scrape_runs "
                "(id, server_id, ranking_category, trigger_type, status, records_extracted, "
                "records_persisted, execution_time_ms, created_at, updated_at) "
                "VALUES (:id, :sid, 'all', 'SCHEDULED', 'SUCCESS', 2, 2, 100, :ts, :ts)"
            ),
            {"id": run_id, "sid": server_id, "ts": ts},
        )
        # BotChar grinds every interval
        session.execute(
            text(
                "INSERT INTO character_snapshots "
                "(scrape_run_id, server_id, character_id, guild_id, rank_position, level, race, "
                "item_points, job_points, created_at, updated_at) "
                "VALUES (:rid, :sid, :cid, :gid, 1, :lvl, 'CH', :ip, :jp, :ts, :ts)"
            ),
            {
                "rid": run_id,
                "sid": server_id,
                "cid": char_ids[0],
                "gid": guild_id,
                "lvl": 100 + step,
                "ip": Decimal("400.00") + Decimal(step * 5),
                "jp": step * 10,
                "ts": ts,
            },
        )
        # WhaleChar has a +200 jump on the last step and low grind persistence
        whale_ip = Decimal("300.00") if step < len(timestamps) - 1 else Decimal("500.00")
        session.execute(
            text(
                "INSERT INTO character_snapshots "
                "(scrape_run_id, server_id, character_id, guild_id, rank_position, level, race, "
                "item_points, job_points, created_at, updated_at) "
                "VALUES (:rid, :sid, :cid, NULL, 2, 100, 'EU', :ip, 0, :ts, :ts)"
            ),
            {
                "rid": run_id,
                "sid": server_id,
                "cid": char_ids[1],
                "ip": whale_ip,
                "ts": ts,
            },
        )

    assert latest_run_id is not None and latest_run_ts is not None

    # Execute all 8 queries
    servers = list_active_servers(session)
    assert any(s.id == server_id for s in servers)

    freshness = get_server_freshness(session, server_id)
    assert freshness.last_snapshot_at == latest_run_ts

    evolution = get_top_n_item_points_evolution(
        session, server_id, window_days=14, reference_time=ref
    )
    assert len(evolution) > 0

    drilldown = get_top_characters_drilldown(
        session, server_id, window_days=14, top_n=2, reference_time=ref
    )
    assert len(drilldown) > 0

    guilds = get_guild_dominance(session, server_id)
    assert {g.guild_name for g in guilds} == {"AlphaGuild", "[SEM GUILDA]"}

    races = get_race_balance(session, server_id)
    assert {r.race for r in races} == {"CH", "EU"}

    retention = get_weekly_retention(session, server_id, window_weeks=2, reference_time=ref)
    assert len(retention) >= 1

    radar = get_bot_vs_donor_classification(session, server_id, reference_time=ref)
    segments = {r.char_name: r.player_behavior_segment for r in radar}
    assert segments["BotChar"] == "SUSPECTED_BOT_24_7"
    assert segments["WhaleChar"] == "SUSPECTED_DONATOR_WHALE"

    # Verify partition pruning via EXPLAIN (FORMAT JSON): October partition must be pruned
    win_start = latest_run_ts - timedelta(hours=1)
    win_end = latest_run_ts + timedelta(hours=1)
    current_week_start = (ref - timedelta(days=ref.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    retention_window_start = current_week_start - timedelta(weeks=1)
    explain_cases: list[tuple[str, dict[str, Any]]] = [
        (
            TOP_N_ITEM_POINTS_EVOLUTION_SQL.text,
            {"server_id": server_id, "window_start": ref - timedelta(days=10)},
        ),
        (
            TOP_CHARACTERS_DRILLDOWN_SQL.text,
            {
                "server_id": server_id,
                "run_id": latest_run_id,
                "win_start": win_start,
                "win_end": win_end,
                "top_n": 2,
                "window_start": ref - timedelta(days=10),
            },
        ),
        (
            GUILD_DOMINANCE_SQL.text,
            {
                "server_id": server_id,
                "run_id": latest_run_id,
                "win_start": win_start,
                "win_end": win_end,
            },
        ),
        (
            RACE_BALANCE_SQL.text,
            {
                "server_id": server_id,
                "run_id": latest_run_id,
                "win_start": win_start,
                "win_end": win_end,
            },
        ),
        (
            WEEKLY_RETENTION_SQL.text,
            {
                "server_id": server_id,
                "lookback_start": retention_window_start - timedelta(days=7),
                "window_start": retention_window_start,
            },
        ),
        (
            BOT_VS_DONOR_SQL.text,
            {
                "server_id": server_id,
                "lookback_start": ref - timedelta(days=9),
                "window_start": ref - timedelta(days=7),
            },
        ),
    ]
    for query_sql, params in explain_cases:
        plan_json = session.execute(text(f"EXPLAIN (FORMAT JSON) {query_sql}"), params).scalar_one()
        plan_root = plan_json[0]["Plan"]
        relations = _collect_relation_names(plan_root)
        assert "character_snapshots_2026_10" not in relations
