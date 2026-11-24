"""Pure SQLAlchemy read-only analytical queries with PostgreSQL 16 partition pruning.

Classification thresholds in `BOT_VS_DONOR_SQL` and activity delta rules in
`WEEKLY_RETENTION_SQL` mirror the analytical view contract defined in
`alembic/versions/002_analytical_views.py` while adding explicit temporal bounds
on `created_at` before `LAG()` for deterministic partition pruning.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any, Final, cast
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import TextClause

from src.dashboard.schemas import (
    BehaviorClassificationRow,
    BehaviorSegment,
    GuildDominanceRow,
    RaceBalanceRow,
    RaceCode,
    ServerFreshness,
    ServerOption,
    TopCharacterTrendPoint,
    TopNAggregatePoint,
    TopNTier,
    WeeklyRetentionRow,
)

_VALID_RACES: Final[frozenset[str]] = frozenset({"CH", "EU", "UNK"})
_VALID_TIERS: Final[frozenset[str]] = frozenset({"Top 10", "Top 50", "Top 100"})
_VALID_SEGMENTS: Final[frozenset[str]] = frozenset(
    {
        "SUSPECTED_BOT_24_7",
        "SUSPECTED_DONATOR_WHALE",
        "HARDCORE_HUMAN",
        "CASUAL_OR_BALANCED",
    }
)

LIST_ACTIVE_SERVERS_SQL: Final[TextClause] = text(
    """
    SELECT id, display_name, min_records_expected
    FROM servers
    WHERE is_active = TRUE
    ORDER BY display_name ASC
    """
)

LATEST_VALID_RUN_SQL: Final[TextClause] = text(
    """
    SELECT id, created_at
    FROM scrape_runs
    WHERE server_id = :server_id
      AND status IN ('SUCCESS', 'PARTIAL_SUCCESS')
    ORDER BY created_at DESC
    LIMIT 1
    """
)

TOP_N_ITEM_POINTS_EVOLUTION_SQL: Final[TextClause] = text(
    """
    WITH daily_latest AS (
        SELECT DISTINCT ON (
            (DATE_TRUNC('day', cs.created_at, 'UTC') AT TIME ZONE 'UTC')::date,
            cs.character_id
        )
            (DATE_TRUNC('day', cs.created_at, 'UTC') AT TIME ZONE 'UTC')::date AS snapshot_day,
            cs.character_id,
            cs.rank_position,
            cs.item_points
        FROM character_snapshots cs
        WHERE cs.server_id = :server_id
          AND cs.created_at >= :window_start
          AND cs.rank_position <= 100
        ORDER BY
            (DATE_TRUNC('day', cs.created_at, 'UTC') AT TIME ZONE 'UTC')::date,
            cs.character_id,
            cs.created_at DESC
    ),
    daily_rank1 AS (
        SELECT
            snapshot_day,
            COALESCE(
                MAX(item_points) FILTER (WHERE rank_position = 1),
                MAX(item_points)
            ) AS rank1_item_points
        FROM daily_latest
        GROUP BY snapshot_day
    ),
    tiers(tier, max_rank) AS (
        VALUES
            ('Top 10', 10),
            ('Top 50', 50),
            ('Top 100', 100)
    )
    SELECT
        dl.snapshot_day,
        t.tier,
        ROUND(AVG(dl.item_points)::numeric, 2) AS avg_item_points,
        ROUND(
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dl.item_points)::numeric,
            2
        ) AS median_item_points,
        ROUND(
            PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY dl.item_points)::numeric,
            2
        ) AS p90_item_points,
        ROUND(dr1.rank1_item_points::numeric, 2) AS rank1_item_points,
        COUNT(*) AS sample_size
    FROM daily_latest dl
    JOIN daily_rank1 dr1 ON dr1.snapshot_day = dl.snapshot_day
    JOIN tiers t ON dl.rank_position <= t.max_rank
    GROUP BY dl.snapshot_day, t.tier, dr1.rank1_item_points
    ORDER BY dl.snapshot_day ASC, t.tier ASC
    """
)

TOP_CHARACTERS_DRILLDOWN_SQL: Final[TextClause] = text(
    """
    WITH latest_top_chars AS (
        SELECT cs.character_id
        FROM character_snapshots cs
        WHERE cs.server_id = :server_id
          AND cs.scrape_run_id = :run_id
          AND cs.created_at BETWEEN :win_start AND :win_end
          AND cs.rank_position <= :top_n
    ),
    daily_series AS (
        SELECT DISTINCT ON (
            (DATE_TRUNC('day', cs.created_at, 'UTC') AT TIME ZONE 'UTC')::date,
            cs.character_id
        )
            (DATE_TRUNC('day', cs.created_at, 'UTC') AT TIME ZONE 'UTC')::date AS snapshot_day,
            c.char_name,
            cs.rank_position,
            cs.item_points
        FROM character_snapshots cs
        JOIN latest_top_chars ltc ON ltc.character_id = cs.character_id
        JOIN characters c ON c.id = cs.character_id
        WHERE cs.server_id = :server_id
          AND cs.created_at >= :window_start
        ORDER BY
            (DATE_TRUNC('day', cs.created_at, 'UTC') AT TIME ZONE 'UTC')::date,
            cs.character_id,
            cs.created_at DESC
    )
    SELECT
        snapshot_day,
        char_name,
        rank_position,
        ROUND(item_points::numeric, 2) AS item_points
    FROM daily_series
    ORDER BY snapshot_day ASC, rank_position ASC
    """
)

GUILD_DOMINANCE_SQL: Final[TextClause] = text(
    """
    WITH top_players AS (
        SELECT
            COALESCE(g.guild_name, '[SEM GUILDA]') AS guild_name,
            cs.rank_position,
            cs.item_points
        FROM character_snapshots cs
        LEFT JOIN guilds g ON g.id = cs.guild_id
        WHERE cs.server_id = :server_id
          AND cs.scrape_run_id = :run_id
          AND cs.created_at BETWEEN :win_start AND :win_end
          AND cs.rank_position <= 100
    ),
    sample_totals AS (
        SELECT COUNT(*) AS sample_total_players
        FROM top_players
    )
    SELECT
        tp.guild_name,
        COUNT(*) AS players_in_top_100,
        COUNT(*) FILTER (WHERE tp.rank_position <= 50) AS players_in_top_50,
        ROUND(SUM(tp.item_points)::numeric, 2) AS total_top_item_points,
        ROUND(CAST(COUNT(*) * 100.0 / 100 AS NUMERIC), 2) AS nominal_top_100_pct,
        ROUND(
            CAST(COUNT(*) * 100.0 / NULLIF(st.sample_total_players, 0) AS NUMERIC),
            2
        ) AS sample_share_pct,
        st.sample_total_players
    FROM top_players tp
    CROSS JOIN sample_totals st
    GROUP BY tp.guild_name, st.sample_total_players
    ORDER BY players_in_top_100 DESC, total_top_item_points DESC
    """
)

RACE_BALANCE_SQL: Final[TextClause] = text(
    """
    SELECT
        COALESCE(cs.race, c.race, 'UNK') AS race,
        COUNT(*) AS total_ranked_characters,
        COUNT(*) FILTER (WHERE cs.rank_position <= 100) AS top_100_count,
        ROUND(AVG(cs.level)::numeric, 1) AS avg_level,
        ROUND(AVG(cs.item_points)::numeric, 2) AS avg_item_points,
        ROUND(MAX(cs.item_points)::numeric, 2) AS max_item_points
    FROM character_snapshots cs
    JOIN characters c ON c.id = cs.character_id
    WHERE cs.server_id = :server_id
      AND cs.scrape_run_id = :run_id
      AND cs.created_at BETWEEN :win_start AND :win_end
    GROUP BY COALESCE(cs.race, c.race, 'UNK')
    ORDER BY total_ranked_characters DESC
    """
)

WEEKLY_RETENTION_SQL: Final[TextClause] = text(
    """
    WITH windowed_snapshots AS (
        SELECT
            cs.server_id,
            cs.character_id,
            cs.level,
            cs.item_points,
            cs.job_points,
            cs.job_exp,
            cs.created_at
        FROM character_snapshots cs
        WHERE cs.server_id = :server_id
          AND cs.created_at >= :lookback_start
    ),
    sequenced AS (
        SELECT
            ws.character_id,
            ws.created_at,
            (ws.level - LAG(ws.level) OVER w) AS delta_level,
            (ws.item_points - LAG(ws.item_points) OVER w) AS delta_item_points,
            (ws.job_points - LAG(ws.job_points) OVER w) AS delta_job_points,
            (ws.job_exp - LAG(ws.job_exp) OVER w) AS delta_job_exp,
            LAG(ws.created_at) OVER w AS prev_created_at
        FROM windowed_snapshots ws
        WINDOW w AS (
            PARTITION BY ws.server_id, ws.character_id
            ORDER BY ws.created_at
        )
    )
    SELECT
        (DATE_TRUNC('week', created_at, 'UTC') AT TIME ZONE 'UTC')::date AS week_start,
        COUNT(DISTINCT character_id) AS total_tracked_characters,
        COUNT(DISTINCT character_id) FILTER (WHERE
            COALESCE(delta_level, 0) > 0
            OR COALESCE(delta_item_points, 0) > 0
            OR COALESCE(delta_job_points, 0) > 0
            OR COALESCE(delta_job_exp, 0) > 0
        ) AS active_characters,
        ROUND(
            CAST(
                COUNT(DISTINCT character_id) FILTER (WHERE
                    COALESCE(delta_level, 0) > 0
                    OR COALESCE(delta_item_points, 0) > 0
                    OR COALESCE(delta_job_points, 0) > 0
                    OR COALESCE(delta_job_exp, 0) > 0
                ) * 100.0
                / NULLIF(COUNT(DISTINCT character_id), 0)
                AS NUMERIC
            ),
            2
        ) AS activity_retention_rate_pct
    FROM sequenced
    WHERE prev_created_at IS NOT NULL
      AND created_at >= :window_start
    GROUP BY (DATE_TRUNC('week', created_at, 'UTC') AT TIME ZONE 'UTC')::date
    ORDER BY week_start ASC
    """
)

# Thresholds mirror `v_bot_vs_donor_classification` in `alembic/versions/002_analytical_views.py`.
BOT_VS_DONOR_SQL: Final[TextClause] = text(
    """
    WITH windowed_snapshots AS (
        SELECT
            cs.server_id,
            cs.character_id,
            c.char_name,
            COALESCE(cs.race, c.race, 'UNK') AS race,
            cs.level,
            cs.item_points,
            cs.job_points,
            cs.job_exp,
            cs.honor_points,
            cs.created_at
        FROM character_snapshots cs
        JOIN characters c ON c.id = cs.character_id
        WHERE cs.server_id = :server_id
          AND cs.created_at >= :lookback_start
    ),
    sequenced AS (
        SELECT
            ws.character_id,
            ws.char_name,
            ws.race,
            ws.created_at,
            (ws.level - LAG(ws.level) OVER w) AS delta_level,
            (ws.item_points - LAG(ws.item_points) OVER w) AS delta_item_points,
            (ws.job_points - LAG(ws.job_points) OVER w) AS delta_job_points,
            (ws.job_exp - LAG(ws.job_exp) OVER w) AS delta_job_exp,
            (ws.honor_points - LAG(ws.honor_points) OVER w) AS delta_honor_points,
            LAG(ws.created_at) OVER w AS prev_created_at
        FROM windowed_snapshots ws
        WINDOW w AS (
            PARTITION BY ws.server_id, ws.character_id
            ORDER BY ws.created_at
        )
    ),
    recent_activity AS (
        SELECT
            character_id,
            char_name,
            race,
            COUNT(*) AS evaluated_intervals,
            SUM(delta_item_points) AS total_item_points_gain,
            MAX(delta_item_points) AS max_single_jump_item_points,
            COUNT(*) FILTER (WHERE
                COALESCE(delta_level, 0) > 0
                OR COALESCE(delta_job_exp, 0) > 0
                OR COALESCE(delta_job_points, 0) > 0
                OR COALESCE(delta_honor_points, 0) > 0
            ) AS active_grind_intervals
        FROM sequenced
        WHERE prev_created_at IS NOT NULL
          AND created_at >= :window_start
        GROUP BY character_id, char_name, race
        HAVING COUNT(*) >= 6
    )
    SELECT
        char_name,
        race,
        evaluated_intervals,
        ROUND(COALESCE(total_item_points_gain, 0)::numeric, 2) AS total_item_points_gain,
        ROUND(COALESCE(max_single_jump_item_points, 0)::numeric, 2) AS max_single_jump_item_points,
        ROUND(
            CAST(active_grind_intervals::NUMERIC / evaluated_intervals AS NUMERIC),
            2
        ) AS grind_persistence_ratio,
        CASE
            WHEN (active_grind_intervals::NUMERIC / evaluated_intervals) >= 0.90
                THEN 'SUSPECTED_BOT_24_7'
            WHEN max_single_jump_item_points >= 150.0
                AND (active_grind_intervals::NUMERIC / evaluated_intervals) < 0.60
                THEN 'SUSPECTED_DONATOR_WHALE'
            WHEN (active_grind_intervals::NUMERIC / evaluated_intervals)
                BETWEEN 0.50 AND 0.89
                THEN 'HARDCORE_HUMAN'
            ELSE 'CASUAL_OR_BALANCED'
        END AS player_behavior_segment
    FROM recent_activity
    ORDER BY grind_persistence_ratio DESC, max_single_jump_item_points DESC
    """
)


def _resolve_reference_time(reference_time: datetime | None) -> datetime:
    if reference_time is None:
        return datetime.now(UTC)
    if reference_time.tzinfo is None:
        return reference_time.replace(tzinfo=UTC)
    return reference_time.astimezone(UTC)


def _coerce_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _coerce_race(value: Any) -> RaceCode:
    race_str = str(value).upper() if value is not None else "UNK"
    if race_str in _VALID_RACES:
        return cast(RaceCode, race_str)
    return "UNK"


def _coerce_tier(value: Any) -> TopNTier:
    tier_str = str(value)
    if tier_str not in _VALID_TIERS:
        raise ValueError(f"Unexpected Top N tier: {tier_str!r}")
    return cast(TopNTier, tier_str)


def _coerce_segment(value: Any) -> BehaviorSegment:
    seg_str = str(value)
    if seg_str not in _VALID_SEGMENTS:
        raise ValueError(f"Unexpected behavior segment: {seg_str!r}")
    return cast(BehaviorSegment, seg_str)


def _get_latest_valid_run(session: Session, server_id: str) -> tuple[UUID, datetime] | None:
    row = session.execute(LATEST_VALID_RUN_SQL, {"server_id": server_id}).mappings().first()
    if row is None:
        return None
    run_id = row["id"]
    created_at = row["created_at"]
    if not isinstance(run_id, UUID):
        run_id = UUID(str(run_id))
    if isinstance(created_at, datetime) and created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=UTC)
    return run_id, cast(datetime, created_at)


def list_active_servers(session: Session) -> list[ServerOption]:
    """Return active servers ordered alphabetically by display name."""
    rows = session.execute(LIST_ACTIVE_SERVERS_SQL).mappings().all()
    return [
        ServerOption(
            id=str(row["id"]),
            display_name=str(row["display_name"]),
            min_records_expected=int(row["min_records_expected"]),
        )
        for row in rows
    ]


def get_server_freshness(session: Session, server_id: str) -> ServerFreshness:
    """Return the timestamp of the latest valid scrape run for a server."""
    latest = _get_latest_valid_run(session, server_id)
    return ServerFreshness(
        server_id=server_id,
        last_snapshot_at=latest[1] if latest is not None else None,
    )


def get_top_n_item_points_evolution(
    session: Session,
    server_id: str,
    window_days: int = 30,
    reference_time: datetime | None = None,
) -> list[TopNAggregatePoint]:
    """Return daily aggregated item_points (avg, P50, P90, rank #1) for Top 10/50/100."""
    if not (1 <= window_days <= 185):
        raise ValueError(f"window_days must be between 1 and 185, got {window_days}")

    ref = _resolve_reference_time(reference_time)
    window_start = ref - timedelta(days=window_days)

    rows = (
        session.execute(
            TOP_N_ITEM_POINTS_EVOLUTION_SQL,
            {"server_id": server_id, "window_start": window_start},
        )
        .mappings()
        .all()
    )
    return [
        TopNAggregatePoint(
            snapshot_day=_coerce_date(row["snapshot_day"]),
            tier=_coerce_tier(row["tier"]),
            avg_item_points=float(row["avg_item_points"]),
            median_item_points=float(row["median_item_points"]),
            p90_item_points=float(row["p90_item_points"]),
            rank1_item_points=float(row["rank1_item_points"]),
            sample_size=int(row["sample_size"]),
        )
        for row in rows
    ]


def get_top_characters_drilldown(
    session: Session,
    server_id: str,
    window_days: int = 30,
    top_n: int = 5,
    reference_time: datetime | None = None,
) -> list[TopCharacterTrendPoint]:
    """Return daily item_points and rank_position trajectories for the current Top N."""
    if not (1 <= window_days <= 185):
        raise ValueError(f"window_days must be between 1 and 185, got {window_days}")
    if not (1 <= top_n <= 10):
        raise ValueError(f"top_n must be between 1 and 10, got {top_n}")

    latest = _get_latest_valid_run(session, server_id)
    if latest is None:
        return []

    run_id, run_created_at = latest
    ref = _resolve_reference_time(reference_time)
    window_start = ref - timedelta(days=window_days)
    win_start = run_created_at - timedelta(hours=1)
    win_end = run_created_at + timedelta(hours=1)

    rows = (
        session.execute(
            TOP_CHARACTERS_DRILLDOWN_SQL,
            {
                "server_id": server_id,
                "run_id": run_id,
                "win_start": win_start,
                "win_end": win_end,
                "top_n": top_n,
                "window_start": window_start,
            },
        )
        .mappings()
        .all()
    )
    return [
        TopCharacterTrendPoint(
            snapshot_day=_coerce_date(row["snapshot_day"]),
            char_name=str(row["char_name"]),
            rank_position=int(row["rank_position"]),
            item_points=float(row["item_points"]),
        )
        for row in rows
    ]


def get_guild_dominance(session: Session, server_id: str) -> list[GuildDominanceRow]:
    """Return Top 100 guild dominance metrics from the latest valid snapshot."""
    latest = _get_latest_valid_run(session, server_id)
    if latest is None:
        return []

    run_id, run_created_at = latest
    win_start = run_created_at - timedelta(hours=1)
    win_end = run_created_at + timedelta(hours=1)

    rows = (
        session.execute(
            GUILD_DOMINANCE_SQL,
            {
                "server_id": server_id,
                "run_id": run_id,
                "win_start": win_start,
                "win_end": win_end,
            },
        )
        .mappings()
        .all()
    )
    return [
        GuildDominanceRow(
            guild_name=str(row["guild_name"]),
            players_in_top_100=int(row["players_in_top_100"]),
            players_in_top_50=int(row["players_in_top_50"]),
            total_top_item_points=float(row["total_top_item_points"]),
            nominal_top_100_pct=float(row["nominal_top_100_pct"]),
            sample_share_pct=float(row["sample_share_pct"]),
            sample_total_players=int(row["sample_total_players"]),
        )
        for row in rows
    ]


def get_race_balance(session: Session, server_id: str) -> list[RaceBalanceRow]:
    """Return CH vs EU vs UNK race distribution from the latest valid snapshot."""
    latest = _get_latest_valid_run(session, server_id)
    if latest is None:
        return []

    run_id, run_created_at = latest
    win_start = run_created_at - timedelta(hours=1)
    win_end = run_created_at + timedelta(hours=1)

    rows = (
        session.execute(
            RACE_BALANCE_SQL,
            {
                "server_id": server_id,
                "run_id": run_id,
                "win_start": win_start,
                "win_end": win_end,
            },
        )
        .mappings()
        .all()
    )
    return [
        RaceBalanceRow(
            race=_coerce_race(row["race"]),
            total_ranked_characters=int(row["total_ranked_characters"]),
            top_100_count=int(row["top_100_count"]),
            avg_level=float(row["avg_level"]),
            avg_item_points=float(row["avg_item_points"]),
            max_item_points=float(row["max_item_points"]),
        )
        for row in rows
    ]


def get_weekly_retention(
    session: Session,
    server_id: str,
    window_weeks: int = 12,
    reference_time: datetime | None = None,
) -> list[WeeklyRetentionRow]:
    """Return weekly activity retention aligned to UTC Monday, marking the current week."""
    if not (2 <= window_weeks <= 26):
        raise ValueError(f"window_weeks must be between 2 and 26, got {window_weeks}")

    ref = _resolve_reference_time(reference_time)
    current_week_start_dt = (ref - timedelta(days=ref.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    window_start = current_week_start_dt - timedelta(weeks=window_weeks - 1)
    lookback_start = window_start - timedelta(days=7)
    current_week_date = current_week_start_dt.date()

    rows = (
        session.execute(
            WEEKLY_RETENTION_SQL,
            {
                "server_id": server_id,
                "lookback_start": lookback_start,
                "window_start": window_start,
            },
        )
        .mappings()
        .all()
    )
    return [
        WeeklyRetentionRow(
            week_start=(ws := _coerce_date(row["week_start"])),
            total_tracked_characters=int(row["total_tracked_characters"]),
            active_characters=int(row["active_characters"]),
            activity_retention_rate_pct=float(row["activity_retention_rate_pct"]),
            is_current_week=(ws == current_week_date),
        )
        for row in rows
    ]


def get_bot_vs_donor_classification(
    session: Session,
    server_id: str,
    reference_time: datetime | None = None,
) -> list[BehaviorClassificationRow]:
    """Return 7-day behavioral radar classifications (Bot vs Whale) for a server."""
    ref = _resolve_reference_time(reference_time)
    window_start = ref - timedelta(days=7)
    lookback_start = ref - timedelta(days=9)

    rows = (
        session.execute(
            BOT_VS_DONOR_SQL,
            {
                "server_id": server_id,
                "lookback_start": lookback_start,
                "window_start": window_start,
            },
        )
        .mappings()
        .all()
    )
    return [
        BehaviorClassificationRow(
            char_name=str(row["char_name"]),
            race=_coerce_race(row["race"]),
            evaluated_intervals=int(row["evaluated_intervals"]),
            total_item_points_gain=float(row["total_item_points_gain"]),
            max_single_jump_item_points=float(row["max_single_jump_item_points"]),
            grind_persistence_ratio=float(row["grind_persistence_ratio"]),
            player_behavior_segment=_coerce_segment(row["player_behavior_segment"]),
        )
        for row in rows
    ]
