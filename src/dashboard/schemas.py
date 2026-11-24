"""Immutable DTO contracts for the analytical dashboard (no UI dependencies)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal

RaceCode = Literal["CH", "EU", "UNK"]
TopNTier = Literal["Top 10", "Top 50", "Top 100"]
BehaviorSegment = Literal[
    "SUSPECTED_BOT_24_7",
    "SUSPECTED_DONATOR_WHALE",
    "HARDCORE_HUMAN",
    "CASUAL_OR_BALANCED",
]


@dataclass(frozen=True, slots=True)
class ServerOption:
    """Active server entry displayed in the dashboard sidebar selector."""

    id: str
    display_name: str
    min_records_expected: int


@dataclass(frozen=True, slots=True)
class ServerFreshness:
    """Latest valid snapshot timestamp for a server."""

    server_id: str
    last_snapshot_at: datetime | None


@dataclass(frozen=True, slots=True)
class TopNAggregatePoint:
    """Daily aggregated item_points distribution for a Top N rank tier."""

    snapshot_day: date
    tier: TopNTier
    avg_item_points: float
    median_item_points: float
    p90_item_points: float
    rank1_item_points: float
    sample_size: int


@dataclass(frozen=True, slots=True)
class TopCharacterTrendPoint:
    """Daily item_points and rank_position point for a single top character."""

    snapshot_day: date
    char_name: str
    rank_position: int
    item_points: float


@dataclass(frozen=True, slots=True)
class GuildDominanceRow:
    """Guild presence and share within the Top 100 of the latest snapshot."""

    guild_name: str
    players_in_top_100: int
    players_in_top_50: int
    total_top_item_points: float
    nominal_top_100_pct: float
    sample_share_pct: float
    sample_total_players: int


@dataclass(frozen=True, slots=True)
class RaceBalanceRow:
    """Race distribution and stats from the latest valid snapshot."""

    race: RaceCode
    total_ranked_characters: int
    top_100_count: int
    avg_level: float
    avg_item_points: float
    max_item_points: float


@dataclass(frozen=True, slots=True)
class WeeklyRetentionRow:
    """Weekly character activity retention aligned to UTC Monday."""

    week_start: date
    total_tracked_characters: int
    active_characters: int
    activity_retention_rate_pct: float
    is_current_week: bool


@dataclass(frozen=True, slots=True)
class BehaviorClassificationRow:
    """7-day behavioral radar classification (Bot vs Whale) for a character."""

    char_name: str
    race: RaceCode
    evaluated_intervals: int
    total_item_points_gain: float
    max_single_jump_item_points: float
    grind_persistence_ratio: float
    player_behavior_segment: BehaviorSegment
