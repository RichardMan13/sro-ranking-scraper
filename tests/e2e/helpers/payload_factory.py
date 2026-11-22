"""Programmatic HTML, JSON, Turnstile, Maintenance, and Drift payload generators."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import orjson


@dataclass(frozen=True)
class PlayerRowSpec:
    """Typed specification for generating a player ranking row across targets."""

    rank: int
    char_name: str
    level: int | str = 120
    guild_name: str = "Titans"
    race: str | None = "CH"
    item_points: str | float = "1,250.50"
    honor_points: str | int = "500"
    title: str = "General"
    strength: str | int = "450"
    intellect: str | int = "120"
    job_type: str | None = "HUNTER"
    job_level: int | None = 75
    job_points: int | None = 1200
    pvp_kd: str = "2.5"
    unique_points: int = 40
    elo_points: int = 1600
    sox_parts: int = 4
    hours_played: int = 120
    gold: str = "500,000"
    last_logout: str = "2026-11-19"


@dataclass(frozen=True)
class JobRowSpec:
    """Typed specification for generating a job ranking row."""

    rank: int
    char_name: str
    guild_name: str = "Titans"
    job_type: str = "HUNTER"
    job_level: int | str = 80
    job_exp: int | str = "1,500,000"


@dataclass(frozen=True)
class GuildRowSpec:
    """Typed specification for generating a guild ranking row."""

    rank: int
    guild_name: str
    master_name: str = "GuildBoss"
    members_count: str | int = "45/50"
    total_item_points: str | float = "95,400.25"
    fortress_held: str = "Jangan"


def make_player_specs(
    count: int,
    *,
    start_rank: int = 1,
    prefix: str = "Player",
    guild_cycle: Sequence[str] = ("Titans", "Shadows", "Empire", "-"),
    race_cycle: Sequence[str | None] = ("CH", "EU"),
    invalid_ranks: dict[int, dict[str, Any]] | None = None,
) -> list[PlayerRowSpec]:
    """Generate `count` deterministic PlayerRowSpec items."""
    invalid_map = invalid_ranks or {}
    specs: list[PlayerRowSpec] = []
    for offset in range(count):
        rank = start_rank + offset
        overrides = invalid_map.get(rank, {})
        race = overrides.get("race", race_cycle[offset % len(race_cycle)])
        guild = str(overrides.get("guild_name", guild_cycle[offset % len(guild_cycle)]))
        char_name = str(overrides.get("char_name", f"{prefix}_{rank:04d}"))
        level: int | str = overrides.get("level", max(1, 120 - (offset % 30)))
        item_points: str | float = overrides.get(
            "item_points", f"{max(100, 5000 - offset * 2):.2f}"
        )
        specs.append(
            PlayerRowSpec(
                rank=rank,
                char_name=char_name,
                level=level,
                guild_name=guild,
                race=race,
                item_points=item_points,
                honor_points=str(max(0, 1000 - offset)),
                title="Baron" if offset % 2 == 0 else "-",
            )
        )
    return specs


def make_job_specs(
    count: int,
    *,
    start_rank: int = 1,
    prefix: str = "Player",
    job_cycle: Sequence[str] = ("TRADER", "THIEF", "HUNTER"),
) -> list[JobRowSpec]:
    """Generate `count` deterministic JobRowSpec items."""
    return [
        JobRowSpec(
            rank=start_rank + i,
            char_name=f"{prefix}_{start_rank + i:04d}",
            guild_name="Titans" if i % 2 == 0 else "Shadows",
            job_type=job_cycle[i % len(job_cycle)],
            job_level=90 - (i % 20),
            job_exp=str(1_000_000 - i * 1000),
        )
        for i in range(count)
    ]


def make_guild_specs(
    count: int,
    *,
    start_rank: int = 1,
    prefix: str = "Guild",
    invalid_ranks: dict[int, dict[str, Any]] | None = None,
) -> list[GuildRowSpec]:
    """Generate `count` deterministic GuildRowSpec items."""
    invalid_map = invalid_ranks or {}
    fortresses = ("Jangan", "Hotan", "Bandit", "-")
    specs: list[GuildRowSpec] = []
    for i in range(count):
        rank = start_rank + i
        overrides = invalid_map.get(rank, {})
        specs.append(
            GuildRowSpec(
                rank=rank,
                guild_name=str(overrides.get("guild_name", f"{prefix}_{rank:03d}")),
                master_name=str(overrides.get("master_name", f"Master_{rank:03d}")),
                members_count=overrides.get("members_count", f"{max(1, 50 - i)}/50"),
                total_item_points=overrides.get(
                    "total_item_points", f"{max(1000, 100000 - i * 500):.2f}"
                ),
                fortress_held=str(overrides.get("fortress_held", fortresses[i % len(fortresses)])),
            )
        )
    return specs


def render_chronicle_player_html(rows: Sequence[PlayerRowSpec]) -> str:
    """Render Chronicle SRO player ranking HTML (`table.ranking-table tbody tr`)."""
    trs: list[str] = []
    for r in rows:
        if r.race == "CH":
            race_img = '<img src="/img/ch.png" data-race="CH" alt="Chinese" />'
        elif r.race == "EU":
            race_img = '<img src="/img/eu.png" data-race="EU" alt="European" />'
        else:
            race_img = ""
        trs.append(
            "<tr>"
            f"<td>{r.rank}</td>"
            f'<td>{race_img}<a class="char-link" href="/char/{r.char_name}">{r.char_name}</a></td>'
            f"<td>{r.guild_name}</td>"
            f"<td>{r.level}</td>"
            f"<td>{r.item_points}</td>"
            f"<td>{r.honor_points}</td>"
            f"<td>{r.title}</td>"
            f"<td>{r.pvp_kd}</td>"
            f"<td>{r.unique_points}</td>"
            "</tr>"
        )
    return (
        "<!DOCTYPE html><html><body>"
        '<table class="ranking-table"><thead><tr>'
        "<th>#</th><th>Character</th><th>Guild</th><th>Level</th>"
        "<th>Item Points</th><th>Honor</th><th>Title</th><th>K/D</th><th>Uniques</th>"
        "</tr></thead>"
        f"<tbody>{''.join(trs)}</tbody></table></body></html>"
    )


def render_chronicle_job_html(rows: Sequence[JobRowSpec]) -> str:
    """Render Chronicle SRO job ranking HTML (`table.ranking-table tbody tr`)."""
    trs = [
        "<tr>"
        f"<td>{r.rank}</td>"
        f"<td>{r.char_name}</td>"
        f"<td>{r.guild_name}</td>"
        f"<td>{r.job_type}</td>"
        f"<td>{r.job_level}</td>"
        f"<td>{r.job_exp}</td>"
        "</tr>"
        for r in rows
    ]
    return (
        "<!DOCTYPE html><html><body>"
        '<table class="ranking-table"><tbody>'
        f"{''.join(trs)}</tbody></table></body></html>"
    )


def render_chronicle_guild_html(rows: Sequence[GuildRowSpec]) -> str:
    """Render Chronicle SRO guild ranking HTML (`table.ranking-table tbody tr`)."""
    trs = [
        "<tr>"
        f"<td>{r.rank}</td>"
        f"<td>{r.guild_name}</td>"
        f"<td>{r.master_name}</td>"
        f"<td>{r.members_count}</td>"
        f"<td>{r.total_item_points}</td>"
        f"<td>{r.fortress_held}</td>"
        "</tr>"
        for r in rows
    ]
    return (
        "<!DOCTYPE html><html><body>"
        '<table class="ranking-table"><tbody>'
        f"{''.join(trs)}</tbody></table></body></html>"
    )


def render_electus_player_json(
    rows: Sequence[PlayerRowSpec],
    *,
    root_key: str = "data",
    use_race_id: bool = True,
) -> bytes:
    """Render Electus Online player ranking JSON payload via `orjson`."""
    items: list[dict[str, Any]] = []
    for r in rows:
        item: dict[str, Any] = {
            "rank": r.rank,
            "charname": r.char_name,
            "guild": r.guild_name,
            "level": r.level,
            "itempoints": r.item_points,
            "job_type": r.job_type,
            "job_level": r.job_level,
            "job_points": r.job_points,
            "honor": r.honor_points,
            "elo_points": r.elo_points,
            "sox_parts": r.sox_parts,
            "hours_played": r.hours_played,
        }
        if use_race_id:
            item["race_id"] = 1 if r.race == "CH" else (2 if r.race == "EU" else None)
        else:
            item["race"] = r.race
        items.append(item)
    return orjson.dumps({root_key: items})


def render_electus_job_json(rows: Sequence[JobRowSpec], *, root_key: str = "data") -> bytes:
    """Render Electus Online job ranking JSON payload via `orjson`."""
    items = [
        {
            "rank": r.rank,
            "charname": r.char_name,
            "guild": r.guild_name,
            "job_type": r.job_type,
            "job_level": r.job_level,
            "job_exp": r.job_exp,
        }
        for r in rows
    ]
    return orjson.dumps({root_key: items})


def render_electus_guild_json(rows: Sequence[GuildRowSpec], *, root_key: str = "data") -> bytes:
    """Render Electus Online guild ranking JSON payload via `orjson`."""
    items = [
        {
            "rank": r.rank,
            "guild_name": r.guild_name,
            "master_name": r.master_name,
            "members_count": r.members_count,
            "total_item_points": r.total_item_points,
            "fortress_held": r.fortress_held,
        }
        for r in rows
    ]
    return orjson.dumps({root_key: items})


def render_electus_player_html_fallback(rows: Sequence[PlayerRowSpec]) -> str:
    """Render Electus Online HTML fallback table (`table#ranking-table tbody tr`)."""
    trs = [
        "<tr>"
        f"<td>{r.rank}</td>"
        f"<td>{r.char_name}</td>"
        f"<td>{r.guild_name}</td>"
        f"<td>{r.level}</td>"
        f"<td>{r.item_points}</td>"
        f"<td>{r.honor_points}</td>"
        "</tr>"
        for r in rows
    ]
    return (
        "<!DOCTYPE html><html><body>"
        '<table id="ranking-table"><tbody>'
        f"{''.join(trs)}</tbody></table></body></html>"
    )


def render_electus_job_html_fallback(rows: Sequence[JobRowSpec]) -> str:
    """Render Electus Online HTML fallback job table (`table#ranking-table tbody tr`)."""
    trs = [
        "<tr>"
        f"<td>{r.rank}</td>"
        f"<td>{r.char_name}</td>"
        f"<td>{r.guild_name}</td>"
        f"<td>{r.job_type}</td>"
        f"<td>{r.job_level}</td>"
        f"<td>{r.job_exp}</td>"
        "</tr>"
        for r in rows
    ]
    return (
        "<!DOCTYPE html><html><body>"
        '<table id="ranking-table"><tbody>'
        f"{''.join(trs)}</tbody></table></body></html>"
    )


def render_electus_guild_html_fallback(rows: Sequence[GuildRowSpec]) -> str:
    """Render Electus Online HTML fallback guild table (`table#ranking-table tbody tr`)."""
    trs = [
        "<tr>"
        f"<td>{r.rank}</td>"
        f"<td>{r.guild_name}</td>"
        f"<td>{r.master_name}</td>"
        f"<td>{r.members_count}</td>"
        f"<td>{r.total_item_points}</td>"
        f"<td>{r.fortress_held}</td>"
        "</tr>"
        for r in rows
    ]
    return (
        "<!DOCTYPE html><html><body>"
        '<table id="ranking-table"><tbody>'
        f"{''.join(trs)}</tbody></table></body></html>"
    )


def render_golden_player_html(rows: Sequence[PlayerRowSpec]) -> str:
    """Render Play Golden player ranking HTML (`table.table-ranking tbody tr`)."""
    trs: list[str] = []
    for r in rows:
        if r.race == "CH":
            race_cell = '<img src="/assets/races/chinese.png" alt="Chinese" />'
        elif r.race == "EU":
            race_cell = '<img src="/assets/races/european.png" alt="European" />'
        else:
            race_cell = ""
        trs.append(
            "<tr>"
            f"<td>{r.rank}</td>"
            f"<td>{race_cell}</td>"
            f"<td>{r.char_name}</td>"
            f"<td>{r.guild_name}</td>"
            f"<td>{r.level}</td>"
            f"<td>{r.strength}</td>"
            f"<td>{r.intellect}</td>"
            f"<td>{r.item_points}</td>"
            f"<td>{r.honor_points}</td>"
            f"<td>{r.gold}</td>"
            f"<td>{r.last_logout}</td>"
            "</tr>"
        )
    return (
        "<!DOCTYPE html><html><body>"
        '<table class="table-ranking"><tbody>'
        f"{''.join(trs)}</tbody></table></body></html>"
    )


def render_golden_job_html(rows: Sequence[JobRowSpec]) -> str:
    """Render Play Golden job ranking HTML (`table.table-ranking tbody tr`)."""
    # Play Golden job columns: [0] rank, [1] char_name, [2] job_type, [3] job_level,
    # [4] job_exp, [5] guild_name
    trs = [
        "<tr>"
        f"<td>{r.rank}</td>"
        f"<td>{r.char_name}</td>"
        f"<td>{r.job_type}</td>"
        f"<td>{r.job_level}</td>"
        f"<td>{r.job_exp}</td>"
        f"<td>{r.guild_name}</td>"
        "</tr>"
        for r in rows
    ]
    return (
        "<!DOCTYPE html><html><body>"
        '<table class="table-ranking"><tbody>'
        f"{''.join(trs)}</tbody></table></body></html>"
    )


def render_golden_guild_html(rows: Sequence[GuildRowSpec]) -> str:
    """Render Play Golden guild ranking HTML (`table.table-ranking tbody tr`)."""
    trs = [
        "<tr>"
        f"<td>{r.rank}</td>"
        f"<td>{r.guild_name}</td>"
        f"<td>{r.master_name}</td>"
        f"<td>{r.members_count}</td>"
        f"<td>{r.total_item_points}</td>"
        f"<td>{r.fortress_held}</td>"
        "</tr>"
        for r in rows
    ]
    return (
        "<!DOCTYPE html><html><body>"
        '<table class="table-ranking"><tbody>'
        f"{''.join(trs)}</tbody></table></body></html>"
    )


def render_turnstile_html(marker: str = '<div id="turnstile-wrapper"></div>') -> str:
    """Render a Cloudflare / Turnstile anti-bot challenge HTML page."""
    return (
        "<!DOCTYPE html><html><head><title>Just a moment...</title></head>"
        f"<body>{marker}<p>Checking your browser before accessing.</p></body></html>"
    )


def render_maintenance_html(banner: str = "Server under maintenance") -> str:
    """Render a scheduled maintenance HTML page."""
    return f'<!DOCTYPE html><html><body><div class="alert">{banner}</div></body></html>'
