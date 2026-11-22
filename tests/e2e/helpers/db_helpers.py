"""PostgreSQL 16 inspection, partition, view query, and historical seeder helpers.

Uses `sqlalchemy` / `sqlalchemy.ext.asyncio` exclusively — NEVER imports `asyncpg`
directly so `mypy --strict` passes without stub overrides.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def list_table_partitions(session: AsyncSession, parent_table: str) -> list[str]:
    """Return sorted child partition table names for `parent_table` via `pg_inherits`."""
    sql = text(
        """
        SELECT child.relname
        FROM pg_inherits
        JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child ON pg_inherits.inhrelid = child.oid
        WHERE parent.relname = :parent_table
        ORDER BY child.relname
        """
    )
    result = await session.execute(sql, {"parent_table": parent_table})
    return [str(row[0]) for row in result.all()]


async def get_snapshot_partition_name(
    session: AsyncSession, table_name: str, row_id: int, created_at: datetime
) -> str:
    """Return exact partition name where `(id, created_at)` was routed (`tableoid::regclass`)."""
    allowed = {"character_snapshots", "guild_snapshots"}
    if table_name not in allowed:
        msg = f"Unsupported partitioned table: {table_name}"
        raise ValueError(msg)
    sql = text(
        f"SELECT tableoid::regclass::text FROM {table_name} "
        "WHERE id = :row_id AND created_at = :created_at"
    )
    result = await session.execute(sql, {"row_id": row_id, "created_at": created_at})
    scalar = result.scalar_one()
    return str(scalar)


async def count_rows(session: AsyncSession, table_name: str, server_id: str | None = None) -> int:
    """Count rows in an allowlisted table, optionally filtered by `server_id`."""
    allowed = {
        "servers",
        "scrape_runs",
        "characters",
        "guilds",
        "character_snapshots",
        "guild_snapshots",
    }
    if table_name not in allowed:
        msg = f"Unsupported table: {table_name}"
        raise ValueError(msg)
    if server_id is not None:
        col = "id" if table_name == "servers" else "server_id"
        sql = text(f"SELECT COUNT(*) FROM {table_name} WHERE {col} = :server_id")
        res = await session.execute(sql, {"server_id": server_id})
    else:
        sql = text(f"SELECT COUNT(*) FROM {table_name}")
        res = await session.execute(sql)
    return int(res.scalar_one())


async def fetch_latest_scrape_run(session: AsyncSession, server_id: str) -> dict[str, Any] | None:
    """Fetch the most recent `scrape_runs` row for `server_id` as a mapping."""
    sql = text(
        """
        SELECT id, server_id, ranking_category, trigger_type, status,
               records_extracted, records_persisted, validation_errors_count,
               execution_time_ms, http_status_code,
               quarantine_file_path, quarantine_sha256, quarantine_size_bytes,
               error_details, created_at, updated_at
        FROM scrape_runs
        WHERE server_id = :server_id
        ORDER BY created_at DESC
        LIMIT 1
        """
    )
    res = await session.execute(sql, {"server_id": server_id})
    row = res.mappings().first()
    return dict(row) if row is not None else None


async def fetch_server_row(session: AsyncSession, server_id: str) -> dict[str, Any]:
    """Fetch `servers` row for `server_id`."""
    sql = text(
        """
        SELECT id, display_name, base_url, current_season, is_active,
               fw_day_of_week, fw_end_time_utc, min_records_expected,
               circuit_breaker_until, created_at, updated_at
        FROM servers
        WHERE id = :server_id
        """
    )
    res = await session.execute(sql, {"server_id": server_id})
    return dict(res.mappings().one())


@dataclass(frozen=True)
class HistoricalCharSeed:
    """Character snapshot specification for historical multi-day seeding."""

    char_name: str
    rank_position: int
    level: int
    item_points: float
    race: str = "CH"
    guild_name: str | None = None
    job_type: str | None = None
    job_level: int | None = None
    job_points: int | None = None
    job_exp: int | None = None
    honor_points: int | None = None


async def seed_historical_run(
    session: AsyncSession,
    *,
    server_id: str,
    created_at: datetime,
    characters: list[HistoricalCharSeed],
    status: str = "SUCCESS",
    trigger_type: str = "scheduled_12h",
) -> uuid.UUID:
    """Seed an immutable historical `scrape_runs` + dimensions + `character_snapshots` batch."""
    run_id = uuid.uuid4()
    await session.execute(
        text(
            """
            INSERT INTO scrape_runs (
                id, server_id, ranking_category, trigger_type, status,
                records_extracted, records_persisted, validation_errors_count,
                execution_time_ms, http_status_code, created_at, updated_at
            ) VALUES (
                :id, :server_id, 'all', :trigger_type, :status,
                :extracted, :persisted, 0, 150, 200, :created_at, :created_at
            )
            """
        ),
        {
            "id": run_id,
            "server_id": server_id,
            "trigger_type": trigger_type,
            "status": status,
            "extracted": len(characters),
            "persisted": len(characters),
            "created_at": created_at,
        },
    )

    guild_names = sorted({c.guild_name for c in characters if c.guild_name is not None})
    guild_ids: dict[str, uuid.UUID] = {}
    for g_name in guild_names:
        res_g = await session.execute(
            text(
                """
                INSERT INTO guilds (server_id, guild_name, first_seen_at, last_seen_at, is_active)
                VALUES (:server_id, :guild_name, :ts, :ts, TRUE)
                ON CONFLICT (server_id, guild_name) DO UPDATE
                SET last_seen_at = EXCLUDED.last_seen_at
                RETURNING id
                """
            ),
            {"server_id": server_id, "guild_name": g_name, "ts": created_at},
        )
        guild_ids[g_name] = uuid.UUID(str(res_g.scalar_one()))

    for c in characters:
        res_c = await session.execute(
            text(
                """
                INSERT INTO characters (server_id, char_name, race, first_seen_at, last_seen_at)
                VALUES (:server_id, :char_name, :race, :ts, :ts)
                ON CONFLICT (server_id, char_name) DO UPDATE
                SET last_seen_at = EXCLUDED.last_seen_at,
                    race = CASE
                        WHEN EXCLUDED.race IN ('CH', 'EU') THEN EXCLUDED.race
                        ELSE characters.race
                    END
                RETURNING id
                """
            ),
            {
                "server_id": server_id,
                "char_name": c.char_name,
                "race": c.race,
                "ts": created_at,
            },
        )
        char_id = uuid.UUID(str(res_c.scalar_one()))
        g_id = guild_ids.get(c.guild_name) if c.guild_name is not None else None
        await session.execute(
            text(
                """
                INSERT INTO character_snapshots (
                    scrape_run_id, server_id, character_id, guild_id,
                    rank_position, level, race, item_points,
                    job_type, job_level, job_points, job_exp, honor_points,
                    created_at, updated_at
                ) VALUES (
                    :run_id, :server_id, :char_id, :guild_id,
                    :rank_position, :level, :race, :item_points,
                    :job_type, :job_level, :job_points, :job_exp, :honor_points,
                    :ts, :ts
                )
                """
            ),
            {
                "run_id": run_id,
                "server_id": server_id,
                "char_id": char_id,
                "guild_id": g_id,
                "rank_position": c.rank_position,
                "level": c.level,
                "race": c.race,
                "item_points": c.item_points,
                "job_type": c.job_type,
                "job_level": c.job_level,
                "job_points": c.job_points,
                "job_exp": c.job_exp,
                "honor_points": c.honor_points,
                "ts": created_at,
            },
        )
    await session.commit()
    return run_id


async def query_view(
    session: AsyncSession,
    view_name: str,
    where_clause: str = "",
    params: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Query one of the 5 allowlisted analytical SQL views."""
    allowed = {
        "v_character_activity_deltas",
        "v_bot_vs_donor_classification",
        "v_guild_turnover_dominance",
        "v_race_balance_ch_vs_eu",
        "v_server_retention_weekly",
    }
    if view_name not in allowed:
        msg = f"Unsupported view: {view_name}"
        raise ValueError(msg)
    clause = f" WHERE {where_clause}" if where_clause else ""
    res = await session.execute(text(f"SELECT * FROM {view_name}{clause}"), params or {})
    return [dict(row) for row in res.mappings().all()]
