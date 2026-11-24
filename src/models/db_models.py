from __future__ import annotations

import uuid
from datetime import datetime, time
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    ForeignKey,
    Identity,
    Index,
    PrimaryKeyConstraint,
    UniqueConstraint,
    desc,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import (
    BIGINT,
    BOOLEAN,
    CHAR,
    INTEGER,
    JSONB,
    NUMERIC,
    SMALLINT,
    TIME,
    TIMESTAMP,
    UUID,
    VARCHAR,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """SQLAlchemy declarative base for all SRO Ranking Scraper models."""


class Server(Base):
    """Target Silkroad server configuration and circuit breaker state."""

    __tablename__ = "servers"

    id: Mapped[str] = mapped_column(VARCHAR(32), primary_key=True)
    display_name: Mapped[str] = mapped_column(VARCHAR(64), nullable=False)
    base_url: Mapped[str] = mapped_column(VARCHAR(255), nullable=False)
    current_season: Mapped[str] = mapped_column(
        VARCHAR(32), nullable=False, server_default=text("'v1'")
    )
    is_active: Mapped[bool] = mapped_column(BOOLEAN, nullable=False, server_default=text("TRUE"))
    fw_day_of_week: Mapped[int] = mapped_column(SMALLINT, nullable=False, server_default=text("0"))
    fw_end_time_utc: Mapped[time] = mapped_column(
        TIME, nullable=False, server_default=text("'22:00:00'")
    )
    min_records_expected: Mapped[int] = mapped_column(
        INTEGER, nullable=False, server_default=text("50")
    )
    circuit_breaker_until: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True, server_default=text("NULL")
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.current_timestamp()
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.current_timestamp()
    )


class ScrapeRun(Base):
    """Audit log recording every scraper execution and quarantine metadata."""

    __tablename__ = "scrape_runs"
    __table_args__ = (
        CheckConstraint(
            "status IN ("
            "'SUCCESS', 'PARTIAL_SUCCESS', 'SCHEMA_DRIFT', "
            "'LOW_COUNT_QUARANTINE', 'BLOCKED_CHALLENGE', "
            "'TARGET_MAINTENANCE', 'HTTP_ERROR'"
            ")",
            name="ck_scrape_runs_status",
        ),
        Index(
            "idx_scrape_runs_server_latest",
            "server_id",
            text("created_at DESC"),
            postgresql_where=text("status IN ('SUCCESS', 'PARTIAL_SUCCESS')"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    server_id: Mapped[str] = mapped_column(VARCHAR(32), ForeignKey("servers.id"), nullable=False)
    ranking_category: Mapped[str] = mapped_column(
        VARCHAR(32), nullable=False, server_default=text("'all'")
    )
    trigger_type: Mapped[str] = mapped_column(VARCHAR(16), nullable=False)
    status: Mapped[str] = mapped_column(VARCHAR(24), nullable=False)
    records_extracted: Mapped[int] = mapped_column(
        INTEGER, nullable=False, server_default=text("0")
    )
    records_persisted: Mapped[int] = mapped_column(
        INTEGER, nullable=False, server_default=text("0")
    )
    validation_errors_count: Mapped[int] = mapped_column(
        INTEGER, nullable=False, server_default=text("0")
    )
    execution_time_ms: Mapped[int] = mapped_column(INTEGER, nullable=False)
    http_status_code: Mapped[int | None] = mapped_column(INTEGER, nullable=True)
    quarantine_file_path: Mapped[str | None] = mapped_column(VARCHAR(255), nullable=True)
    quarantine_sha256: Mapped[str | None] = mapped_column(CHAR(64), nullable=True)
    quarantine_size_bytes: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    error_details: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.current_timestamp()
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.current_timestamp()
    )


class Character(Base):
    """Server-scoped character dimension."""

    __tablename__ = "characters"
    __table_args__ = (
        CheckConstraint("race IN ('CH', 'EU', 'UNK')", name="ck_characters_race"),
        UniqueConstraint("server_id", "char_name", name="uq_char_per_server"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    server_id: Mapped[str] = mapped_column(VARCHAR(32), ForeignKey("servers.id"), nullable=False)
    char_name: Mapped[str] = mapped_column(VARCHAR(64), nullable=False)
    race: Mapped[str] = mapped_column(VARCHAR(4), nullable=False, server_default=text("'UNK'"))
    first_seen_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)


class Guild(Base):
    """Server-scoped guild dimension."""

    __tablename__ = "guilds"
    __table_args__ = (UniqueConstraint("server_id", "guild_name", name="uq_guild_per_server"),)

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    server_id: Mapped[str] = mapped_column(VARCHAR(32), ForeignKey("servers.id"), nullable=False)
    guild_name: Mapped[str] = mapped_column(VARCHAR(64), nullable=False)
    master_char_name: Mapped[str | None] = mapped_column(VARCHAR(64), nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    is_active: Mapped[bool] = mapped_column(BOOLEAN, nullable=False, server_default=text("TRUE"))


class CharacterSnapshot(Base):
    """Append-only temporal fact table for character rankings, partitioned by month."""

    __tablename__ = "character_snapshots"
    __table_args__ = (
        PrimaryKeyConstraint("id", "created_at", name="pk_character_snapshots"),
        UniqueConstraint(
            "scrape_run_id",
            "character_id",
            "created_at",
            name="uq_char_snapshot_run",
        ),
        Index(
            "idx_char_snapshots_lookup",
            "server_id",
            "character_id",
            desc("created_at"),
        ),
        Index(
            "idx_char_snapshots_rank",
            "server_id",
            "created_at",
            "rank_position",
        ),
        Index(
            "idx_char_snapshots_brin_time",
            "created_at",
            postgresql_using="brin",
        ),
        Index(
            "idx_char_snapshots_extra_metrics",
            "extra_metrics",
            postgresql_using="gin",
        ),
        {"postgresql_partition_by": "RANGE (created_at)"},
    )

    id: Mapped[int] = mapped_column(BIGINT, Identity(always=True), primary_key=True)
    scrape_run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("scrape_runs.id"), nullable=False
    )
    server_id: Mapped[str] = mapped_column(VARCHAR(32), ForeignKey("servers.id"), nullable=False)
    character_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("characters.id"), nullable=False
    )
    guild_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("guilds.id"), nullable=True
    )
    rank_position: Mapped[int] = mapped_column(INTEGER, nullable=False)
    level: Mapped[int] = mapped_column(INTEGER, nullable=False)
    race: Mapped[str | None] = mapped_column(VARCHAR(4), nullable=True)
    item_points: Mapped[Decimal] = mapped_column(
        NUMERIC(10, 2), nullable=False, server_default=text("0.0")
    )
    strength: Mapped[int | None] = mapped_column(INTEGER, nullable=True)
    intellect: Mapped[int | None] = mapped_column(INTEGER, nullable=True)
    title: Mapped[str | None] = mapped_column(VARCHAR(64), nullable=True)
    job_type: Mapped[str | None] = mapped_column(VARCHAR(16), nullable=True)
    job_level: Mapped[int | None] = mapped_column(INTEGER, nullable=True)
    job_points: Mapped[int | None] = mapped_column(INTEGER, nullable=True)
    job_exp: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    honor_points: Mapped[int | None] = mapped_column(INTEGER, nullable=True)
    extra_metrics: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        primary_key=True,
        nullable=False,
        server_default=func.current_timestamp(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.current_timestamp()
    )


class GuildSnapshot(Base):
    """Append-only temporal fact table for guild rankings, partitioned by month."""

    __tablename__ = "guild_snapshots"
    __table_args__ = (
        PrimaryKeyConstraint("id", "created_at", name="pk_guild_snapshots"),
        UniqueConstraint(
            "scrape_run_id",
            "guild_id",
            "created_at",
            name="uq_guild_snapshot_run",
        ),
        Index(
            "idx_guild_snapshots_lookup",
            "server_id",
            "guild_id",
            desc("created_at"),
        ),
        Index(
            "idx_guild_snapshots_rank",
            "server_id",
            "created_at",
            "rank_position",
        ),
        Index(
            "idx_guild_snapshots_brin_time",
            "created_at",
            postgresql_using="brin",
        ),
        {"postgresql_partition_by": "RANGE (created_at)"},
    )

    id: Mapped[int] = mapped_column(BIGINT, Identity(always=True), primary_key=True)
    scrape_run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("scrape_runs.id"), nullable=False
    )
    server_id: Mapped[str] = mapped_column(VARCHAR(32), ForeignKey("servers.id"), nullable=False)
    guild_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("guilds.id"), nullable=False
    )
    rank_position: Mapped[int] = mapped_column(INTEGER, nullable=False)
    members_count: Mapped[int] = mapped_column(INTEGER, nullable=False, server_default=text("0"))
    total_item_points: Mapped[Decimal] = mapped_column(
        NUMERIC(12, 2), nullable=False, server_default=text("0.0")
    )
    fortress_held: Mapped[str | None] = mapped_column(VARCHAR(32), nullable=True)
    extra_metrics: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        primary_key=True,
        nullable=False,
        server_default=func.current_timestamp(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.current_timestamp()
    )
