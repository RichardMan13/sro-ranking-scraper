"""Initial PostgreSQL 16 schema, seeds, monthly partitions, and indexes."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "001_initial_schema"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute('CREATE EXTENSION IF NOT EXISTS "pgcrypto"')

    op.execute(
        """
        CREATE TABLE servers (
            id VARCHAR(32) PRIMARY KEY,
            display_name VARCHAR(64) NOT NULL,
            base_url VARCHAR(255) NOT NULL,
            current_season VARCHAR(32) NOT NULL DEFAULT 'v1',
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            fw_day_of_week SMALLINT NOT NULL DEFAULT 0,
            fw_end_time_utc TIME NOT NULL DEFAULT '22:00:00',
            min_records_expected INTEGER NOT NULL DEFAULT 50,
            circuit_breaker_until TIMESTAMPTZ DEFAULT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    op.execute(
        """
        INSERT INTO servers (
            id,
            display_name,
            base_url,
            current_season,
            is_active,
            fw_day_of_week,
            fw_end_time_utc,
            min_records_expected
        ) VALUES
            (
                'chronicle',
                'Chronicle SRO',
                'https://play-chronicle.com',
                'v1',
                TRUE,
                0,
                '22:00:00',
                100
            ),
            (
                'electus',
                'Electus Online',
                'https://electus.online',
                'v1',
                TRUE,
                0,
                '21:00:00',
                100
            ),
            (
                'play_golden',
                'Play Golden',
                'https://play-golden.online',
                'v1',
                TRUE,
                6,
                '20:00:00',
                50
            )
        ON CONFLICT (id) DO NOTHING
        """
    )

    op.execute(
        """
        CREATE TABLE scrape_runs (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            server_id VARCHAR(32) NOT NULL REFERENCES servers(id),
            ranking_category VARCHAR(32) NOT NULL DEFAULT 'all',
            trigger_type VARCHAR(16) NOT NULL,
            status VARCHAR(24) NOT NULL CHECK (status IN (
                'SUCCESS', 'PARTIAL_SUCCESS', 'SCHEMA_DRIFT',
                'LOW_COUNT_QUARANTINE', 'BLOCKED_CHALLENGE',
                'TARGET_MAINTENANCE', 'HTTP_ERROR'
            )),
            records_extracted INTEGER NOT NULL DEFAULT 0,
            records_persisted INTEGER NOT NULL DEFAULT 0,
            validation_errors_count INTEGER NOT NULL DEFAULT 0,
            execution_time_ms INTEGER NOT NULL,
            http_status_code INTEGER,
            quarantine_file_path VARCHAR(255),
            quarantine_sha256 CHAR(64),
            quarantine_size_bytes BIGINT,
            error_details JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    op.execute(
        """
        CREATE TABLE characters (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            server_id VARCHAR(32) NOT NULL REFERENCES servers(id),
            char_name VARCHAR(64) NOT NULL,
            race VARCHAR(4) NOT NULL DEFAULT 'UNK' CHECK (race IN ('CH', 'EU', 'UNK')),
            first_seen_at TIMESTAMPTZ NOT NULL,
            last_seen_at TIMESTAMPTZ NOT NULL,
            CONSTRAINT uq_char_per_server UNIQUE (server_id, char_name)
        )
        """
    )

    op.execute(
        """
        CREATE TABLE guilds (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            server_id VARCHAR(32) NOT NULL REFERENCES servers(id),
            guild_name VARCHAR(64) NOT NULL,
            master_char_name VARCHAR(64),
            first_seen_at TIMESTAMPTZ NOT NULL,
            last_seen_at TIMESTAMPTZ NOT NULL,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            CONSTRAINT uq_guild_per_server UNIQUE (server_id, guild_name)
        )
        """
    )

    op.execute(
        """
        CREATE TABLE character_snapshots (
            id BIGINT GENERATED ALWAYS AS IDENTITY,
            scrape_run_id UUID NOT NULL REFERENCES scrape_runs(id),
            server_id VARCHAR(32) NOT NULL REFERENCES servers(id),
            character_id UUID NOT NULL REFERENCES characters(id),
            guild_id UUID REFERENCES guilds(id),
            rank_position INTEGER NOT NULL,
            level INTEGER NOT NULL,
            race VARCHAR(4),
            item_points NUMERIC(10, 2) NOT NULL DEFAULT 0.0,
            strength INTEGER,
            intellect INTEGER,
            title VARCHAR(64),
            job_type VARCHAR(16),
            job_level INTEGER,
            job_points INTEGER,
            job_exp BIGINT,
            honor_points INTEGER,
            extra_metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT pk_character_snapshots PRIMARY KEY (id, created_at),
            CONSTRAINT uq_char_snapshot_run UNIQUE (scrape_run_id, character_id, created_at)
        ) PARTITION BY RANGE (created_at)
        """
    )

    op.execute(
        """
        CREATE TABLE guild_snapshots (
            id BIGINT GENERATED ALWAYS AS IDENTITY,
            scrape_run_id UUID NOT NULL REFERENCES scrape_runs(id),
            server_id VARCHAR(32) NOT NULL REFERENCES servers(id),
            guild_id UUID NOT NULL REFERENCES guilds(id),
            rank_position INTEGER NOT NULL,
            members_count INTEGER NOT NULL DEFAULT 0,
            total_item_points NUMERIC(12, 2) NOT NULL DEFAULT 0.0,
            fortress_held VARCHAR(32),
            extra_metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT pk_guild_snapshots PRIMARY KEY (id, created_at),
            CONSTRAINT uq_guild_snapshot_run UNIQUE (scrape_run_id, guild_id, created_at)
        ) PARTITION BY RANGE (created_at)
        """
    )

    op.execute(
        """
        CREATE TABLE character_snapshots_2026_11 PARTITION OF character_snapshots
            FOR VALUES FROM ('2026-11-01 00:00:00+00') TO ('2026-12-01 00:00:00+00')
        """
    )
    op.execute(
        """
        CREATE TABLE character_snapshots_2026_12 PARTITION OF character_snapshots
            FOR VALUES FROM ('2026-12-01 00:00:00+00') TO ('2027-01-01 00:00:00+00')
        """
    )
    op.execute(
        """
        CREATE TABLE character_snapshots_default PARTITION OF character_snapshots DEFAULT
        """
    )

    op.execute(
        """
        CREATE TABLE guild_snapshots_2026_11 PARTITION OF guild_snapshots
            FOR VALUES FROM ('2026-11-01 00:00:00+00') TO ('2026-12-01 00:00:00+00')
        """
    )
    op.execute(
        """
        CREATE TABLE guild_snapshots_2026_12 PARTITION OF guild_snapshots
            FOR VALUES FROM ('2026-12-01 00:00:00+00') TO ('2027-01-01 00:00:00+00')
        """
    )
    op.execute(
        """
        CREATE TABLE guild_snapshots_default PARTITION OF guild_snapshots DEFAULT
        """
    )

    op.execute(
        """
        CREATE INDEX idx_char_snapshots_lookup
            ON character_snapshots (server_id, character_id, created_at DESC)
        """
    )
    op.execute(
        """
        CREATE INDEX idx_char_snapshots_rank
            ON character_snapshots (server_id, created_at, rank_position)
        """
    )
    op.execute(
        """
        CREATE INDEX idx_char_snapshots_brin_time
            ON character_snapshots USING BRIN (created_at)
        """
    )
    op.execute(
        """
        CREATE INDEX idx_char_snapshots_extra_metrics
            ON character_snapshots USING GIN (extra_metrics)
        """
    )

    op.execute(
        """
        CREATE INDEX idx_guild_snapshots_lookup
            ON guild_snapshots (server_id, guild_id, created_at DESC)
        """
    )
    op.execute(
        """
        CREATE INDEX idx_guild_snapshots_rank
            ON guild_snapshots (server_id, created_at, rank_position)
        """
    )
    op.execute(
        """
        CREATE INDEX idx_guild_snapshots_brin_time
            ON guild_snapshots USING BRIN (created_at)
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_guild_snapshots_brin_time")
    op.execute("DROP INDEX IF EXISTS idx_guild_snapshots_rank")
    op.execute("DROP INDEX IF EXISTS idx_guild_snapshots_lookup")

    op.execute("DROP INDEX IF EXISTS idx_char_snapshots_extra_metrics")
    op.execute("DROP INDEX IF EXISTS idx_char_snapshots_brin_time")
    op.execute("DROP INDEX IF EXISTS idx_char_snapshots_rank")
    op.execute("DROP INDEX IF EXISTS idx_char_snapshots_lookup")

    op.execute("DROP TABLE IF EXISTS guild_snapshots_default")
    op.execute("DROP TABLE IF EXISTS guild_snapshots_2026_12")
    op.execute("DROP TABLE IF EXISTS guild_snapshots_2026_11")
    op.execute("DROP TABLE IF EXISTS guild_snapshots")

    op.execute("DROP TABLE IF EXISTS character_snapshots_default")
    op.execute("DROP TABLE IF EXISTS character_snapshots_2026_12")
    op.execute("DROP TABLE IF EXISTS character_snapshots_2026_11")
    op.execute("DROP TABLE IF EXISTS character_snapshots")

    op.execute("DROP TABLE IF EXISTS guilds")
    op.execute("DROP TABLE IF EXISTS characters")
    op.execute("DROP TABLE IF EXISTS scrape_runs")
    op.execute("DROP TABLE IF EXISTS servers")
