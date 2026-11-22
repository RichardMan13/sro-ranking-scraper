"""Create analytical PostgreSQL 16 views for time-series deltas and classification."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "002_analytical_views"
down_revision: str | Sequence[str] | None = "001_initial_schema"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE OR REPLACE VIEW v_character_activity_deltas AS
        WITH sequenced_snapshots AS (
            SELECT
                cs.server_id,
                cs.character_id,
                c.char_name,
                COALESCE(cs.race, c.race) AS race,
                cs.rank_position,
                cs.level,
                cs.item_points,
                cs.job_type,
                cs.job_level,
                cs.job_points,
                cs.job_exp,
                cs.honor_points,
                cs.created_at,
                LAG(cs.level) OVER w AS prev_level,
                LAG(cs.item_points) OVER w AS prev_item_points,
                LAG(cs.job_points) OVER w AS prev_job_points,
                LAG(cs.job_exp) OVER w AS prev_job_exp,
                LAG(cs.honor_points) OVER w AS prev_honor_points,
                LAG(cs.created_at) OVER w AS prev_created_at,
                LAG(cs.rank_position) OVER w AS prev_rank
            FROM character_snapshots cs
            JOIN characters c ON c.id = cs.character_id
            WINDOW w AS (
                PARTITION BY cs.server_id, cs.character_id
                ORDER BY cs.created_at
            )
        )
        SELECT
            server_id,
            character_id,
            char_name,
            race,
            rank_position,
            (prev_rank - rank_position) AS rank_gain,
            level,
            (level - prev_level) AS delta_level,
            (item_points - prev_item_points) AS delta_item_points,
            (job_points - prev_job_points) AS delta_job_points,
            (job_exp - prev_job_exp) AS delta_job_exp,
            (honor_points - prev_honor_points) AS delta_honor_points,
            created_at,
            prev_created_at,
            ROUND(
                CAST(
                    EXTRACT(EPOCH FROM (created_at - prev_created_at)) / 3600
                    AS NUMERIC
                ),
                2
            ) AS hours_since_last_snapshot,
            CASE
                WHEN prev_created_at IS NOT NULL
                    AND EXTRACT(EPOCH FROM (created_at - prev_created_at)) > 0
                THEN ROUND(
                    CAST(
                        (item_points - prev_item_points)
                        / (EXTRACT(EPOCH FROM (created_at - prev_created_at)) / 3600)
                        AS NUMERIC
                    ),
                    2
                )
                ELSE 0
            END AS item_points_rate_per_hour
        FROM sequenced_snapshots
        WHERE prev_created_at IS NOT NULL
        """
    )

    op.execute(
        """
        CREATE OR REPLACE VIEW v_bot_vs_donor_classification AS
        WITH recent_activity AS (
            SELECT
                server_id,
                character_id,
                char_name,
                race,
                COUNT(*) AS evaluated_intervals,
                SUM(delta_item_points) AS total_item_points_gain,
                MAX(delta_item_points) AS max_single_jump_item_points,
                MIN(hours_since_last_snapshot) AS min_interval_hours,
                COUNT(*) FILTER (WHERE
                    COALESCE(delta_level, 0) > 0
                    OR COALESCE(delta_job_exp, 0) > 0
                    OR COALESCE(delta_job_points, 0) > 0
                    OR COALESCE(delta_honor_points, 0) > 0
                ) AS active_grind_intervals
            FROM v_character_activity_deltas
            WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '7 days'
            GROUP BY server_id, character_id, char_name, race
            HAVING COUNT(*) >= 6
        )
        SELECT
            server_id,
            character_id,
            char_name,
            race,
            evaluated_intervals,
            total_item_points_gain,
            max_single_jump_item_points,
            ROUND(
                CAST(
                    active_grind_intervals::NUMERIC / evaluated_intervals
                    AS NUMERIC
                ),
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
        """
    )

    op.execute(
        """
        CREATE OR REPLACE VIEW v_guild_turnover_dominance AS
        WITH latest_runs AS (
            SELECT DISTINCT ON (server_id) id, server_id, created_at
            FROM scrape_runs
            WHERE status IN ('SUCCESS', 'PARTIAL_SUCCESS')
            ORDER BY server_id, created_at DESC
        ),
        top_players AS (
            SELECT
                cs.server_id,
                cs.guild_id,
                g.guild_name,
                cs.rank_position,
                cs.item_points
            FROM character_snapshots cs
            JOIN latest_runs lr ON lr.id = cs.scrape_run_id
            LEFT JOIN guilds g ON g.id = cs.guild_id
            WHERE cs.rank_position <= 100
        )
        SELECT
            server_id,
            COALESCE(guild_name, '[SEM GUILDA]') AS guild_name,
            COUNT(*) AS players_in_top_100,
            COUNT(*) FILTER (WHERE rank_position <= 50) AS players_in_top_50,
            ROUND(SUM(item_points), 2) AS total_top_item_points,
            ROUND(
                CAST(COUNT(*) * 100.0 / 100 AS NUMERIC),
                2
            ) AS top_100_market_share_percent
        FROM top_players
        GROUP BY server_id, guild_name
        ORDER BY players_in_top_100 DESC, total_top_item_points DESC
        """
    )

    op.execute(
        """
        CREATE OR REPLACE VIEW v_race_balance_ch_vs_eu AS
        WITH latest_runs AS (
            SELECT DISTINCT ON (server_id) id, server_id, created_at
            FROM scrape_runs
            WHERE status IN ('SUCCESS', 'PARTIAL_SUCCESS')
            ORDER BY server_id, created_at DESC
        )
        SELECT
            cs.server_id,
            COALESCE(cs.race, c.race, 'UNK') AS race,
            COUNT(*) AS total_ranked_characters,
            COUNT(*) FILTER (WHERE cs.rank_position <= 100) AS top_100_count,
            ROUND(AVG(cs.level), 1) AS avg_level,
            ROUND(AVG(cs.item_points), 2) AS avg_item_points,
            ROUND(MAX(cs.item_points), 2) AS max_item_points
        FROM character_snapshots cs
        JOIN latest_runs lr ON lr.id = cs.scrape_run_id
        JOIN characters c ON c.id = cs.character_id
        GROUP BY cs.server_id, COALESCE(cs.race, c.race, 'UNK')
        """
    )

    op.execute(
        """
        CREATE OR REPLACE VIEW v_server_retention_weekly AS
        SELECT
            server_id,
            DATE_TRUNC('week', created_at) AS week_start,
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
        FROM v_character_activity_deltas
        GROUP BY server_id, DATE_TRUNC('week', created_at)
        ORDER BY server_id, week_start DESC
        """
    )


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS v_server_retention_weekly")
    op.execute("DROP VIEW IF EXISTS v_race_balance_ch_vs_eu")
    op.execute("DROP VIEW IF EXISTS v_guild_turnover_dominance")
    op.execute("DROP VIEW IF EXISTS v_bot_vs_donor_classification")
    op.execute("DROP VIEW IF EXISTS v_character_activity_deltas")
