"""Seed RSilkroad target server into the servers table."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "004_seed_rsilkroad"
down_revision: str | Sequence[str] | None = "003_dashboard_indexes"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
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
        ) VALUES (
            'rsilkroad',
            'RSilkroad',
            'https://www.rsilkroad.com',
            'v1',
            TRUE,
            0,
            '20:00:00',
            100
        )
        ON CONFLICT (id) DO NOTHING
        """
    )


def downgrade() -> None:
    op.execute("DELETE FROM servers WHERE id = 'rsilkroad'")
