"""Add partial index on scrape_runs for fast latest valid run lookup in dashboard."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "003_dashboard_indexes"
down_revision: str | Sequence[str] | None = "002_analytical_views"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_scrape_runs_server_latest
        ON scrape_runs (server_id, created_at DESC)
        WHERE status IN ('SUCCESS', 'PARTIAL_SUCCESS')
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_scrape_runs_server_latest")
