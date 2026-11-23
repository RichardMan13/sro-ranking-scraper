from __future__ import annotations

import argparse
import asyncio
import os
import tempfile
from collections.abc import Awaitable, Sequence
from datetime import time
from pathlib import Path
from typing import cast


def _ensure_accessible_prefect_home() -> None:
    """Redirect `PREFECT_HOME` to temp storage only if `~/.prefect` is inaccessible."""
    if os.environ.get("PREFECT_HOME"):
        return
    try:
        (Path.home() / ".prefect" / "profiles.toml").exists()
    except OSError:
        fallback = Path(tempfile.gettempdir()) / "sro_ranking_scraper_prefect_home"
        fallback.mkdir(parents=True, exist_ok=True)
        os.environ["PREFECT_HOME"] = str(fallback)


_ensure_accessible_prefect_home()

from prefect import State, flow, serve  # noqa: E402
from prefect.deployments.runner import RunnerDeployment  # noqa: E402
from sqlalchemy import select  # noqa: E402
from sqlalchemy.ext.asyncio import AsyncEngine  # noqa: E402

from src.core.config import Settings, get_settings  # noqa: E402
from src.core.database import create_engine_from_url, create_session_factory  # noqa: E402
from src.core.logging import configure_logging, get_logger  # noqa: E402
from src.core.notifications import (  # noqa: E402
    POST_FW_SUCCESS_STATUSES,
    PostFWComparison,
    load_post_fw_comparison,
    notify_post_fw_summary,
)
from src.flows.tasks import (  # noqa: E402
    ensure_partitions_task,
    purge_old_quarantine_task,
    scrape_target_task,
)
from src.models.db_models import Server  # noqa: E402
from src.scrapers.base import ScrapeRunSummary  # noqa: E402

VALID_MANUAL_TARGETS: tuple[str, ...] = ("all", "chronicle", "electus", "play_golden")


def compute_post_fw_cron(fw_day_of_week: int, fw_end_time_utc: time) -> str:
    """Compute a weekly cron expression 30 minutes after Fortress War ends.

    Uses standard cron weekday numbering (`0=Sunday .. 6=Saturday`) with
    minute, hour, and week rollover.
    """
    if not 0 <= fw_day_of_week <= 6:
        raise ValueError(f"fw_day_of_week must be between 0 and 6, got {fw_day_of_week}")

    total_minutes = fw_end_time_utc.hour * 60 + fw_end_time_utc.minute + 30
    day_carry, rem_minutes = divmod(total_minutes, 24 * 60)
    hour, minute = divmod(rem_minutes, 60)
    cron_day = (fw_day_of_week + day_carry) % 7
    return f"{minute} {hour} * * {cron_day}"


async def get_active_servers(settings: Settings | None = None) -> list[Server]:
    """Load all active server rows ordered deterministically by `Server.id`."""
    resolved_settings = settings if settings is not None else get_settings()
    engine = create_engine_from_url(str(resolved_settings.database_url))
    session_factory = create_session_factory(engine)

    try:
        async with session_factory() as session:
            stmt = select(Server).where(Server.is_active.is_(True)).order_by(Server.id)
            result = await session.scalars(stmt)
            return list(result.all())
    finally:
        await engine.dispose()


@flow(name="scheduled-12h-flow")
async def scheduled_12h_flow() -> dict[str, ScrapeRunSummary | str]:
    """Run the 12-hour scheduled scrape across all active servers with failure isolation."""
    settings = get_settings()
    configure_logging(settings.log_level)
    logger = get_logger("orchestrator")

    await ensure_partitions_task()
    servers = await get_active_servers(settings=settings)
    outcomes: dict[str, ScrapeRunSummary | str] = {}

    for server in servers:
        state = await cast(
            Awaitable[State[ScrapeRunSummary]],
            scrape_target_task(
                server_id=server.id,
                trigger_type="scheduled_12h",
                return_state=True,
            ),
        )
        if state.is_completed():
            result = await state.aresult(raise_on_failure=False)
            outcomes[server.id] = result if isinstance(result, ScrapeRunSummary) else str(result)
        else:
            logger.error(
                "scheduled_target_failed",
                server_id=server.id,
                state_name=state.name,
                state_message=state.message,
            )
            outcomes[server.id] = f"FAILED:{state.name}"

    purge_old_quarantine_task(
        retention_days=settings.quarantine_retention_days,
        quarantine_dir=settings.quarantine_dir,
    )
    return outcomes


@flow(name="post-fw-flow")
async def post_fw_flow(server_id: str) -> ScrapeRunSummary:
    """Run an extraordinary post-Fortress War scrape for a single server."""
    settings = get_settings()
    configure_logging(settings.log_level)
    logger = get_logger("orchestrator")

    await ensure_partitions_task()
    summary = await scrape_target_task(server_id=server_id, trigger_type="post_fw_extra")

    if (
        summary.status in POST_FW_SUCCESS_STATUSES
        and settings.discord_community_webhook_url is not None
    ):
        comparison: PostFWComparison | None = None
        engine: AsyncEngine | None = None
        try:
            engine = create_engine_from_url(str(settings.database_url))
            session_factory = create_session_factory(engine)
            async with session_factory() as session:
                comparison = await load_post_fw_comparison(session, summary)
        except Exception as exc:
            logger.warning(
                "post_fw_comparison_load_failed",
                server_id=summary.server_id,
                run_id=str(summary.run_id),
                error_type=type(exc).__name__,
            )
        finally:
            if engine is not None:
                await engine.dispose()

        await notify_post_fw_summary(comparison, settings=settings)

    return summary


@flow(name="manual-scrape-flow")
async def manual_scrape_flow(target: str = "all") -> dict[str, ScrapeRunSummary | str]:
    """Run a manual scrape immediately for either `'all'` active servers or one `server_id`."""
    settings = get_settings()
    configure_logging(settings.log_level)
    logger = get_logger("orchestrator")

    await ensure_partitions_task()

    if target == "all":
        servers = await get_active_servers(settings=settings)
        outcomes: dict[str, ScrapeRunSummary | str] = {}
        for server in servers:
            state = await cast(
                Awaitable[State[ScrapeRunSummary]],
                scrape_target_task(
                    server_id=server.id,
                    trigger_type="manual",
                    return_state=True,
                ),
            )
            if state.is_completed():
                result = await state.aresult(raise_on_failure=False)
                outcomes[server.id] = (
                    result if isinstance(result, ScrapeRunSummary) else str(result)
                )
            else:
                logger.error(
                    "manual_target_failed",
                    server_id=server.id,
                    state_name=state.name,
                    state_message=state.message,
                )
                outcomes[server.id] = f"FAILED:{state.name}"
        return outcomes

    summary = await scrape_target_task(server_id=target, trigger_type="manual")
    return {target: summary}


def build_deployments(servers: Sequence[Server]) -> list[RunnerDeployment]:
    """Build Prefect 3 runner deployments for 12h scheduled and dynamic post-FW runs."""
    deployments: list[RunnerDeployment] = [
        cast(
            RunnerDeployment,
            scheduled_12h_flow.to_deployment(
                name="scheduled-12h",
                cron="0 0,12 * * *",
            ),
        )
    ]
    for server in servers:
        cron_expr = compute_post_fw_cron(server.fw_day_of_week, server.fw_end_time_utc)
        deployments.append(
            cast(
                RunnerDeployment,
                post_fw_flow.to_deployment(
                    name=f"post-fw-{server.id}",
                    cron=cron_expr,
                    parameters={"server_id": server.id},
                ),
            )
        )
    return deployments


def serve_deployments() -> None:
    """Load active servers from PostgreSQL and serve all Prefect 3 deployments."""
    settings = get_settings()
    configure_logging(settings.log_level)
    servers = asyncio.run(get_active_servers(settings=settings))
    deployments = build_deployments(servers)
    serve(*deployments)


def build_parser() -> argparse.ArgumentParser:
    """Create the CLI argument parser for the SRO Ranking Scraper orchestrator."""
    parser = argparse.ArgumentParser(
        prog="python -m src.flows.orchestrator",
        description="SRO Ranking Scraper Prefect 3 orchestrator and manual trigger CLI.",
    )
    parser.add_argument(
        "--mode",
        choices=("serve", "manual"),
        required=True,
        help="Execution mode: 'serve' registers cron deployments; 'manual' runs immediately.",
    )
    parser.add_argument(
        "--target",
        choices=VALID_MANUAL_TARGETS,
        default="all",
        help="Target server for '--mode manual' (default: all active servers).",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint dispatching between `--mode serve` and `--mode manual`."""
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.mode == "serve":
        serve_deployments()
        return 0

    asyncio.run(manual_scrape_flow(target=args.target))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
