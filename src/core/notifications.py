from __future__ import annotations

import asyncio
import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Final, Literal

import httpx
from pydantic import SecretStr
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import Settings
from src.core.logging import get_logger
from src.models.db_models import Guild, GuildSnapshot, ScrapeRun, Server
from src.models.pydantic_schemas import ScrapeStatus
from src.scrapers.base import ScrapeRunSummary

OPS_ALERT_STATUSES: Final[frozenset[ScrapeStatus]] = frozenset(
    {
        "SCHEMA_DRIFT",
        "LOW_COUNT_QUARANTINE",
        "BLOCKED_CHALLENGE",
        "HTTP_ERROR",
    }
)

POST_FW_SUCCESS_STATUSES: Final[frozenset[ScrapeStatus]] = frozenset(
    {
        "SUCCESS",
        "PARTIAL_SUCCESS",
    }
)

WEBHOOK_TIMEOUT_SECONDS: Final[float] = 10.0
WEBHOOK_HTTP_TIMEOUT: Final[httpx.Timeout] = httpx.Timeout(
    connect=5.0,
    read=10.0,
    write=5.0,
    pool=5.0,
)
POST_FW_BASELINE_WINDOW_HOURS: Final[int] = 24
POST_FW_SNAPSHOT_TOLERANCE_MINUTES: Final[int] = 5
POST_FW_TOP_GUILDS_LIMIT: Final[int] = 5

EMBED_TITLE_MAX_LEN: Final[int] = 256
EMBED_DESCRIPTION_MAX_LEN: Final[int] = 4096
EMBED_FIELD_VALUE_MAX_LEN: Final[int] = 1024
EMBED_FOOTER_MAX_LEN: Final[int] = 2048

_OPS_COLORS: Final[dict[str, int]] = {
    "SCHEMA_DRIFT": 0xE74C3C,
    "LOW_COUNT_QUARANTINE": 0xE74C3C,
    "BLOCKED_CHALLENGE": 0xE67E22,
    "HTTP_ERROR": 0xF1C40F,
}
_POST_FW_COLORS: Final[dict[str, int]] = {
    "SUCCESS": 0x3498DB,
    "PARTIAL_SUCCESS": 0xF39C12,
}

_NEWLINE_RE: Final[re.Pattern[str]] = re.compile(r"[\r\n]+")
_MARKDOWN_ESCAPE_RE: Final[re.Pattern[str]] = re.compile(r"([\\*_`~\[\]()|>])")


@dataclass(frozen=True, slots=True)
class GuildRankEntry:
    """Single guild entry rendered in the post-Fortress War Top 5 summary."""

    rank: int
    guild_name: str
    total_item_points: Decimal
    members_count: int
    fortress_held: str | None


@dataclass(frozen=True, slots=True)
class PostFWComparison:
    """Read-only comparison between a post-FW snapshot and its pre-FW baseline."""

    server_id: str
    server_display_name: str
    current_run_id: uuid.UUID
    current_run_at: datetime
    current_status: str
    baseline_run_id: uuid.UUID | None
    baseline_run_at: datetime | None
    current_fortresses: dict[str, str]
    baseline_fortresses: dict[str, str]
    current_top_guilds: tuple[GuildRankEntry, ...]
    baseline_rank_by_guild: dict[str, int]


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _format_utc(dt: datetime) -> str:
    return _ensure_utc(dt).strftime("%Y-%m-%d %H:%M UTC")


def _escape_markdown(text: str) -> str:
    collapsed = _NEWLINE_RE.sub(" ", text)
    return _MARKDOWN_ESCAPE_RE.sub(r"\\\1", collapsed)


def _truncate(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    return f"{text[: max_len - 3]}..."


def _format_item_points_ptbr(points: Decimal) -> str:
    rounded = int(points.quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    formatted = f"{rounded:,}".replace(",", ".")
    return f"{formatted} IP"


def build_ops_alert_payload(summary: ScrapeRunSummary) -> dict[str, Any]:
    """Build a Portuguese Discord embed payload for critical operational failures."""
    color = _OPS_COLORS.get(summary.status, 0xE74C3C)
    title = _truncate(
        f"[SRO Alert] {summary.server_id} — {summary.status}",
        EMBED_TITLE_MAX_LEN,
    )
    http_status = str(summary.http_status_code) if summary.http_status_code is not None else "N/A"
    quarantine_value = (
        _truncate(
            f"`{_escape_markdown(summary.quarantine_file_path)}`",
            EMBED_FIELD_VALUE_MAX_LEN,
        )
        if summary.quarantine_file_path
        else "N/A"
    )

    fields: list[dict[str, Any]] = [
        {
            "name": "Servidor",
            "value": _truncate(_escape_markdown(summary.server_id), EMBED_FIELD_VALUE_MAX_LEN),
            "inline": True,
        },
        {
            "name": "Status",
            "value": _truncate(summary.status, EMBED_FIELD_VALUE_MAX_LEN),
            "inline": True,
        },
        {
            "name": "Gatilho",
            "value": _truncate(summary.trigger_type, EMBED_FIELD_VALUE_MAX_LEN),
            "inline": True,
        },
        {"name": "HTTP Status", "value": http_status, "inline": True},
        {"name": "Duração", "value": f"{summary.execution_time_ms} ms", "inline": True},
        {
            "name": "Extração / Erros",
            "value": (
                f"{summary.records_extracted} extraídos / {summary.validation_errors_count} erros"
            ),
            "inline": True,
        },
        {"name": "Run ID", "value": f"`{summary.run_id}`", "inline": False},
        {"name": "Quarentena", "value": quarantine_value, "inline": False},
    ]

    if summary.circuit_breaker_until is not None:
        fields.append(
            {
                "name": "Circuit Breaker",
                "value": _format_utc(summary.circuit_breaker_until),
                "inline": True,
            }
        )

    if summary.error_details is not None:
        details_raw = (
            f"{_escape_markdown(summary.error_details.failed_category)}: "
            f"{_escape_markdown(summary.error_details.reason)}"
        )
        fields.append(
            {
                "name": "Detalhes",
                "value": _truncate(details_raw, EMBED_FIELD_VALUE_MAX_LEN),
                "inline": False,
            }
        )

    footer_text = _truncate(
        f"SRO Ranking Scraper • {_format_utc(datetime.now(UTC))}",
        EMBED_FOOTER_MAX_LEN,
    )

    return {
        "allowed_mentions": {"parse": []},
        "embeds": [
            {
                "title": title,
                "color": color,
                "fields": fields,
                "footer": {"text": footer_text},
            }
        ],
    }


def build_post_fw_payload(comparison: PostFWComparison) -> dict[str, Any]:
    """Build a Portuguese Discord embed payload summarizing post-Fortress War results."""
    color = _POST_FW_COLORS.get(comparison.current_status, 0x3498DB)
    title = _truncate(
        f"Resumo Pós-Fortress War — {_escape_markdown(comparison.server_display_name)}",
        EMBED_TITLE_MAX_LEN,
    )
    current_ts = _format_utc(comparison.current_run_at)
    has_baseline = comparison.baseline_run_id is not None and comparison.baseline_run_at is not None

    if has_baseline and comparison.baseline_run_at is not None:
        baseline_ts = _format_utc(comparison.baseline_run_at)
        description = f"Comparação entre **{current_ts}** e baseline pré-FW (**{baseline_ts}**)."
    else:
        description = (
            f"Retrato pós-FW em **{current_ts}**. "
            "*(Sem baseline pré-FW nas últimas 24h para comparação.)*"
        )

    if comparison.current_status == "PARTIAL_SUCCESS":
        description += " *(Coleta parcial com <= 5% de erros de validação.)*"
    description = _truncate(description, EMBED_DESCRIPTION_MAX_LEN)

    all_fortresses = sorted(
        set(comparison.current_fortresses) | set(comparison.baseline_fortresses)
    )
    if not all_fortresses:
        fortresses_value = "Nenhuma fortaleza registrada neste snapshot."
    else:
        fortress_lines: list[str] = []
        for fortress in all_fortresses:
            esc_fortress = _escape_markdown(fortress)
            curr_guild = comparison.current_fortresses.get(fortress)
            prev_guild = comparison.baseline_fortresses.get(fortress)
            if not has_baseline:
                if curr_guild is not None:
                    fortress_lines.append(f"• **{esc_fortress}:** {_escape_markdown(curr_guild)}")
            elif curr_guild is not None and prev_guild is not None:
                if curr_guild == prev_guild:
                    fortress_lines.append(
                        f"• **{esc_fortress}:** {_escape_markdown(curr_guild)} (mantida)"
                    )
                else:
                    fortress_lines.append(
                        f"• **{esc_fortress}:** {_escape_markdown(prev_guild)} → "
                        f"{_escape_markdown(curr_guild)} (nova posse)"
                    )
            elif curr_guild is not None:
                fortress_lines.append(
                    f"• **{esc_fortress}:** Sem dono → {_escape_markdown(curr_guild)}"
                )
            elif prev_guild is not None:
                fortress_lines.append(
                    f"• **{esc_fortress}:** {_escape_markdown(prev_guild)} → Sem dono"
                )
        fortresses_value = _truncate(
            "\n".join(fortress_lines)
            if fortress_lines
            else "Nenhuma fortaleza registrada neste snapshot.",
            EMBED_FIELD_VALUE_MAX_LEN,
        )

    if not comparison.current_top_guilds:
        top_guilds_value = "Ranking de guildas indisponível neste snapshot."
    else:
        guild_lines: list[str] = []
        for entry in comparison.current_top_guilds:
            esc_guild = _escape_markdown(entry.guild_name)
            pts_str = _format_item_points_ptbr(entry.total_item_points)
            if not has_baseline:
                guild_lines.append(
                    f"#{entry.rank} **{esc_guild}** — {pts_str} ({entry.members_count} membros)"
                )
            else:
                prev_rank = comparison.baseline_rank_by_guild.get(entry.guild_name)
                if prev_rank is None:
                    delta = "(nova)"
                else:
                    diff = prev_rank - entry.rank
                    if diff > 0:
                        delta = f"(+{diff})"
                    elif diff < 0:
                        delta = f"({diff})"
                    else:
                        delta = "(=)"
                guild_lines.append(
                    f"#{entry.rank} **{esc_guild}** {delta} — {pts_str} "
                    f"({entry.members_count} membros)"
                )
        top_guilds_value = _truncate("\n".join(guild_lines), EMBED_FIELD_VALUE_MAX_LEN)

    footer_text = _truncate(
        f"Run {comparison.current_run_id} • {current_ts}",
        EMBED_FOOTER_MAX_LEN,
    )

    return {
        "allowed_mentions": {"parse": []},
        "embeds": [
            {
                "title": title,
                "description": description,
                "color": color,
                "fields": [
                    {"name": "Fortalezas", "value": fortresses_value, "inline": False},
                    {"name": "Top 5 Guildas", "value": top_guilds_value, "inline": False},
                ],
                "footer": {"text": footer_text},
            }
        ],
    }


async def send_discord_webhook(
    webhook_url: SecretStr | None,
    payload: Mapping[str, Any],
    *,
    channel: Literal["ops", "community"],
    server_id: str,
    run_id: uuid.UUID,
) -> bool:
    """Send a best-effort Discord webhook POST with strict timeout and zero retries."""
    if webhook_url is None:
        return False

    logger = get_logger("notifications")
    run_id_str = str(run_id)

    try:
        async with (
            asyncio.timeout(WEBHOOK_TIMEOUT_SECONDS),
            httpx.AsyncClient(
                timeout=WEBHOOK_HTTP_TIMEOUT,
                follow_redirects=False,
            ) as client,
        ):
            response = await client.post(
                webhook_url.get_secret_value(),
                json=dict(payload),
            )
    except Exception as exc:
        logger.warning(
            "discord_webhook_failed",
            channel=channel,
            server_id=server_id,
            run_id=run_id_str,
            error_type=type(exc).__name__,
        )
        return False

    if 200 <= response.status_code < 300:
        logger.info(
            "discord_webhook_sent",
            channel=channel,
            server_id=server_id,
            run_id=run_id_str,
            http_status_code=response.status_code,
        )
        return True

    logger.warning(
        "discord_webhook_http_error",
        channel=channel,
        server_id=server_id,
        run_id=run_id_str,
        http_status_code=response.status_code,
    )
    return False


async def notify_operational_failure(
    summary: ScrapeRunSummary,
    settings: Settings,
) -> bool:
    """Send an operational Discord alert when a run ends in a critical failure status."""
    if summary.status not in OPS_ALERT_STATUSES or settings.discord_ops_webhook_url is None:
        return False

    try:
        payload = build_ops_alert_payload(summary)
        return await send_discord_webhook(
            settings.discord_ops_webhook_url,
            payload,
            channel="ops",
            server_id=summary.server_id,
            run_id=summary.run_id,
        )
    except Exception as exc:
        get_logger("notifications").warning(
            "discord_ops_notification_error",
            channel="ops",
            server_id=summary.server_id,
            run_id=str(summary.run_id),
            error_type=type(exc).__name__,
        )
        return False


async def load_post_fw_comparison(
    session: AsyncSession,
    summary: ScrapeRunSummary,
) -> PostFWComparison | None:
    """Load current post-FW guild snapshot and compare against the 24h pre-FW baseline."""
    logger = get_logger("notifications")

    current_run_stmt = select(ScrapeRun.created_at).where(ScrapeRun.id == summary.run_id).limit(1)
    current_created_at = await session.scalar(current_run_stmt)
    if current_created_at is None:
        logger.warning(
            "post_fw_run_not_found",
            server_id=summary.server_id,
            run_id=str(summary.run_id),
        )
        return None

    current_run_at = _ensure_utc(current_created_at)

    server_name_stmt = select(Server.display_name).where(Server.id == summary.server_id).limit(1)
    display_name = await session.scalar(server_name_stmt)
    server_display_name = display_name if display_name is not None else summary.server_id

    window_start = current_run_at - timedelta(hours=POST_FW_BASELINE_WINDOW_HOURS)
    primary_baseline_stmt = (
        select(ScrapeRun.id, ScrapeRun.created_at)
        .where(
            ScrapeRun.server_id == summary.server_id,
            ScrapeRun.id != summary.run_id,
            ScrapeRun.status.in_(POST_FW_SUCCESS_STATUSES),
            ScrapeRun.created_at >= window_start,
            ScrapeRun.created_at <= current_run_at,
            ScrapeRun.trigger_type != "post_fw_extra",
        )
        .order_by(ScrapeRun.created_at.desc(), ScrapeRun.id.desc())
        .limit(1)
    )
    baseline_row = (await session.execute(primary_baseline_stmt)).first()

    if baseline_row is None:
        fallback_baseline_stmt = (
            select(ScrapeRun.id, ScrapeRun.created_at)
            .where(
                ScrapeRun.server_id == summary.server_id,
                ScrapeRun.id != summary.run_id,
                ScrapeRun.status.in_(POST_FW_SUCCESS_STATUSES),
                ScrapeRun.created_at >= window_start,
                ScrapeRun.created_at <= current_run_at,
            )
            .order_by(ScrapeRun.created_at.desc(), ScrapeRun.id.desc())
            .limit(1)
        )
        baseline_row = (await session.execute(fallback_baseline_stmt)).first()

    baseline_run_id: uuid.UUID | None = None
    baseline_run_at: datetime | None = None
    if baseline_row is not None:
        baseline_run_id = baseline_row.id
        baseline_run_at = _ensure_utc(baseline_row.created_at)

    tolerance = timedelta(minutes=POST_FW_SNAPSHOT_TOLERANCE_MINUTES)
    current_snap_stmt = (
        select(
            GuildSnapshot.guild_id,
            GuildSnapshot.rank_position,
            Guild.guild_name,
            GuildSnapshot.total_item_points,
            GuildSnapshot.members_count,
            GuildSnapshot.fortress_held,
        )
        .join(Guild, Guild.id == GuildSnapshot.guild_id)
        .where(
            GuildSnapshot.server_id == summary.server_id,
            GuildSnapshot.scrape_run_id == summary.run_id,
            GuildSnapshot.created_at >= current_run_at - tolerance,
            GuildSnapshot.created_at <= current_run_at + tolerance,
            or_(
                GuildSnapshot.rank_position <= POST_FW_TOP_GUILDS_LIMIT,
                GuildSnapshot.fortress_held.is_not(None),
            ),
        )
        .order_by(GuildSnapshot.rank_position.asc())
    )
    current_rows = (await session.execute(current_snap_stmt)).all()

    current_top_guilds: list[GuildRankEntry] = []
    current_top_guild_ids: list[uuid.UUID] = []
    current_fortresses: dict[str, str] = {}

    for row in current_rows:
        cleaned_fortress = (
            row.fortress_held.strip()
            if isinstance(row.fortress_held, str) and row.fortress_held.strip()
            else None
        )
        if cleaned_fortress is not None and cleaned_fortress not in current_fortresses:
            current_fortresses[cleaned_fortress] = row.guild_name

        if row.rank_position <= POST_FW_TOP_GUILDS_LIMIT:
            current_top_guild_ids.append(row.guild_id)
            current_top_guilds.append(
                GuildRankEntry(
                    rank=int(row.rank_position),
                    guild_name=str(row.guild_name),
                    total_item_points=Decimal(str(row.total_item_points)),
                    members_count=int(row.members_count),
                    fortress_held=cleaned_fortress,
                )
            )

    baseline_fortresses: dict[str, str] = {}
    baseline_rank_by_guild: dict[str, int] = {}

    if baseline_run_id is not None and baseline_run_at is not None:
        baseline_filter = (
            or_(
                GuildSnapshot.guild_id.in_(current_top_guild_ids),
                GuildSnapshot.fortress_held.is_not(None),
            )
            if current_top_guild_ids
            else GuildSnapshot.fortress_held.is_not(None)
        )
        baseline_snap_stmt = (
            select(
                GuildSnapshot.rank_position,
                Guild.guild_name,
                GuildSnapshot.fortress_held,
            )
            .join(Guild, Guild.id == GuildSnapshot.guild_id)
            .where(
                GuildSnapshot.server_id == summary.server_id,
                GuildSnapshot.scrape_run_id == baseline_run_id,
                GuildSnapshot.created_at >= baseline_run_at - tolerance,
                GuildSnapshot.created_at <= baseline_run_at + tolerance,
                baseline_filter,
            )
            .order_by(GuildSnapshot.rank_position.asc())
        )
        baseline_rows = (await session.execute(baseline_snap_stmt)).all()

        for base_row in baseline_rows:
            baseline_rank_by_guild.setdefault(
                str(base_row.guild_name),
                int(base_row.rank_position),
            )
            cleaned_fortress = (
                base_row.fortress_held.strip()
                if isinstance(base_row.fortress_held, str) and base_row.fortress_held.strip()
                else None
            )
            if cleaned_fortress is not None and cleaned_fortress not in baseline_fortresses:
                baseline_fortresses[cleaned_fortress] = str(base_row.guild_name)

    return PostFWComparison(
        server_id=summary.server_id,
        server_display_name=server_display_name,
        current_run_id=summary.run_id,
        current_run_at=current_run_at,
        current_status=summary.status,
        baseline_run_id=baseline_run_id,
        baseline_run_at=baseline_run_at,
        current_fortresses=current_fortresses,
        baseline_fortresses=baseline_fortresses,
        current_top_guilds=tuple(current_top_guilds),
        baseline_rank_by_guild=baseline_rank_by_guild,
    )


async def notify_post_fw_summary(
    comparison: PostFWComparison | None,
    settings: Settings,
) -> bool:
    """Send the post-Fortress War summary embed to the community Discord channel."""
    if comparison is None or settings.discord_community_webhook_url is None:
        return False

    try:
        payload = build_post_fw_payload(comparison)
        return await send_discord_webhook(
            settings.discord_community_webhook_url,
            payload,
            channel="community",
            server_id=comparison.server_id,
            run_id=comparison.current_run_id,
        )
    except Exception as exc:
        get_logger("notifications").warning(
            "discord_community_notification_error",
            channel="community",
            server_id=comparison.server_id,
            run_id=str(comparison.current_run_id),
            error_type=type(exc).__name__,
        )
        return False
