"""Read-only analytical Streamlit dashboard for SRO Ranking Scraper (v3.0).

This module is intentionally free of `pandas` imports and has zero side-effects
at import time. All database queries execute lazily for the active view only,
through a synchronous `psycopg` read-only SQLAlchemy engine.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC
from typing import Any, Final

import altair as alt
import streamlit as st
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from src.core.logging import configure_logging, get_logger
from src.dashboard.config import get_dashboard_settings
from src.dashboard.db import create_dashboard_engine, create_dashboard_session_factory
from src.dashboard.queries import (
    get_bot_vs_donor_classification,
    get_guild_dominance,
    get_race_balance,
    get_server_freshness,
    get_top_characters_drilldown,
    get_top_n_item_points_evolution,
    get_weekly_retention,
    list_active_servers,
)
from src.dashboard.schemas import (
    BehaviorClassificationRow,
    BehaviorSegment,
    GuildDominanceRow,
    RaceBalanceRow,
    ServerFreshness,
    ServerOption,
    TopCharacterTrendPoint,
    TopNAggregatePoint,
    WeeklyRetentionRow,
)

VIEW_OPTIONS: Final[tuple[str, ...]] = (
    "Evolução Item Points",
    "Dominância de Guildas",
    "Balanço de Raças",
    "Retenção Semanal",
    "Radar Bot vs Whale",
)

ALL_BEHAVIOR_SEGMENTS: Final[tuple[BehaviorSegment, ...]] = (
    "SUSPECTED_BOT_24_7",
    "SUSPECTED_DONATOR_WHALE",
    "HARDCORE_HUMAN",
    "CASUAL_OR_BALANCED",
)

RACE_DOMAIN: Final[list[str]] = ["CH", "EU", "UNK"]
RACE_COLOR_RANGE: Final[list[str]] = ["#e15759", "#4e79a7", "#bab0ac"]

SEGMENT_DOMAIN: Final[list[str]] = list(ALL_BEHAVIOR_SEGMENTS)
SEGMENT_COLOR_RANGE: Final[list[str]] = ["#d62728", "#ff7f0e", "#2ca02c", "#1f77b4"]


@dataclass(frozen=True, slots=True)
class _CachedLoaders:
    list_servers: Callable[[], list[ServerOption]]
    freshness: Callable[[str], ServerFreshness]
    top_n_evolution: Callable[[str, int], list[TopNAggregatePoint]]
    top_characters_drilldown: Callable[[str, int, int], list[TopCharacterTrendPoint]]
    guild_dominance: Callable[[str], list[GuildDominanceRow]]
    race_balance: Callable[[str], list[RaceBalanceRow]]
    weekly_retention: Callable[[str, int], list[WeeklyRetentionRow]]
    bot_vs_donor: Callable[[str], list[BehaviorClassificationRow]]


@st.cache_resource
def _get_session_factory() -> sessionmaker[Session]:
    settings = get_dashboard_settings()
    engine = create_dashboard_engine(settings)
    return create_dashboard_session_factory(engine)


@st.cache_resource
def _get_cached_loaders(ttl_seconds: int) -> _CachedLoaders:
    @st.cache_data(ttl=ttl_seconds, max_entries=10)
    def _load_active_servers() -> list[ServerOption]:
        factory = _get_session_factory()
        with factory() as session:
            return list_active_servers(session)

    @st.cache_data(ttl=ttl_seconds, max_entries=10)
    def _load_freshness(server_id: str) -> ServerFreshness:
        factory = _get_session_factory()
        with factory() as session:
            return get_server_freshness(session, server_id)

    @st.cache_data(ttl=ttl_seconds, max_entries=10)
    def _load_top_n_evolution(server_id: str, window_days: int) -> list[TopNAggregatePoint]:
        factory = _get_session_factory()
        with factory() as session:
            return get_top_n_item_points_evolution(session, server_id, window_days=window_days)

    @st.cache_data(ttl=ttl_seconds, max_entries=10)
    def _load_top_characters_drilldown(
        server_id: str, window_days: int, top_n: int
    ) -> list[TopCharacterTrendPoint]:
        factory = _get_session_factory()
        with factory() as session:
            return get_top_characters_drilldown(
                session, server_id, window_days=window_days, top_n=top_n
            )

    @st.cache_data(ttl=ttl_seconds, max_entries=10)
    def _load_guild_dominance(server_id: str) -> list[GuildDominanceRow]:
        factory = _get_session_factory()
        with factory() as session:
            return get_guild_dominance(session, server_id)

    @st.cache_data(ttl=ttl_seconds, max_entries=10)
    def _load_race_balance(server_id: str) -> list[RaceBalanceRow]:
        factory = _get_session_factory()
        with factory() as session:
            return get_race_balance(session, server_id)

    @st.cache_data(ttl=ttl_seconds, max_entries=10)
    def _load_weekly_retention(server_id: str, window_weeks: int) -> list[WeeklyRetentionRow]:
        factory = _get_session_factory()
        with factory() as session:
            return get_weekly_retention(session, server_id, window_weeks=window_weeks)

    @st.cache_data(ttl=ttl_seconds, max_entries=10)
    def _load_bot_vs_donor(server_id: str) -> list[BehaviorClassificationRow]:
        factory = _get_session_factory()
        with factory() as session:
            return get_bot_vs_donor_classification(session, server_id)

    return _CachedLoaders(
        list_servers=_load_active_servers,
        freshness=_load_freshness,
        top_n_evolution=_load_top_n_evolution,
        top_characters_drilldown=_load_top_characters_drilldown,
        guild_dominance=_load_guild_dominance,
        race_balance=_load_race_balance,
        weekly_retention=_load_weekly_retention,
        bot_vs_donor=_load_bot_vs_donor,
    )


def _altair_data(values: list[dict[str, Any]]) -> Any:
    """Wrap a list of dicts as an Altair InlineData object without pandas."""
    return alt.Data(values=values)  # type: ignore[no-untyped-call]


def _handle_query_error(server_id: str, view_name: str, exc: SQLAlchemyError) -> None:
    logger = get_logger("dashboard.app")
    logger.warning(
        "dashboard_query_failed",
        server_id=server_id,
        view_name=view_name,
        error_type=type(exc).__name__,
    )
    st.warning(
        "Não foi possível carregar os dados desta visão no momento. "
        "Tente novamente em alguns instantes ou clique em 'Atualizar dados'."
    )


def _render_view_1_item_points(loaders: _CachedLoaders, server_id: str) -> None:
    st.subheader("Evolução Diária de Item Points (Top N)")
    col_win, col_tier = st.columns([1, 2])
    with col_win:
        window_label = st.selectbox("Janela temporal", options=("30d", "90d", "180d"), index=0)
        window_days = int(window_label.rstrip("d"))
    with col_tier:
        selected_tier = st.segmented_control(
            "Faixa de rank",
            options=("Top 10", "Top 50", "Top 100"),
            default="Top 100",
            key="view1_tier",
        )
        if selected_tier is None:
            selected_tier = "Top 100"

    try:
        rows = loaders.top_n_evolution(server_id, window_days)
    except SQLAlchemyError as exc:
        _handle_query_error(server_id, "item_points_evolution", exc)
        return

    if not rows:
        st.info("Ainda não há snapshots suficientes neste servidor para o período selecionado.")
        return

    tier_rows = [r for r in rows if r.tier == selected_tier]
    if not tier_rows:
        st.info(f"Sem registros disponíveis para a faixa {selected_tier} no período selecionado.")
        return

    long_values: list[dict[str, Any]] = []
    for point in tier_rows:
        day_iso = point.snapshot_day.isoformat()
        long_values.extend(
            (
                {
                    "snapshot_day": day_iso,
                    "metric": "Média",
                    "item_points": point.avg_item_points,
                    "sample_size": point.sample_size,
                },
                {
                    "snapshot_day": day_iso,
                    "metric": "Mediana (P50)",
                    "item_points": point.median_item_points,
                    "sample_size": point.sample_size,
                },
                {
                    "snapshot_day": day_iso,
                    "metric": "P90",
                    "item_points": point.p90_item_points,
                    "sample_size": point.sample_size,
                },
                {
                    "snapshot_day": day_iso,
                    "metric": "Teto Rank #1",
                    "item_points": point.rank1_item_points,
                    "sample_size": point.sample_size,
                },
            )
        )

    chart = (
        alt.Chart(_altair_data(long_values))
        .mark_line(point=True)
        .encode(
            x=alt.X("snapshot_day:T", title="Data (UTC)"),
            y=alt.Y("item_points:Q", title="Item Points"),
            color=alt.Color("metric:N", title="Série"),
            tooltip=[
                alt.Tooltip("snapshot_day:T", title="Data (UTC)"),
                alt.Tooltip("metric:N", title="Série"),
                alt.Tooltip("item_points:Q", title="Item Points", format=",.2f"),
                alt.Tooltip("sample_size:Q", title="Amostra"),
            ],
        )
        .properties(height=380)
        .interactive()
    )
    st.altair_chart(chart, use_container_width=True)

    show_drilldown = st.toggle("Exibir drill-down individual (Top 5-10)", value=False)
    if show_drilldown:
        top_n = st.slider("Quantidade de personagens (Top N atual)", min_value=5, max_value=10)
        try:
            dd_rows = loaders.top_characters_drilldown(server_id, window_days, top_n)
        except SQLAlchemyError as exc:
            _handle_query_error(server_id, "top_characters_drilldown", exc)
            return

        if not dd_rows:
            st.info("Sem histórico suficiente para o drill-down individual.")
            return

        dd_values = [
            {
                "snapshot_day": r.snapshot_day.isoformat(),
                "char_name": r.char_name,
                "rank_position": r.rank_position,
                "item_points": r.item_points,
            }
            for r in dd_rows
        ]
        col_ip, col_rank = st.columns(2)
        with col_ip:
            ip_chart = (
                alt.Chart(_altair_data(dd_values))
                .mark_line(point=True)
                .encode(
                    x=alt.X("snapshot_day:T", title="Data (UTC)"),
                    y=alt.Y("item_points:Q", title="Item Points"),
                    color=alt.Color("char_name:N", title="Personagem"),
                    tooltip=[
                        alt.Tooltip("snapshot_day:T", title="Data (UTC)"),
                        alt.Tooltip("char_name:N", title="Personagem"),
                        alt.Tooltip("item_points:Q", title="Item Points", format=",.2f"),
                        alt.Tooltip("rank_position:Q", title="Rank"),
                    ],
                )
                .properties(title="Item Points por Personagem", height=320)
                .interactive()
            )
            st.altair_chart(ip_chart, use_container_width=True)
        with col_rank:
            rank_chart = (
                alt.Chart(_altair_data(dd_values))
                .mark_line(point=True)
                .encode(
                    x=alt.X("snapshot_day:T", title="Data (UTC)"),
                    y=alt.Y(
                        "rank_position:Q",
                        title="Posição no Rank",
                        scale=alt.Scale(reverse=True),
                    ),
                    color=alt.Color("char_name:N", title="Personagem"),
                    tooltip=[
                        alt.Tooltip("snapshot_day:T", title="Data (UTC)"),
                        alt.Tooltip("char_name:N", title="Personagem"),
                        alt.Tooltip("rank_position:Q", title="Rank"),
                        alt.Tooltip("item_points:Q", title="Item Points", format=",.2f"),
                    ],
                )
                .properties(title="Evolução de Rank (menor é melhor)", height=320)
                .interactive()
            )
            st.altair_chart(rank_chart, use_container_width=True)


def _render_view_2_guild_dominance(loaders: _CachedLoaders, server_id: str) -> None:
    st.subheader("Dominância de Guildas no Top 100")
    try:
        rows = loaders.guild_dominance(server_id)
    except SQLAlchemyError as exc:
        _handle_query_error(server_id, "guild_dominance", exc)
        return

    if not rows:
        st.info(
            "Ainda não há snapshots válidos neste servidor para calcular dominância de guildas."
        )
        return

    sample_total = rows[0].sample_total_players
    st.caption(
        f"Amostra atual do Top 100: **{sample_total}** jogadores ranqueados. "
        "Percentuais exibidos tanto sobre a amostra real quanto sobre 100 vagas nominais."
    )

    values = [
        {
            **asdict(r),
            "is_no_guild": "Sem Guilda" if r.guild_name == "[SEM GUILDA]" else "Guilda",
            "label": (
                f"{r.players_in_top_100}/{r.sample_total_players} "
                f"({r.sample_share_pct:.1f}% amostra | {r.nominal_top_100_pct:.1f}% / 100)"
            ),
        }
        for r in rows
    ]

    base = alt.Chart(_altair_data(values)).encode(
        y=alt.Y("guild_name:N", sort="-x", title="Guilda"),
        x=alt.X("players_in_top_100:Q", title="Jogadores no Top 100"),
    )
    bars = base.mark_bar().encode(
        color=alt.Color(
            "is_no_guild:N",
            title="Categoria",
            scale=alt.Scale(
                domain=["Guilda", "Sem Guilda"],
                range=["#4e79a7", "#bab0ac"],
            ),
        ),
        tooltip=[
            alt.Tooltip("guild_name:N", title="Guilda"),
            alt.Tooltip("players_in_top_100:Q", title="Top 100"),
            alt.Tooltip("players_in_top_50:Q", title="Top 50"),
            alt.Tooltip("sample_share_pct:Q", title="% da Amostra", format=".2f"),
            alt.Tooltip("nominal_top_100_pct:Q", title="% Nominal (100 vagas)", format=".2f"),
            alt.Tooltip("total_top_item_points:Q", title="Soma Item Points", format=",.2f"),
        ],
    )
    labels = base.mark_text(align="left", baseline="middle", dx=4, fontSize=11).encode(
        text=alt.Text("label:N"),
    )
    chart = (bars + labels).properties(height=max(260, min(600, len(rows) * 28)))
    st.altair_chart(chart, use_container_width=True)
    st.dataframe([asdict(r) for r in rows], use_container_width=True, hide_index=True)


def _render_view_3_race_balance(loaders: _CachedLoaders, server_id: str) -> None:
    st.subheader("Balanço de Raças (CH vs EU)")
    try:
        rows = loaders.race_balance(server_id)
    except SQLAlchemyError as exc:
        _handle_query_error(server_id, "race_balance", exc)
        return

    if not rows:
        st.info("Ainda não há snapshots válidos neste servidor para calcular o balanço de raças.")
        return

    total_chars = sum(r.total_ranked_characters for r in rows)
    cols = st.columns(len(rows))
    for col, row in zip(cols, rows, strict=True):
        share = (row.total_ranked_characters * 100.0 / total_chars) if total_chars > 0 else 0.0
        col.metric(
            label=f"Raça {row.race}",
            value=f"{row.total_ranked_characters} ({share:.1f}%)",
            delta=f"Top 100: {row.top_100_count} | Avg IP: {row.avg_item_points:.1f}",
        )

    values = [asdict(r) for r in rows]
    chart = (
        alt.Chart(_altair_data(values))
        .mark_bar()
        .encode(
            x=alt.X("race:N", title="Raça", sort=RACE_DOMAIN),
            y=alt.Y("total_ranked_characters:Q", title="Personagens Ranqueados"),
            color=alt.Color(
                "race:N",
                title="Raça",
                scale=alt.Scale(domain=RACE_DOMAIN, range=RACE_COLOR_RANGE),
            ),
            tooltip=[
                alt.Tooltip("race:N", title="Raça"),
                alt.Tooltip("total_ranked_characters:Q", title="Total Ranqueados"),
                alt.Tooltip("top_100_count:Q", title="No Top 100"),
                alt.Tooltip("avg_level:Q", title="Nível Médio", format=".1f"),
                alt.Tooltip("avg_item_points:Q", title="Item Points Médio", format=",.2f"),
                alt.Tooltip("max_item_points:Q", title="Item Points Máximo", format=",.2f"),
            ],
        )
        .properties(height=340)
    )
    st.altair_chart(chart, use_container_width=True)
    st.dataframe(values, use_container_width=True, hide_index=True)


def _render_view_4_weekly_retention(loaders: _CachedLoaders, server_id: str) -> None:
    st.subheader("Retenção Semanal de Atividade")
    window_label = st.selectbox("Janela semanal", options=("12w", "26w"), index=0)
    window_weeks = int(window_label.rstrip("w"))

    try:
        rows = loaders.weekly_retention(server_id, window_weeks)
    except SQLAlchemyError as exc:
        _handle_query_error(server_id, "weekly_retention", exc)
        return

    if not rows:
        st.info(
            "São necessários pelo menos 2 snapshots em datas distintas para calcular "
            "a retenção semanal de atividade."
        )
        return

    values = [
        {
            "week_start": r.week_start.isoformat(),
            "total_tracked_characters": r.total_tracked_characters,
            "active_characters": r.active_characters,
            "activity_retention_rate_pct": r.activity_retention_rate_pct,
            "status": "Semana em andamento" if r.is_current_week else "Semana fechada",
            "bar_opacity": 0.45 if r.is_current_week else 0.95,
        }
        for r in rows
    ]

    if any(r.is_current_week for r in rows):
        st.caption(
            "A barra translúcida representa a **semana corrente em andamento** "
            "(alinhada à segunda-feira 00:00 UTC) e pode variar até o fechamento da semana."
        )

    chart = (
        alt.Chart(_altair_data(values))
        .mark_bar()
        .encode(
            x=alt.X("week_start:O", title="Início da Semana (UTC)"),
            y=alt.Y(
                "activity_retention_rate_pct:Q",
                title="Taxa de Retenção (%)",
                scale=alt.Scale(domain=[0, 100]),
            ),
            color=alt.Color(
                "status:N",
                title="Status da Semana",
                scale=alt.Scale(
                    domain=["Semana fechada", "Semana em andamento"],
                    range=["#4e79a7", "#f28e2b"],
                ),
            ),
            opacity=alt.Opacity("bar_opacity:Q", legend=None, scale=None),
            tooltip=[
                alt.Tooltip("week_start:O", title="Semana (UTC)"),
                alt.Tooltip("status:N", title="Status"),
                alt.Tooltip("active_characters:Q", title="Ativos"),
                alt.Tooltip("total_tracked_characters:Q", title="Rastreados"),
                alt.Tooltip("activity_retention_rate_pct:Q", title="Retenção (%)", format=".2f"),
            ],
        )
        .properties(height=360)
    )
    st.altair_chart(chart, use_container_width=True)
    st.dataframe(
        [
            {
                "week_start": r.week_start.isoformat(),
                "total_tracked_characters": r.total_tracked_characters,
                "active_characters": r.active_characters,
                "activity_retention_rate_pct": r.activity_retention_rate_pct,
                "is_current_week": r.is_current_week,
            }
            for r in rows
        ],
        use_container_width=True,
        hide_index=True,
    )


def _render_view_5_bot_vs_whale(loaders: _CachedLoaders, server_id: str) -> None:
    st.subheader("Radar Comportamental: Bot vs Whale (Últimos 7 dias)")
    try:
        rows = loaders.bot_vs_donor(server_id)
    except SQLAlchemyError as exc:
        _handle_query_error(server_id, "bot_vs_donor", exc)
        return

    if not rows:
        st.info(
            "Ainda não há histórico suficiente para o Radar Bot vs Whale. "
            "São necessários pelo menos 7 snapshots nos últimos 7 dias (6 intervalos avaliados)."
        )
        return

    selected_segments = st.multiselect(
        "Filtrar por segmento comportamental",
        options=list(ALL_BEHAVIOR_SEGMENTS),
        default=list(ALL_BEHAVIOR_SEGMENTS),
    )

    filtered = [r for r in rows if r.player_behavior_segment in selected_segments]
    if not filtered:
        st.info("Nenhum personagem corresponde aos segmentos selecionados.")
        return

    values = [asdict(r) for r in filtered]
    scatter = (
        alt.Chart(_altair_data(values))
        .mark_circle(size=85, opacity=0.8)
        .encode(
            x=alt.X(
                "grind_persistence_ratio:Q",
                title="Persistência de Grind (0.0 - 1.0)",
                scale=alt.Scale(domain=[0, 1]),
            ),
            y=alt.Y(
                "max_single_jump_item_points:Q",
                title="Maior Salto Único de Item Points",
            ),
            color=alt.Color(
                "player_behavior_segment:N",
                title="Segmento",
                scale=alt.Scale(domain=SEGMENT_DOMAIN, range=SEGMENT_COLOR_RANGE),
            ),
            tooltip=[
                alt.Tooltip("char_name:N", title="Personagem"),
                alt.Tooltip("race:N", title="Raça"),
                alt.Tooltip("player_behavior_segment:N", title="Segmento"),
                alt.Tooltip("grind_persistence_ratio:Q", title="Persistência", format=".2f"),
                alt.Tooltip("max_single_jump_item_points:Q", title="Maior Salto IP", format=",.2f"),
                alt.Tooltip("total_item_points_gain:Q", title="Ganho Total IP (7d)", format=",.2f"),
                alt.Tooltip("evaluated_intervals:Q", title="Intervalos Avaliados"),
            ],
        )
        .properties(height=400)
        .interactive()
    )
    st.altair_chart(scatter, use_container_width=True)
    st.dataframe(values, use_container_width=True, hide_index=True)


def main() -> None:
    """Streamlit entrypoint for the SRO Ranking Scraper analytical dashboard."""
    st.set_page_config(
        page_title="SRO Ranking Analytics",
        page_icon="📊",
        layout="wide",
    )

    settings = get_dashboard_settings()
    configure_logging(settings.log_level)
    loaders = _get_cached_loaders(settings.dashboard_cache_ttl_seconds)

    st.title("SRO Ranking Scraper — Painel Analítico")

    try:
        servers = loaders.list_servers()
    except SQLAlchemyError as exc:
        _handle_query_error("all", "list_active_servers", exc)
        return

    if not servers:
        st.info("Nenhum servidor ativo encontrado no banco de dados.")
        return

    server_map = {s.display_name: s for s in servers}
    with st.sidebar:
        st.header("Configuração")
        selected_name = st.selectbox("Servidor ativo", options=list(server_map.keys()))
        active_server = server_map[selected_name]

        try:
            freshness = loaders.freshness(active_server.id)
        except SQLAlchemyError as exc:
            _handle_query_error(active_server.id, "server_freshness", exc)
            freshness = ServerFreshness(server_id=active_server.id, last_snapshot_at=None)

        if freshness.last_snapshot_at is not None:
            ts_str = freshness.last_snapshot_at.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC")
            st.caption(f"Último snapshot: {ts_str}")
        else:
            st.caption("Sem snapshots registrados")

        if st.button("Atualizar dados", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    selected_view = st.segmented_control(
        "Visão analítica",
        options=VIEW_OPTIONS,
        default=VIEW_OPTIONS[0],
    )
    if selected_view is None:
        selected_view = VIEW_OPTIONS[0]

    if selected_view == VIEW_OPTIONS[0]:
        _render_view_1_item_points(loaders, active_server.id)
    elif selected_view == VIEW_OPTIONS[1]:
        _render_view_2_guild_dominance(loaders, active_server.id)
    elif selected_view == VIEW_OPTIONS[2]:
        _render_view_3_race_balance(loaders, active_server.id)
    elif selected_view == VIEW_OPTIONS[3]:
        _render_view_4_weekly_retention(loaders, active_server.id)
    elif selected_view == VIEW_OPTIONS[4]:
        _render_view_5_bot_vs_whale(loaders, active_server.id)


if __name__ == "__main__":
    main()
