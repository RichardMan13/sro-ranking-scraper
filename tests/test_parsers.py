from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
import respx
from sqlalchemy.ext.asyncio import AsyncSession
from src.core.config import Settings
from src.models.db_models import Server
from src.models.pydantic_schemas import (
    RawGuildRankingItem,
    RawJobRankingItem,
    RawPlayerRankingItem,
)
from src.scrapers import (
    BaseRankingScraper,
    ChronicleScraper,
    ElectusScraper,
    PlayGoldenScraper,
    RSilkroadScraper,
    get_scraper,
)
from src.scrapers.base import (
    BlockedChallengeError,
    CategoryFetchResult,
    ScrapeRunSummary,
    TargetMaintenanceError,
    TransportFetchError,
)

from tests.test_validation import (
    FakeAsyncSession,
    _make_settings,
    _run_async,
    _verify_quarantine_artifact,
)

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def _load_fixture_bytes(name: str) -> bytes:
    return (FIXTURES_DIR / name).read_bytes()


def _load_fixture_text(name: str) -> str:
    return (FIXTURES_DIR / name).read_text(encoding="utf-8")


def _make_target_server(
    server_id: str,
    base_url: str,
    min_records_expected: int,
    *,
    display_name: str | None = None,
    circuit_breaker_until: datetime | None = None,
) -> Server:
    return Server(
        id=server_id,
        display_name=display_name or server_id.replace("_", " ").title(),
        base_url=base_url,
        min_records_expected=min_records_expected,
        circuit_breaker_until=circuit_breaker_until,
    )


def _make_electus_settings(
    tmp_path: Path,
    *,
    cf_clearance: str | None = None,
) -> Settings:
    return Settings(
        _env_file=None,
        database_url="postgresql+asyncpg://user:pass@localhost:5432/sro_test",
        quarantine_dir=tmp_path / "quarantine",
        circuit_breaker_cooldown_hours=6,
        electus_cf_clearance=cf_clearance,
    )


# ---------------------------------------------------------------------------
# 1. Registry & Exports (`src/scrapers/__init__.py`)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("server_id", "expected_cls"),
    [
        ("chronicle", ChronicleScraper),
        ("electus", ElectusScraper),
        ("play_golden", PlayGoldenScraper),
        ("rsilkroad", RSilkroadScraper),
    ],
)
def test_get_scraper_returns_expected_subclass(
    tmp_path: Path,
    server_id: str,
    expected_cls: type[BaseRankingScraper],
) -> None:
    settings = _make_settings(tmp_path)
    scraper = get_scraper(server_id, settings=settings)

    assert isinstance(scraper, expected_cls)
    assert isinstance(scraper, BaseRankingScraper)
    assert scraper.server_id == server_id
    assert scraper.settings is settings


@pytest.mark.parametrize("bad_server_id", ["unknown", "", "CHRONICLE", "golden"])
def test_get_scraper_unknown_server_raises_value_error(
    tmp_path: Path,
    bad_server_id: str,
) -> None:
    settings = _make_settings(tmp_path)
    with pytest.raises(ValueError, match="Unknown server_id"):
        get_scraper(bad_server_id, settings=settings)


# ---------------------------------------------------------------------------
# 2. ChronicleScraper Parser & Pagination Tests
# ---------------------------------------------------------------------------


def test_chronicle_parsers_extract_all_categories_and_stop_early(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    scraper = ChronicleScraper(settings, retry_delays=(0.0, 0.0))
    base_url = "https://play-chronicle.com"

    player_html = _load_fixture_bytes("chronicle_player.html")
    trader_page1 = _load_fixture_bytes("chronicle_trader_page1.html")
    trader_page2 = _load_fixture_bytes("chronicle_trader_page2.html")
    hunter_page1 = _load_fixture_bytes("chronicle_hunter_page1.html")
    thieves_page1 = _load_fixture_bytes("chronicle_thieves_page1.html")
    guild_html = _load_fixture_bytes("chronicle_guild.html")
    empty_html = _load_fixture_bytes("chronicle_empty_page.html")

    async def execute() -> tuple[CategoryFetchResult, CategoryFetchResult, CategoryFetchResult]:
        with respx.mock(assert_all_called=True) as mock_router:
            mock_router.get(f"{base_url}/ranking/top-player-1.html").mock(
                return_value=httpx.Response(200, content=player_html)
            )
            mock_router.get(f"{base_url}/ranking/top-player-2.html").mock(
                return_value=httpx.Response(200, content=empty_html)
            )
            mock_router.get(f"{base_url}/ranking/job/top-trader-1.html").mock(
                return_value=httpx.Response(200, content=trader_page1)
            )
            mock_router.get(f"{base_url}/ranking/job/top-trader-2.html").mock(
                return_value=httpx.Response(200, content=trader_page2)
            )
            mock_router.get(f"{base_url}/ranking/job/top-hunter-1.html").mock(
                return_value=httpx.Response(200, content=hunter_page1)
            )
            mock_router.get(f"{base_url}/ranking/job/top-hunter-2.html").mock(
                return_value=httpx.Response(200, content=empty_html)
            )
            mock_router.get(f"{base_url}/ranking/job/top-thieves-1.html").mock(
                return_value=httpx.Response(200, content=thieves_page1)
            )
            mock_router.get(f"{base_url}/ranking/top-guild-1.html").mock(
                return_value=httpx.Response(200, content=guild_html)
            )
            mock_router.get(f"{base_url}/ranking/top-guild-2.html").mock(
                return_value=httpx.Response(200, content=empty_html)
            )
            async with httpx.AsyncClient(http2=True) as client:
                p_res = await scraper.fetch_player_pages(client, base_url)
                j_res = await scraper.fetch_job_pages(client, base_url)
                g_res = await scraper.fetch_guild_pages(client, base_url)
                return p_res, j_res, g_res

    player_res, job_res, guild_res = _run_async(execute())

    assert len(player_res.raw_records) == 100
    assert len(player_res.raw_payloads) == 2

    p1 = RawPlayerRankingItem.model_validate(player_res.raw_records[0])
    assert p1.rank == 1
    assert p1.char_name == "Chron_Hero_001"
    assert p1.race == "CH"
    assert p1.guild_name == "ApexLegion"
    assert p1.item_points == 1501.5

    # Rank 4 has '-' guild placeholder and alt="European" race fallback
    p4 = RawPlayerRankingItem.model_validate(player_res.raw_records[3])
    assert p4.char_name == "Chron_Hero_004"
    assert p4.race == "EU"
    assert p4.guild_name is None

    # Rank 3 uses EU float formatting 1.503,50
    p3 = RawPlayerRankingItem.model_validate(player_res.raw_records[2])
    assert p3.item_points == 1503.5

    # Job parsing extracts TRADER, HUNTER, and THIEF filters with job_level and job_exp
    assert len(job_res.raw_records) == 10
    jobs = [RawJobRankingItem.model_validate(rec) for rec in job_res.raw_records]
    assert jobs[0].char_name == "Chron_Hero_001"
    assert jobs[0].job_type == "TRADER"
    assert jobs[0].job_level == 7
    assert jobs[0].job_exp == 1250000
    assert jobs[5].char_name == "Chron_Hero_005"
    assert jobs[5].job_type == "HUNTER"
    assert jobs[5].job_level == 7
    assert jobs[5].job_exp == 910000
    assert jobs[7].char_name == "Chron_Hero_007"
    assert jobs[7].job_type == "THIEF"
    assert jobs[7].job_level == 7
    assert jobs[7].job_exp == 845000

    # Guild parsing pre-splits "45/50", "1,234/2,000", "-/50" and captures guild_level
    assert len(guild_res.raw_records) == 6
    assert guild_res.raw_records[0]["members_count"] == "45"
    assert guild_res.raw_records[1]["members_count"] == "1,234"
    assert guild_res.raw_records[2]["members_count"] == "-"
    guilds = [RawGuildRankingItem.model_validate(rec) for rec in guild_res.raw_records]
    assert guilds[0].members_count == 45
    assert guilds[0].master_name is None
    assert guilds[0].extra_metrics["guild_level"] == 5
    assert guilds[1].members_count == 1234
    assert guilds[1].total_item_points == 118200.25
    assert guilds[2].members_count is None
    assert guilds[3].fortress_held is None


def test_chronicle_handles_missing_tbody_empty_200_and_multipage_dedup(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    scraper = ChronicleScraper(settings, retry_delays=(0.0, 0.0))
    base_url = "https://play-chronicle.com"

    no_tbody_page1 = b"""
    <html><body>
      <table class="table table-striped table-hover">
        <tr>
          <th>#</th><th>Character</th><th>Guild</th><th>Level</th><th>ItemPoints</th>
        </tr>
        <tr>
          <td>1</td><td><img data-race="CH"><a class="char-link">Alpha</a></td>
          <td>G1</td><td>110</td><td>100.0</td>
        </tr>
        <tr>
          <td>2</td><td><img alt="European"><a class="char-link">Bravo</a></td>
          <td>G2</td><td>109</td><td>90.0</td>
        </tr>
      </table>
    </body></html>
    """
    no_tbody_page2 = b"""
    <html><body>
      <table class="table table-striped table-hover">
        <tr>
          <td>3</td><td><img data-race="EU"><a class="char-link">Bravo</a></td>
          <td>G2</td><td>109</td><td>89.0</td>
        </tr>
        <tr>
          <td>4</td><td><img data-race="CH"><a class="char-link">Charlie</a></td>
          <td>G1</td><td>108</td><td>80.0</td>
        </tr>
      </table>
    </body></html>
    """
    colspan_empty_page = b"""
    <html><body>
      <table class="table table-striped table-hover">
        <tbody><tr><td colspan="5">No records found</td></tr></tbody>
      </table>
    </body></html>
    """

    async def execute() -> tuple[CategoryFetchResult, CategoryFetchResult]:
        with respx.mock(assert_all_called=True) as mock_router:
            mock_router.get(f"{base_url}/ranking/top-player-1.html").mock(
                return_value=httpx.Response(200, content=no_tbody_page1)
            )
            mock_router.get(f"{base_url}/ranking/top-player-2.html").mock(
                return_value=httpx.Response(200, content=no_tbody_page2)
            )
            mock_router.get(f"{base_url}/ranking/top-player-3.html").mock(
                return_value=httpx.Response(200, content=colspan_empty_page)
            )
            mock_router.get(f"{base_url}/ranking/job/top-trader-1.html").mock(
                return_value=httpx.Response(200, content=b"")
            )
            mock_router.get(f"{base_url}/ranking/job/top-hunter-1.html").mock(
                return_value=httpx.Response(200, content=b"")
            )
            mock_router.get(f"{base_url}/ranking/job/top-thieves-1.html").mock(
                return_value=httpx.Response(200, content=b"")
            )
            async with httpx.AsyncClient(http2=True) as client:
                p_res = await scraper.fetch_player_pages(client, base_url)
                j_res = await scraper.fetch_job_pages(client, base_url)
                return p_res, j_res

    player_res, job_res = _run_async(execute())

    assert len(player_res.raw_records) == 4
    assert len(player_res.raw_payloads) == 3
    assert player_res.raw_records[1]["char_name"] == "Bravo"
    assert player_res.raw_records[1]["rank"] == "2"
    assert player_res.raw_records[2]["char_name"] == "Bravo"
    assert player_res.raw_records[2]["rank"] == "3"
    assert job_res.raw_records == []
    assert len(job_res.raw_payloads) == 3


# ---------------------------------------------------------------------------
# 3. ElectusScraper JSON API, HTML Fallback, Cookie & Challenge Tests
# ---------------------------------------------------------------------------


def test_electus_json_api_aliases_race_id_zero_values_and_early_stop(tmp_path: Path) -> None:
    settings = _make_electus_settings(tmp_path)
    scraper = ElectusScraper(settings, retry_delays=(0.0, 0.0))
    base_url = "https://electus.online"

    player_json = _load_fixture_bytes("electus_player.json")
    job_json = _load_fixture_bytes("electus_job.json")
    guild_json = _load_fixture_bytes("electus_guild.json")
    empty_json = _load_fixture_bytes("electus_empty.json")

    async def execute() -> tuple[CategoryFetchResult, CategoryFetchResult, CategoryFetchResult]:
        with respx.mock(assert_all_called=True) as mock_router:
            mock_router.get(f"{base_url}/api/ranking/player?page=1&limit=100").mock(
                return_value=httpx.Response(200, content=player_json)
            )
            mock_router.get(f"{base_url}/api/ranking/player?page=2&limit=100").mock(
                return_value=httpx.Response(200, content=empty_json)
            )
            mock_router.get(f"{base_url}/api/ranking/job?page=1&limit=100").mock(
                return_value=httpx.Response(200, content=job_json)
            )
            mock_router.get(f"{base_url}/api/ranking/job?page=2&limit=100").mock(
                return_value=httpx.Response(200, content=empty_json)
            )
            mock_router.get(f"{base_url}/api/ranking/guild?page=1&limit=50").mock(
                return_value=httpx.Response(200, content=guild_json)
            )
            mock_router.get(f"{base_url}/api/ranking/guild?page=2&limit=50").mock(
                return_value=httpx.Response(200, content=empty_json)
            )
            async with httpx.AsyncClient(http2=True) as client:
                p_res = await scraper.fetch_player_pages(client, base_url)
                j_res = await scraper.fetch_job_pages(client, base_url)
                g_res = await scraper.fetch_guild_pages(client, base_url)
                return p_res, j_res, g_res

    player_res, job_res, guild_res = _run_async(execute())

    assert len(player_res.raw_records) == 100
    assert len(player_res.raw_payloads) == 2

    p1 = RawPlayerRankingItem.model_validate(player_res.raw_records[0])
    assert p1.char_name == "Elec_Player_001"
    assert p1.race == "CH"
    assert p1.guild_name == "ElecEmpire"
    assert p1.item_points == 2497.25
    assert p1.extra_metrics == {"elo_points": 1996, "sox_parts": 10, "hours_played": 121}

    p2 = RawPlayerRankingItem.model_validate(player_res.raw_records[1])
    assert p2.race == "EU"

    # Rank 10 verifies 0 values are NOT dropped by falsy `or` coercion
    p10 = RawPlayerRankingItem.model_validate(player_res.raw_records[9])
    assert p10.honor_points == 0
    assert p10.extra_metrics["elo_points"] == 0

    # Job uses `"items"` envelope key + UTF-8 job types
    assert len(job_res.raw_records) == 10
    j5 = RawJobRankingItem.model_validate(job_res.raw_records[4])
    assert j5.job_type == "THIEF"

    assert len(guild_res.raw_records) == 6
    g1 = RawGuildRankingItem.model_validate(guild_res.raw_records[0])
    assert g1.guild_name == "ElecEmpire"
    assert g1.master_name == "Elec_Player_001"
    assert g1.members_count == 50
    assert g1.total_item_points == 210000.5
    assert g1.fortress_held == "Hotan"


@pytest.mark.parametrize("api_failure_mode", ["http_404", "non_json_html"])
def test_electus_html_fallback_on_404_and_non_json_runs_end_to_end(
    tmp_path: Path,
    api_failure_mode: str,
) -> None:
    settings = _make_electus_settings(tmp_path)
    scraper = ElectusScraper(settings, retry_delays=(0.0, 0.0))
    base_url = "https://electus.online"
    server = _make_target_server("electus", base_url, min_records_expected=100)
    session = FakeAsyncSession(server)

    p_html = _load_fixture_bytes("electus_player_fallback.html")
    j_html = _load_fixture_bytes("electus_job_fallback.html")
    g_html = _load_fixture_bytes("electus_guild_fallback.html")
    empty_html = b"<html><body><table id='ranking-table'><tbody></tbody></table></body></html>"

    api_resp = (
        httpx.Response(404, text="Not Found")
        if api_failure_mode == "http_404"
        else httpx.Response(200, text="<html><body>HTML ranking portal</body></html>")
    )

    async def execute() -> ScrapeRunSummary:
        with respx.mock(assert_all_called=True) as mock_router:
            for cat, html_bytes in (
                ("player", p_html),
                ("job", j_html),
                ("guild", g_html),
            ):
                limit = 50 if cat == "guild" else 100
                mock_router.get(f"{base_url}/api/ranking/{cat}?page=1&limit={limit}").mock(
                    return_value=api_resp
                )
                mock_router.get(f"{base_url}/ranking/{cat}?page=1").mock(
                    return_value=httpx.Response(200, content=html_bytes)
                )
                mock_router.get(f"{base_url}/ranking/{cat}?page=2").mock(
                    return_value=httpx.Response(200, content=empty_html)
                )
            return await scraper.run(cast(AsyncSession, session), trigger_type="manual")

    summary = _run_async(execute())

    assert summary.status == "SUCCESS"
    assert summary.records_extracted == 110
    assert summary.records_persisted == 105
    assert session.commit_count == 1
    first_snap = session.char_snapshot_chunks[0][0]
    assert first_snap["race"] == "CH"
    assert first_snap["job_type"] == "TRADER"
    assert first_snap["extra_metrics"]["job_rank"] == 1


@pytest.mark.parametrize(
    ("cf_token", "expected_present"),
    [
        ("cf-abc-123", True),
        (None, False),
        ("   ", False),
    ],
)
def test_electus_cf_clearance_cookie_injection(
    tmp_path: Path,
    cf_token: str | None,
    expected_present: bool,
) -> None:
    settings = _make_electus_settings(tmp_path, cf_clearance=cf_token)
    scraper = ElectusScraper(settings, retry_delays=(0.0, 0.0))
    captured_cookies: list[str | None] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        captured_cookies.append(request.headers.get("cookie"))
        return httpx.Response(200, content=b'{"data": []}')

    async def execute() -> CategoryFetchResult:
        client = httpx.AsyncClient(http2=True, transport=httpx.MockTransport(handler))
        async with client:
            return await scraper.fetch_guild_pages(client, "https://electus.online")

    _run_async(execute())

    assert len(captured_cookies) == 1
    cookie_header = captured_cookies[0]
    if expected_present:
        assert cookie_header is not None
        assert "cf_clearance=cf-abc-123" in cookie_header
    else:
        assert cookie_header is None or "cf_clearance=" not in cookie_header


def test_electus_unknown_json_root_triggers_schema_drift_without_fallback(tmp_path: Path) -> None:
    settings = _make_electus_settings(tmp_path)
    scraper = ElectusScraper(settings, retry_delays=(0.0, 0.0))
    base_url = "https://electus.online"
    server = _make_target_server("electus", base_url, min_records_expected=100)
    session = FakeAsyncSession(server)

    async def execute() -> ScrapeRunSummary:
        with respx.mock(assert_all_called=True) as mock_router:
            mock_router.get(f"{base_url}/api/ranking/player?page=1&limit=100").mock(
                return_value=httpx.Response(200, content=b'{"rows": [{"rank": 1}]}')
            )
            mock_router.get(f"{base_url}/api/ranking/job?page=1&limit=100").mock(
                return_value=httpx.Response(200, content=b'{"data": []}')
            )
            mock_router.get(f"{base_url}/api/ranking/guild?page=1&limit=50").mock(
                return_value=httpx.Response(200, content=b'{"data": []}')
            )
            return await scraper.run(cast(AsyncSession, session), trigger_type="manual")

    summary = _run_async(execute())

    assert summary.status == "SCHEMA_DRIFT"
    assert summary.records_persisted == 0
    _verify_quarantine_artifact(
        summary=summary,
        run_row=session.added_runs[0],
        expected_dir=settings.quarantine_dir,
        expected_substrings=[b'"rows"'],
    )


@pytest.mark.parametrize(
    ("status_code", "fixture_name", "extra_headers"),
    [
        (403, "electus_turnstile_403.html", {}),
        (429, "electus_ratelimit_429.html", {}),
        (200, "electus_turnstile_403.html", {"cf-mitigated": "challenge"}),
    ],
)
def test_electus_challenge_never_falls_back_or_retries_and_trips_cb(
    tmp_path: Path,
    status_code: int,
    fixture_name: str,
    extra_headers: dict[str, str],
) -> None:
    settings = _make_electus_settings(tmp_path)
    scraper = ElectusScraper(settings, retry_delays=(0.0, 0.0))
    base_url = "https://electus.online"
    server = _make_target_server("electus", base_url, min_records_expected=100)
    session = FakeAsyncSession(server)
    challenge_bytes = _load_fixture_bytes(fixture_name)
    requested_urls: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requested_urls.append(str(request.url))
        return httpx.Response(status_code, headers=extra_headers, content=challenge_bytes)

    async def execute_fetch_and_run() -> tuple[BlockedChallengeError, ScrapeRunSummary]:
        client = httpx.AsyncClient(http2=True, transport=httpx.MockTransport(handler))
        async with client:
            with pytest.raises(BlockedChallengeError) as exc_info:
                await scraper.fetch_player_pages(client, base_url)
            summary = await scraper.run(
                cast(AsyncSession, session),
                trigger_type="manual",
                client=client,
            )
            return exc_info.value, summary

    before = datetime.now(UTC)
    exc, summary = _run_async(execute_fetch_and_run())

    assert exc.status_code == status_code
    assert len(requested_urls) == 2
    assert all("/api/ranking/player" in u for u in requested_urls)
    assert summary.status == "BLOCKED_CHALLENGE"
    assert summary.http_status_code == status_code
    assert len(session.server_updates) == 1
    cb_until = cast(datetime, session.server_updates[0]["circuit_breaker_until"])
    assert before + timedelta(hours=5, minutes=59) <= cb_until
    _verify_quarantine_artifact(
        summary=summary,
        run_row=session.added_runs[0],
        expected_dir=settings.quarantine_dir,
        expected_substrings=[challenge_bytes[:40]],
    )


def test_electus_page2_non_json_raises_transport_error_instead_of_html_fallback(
    tmp_path: Path,
) -> None:
    settings = _make_electus_settings(tmp_path)
    scraper = ElectusScraper(settings, retry_delays=(0.0, 0.0))
    base_url = "https://electus.online"
    player_json = _load_fixture_bytes("electus_player.json")

    async def execute() -> None:
        with respx.mock(assert_all_called=True) as mock_router:
            mock_router.get(f"{base_url}/api/ranking/player?page=1&limit=100").mock(
                return_value=httpx.Response(200, content=player_json)
            )
            mock_router.get(f"{base_url}/api/ranking/player?page=2&limit=100").mock(
                return_value=httpx.Response(200, text="<html>Broken gateway</html>")
            )
            async with httpx.AsyncClient(http2=True) as client:
                with pytest.raises(TransportFetchError, match="Non-JSON response on page 2"):
                    await scraper.fetch_player_pages(client, base_url)

    _run_async(execute())


# ---------------------------------------------------------------------------
# 4. PlayGoldenScraper Parser & Pagination Tests
# ---------------------------------------------------------------------------


def test_play_golden_parsers_extract_all_categories_and_stop_early(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    scraper = PlayGoldenScraper(settings, retry_delays=(0.0, 0.0))
    base_url = "https://play-golden.online"

    player_html = _load_fixture_bytes("golden_player.html")
    job_html = _load_fixture_bytes("golden_job.html")
    guild_html = _load_fixture_bytes("golden_guild.html")
    empty_html = _load_fixture_bytes("golden_empty_page.html")

    async def execute() -> tuple[CategoryFetchResult, CategoryFetchResult, CategoryFetchResult]:
        with respx.mock(assert_all_called=True) as mock_router:
            mock_router.get(f"{base_url}/ranking/characters?page=1").mock(
                return_value=httpx.Response(200, content=player_html)
            )
            mock_router.get(f"{base_url}/ranking/characters?page=2").mock(
                return_value=httpx.Response(200, content=empty_html)
            )
            mock_router.get(f"{base_url}/ranking/jobs?page=1").mock(
                return_value=httpx.Response(200, content=job_html)
            )
            mock_router.get(f"{base_url}/ranking/jobs?page=2").mock(
                return_value=httpx.Response(200, content=empty_html)
            )
            mock_router.get(f"{base_url}/ranking/guilds?page=1").mock(
                return_value=httpx.Response(200, content=guild_html)
            )
            mock_router.get(f"{base_url}/ranking/guilds?page=2").mock(
                return_value=httpx.Response(200, content=empty_html)
            )
            async with httpx.AsyncClient(http2=True) as client:
                p_res = await scraper.fetch_player_pages(client, base_url)
                j_res = await scraper.fetch_job_pages(client, base_url)
                g_res = await scraper.fetch_guild_pages(client, base_url)
                return p_res, j_res, g_res

    player_res, job_res, guild_res = _run_async(execute())

    assert len(player_res.raw_records) == 50
    p1 = RawPlayerRankingItem.model_validate(player_res.raw_records[0])
    assert p1.rank == 1
    assert p1.char_name == "Gold_Char_01"
    # alt="Chinese" takes precedence over "/eu/" segment in img src
    assert p1.race == "CH"
    assert p1.strength == 421
    assert p1.intellect == 181
    assert p1.item_points == 1901.25
    assert p1.honor_points == 2990
    assert p1.extra_metrics == {"gold": 1005000, "last_logout": "2026-11-19 08:00:00"}

    # Even rows rely on img src filename "european.png"
    p2 = RawPlayerRankingItem.model_validate(player_res.raw_records[1])
    assert p2.race == "EU"

    # Job column order has guild_name at [5]
    assert len(job_res.raw_records) == 8
    jobs = [RawJobRankingItem.model_validate(rec) for rec in job_res.raw_records]
    assert jobs[0].guild_name == "GoldenCrown"
    assert jobs[4].job_type == "THIEF"
    assert jobs[5].job_type == "HUNTER"

    assert len(guild_res.raw_records) == 5
    guilds = [RawGuildRankingItem.model_validate(rec) for rec in guild_res.raw_records]
    assert guilds[0].guild_name == "GoldenCrown"
    assert guilds[0].members_count == 47
    assert guilds[0].fortress_held == "Jangan"


# ---------------------------------------------------------------------------
# 5. Maintenance 503 Propagation Across All 3 Targets
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("server_id", "base_url", "min_records"),
    [
        ("chronicle", "https://play-chronicle.com", 100),
        ("electus", "https://electus.online", 100),
        ("play_golden", "https://play-golden.online", 50),
        ("rsilkroad", "https://www.rsilkroad.com", 100),
    ],
)
@pytest.mark.parametrize("status_code", [503, 200])
def test_target_maintenance_aborts_without_retry_quarantine_or_cb(
    tmp_path: Path,
    server_id: str,
    base_url: str,
    min_records: int,
    status_code: int,
) -> None:
    settings = _make_settings(tmp_path)
    scraper = get_scraper(server_id, settings=settings)
    server = _make_target_server(server_id, base_url, min_records_expected=min_records)
    session = FakeAsyncSession(server)
    maint_bytes = _load_fixture_bytes("maintenance_503.html")
    attempts = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(status_code, content=maint_bytes)

    async def execute() -> tuple[TargetMaintenanceError, ScrapeRunSummary]:
        client = httpx.AsyncClient(http2=True, transport=httpx.MockTransport(handler))
        async with client:
            with pytest.raises(TargetMaintenanceError) as exc_info:
                await scraper.fetch_player_pages(client, base_url)
            summary = await scraper.run(
                cast(AsyncSession, session),
                trigger_type="manual",
                client=client,
            )
            return exc_info.value, summary

    exc, summary = _run_async(execute())

    assert exc.status_code == status_code
    assert attempts == 2
    assert summary.status == "TARGET_MAINTENANCE"
    assert summary.http_status_code == status_code
    assert summary.quarantine_file_path is None
    assert session.server_updates == []
    assert session.added_runs[0].status == "TARGET_MAINTENANCE"


# ---------------------------------------------------------------------------
# 6. End-to-End run() Transitions, Reconciliation & Guards Using Fixtures
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    (
        "server_id",
        "base_url",
        "min_records",
        "expected_extracted",
        "expected_persisted",
        "enrichment_char",
        "enriched_guild",
        "orphan_char",
    ),
    [
        (
            "chronicle",
            "https://play-chronicle.com",
            100,
            116,
            106,
            "Chron_Hero_004",
            "StormBringers",
            "Chron_Orphan_Job",
        ),
        (
            "electus",
            "https://electus.online",
            100,
            116,
            106,
            "Elec_Player_004",
            "TitanUnion",
            "Elec_Orphan_Job",
        ),
        (
            "play_golden",
            "https://play-golden.online",
            50,
            63,
            55,
            "Gold_Char_03",
            "SilkRiders",
            "Gold_Orphan_Job",
        ),
    ],
)
def test_end_to_end_run_success_and_job_reconciliation(
    tmp_path: Path,
    server_id: str,
    base_url: str,
    min_records: int,
    expected_extracted: int,
    expected_persisted: int,
    enrichment_char: str,
    enriched_guild: str,
    orphan_char: str,
) -> None:
    settings = _make_settings(tmp_path)
    scraper = get_scraper(server_id, settings=settings)
    server = _make_target_server(server_id, base_url, min_records_expected=min_records)
    session = FakeAsyncSession(server)

    async def execute() -> ScrapeRunSummary:
        with respx.mock(assert_all_called=True) as mock_router:
            if server_id == "chronicle":
                mock_router.get(f"{base_url}/ranking/top-player-1.html").mock(
                    return_value=httpx.Response(
                        200, content=_load_fixture_bytes("chronicle_player.html")
                    )
                )
                mock_router.get(f"{base_url}/ranking/top-player-2.html").mock(
                    return_value=httpx.Response(
                        200, content=_load_fixture_bytes("chronicle_empty_page.html")
                    )
                )
                mock_router.get(f"{base_url}/ranking/job/top-trader-1.html").mock(
                    return_value=httpx.Response(
                        200, content=_load_fixture_bytes("chronicle_trader_page1.html")
                    )
                )
                mock_router.get(f"{base_url}/ranking/job/top-trader-2.html").mock(
                    return_value=httpx.Response(
                        200, content=_load_fixture_bytes("chronicle_trader_page2.html")
                    )
                )
                mock_router.get(f"{base_url}/ranking/job/top-hunter-1.html").mock(
                    return_value=httpx.Response(
                        200, content=_load_fixture_bytes("chronicle_hunter_page1.html")
                    )
                )
                mock_router.get(f"{base_url}/ranking/job/top-hunter-2.html").mock(
                    return_value=httpx.Response(
                        200, content=_load_fixture_bytes("chronicle_empty_page.html")
                    )
                )
                mock_router.get(f"{base_url}/ranking/job/top-thieves-1.html").mock(
                    return_value=httpx.Response(
                        200, content=_load_fixture_bytes("chronicle_thieves_page1.html")
                    )
                )
                mock_router.get(f"{base_url}/ranking/top-guild-1.html").mock(
                    return_value=httpx.Response(
                        200, content=_load_fixture_bytes("chronicle_guild.html")
                    )
                )
                mock_router.get(f"{base_url}/ranking/top-guild-2.html").mock(
                    return_value=httpx.Response(
                        200, content=_load_fixture_bytes("chronicle_empty_page.html")
                    )
                )
            elif server_id == "electus":
                mock_router.get(f"{base_url}/api/ranking/player?page=1&limit=100").mock(
                    return_value=httpx.Response(
                        200, content=_load_fixture_bytes("electus_player.json")
                    )
                )
                mock_router.get(f"{base_url}/api/ranking/player?page=2&limit=100").mock(
                    return_value=httpx.Response(
                        200, content=_load_fixture_bytes("electus_empty.json")
                    )
                )
                mock_router.get(f"{base_url}/api/ranking/job?page=1&limit=100").mock(
                    return_value=httpx.Response(
                        200, content=_load_fixture_bytes("electus_job.json")
                    )
                )
                mock_router.get(f"{base_url}/api/ranking/job?page=2&limit=100").mock(
                    return_value=httpx.Response(
                        200, content=_load_fixture_bytes("electus_empty.json")
                    )
                )
                mock_router.get(f"{base_url}/api/ranking/guild?page=1&limit=50").mock(
                    return_value=httpx.Response(
                        200, content=_load_fixture_bytes("electus_guild.json")
                    )
                )
                mock_router.get(f"{base_url}/api/ranking/guild?page=2&limit=50").mock(
                    return_value=httpx.Response(
                        200, content=_load_fixture_bytes("electus_empty.json")
                    )
                )
            else:
                mock_router.get(f"{base_url}/ranking/characters?page=1").mock(
                    return_value=httpx.Response(
                        200, content=_load_fixture_bytes("golden_player.html")
                    )
                )
                mock_router.get(f"{base_url}/ranking/characters?page=2").mock(
                    return_value=httpx.Response(
                        200, content=_load_fixture_bytes("golden_empty_page.html")
                    )
                )
                mock_router.get(f"{base_url}/ranking/jobs?page=1").mock(
                    return_value=httpx.Response(200, content=_load_fixture_bytes("golden_job.html"))
                )
                mock_router.get(f"{base_url}/ranking/jobs?page=2").mock(
                    return_value=httpx.Response(
                        200, content=_load_fixture_bytes("golden_empty_page.html")
                    )
                )
                mock_router.get(f"{base_url}/ranking/guilds?page=1").mock(
                    return_value=httpx.Response(
                        200, content=_load_fixture_bytes("golden_guild.html")
                    )
                )
                mock_router.get(f"{base_url}/ranking/guilds?page=2").mock(
                    return_value=httpx.Response(
                        200, content=_load_fixture_bytes("golden_empty_page.html")
                    )
                )

            return await scraper.run(cast(AsyncSession, session), trigger_type="scheduled_12h")

    summary = _run_async(execute())

    assert summary.status == "SUCCESS"
    assert summary.records_extracted == expected_extracted
    assert summary.records_persisted == expected_persisted
    assert summary.validation_errors_count == 0
    assert summary.quarantine_file_path is None
    assert session.commit_count == 1
    assert orphan_char not in session.char_ids

    target_char_id = session.char_ids[enrichment_char]
    expected_guild_id = session.guild_ids[enriched_guild]
    matching_snaps = [
        row for row in session.char_snapshot_chunks[0] if row["character_id"] == target_char_id
    ]
    assert len(matching_snaps) == 1
    snap = matching_snaps[0]
    assert snap["guild_id"] == expected_guild_id
    if server_id == "chronicle":
        assert snap["job_type"] == "TRADER"
        assert snap["job_level"] == 6
        assert snap["job_exp"] == 870000
    else:
        assert snap["job_type"] is not None
        assert snap["job_level"] is not None
    assert snap["extra_metrics"]["job_rank"] >= 1
    assert isinstance(snap["item_points"], Decimal)


def test_chronicle_partial_success_le5_percent(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    scraper = ChronicleScraper(settings, retry_delays=(0.0, 0.0))
    base_url = "https://play-chronicle.com"
    server = _make_target_server("chronicle", base_url, min_records_expected=100)
    session = FakeAsyncSession(server)

    async def execute() -> ScrapeRunSummary:
        with respx.mock(assert_all_called=True) as mock_router:
            mock_router.get(f"{base_url}/ranking/top-player-1.html").mock(
                return_value=httpx.Response(
                    200, content=_load_fixture_bytes("chronicle_player_partial_le5.html")
                )
            )
            mock_router.get(f"{base_url}/ranking/top-player-2.html").mock(
                return_value=httpx.Response(
                    200, content=_load_fixture_bytes("chronicle_empty_page.html")
                )
            )
            mock_router.get(f"{base_url}/ranking/job/top-trader-1.html").mock(
                return_value=httpx.Response(
                    200, content=_load_fixture_bytes("chronicle_trader_page1.html")
                )
            )
            mock_router.get(f"{base_url}/ranking/job/top-trader-2.html").mock(
                return_value=httpx.Response(
                    200, content=_load_fixture_bytes("chronicle_trader_page2.html")
                )
            )
            mock_router.get(f"{base_url}/ranking/job/top-hunter-1.html").mock(
                return_value=httpx.Response(
                    200, content=_load_fixture_bytes("chronicle_hunter_page1.html")
                )
            )
            mock_router.get(f"{base_url}/ranking/job/top-hunter-2.html").mock(
                return_value=httpx.Response(
                    200, content=_load_fixture_bytes("chronicle_empty_page.html")
                )
            )
            mock_router.get(f"{base_url}/ranking/job/top-thieves-1.html").mock(
                return_value=httpx.Response(
                    200, content=_load_fixture_bytes("chronicle_thieves_page1.html")
                )
            )
            mock_router.get(f"{base_url}/ranking/top-guild-1.html").mock(
                return_value=httpx.Response(
                    200, content=_load_fixture_bytes("chronicle_guild.html")
                )
            )
            mock_router.get(f"{base_url}/ranking/top-guild-2.html").mock(
                return_value=httpx.Response(
                    200, content=_load_fixture_bytes("chronicle_empty_page.html")
                )
            )
            return await scraper.run(cast(AsyncSession, session), trigger_type="manual")

    summary = _run_async(execute())

    assert summary.status == "PARTIAL_SUCCESS"
    assert summary.validation_errors_count == 5
    assert summary.records_extracted == 121
    assert summary.records_persisted == 106
    assert summary.quarantine_file_path is None
    assert summary.error_details is not None
    assert len(summary.error_details.sample_errors) == 5


@pytest.mark.parametrize(
    ("player_fixture", "guild_fixture", "expected_status", "expected_category", "expected_snippet"),
    [
        (
            "chronicle_player_malformed_podium.html",
            "chronicle_guild.html",
            "SCHEMA_DRIFT",
            "player",
            b"Chron_Hero_002",
        ),
        (
            "chronicle_player_drift_gt5.html",
            "chronicle_guild.html",
            "SCHEMA_DRIFT",
            "player",
            b"Chron_Hero_010",
        ),
        (
            "chronicle_schema_drift_layout.html",
            "chronicle_empty_page.html",
            "SCHEMA_DRIFT",
            "player",
            b"v2-ranking-grid",
        ),
        (
            "chronicle_player_low_count.html",
            "chronicle_guild.html",
            "LOW_COUNT_QUARANTINE",
            "player",
            b"Chron_Hero_020",
        ),
        (
            "chronicle_player.html",
            "chronicle_guild_low_count.html",
            "LOW_COUNT_QUARANTINE",
            "guild",
            b"DawnGuard",
        ),
    ],
)
def test_chronicle_integrity_guards_and_quarantine(
    tmp_path: Path,
    player_fixture: str,
    guild_fixture: str,
    expected_status: str,
    expected_category: str,
    expected_snippet: bytes,
) -> None:
    settings = _make_settings(tmp_path)
    scraper = ChronicleScraper(settings, retry_delays=(0.0, 0.0))
    base_url = "https://play-chronicle.com"
    server = _make_target_server("chronicle", base_url, min_records_expected=100)
    session = FakeAsyncSession(server)

    player_bytes = _load_fixture_bytes(player_fixture)
    guild_bytes = _load_fixture_bytes(guild_fixture)
    empty_bytes = _load_fixture_bytes("chronicle_empty_page.html")

    async def execute() -> ScrapeRunSummary:
        with respx.mock(assert_all_called=True) as mock_router:
            mock_router.get(f"{base_url}/ranking/top-player-1.html").mock(
                return_value=httpx.Response(200, content=player_bytes)
            )
            if player_fixture != "chronicle_schema_drift_layout.html":
                mock_router.get(f"{base_url}/ranking/top-player-2.html").mock(
                    return_value=httpx.Response(200, content=empty_bytes)
                )
            mock_router.get(f"{base_url}/ranking/job/top-trader-1.html").mock(
                return_value=httpx.Response(200, content=empty_bytes)
            )
            mock_router.get(f"{base_url}/ranking/job/top-hunter-1.html").mock(
                return_value=httpx.Response(200, content=empty_bytes)
            )
            mock_router.get(f"{base_url}/ranking/job/top-thieves-1.html").mock(
                return_value=httpx.Response(200, content=empty_bytes)
            )
            mock_router.get(f"{base_url}/ranking/top-guild-1.html").mock(
                return_value=httpx.Response(200, content=guild_bytes)
            )
            if guild_fixture != "chronicle_empty_page.html":
                mock_router.get(f"{base_url}/ranking/top-guild-2.html").mock(
                    return_value=httpx.Response(200, content=empty_bytes)
                )
            return await scraper.run(cast(AsyncSession, session), trigger_type="manual")

    summary = _run_async(execute())

    assert summary.status == expected_status
    assert summary.records_persisted == 0
    assert summary.error_details is not None
    assert summary.error_details.failed_category == expected_category
    assert session.char_snapshot_chunks == []
    assert session.guild_snapshot_chunks == []
    _verify_quarantine_artifact(
        summary=summary,
        run_row=session.added_runs[0],
        expected_dir=settings.quarantine_dir,
        expected_substrings=[expected_snippet],
    )


def test_chronicle_and_golden_data_attributes_and_edge_cases(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    chronicle = ChronicleScraper(settings, retry_delays=(0.0, 0.0))
    golden = PlayGoldenScraper(settings, retry_delays=(0.0, 0.0))

    chronicle_attr_html = b"""
    <html><body>
      <table class="table table-striped table-hover"><tbody>
        <tr>
          <td>#1</td>
          <td><img src="/img/ch.png"><a href="/c/1">AttrHero</a></td>
          <td>AttrGuild</td><td>110</td><td>2500.0</td>
        </tr>
      </tbody></table>
    </body></html>
    """
    golden_attr_html = b"""
    <html><body>
      <table class="table-ranking">
        <tr data-gold="9,500,000" data-last-logout="2026-11-19 10:00:00">
          <td>#1</td><td>CH</td><td><a href="/c/1">GoldAttr</a></td><td>GoldGuild</td>
          <td>110</td><td>400</td><td>200</td><td>1800.0</td><td>900</td>
        </tr>
      </table>
    </body></html>
    """
    golden_guild_slash_html = b"""
    <html><body>
      <table class="table-ranking"><tbody>
        <tr><td>1</td><td><a href="/g/1">GoldGuild</a></td><td>GoldAttr</td>
        <td>42/50</td><td>80,000.00</td><td>Jangan</td></tr>
      </tbody></table>
    </body></html>
    """

    async def execute() -> tuple[CategoryFetchResult, CategoryFetchResult, CategoryFetchResult]:
        with respx.mock(assert_all_called=True) as mock_router:
            mock_router.get("https://play-chronicle.com/ranking/top-player-1.html").mock(
                return_value=httpx.Response(200, content=chronicle_attr_html)
            )
            mock_router.get("https://play-chronicle.com/ranking/top-player-2.html").mock(
                return_value=httpx.Response(200, content=b"")
            )
            mock_router.get("https://play-golden.online/ranking/characters?page=1").mock(
                return_value=httpx.Response(200, content=golden_attr_html)
            )
            mock_router.get("https://play-golden.online/ranking/characters?page=2").mock(
                return_value=httpx.Response(200, content=b"")
            )
            mock_router.get("https://play-golden.online/ranking/guilds?page=1").mock(
                return_value=httpx.Response(200, content=golden_guild_slash_html)
            )
            mock_router.get("https://play-golden.online/ranking/guilds?page=2").mock(
                return_value=httpx.Response(200, content=b"")
            )
            async with httpx.AsyncClient(http2=True) as client:
                c_res = await chronicle.fetch_player_pages(client, "https://play-chronicle.com")
                g_p_res = await golden.fetch_player_pages(client, "https://play-golden.online")
                g_g_res = await golden.fetch_guild_pages(client, "https://play-golden.online")
                return c_res, g_p_res, g_g_res

    c_res, g_p_res, g_g_res = _run_async(execute())

    c_item = RawPlayerRankingItem.model_validate(c_res.raw_records[0])
    assert c_item.race == "CH"
    assert c_item.item_points == 2500.0

    g_p_item = RawPlayerRankingItem.model_validate(g_p_res.raw_records[0])
    assert g_p_item.race == "CH"
    assert g_p_item.extra_metrics == {
        "gold": 9500000,
        "last_logout": "2026-11-19 10:00:00",
    }

    g_g_item = RawGuildRankingItem.model_validate(g_g_res.raw_records[0])
    assert g_g_item.members_count == 42


def test_electus_bare_list_envelope_and_page2_404_propagates(tmp_path: Path) -> None:
    settings = _make_electus_settings(tmp_path)
    scraper = ElectusScraper(settings, retry_delays=(0.0, 0.0))
    base_url = "https://electus.online"
    bare_list_json = (
        b'[{"rank": 1, "charname": "ListHero", "level": 120, '
        b'"race_id": "2", "extra_metrics": {"custom": 7}, "elo_points": 1500}, "bad_row"]'
    )

    async def execute() -> CategoryFetchResult:
        with respx.mock(assert_all_called=True) as mock_router:
            mock_router.get(f"{base_url}/api/ranking/player?page=1&limit=100").mock(
                return_value=httpx.Response(200, content=bare_list_json)
            )
            mock_router.get(f"{base_url}/api/ranking/player?page=2&limit=100").mock(
                return_value=httpx.Response(404, text="Missing page 2")
            )
            async with httpx.AsyncClient(http2=True) as client:
                with pytest.raises(TransportFetchError, match="HTTP 404"):
                    await scraper.fetch_player_pages(client, base_url)
            mock_router.reset()
            mock_router.get(f"{base_url}/api/ranking/player?page=1&limit=100").mock(
                return_value=httpx.Response(200, content=bare_list_json)
            )
            mock_router.get(f"{base_url}/api/ranking/player?page=2&limit=100").mock(
                return_value=httpx.Response(200, content=b"[]")
            )
            async with httpx.AsyncClient(http2=True) as client:
                return await scraper.fetch_player_pages(client, base_url)

    res = _run_async(execute())

    assert len(res.raw_records) == 2
    valid = RawPlayerRankingItem.model_validate(res.raw_records[0])
    assert valid.race == "EU"
    assert valid.extra_metrics == {"custom": 7, "elo_points": 1500}
    assert res.raw_records[1] == {"_invalid_row": "bad_row"}


def test_mypy_annotations_sanity_check() -> None:
    # Ensure fixture loaders return expected types cleanly
    text = _load_fixture_text("electus_empty.json")
    assert '"data"' in text
    items: list[dict[str, Any]] = [{"rank": 1}]
    assert len(items) == 1


# ---------------------------------------------------------------------------
# 7. Adversarial Regression Tests (UTF-8, Unicode isdigit, NaN/Inf, Notices)
# ---------------------------------------------------------------------------


def _build_chronicle_player_row_bytes(
    rank: int,
    char_name_bytes: bytes,
    *,
    unique_points: str = "100",
) -> bytes:
    return (
        b"<tr>"
        + f"<td>#{rank}</td>".encode()
        + b'<td><img data-race="CH"><a class="char-link">'
        + char_name_bytes
        + b"</a></td>"
        + b"<td>ApexLegion</td><td>110</td><td>1500.0</td><td>500</td><td>General</td>"
        + b"<td>10/2</td>"
        + f"<td>{unique_points}</td>".encode()
        + b"</tr>"
    )


def _build_golden_player_row_bytes(
    rank: int,
    char_name_bytes: bytes,
    *,
    gold: str = "1000",
) -> bytes:
    return (
        b"<tr>"
        + f"<td>#{rank}</td>".encode()
        + b'<td><img alt="Chinese" src="/img/ch.png"></td>'
        + b"<td><a>"
        + char_name_bytes
        + b"</a></td>"
        + b"<td>GoldenCrown</td><td>110</td><td>400</td><td>200</td><td>1500.0</td><td>500</td>"
        + f"<td>{gold}</td>".encode()
        + b"<td>2026-11-19 08:00:00</td>"
        + b"</tr>"
    )


def _build_electus_html_player_row_bytes(rank: int, char_name_bytes: bytes) -> bytes:
    return (
        b"<tr>"
        + f"<td>#{rank}</td>".encode()
        + b'<td><img data-race="CH"><a class="char-link">'
        + char_name_bytes
        + b"</a></td>"
        + b"<td>ElecEmpire</td><td>110</td><td>1500.0</td><td>500</td><td>General</td>"
        + b"</tr>"
    )


@pytest.mark.parametrize("server_id", ["chronicle", "play_golden", "electus"])
def test_invalid_utf8_bytes_do_not_crash_run(tmp_path: Path, server_id: str) -> None:
    settings = _make_settings(tmp_path)
    scraper = get_scraper(server_id, settings=settings)

    if server_id == "chronicle":
        base_url = "https://play-chronicle.com"
        rows = [_build_chronicle_player_row_bytes(1, b"Bad\xff\xfeName")] + [
            _build_chronicle_player_row_bytes(i, f"ValidChar_{i:02d}".encode())
            for i in range(2, 11)
        ]
        player_html = (
            b'<html><body><table class="table table-striped table-hover"><tbody>'
            + b"".join(rows)
            + b"</tbody></table></body></html>"
        )
        guild_bytes = _load_fixture_bytes("chronicle_guild.html")
        empty_html = _load_fixture_bytes("chronicle_empty_page.html")
    elif server_id == "play_golden":
        base_url = "https://play-golden.online"
        rows = [_build_golden_player_row_bytes(1, b"Bad\xff\xfeName")] + [
            _build_golden_player_row_bytes(i, f"ValidGold_{i:02d}".encode()) for i in range(2, 11)
        ]
        player_html = (
            b'<html><body><table class="table-ranking"><tbody>'
            + b"".join(rows)
            + b"</tbody></table></body></html>"
        )
        guild_bytes = _load_fixture_bytes("golden_guild.html")
        empty_html = _load_fixture_bytes("golden_empty_page.html")
    else:
        base_url = "https://electus.online"
        rows = [_build_electus_html_player_row_bytes(1, b"Bad\xff\xfeName")] + [
            _build_electus_html_player_row_bytes(i, f"ValidElec_{i:02d}".encode())
            for i in range(2, 11)
        ]
        player_html = (
            b'<html><body><table id="ranking-table"><tbody>'
            + b"".join(rows)
            + b"</tbody></table></body></html>"
        )
        guild_bytes = _load_fixture_bytes("electus_guild_fallback.html")
        empty_html = b"<html><body><table id='ranking-table'><tbody></tbody></table></body></html>"

    server = _make_target_server(server_id, base_url, min_records_expected=10)
    session = FakeAsyncSession(server)

    async def execute() -> ScrapeRunSummary:
        with respx.mock(assert_all_called=True) as mock_router:
            if server_id == "chronicle":
                mock_router.get(f"{base_url}/ranking/top-player-1.html").mock(
                    return_value=httpx.Response(
                        200,
                        headers={"Content-Type": "text/html; charset=utf-8"},
                        content=player_html,
                    )
                )
                mock_router.get(f"{base_url}/ranking/top-player-2.html").mock(
                    return_value=httpx.Response(200, content=empty_html)
                )
                mock_router.get(f"{base_url}/ranking/job/top-trader-1.html").mock(
                    return_value=httpx.Response(200, content=empty_html)
                )
                mock_router.get(f"{base_url}/ranking/job/top-hunter-1.html").mock(
                    return_value=httpx.Response(200, content=empty_html)
                )
                mock_router.get(f"{base_url}/ranking/job/top-thieves-1.html").mock(
                    return_value=httpx.Response(200, content=empty_html)
                )
                mock_router.get(f"{base_url}/ranking/top-guild-1.html").mock(
                    return_value=httpx.Response(200, content=guild_bytes)
                )
                mock_router.get(f"{base_url}/ranking/top-guild-2.html").mock(
                    return_value=httpx.Response(200, content=empty_html)
                )
            elif server_id == "play_golden":
                mock_router.get(f"{base_url}/ranking/characters?page=1").mock(
                    return_value=httpx.Response(
                        200,
                        headers={"Content-Type": "text/html; charset=utf-8"},
                        content=player_html,
                    )
                )
                mock_router.get(f"{base_url}/ranking/characters?page=2").mock(
                    return_value=httpx.Response(200, content=empty_html)
                )
                mock_router.get(f"{base_url}/ranking/jobs?page=1").mock(
                    return_value=httpx.Response(200, content=empty_html)
                )
                mock_router.get(f"{base_url}/ranking/guilds?page=1").mock(
                    return_value=httpx.Response(200, content=guild_bytes)
                )
                mock_router.get(f"{base_url}/ranking/guilds?page=2").mock(
                    return_value=httpx.Response(200, content=empty_html)
                )
            else:
                for cat in ("player", "job", "guild"):
                    limit = 50 if cat == "guild" else 100
                    mock_router.get(f"{base_url}/api/ranking/{cat}?page=1&limit={limit}").mock(
                        return_value=httpx.Response(404, text="Not Found")
                    )
                mock_router.get(f"{base_url}/ranking/player?page=1").mock(
                    return_value=httpx.Response(
                        200,
                        headers={"Content-Type": "text/html; charset=utf-8"},
                        content=player_html,
                    )
                )
                mock_router.get(f"{base_url}/ranking/player?page=2").mock(
                    return_value=httpx.Response(200, content=empty_html)
                )
                mock_router.get(f"{base_url}/ranking/job?page=1").mock(
                    return_value=httpx.Response(200, content=empty_html)
                )
                mock_router.get(f"{base_url}/ranking/guild?page=1").mock(
                    return_value=httpx.Response(200, content=guild_bytes)
                )
                mock_router.get(f"{base_url}/ranking/guild?page=2").mock(
                    return_value=httpx.Response(200, content=empty_html)
                )

            return await scraper.run(cast(AsyncSession, session), trigger_type="manual")

    summary = _run_async(execute())

    assert summary.status == "SUCCESS"
    assert summary.records_persisted >= 15
    assert len(session.added_runs) == 1
    assert any(name.startswith("Bad") and name.endswith("Name") for name in session.char_ids)


def test_element_text_handles_direct_corrupt_lxml_element() -> None:
    from lxml import html as lxml_html
    from src.scrapers._html_utils import element_text as chronicle_element_text
    from src.scrapers._html_utils import xpath_elements as _xpath_elements
    from src.scrapers.electus import _element_text as electus_element_text
    from src.scrapers.golden import _element_text as golden_element_text

    raw_doc = b"<html><body><table><tr><td>Bad\xff\xfeName</td></tr></table></body></html>"
    tree = lxml_html.fromstring(raw_doc, parser=lxml_html.HTMLParser(encoding="utf-8"))
    td = _xpath_elements(tree, "//td")[0]

    assert chronicle_element_text(td) == "Bad\ufffd\ufffdName"
    assert golden_element_text(td) == "Bad\ufffd\ufffdName"
    assert electus_element_text(td) == "Bad\ufffd\ufffdName"


def test_unicode_isdigit_superscript_does_not_crash_chronicle_or_golden(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    chronicle = ChronicleScraper(settings, retry_delays=(0.0, 0.0))
    golden = PlayGoldenScraper(settings, retry_delays=(0.0, 0.0))

    chronicle_guild_html = (
        b'<html><body><table class="table table-striped table-hover"><tbody>'
        b"<tr><td>1</td><td><a>GuildOne</a></td><td>\xc2\xb2</td><td>25</td><td>15000</td></tr>"
        b"</tbody></table></body></html>"
    )
    golden_html = (
        b'<html><body><table class="table-ranking"><tbody>'
        + _build_golden_player_row_bytes(1, b"GoldOne", gold="\u00b2")
        + b"</tbody></table></body></html>"
    )

    async def execute() -> tuple[CategoryFetchResult, CategoryFetchResult]:
        with respx.mock(assert_all_called=True) as mock_router:
            mock_router.get("https://play-chronicle.com/ranking/top-guild-1.html").mock(
                return_value=httpx.Response(200, content=chronicle_guild_html)
            )
            mock_router.get("https://play-chronicle.com/ranking/top-guild-2.html").mock(
                return_value=httpx.Response(200, content=b"")
            )
            mock_router.get("https://play-golden.online/ranking/characters?page=1").mock(
                return_value=httpx.Response(200, content=golden_html)
            )
            mock_router.get("https://play-golden.online/ranking/characters?page=2").mock(
                return_value=httpx.Response(200, content=b"")
            )
            async with httpx.AsyncClient(http2=True) as client:
                c_res = await chronicle.fetch_guild_pages(client, "https://play-chronicle.com")
                g_res = await golden.fetch_player_pages(client, "https://play-golden.online")
                return c_res, g_res

    c_res, g_res = _run_async(execute())

    c_item = RawGuildRankingItem.model_validate(c_res.raw_records[0])
    g_item = RawPlayerRankingItem.model_validate(g_res.raw_records[0])
    assert c_item.extra_metrics["guild_level"] == "\u00b2"
    assert g_item.extra_metrics["gold"] == "\u00b2"


@pytest.mark.parametrize("raw_guild_level", ["NaN", "inf", "-Infinity"])
def test_chronicle_non_finite_floats_remain_json_compliant(
    tmp_path: Path,
    raw_guild_level: str,
) -> None:
    settings = _make_settings(tmp_path)
    scraper = ChronicleScraper(settings, retry_delays=(0.0, 0.0))
    base_url = "https://play-chronicle.com"
    server = _make_target_server("chronicle", base_url, min_records_expected=5)
    session = FakeAsyncSession(server)

    player_rows = [
        _build_chronicle_player_row_bytes(i, f"Hero_{i:02d}".encode()) for i in range(1, 6)
    ]
    player_html = (
        b'<html><body><table class="table table-striped table-hover"><tbody>'
        + b"".join(player_rows)
        + b"</tbody></table></body></html>"
    )
    guild_rows = [
        (
            f"<tr><td>{i}</td><td><a>Guild_{i}</a></td>"
            f"<td>{raw_guild_level if i == 1 else '5'}</td>"
            f"<td>20</td><td>10000</td></tr>"
        ).encode()
        for i in range(1, 6)
    ]
    guild_html = (
        b'<html><body><table class="table table-striped table-hover"><tbody>'
        + b"".join(guild_rows)
        + b"</tbody></table></body></html>"
    )
    empty_html = _load_fixture_bytes("chronicle_empty_page.html")

    async def execute() -> ScrapeRunSummary:
        with respx.mock(assert_all_called=True) as mock_router:
            mock_router.get(f"{base_url}/ranking/top-player-1.html").mock(
                return_value=httpx.Response(200, content=player_html)
            )
            mock_router.get(f"{base_url}/ranking/top-player-2.html").mock(
                return_value=httpx.Response(200, content=empty_html)
            )
            mock_router.get(f"{base_url}/ranking/job/top-trader-1.html").mock(
                return_value=httpx.Response(200, content=empty_html)
            )
            mock_router.get(f"{base_url}/ranking/job/top-hunter-1.html").mock(
                return_value=httpx.Response(200, content=empty_html)
            )
            mock_router.get(f"{base_url}/ranking/job/top-thieves-1.html").mock(
                return_value=httpx.Response(200, content=empty_html)
            )
            mock_router.get(f"{base_url}/ranking/top-guild-1.html").mock(
                return_value=httpx.Response(200, content=guild_html)
            )
            mock_router.get(f"{base_url}/ranking/top-guild-2.html").mock(
                return_value=httpx.Response(200, content=empty_html)
            )
            return await scraper.run(cast(AsyncSession, session), trigger_type="manual")

    summary = _run_async(execute())

    assert summary.status == "SUCCESS"
    first_extras = session.guild_snapshot_chunks[0][0]["extra_metrics"]
    assert first_extras["guild_level"] == raw_guild_level
    assert isinstance(first_extras["guild_level"], str)
    serialized = json.dumps(first_extras, allow_nan=False)
    assert raw_guild_level in serialized


@pytest.mark.parametrize(
    "notice_text",
    [
        "Showing 0 records",
        "Page 2: No records found",
        "0 results",
        "empty",
        "nothing found",
    ],
)
@pytest.mark.parametrize("server_id", ["chronicle", "play_golden", "electus"])
def test_empty_notice_with_digits_stops_pagination_at_page_2(
    tmp_path: Path,
    server_id: str,
    notice_text: str,
) -> None:
    settings = _make_settings(tmp_path)
    scraper = get_scraper(server_id, settings=settings)

    if server_id == "chronicle":
        base_url = "https://play-chronicle.com"
        page1_html = (
            b'<html><body><table class="table table-striped table-hover"><tbody>'
            + b"".join(
                _build_chronicle_player_row_bytes(i, f"Char_{i}".encode()) for i in range(1, 4)
            )
            + b"</tbody></table></body></html>"
        )
        page2_html = (
            b'<html><body><table class="table table-striped table-hover"><tbody>'
            + f'<tr><td colspan="5">{notice_text}</td></tr>'.encode()
            + b"</tbody></table></body></html>"
        )
        expected_html_urls = [
            f"{base_url}/ranking/top-player-1.html",
            f"{base_url}/ranking/top-player-2.html",
        ]
    elif server_id == "play_golden":
        base_url = "https://play-golden.online"
        page1_html = (
            b'<html><body><table class="table-ranking"><tbody>'
            + b"".join(_build_golden_player_row_bytes(i, f"Char_{i}".encode()) for i in range(1, 4))
            + b"</tbody></table></body></html>"
        )
        page2_html = (
            b'<html><body><table class="table-ranking"><tbody>'
            + f'<tr><td colspan="9">{notice_text}</td></tr>'.encode()
            + b"</tbody></table></body></html>"
        )
        expected_html_urls = [
            f"{base_url}/ranking/characters?page=1",
            f"{base_url}/ranking/characters?page=2",
        ]
    else:
        base_url = "https://electus.online"
        page1_html = (
            b'<html><body><table id="ranking-table"><tbody>'
            + b"".join(
                _build_electus_html_player_row_bytes(i, f"Char_{i}".encode()) for i in range(1, 4)
            )
            + b"</tbody></table></body></html>"
        )
        page2_html = (
            b'<html><body><table id="ranking-table"><tbody>'
            + f'<tr><td colspan="7">{notice_text}</td></tr>'.encode()
            + b"</tbody></table></body></html>"
        )
        expected_html_urls = [
            f"{base_url}/ranking/player?page=1",
            f"{base_url}/ranking/player?page=2",
        ]

    requested_urls: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        requested_urls.append(url)
        if "/api/ranking/player" in url:
            return httpx.Response(404, text="Not Found")
        if url == expected_html_urls[0]:
            return httpx.Response(200, content=page1_html)
        if url == expected_html_urls[1]:
            return httpx.Response(200, content=page2_html)
        return httpx.Response(200, content=b"")

    async def execute() -> CategoryFetchResult:
        client = httpx.AsyncClient(http2=True, transport=httpx.MockTransport(handler))
        async with client:
            return await scraper.fetch_player_pages(client, base_url)

    res = _run_async(execute())

    assert len(res.raw_records) == 3
    html_requests = [u for u in requested_urls if "/api/" not in u]
    assert html_requests == expected_html_urls
    if server_id == "electus":
        assert requested_urls == [
            f"{base_url}/api/ranking/player?page=1&limit=100",
            f"{base_url}/ranking/player?page=1",
            f"{base_url}/ranking/player?page=2",
        ]


def test_single_cell_numeric_malformed_row_preserved_for_schema_drift(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    scraper = ChronicleScraper(settings, retry_delays=(0.0, 0.0))
    base_url = "https://play-chronicle.com"
    server = _make_target_server("chronicle", base_url, min_records_expected=10)
    session = FakeAsyncSession(server)

    malformed_html = (
        b'<html><body><table class="table table-striped table-hover"><tbody>'
        b"<tr><td>1</td></tr>"
        b'<tr><td colspan="5">1</td></tr>'
        b"</tbody></table></body></html>"
    )
    empty_html = _load_fixture_bytes("chronicle_empty_page.html")

    async def execute() -> ScrapeRunSummary:
        with respx.mock(assert_all_called=True) as mock_router:
            mock_router.get(f"{base_url}/ranking/top-player-1.html").mock(
                return_value=httpx.Response(200, content=malformed_html)
            )
            mock_router.get(f"{base_url}/ranking/top-player-2.html").mock(
                return_value=httpx.Response(200, content=empty_html)
            )
            mock_router.get(f"{base_url}/ranking/job/top-trader-1.html").mock(
                return_value=httpx.Response(200, content=empty_html)
            )
            mock_router.get(f"{base_url}/ranking/job/top-hunter-1.html").mock(
                return_value=httpx.Response(200, content=empty_html)
            )
            mock_router.get(f"{base_url}/ranking/job/top-thieves-1.html").mock(
                return_value=httpx.Response(200, content=empty_html)
            )
            mock_router.get(f"{base_url}/ranking/top-guild-1.html").mock(
                return_value=httpx.Response(200, content=empty_html)
            )
            return await scraper.run(cast(AsyncSession, session), trigger_type="manual")

    summary = _run_async(execute())

    assert summary.status == "SCHEMA_DRIFT"
    assert summary.validation_errors_count == 2
    assert summary.records_persisted == 0


# ---------------------------------------------------------------------------
# 8. Backlog Regression Tests (FIX-001, FIX-002, FIX-003)
# ---------------------------------------------------------------------------


def test_fix001_electus_hyperfilter_http200_triggers_blocked_challenge_and_cb(
    tmp_path: Path,
) -> None:
    settings = _make_electus_settings(tmp_path)
    scraper = ElectusScraper(settings, retry_delays=(0.0, 0.0))
    base_url = "https://electus.online"
    server = _make_target_server("electus", base_url, min_records_expected=100)
    session = FakeAsyncSession(server)
    hyperfilter_html = _load_fixture_bytes("electus_hyperfilter_200.html")

    async def execute() -> ScrapeRunSummary:
        with respx.mock(assert_all_called=True) as mock_router:
            mock_router.get(f"{base_url}/api/ranking/player?page=1&limit=100").mock(
                return_value=httpx.Response(200, content=hyperfilter_html)
            )
            return await scraper.run(cast(AsyncSession, session), trigger_type="scheduled_12h")

    before = datetime.now(UTC)
    summary = _run_async(execute())

    assert summary.status == "BLOCKED_CHALLENGE"
    assert summary.http_status_code == 200
    assert summary.records_persisted == 0
    assert summary.circuit_breaker_until is not None
    assert summary.circuit_breaker_until >= before + timedelta(hours=5, minutes=59)
    assert len(session.server_updates) == 1
    _verify_quarantine_artifact(
        summary=summary,
        run_row=session.added_runs[0],
        expected_dir=settings.quarantine_dir,
        expected_substrings=[b"/hf-sys/hf-lib.php", b"__HFUID"],
    )


def test_fix001_normal_noscript_page_does_not_false_positive_challenge() -> None:
    response = httpx.Response(
        200,
        text=(
            "<html><body><noscript>Please enable JavaScript for analytics.</noscript>"
            "<table class='table table-striped table-hover'></table></body></html>"
        ),
    )
    assert BaseRankingScraper._is_challenge_response(response) is False


def test_fix002_play_golden_diamwall_http513_triggers_blocked_challenge_and_cb(
    tmp_path: Path,
) -> None:
    settings = _make_settings(tmp_path)
    scraper = PlayGoldenScraper(settings, retry_delays=(0.0, 0.0))
    base_url = "https://play-golden.online"
    server = _make_target_server("play_golden", base_url, min_records_expected=50)
    session = FakeAsyncSession(server)
    diamwall_html = _load_fixture_bytes("golden_diamwall_513.html")

    async def execute() -> ScrapeRunSummary:
        with respx.mock(assert_all_called=True) as mock_router:
            mock_router.get(f"{base_url}/ranking/characters?page=1").mock(
                return_value=httpx.Response(
                    513,
                    headers={
                        "server": "openresty",
                        "set-cookie": "__diamwall=0x3439887867; Path=/; Secure",
                    },
                    content=diamwall_html,
                )
            )
            return await scraper.run(cast(AsyncSession, session), trigger_type="scheduled_12h")

    before = datetime.now(UTC)
    summary = _run_async(execute())

    assert summary.status == "BLOCKED_CHALLENGE"
    assert summary.http_status_code == 513
    assert summary.records_persisted == 0
    assert summary.circuit_breaker_until is not None
    assert summary.circuit_breaker_until >= before + timedelta(hours=5, minutes=59)
    assert len(session.server_updates) == 1
    _verify_quarantine_artifact(
        summary=summary,
        run_row=session.added_runs[0],
        expected_dir=settings.quarantine_dir,
        expected_substrings=[b"Verifying your browser | DiamWall", b"/.well-known/diamwall/"],
    )


def test_fix002_diamwall_513_cookie_correlation_and_negative_guards() -> None:
    # HTTP 513 with __diamwall cookie and empty body -> challenge (defense in depth)
    resp_513_cookie = httpx.Response(
        513,
        headers=[("Set-Cookie", "other=1"), ("Set-Cookie", "__diamwall=0xabc; Path=/")],
        content=b"",
    )
    assert BaseRankingScraper._is_challenge_response(resp_513_cookie) is True

    # Generic HTTP 513 without DiamWall markers/cookie -> NOT a challenge
    resp_513_generic = httpx.Response(513, text="Upstream custom proxy error")
    assert BaseRankingScraper._is_challenge_response(resp_513_generic) is False

    # HTTP 200 renewing __diamwall cookie with normal HTML -> NOT a challenge
    resp_200_cookie = httpx.Response(
        200,
        headers={"Set-Cookie": "__diamwall=0xabc; Path=/"},
        text="<html><body><table class='table-ranking'></table></body></html>",
    )
    assert BaseRankingScraper._is_challenge_response(resp_200_cookie) is False


def test_fix003_chronicle_real_top_routes_layout_and_page2_404_stop(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    scraper = ChronicleScraper(settings, retry_delays=(0.0, 0.0))
    base_url = "https://play-chronicle.com"
    server = _make_target_server("chronicle", base_url, min_records_expected=5)
    session = FakeAsyncSession(server)

    player_html = _load_fixture_bytes("chronicle_real_player_page1.html")
    guild_html = _load_fixture_bytes("chronicle_real_guild_page1.html")
    empty_job_html = (
        b'<div class="table-responsive"><table class="table table-striped table-hover">'
        b"<thead><tr><th>#</th><th>Character</th><th>Guild</th>"
        b"<th>Job K/D Ratio</th><th>Job Points</th></tr></thead></table></div>"
    )

    async def execute() -> ScrapeRunSummary:
        with respx.mock(assert_all_called=True) as mock_router:
            mock_router.get(f"{base_url}/ranking/top-player-1.html").mock(
                return_value=httpx.Response(200, content=player_html)
            )
            mock_router.get(f"{base_url}/ranking/top-player-2.html").mock(
                return_value=httpx.Response(404, text="Not Found")
            )
            mock_router.get(f"{base_url}/ranking/job/top-trader-1.html").mock(
                return_value=httpx.Response(200, content=empty_job_html)
            )
            mock_router.get(f"{base_url}/ranking/job/top-hunter-1.html").mock(
                return_value=httpx.Response(200, content=empty_job_html)
            )
            mock_router.get(f"{base_url}/ranking/job/top-thieves-1.html").mock(
                return_value=httpx.Response(200, content=empty_job_html)
            )
            mock_router.get(f"{base_url}/ranking/top-guild-1.html").mock(
                return_value=httpx.Response(200, content=guild_html)
            )
            mock_router.get(f"{base_url}/ranking/top-guild-2.html").mock(
                return_value=httpx.Response(404, text="Not Found")
            )
            return await scraper.run(cast(AsyncSession, session), trigger_type="manual")

    summary = _run_async(execute())

    assert summary.status == "SUCCESS"
    assert summary.records_extracted == 10
    assert summary.records_persisted == 10
    assert summary.validation_errors_count == 0
    assert "Roc" in session.char_ids
    assert "AmericanPIE" in session.guild_ids
    assert session.guild_snapshot_chunks[0][0]["extra_metrics"] == {"guild_level": 5}


def test_fix003_chronicle_page1_404_still_fails_as_http_error(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    scraper = ChronicleScraper(settings, retry_delays=(0.0, 0.0))
    base_url = "https://play-chronicle.com"
    server = _make_target_server("chronicle", base_url, min_records_expected=5)
    session = FakeAsyncSession(server)

    async def execute() -> ScrapeRunSummary:
        with respx.mock(assert_all_called=True) as mock_router:
            mock_router.get(f"{base_url}/ranking/top-player-1.html").mock(
                return_value=httpx.Response(404, text="Not Found")
            )
            return await scraper.run(cast(AsyncSession, session), trigger_type="manual")

    summary = _run_async(execute())
    assert summary.status == "HTTP_ERROR"
    assert summary.http_status_code == 404


# ---------------------------------------------------------------------------
# 9. Backlog Feature Tests (FEAT-001 & FEAT-002)
# ---------------------------------------------------------------------------


def _build_chronicle_paginated_html(
    route_prefix: str,
    page: int,
    total_pages: int,
    *,
    rows_per_page: int = 2,
    sliding_window: bool = False,
) -> bytes:
    start_rank = (page - 1) * rows_per_page + 1
    rows_html = "".join(
        f"<tr><td>{rank}</td><td><a href='/ranking/character-{rank}.html'>Hero_{rank:03d}</a></td>"
        f"<td>Guild_{rank % 3}</td><td>110</td><td>{2000 - rank}</td></tr>"
        for rank in range(start_rank, start_rank + rows_per_page)
    )
    if sliding_window:
        window_end = min(total_pages, page + 2)
        links = [
            f"<li><a onclick=\"paginatorAjax('#result','{route_prefix}-{p}.html')\">{p}</a></li>"
            for p in range(max(1, page - 1), window_end + 1)
        ]
        if page < total_pages:
            links.append(
                f"<li><a onclick=\"paginatorAjax('#result','{route_prefix}-{page + 1}.html')\">"
                "&gt;&gt;</a></li>"
            )
    else:
        links = [
            f"<li><a onclick=\"paginatorAjax('#result','{route_prefix}-{p}.html')\">{p}</a></li>"
            for p in range(1, total_pages + 1)
        ]
        if page < total_pages:
            links.append(
                f"<li><a onclick=\"paginatorAjax('#result','{route_prefix}-{page + 1}.html')\">"
                "&gt;&gt;</a></li>"
            )
    return (
        f'<div class="table-responsive"><table class="table table-striped table-hover">'
        f"<tbody>{rows_html}</tbody></table>"
        f'<ul class="pagination">{"".join(links)}</ul></div>'
    ).encode()


def test_feat001_chronicle_full_pagination_exceeds_old_limit_and_sliding_window(
    tmp_path: Path,
) -> None:
    settings = _make_settings(tmp_path)
    scraper = ChronicleScraper(settings, retry_delays=(0.0, 0.0))
    base_url = "https://play-chronicle.com"
    total_pages = 12
    requested_urls: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        requested_urls.append(url)
        page_str = url.rsplit("-", maxsplit=1)[-1].removesuffix(".html")
        page = int(page_str)
        # Simulate real site clamping if page > total_pages
        clamped_page = min(page, total_pages)
        return httpx.Response(
            200,
            content=_build_chronicle_paginated_html(
                "/ranking/top-player",
                clamped_page,
                total_pages,
                rows_per_page=2,
                sliding_window=True,
            ),
        )

    async def execute() -> CategoryFetchResult:
        client = httpx.AsyncClient(http2=True, transport=httpx.MockTransport(handler))
        async with client:
            return await scraper.fetch_player_pages(client, base_url)

    res = _run_async(execute())

    assert len(res.raw_records) == total_pages * 2
    assert len(requested_urls) == total_pages
    assert res.raw_records[0]["rank"] == "1"
    assert res.raw_records[-1]["rank"] == str(total_pages * 2)


def test_feat001_chronicle_clamping_guard_stops_without_duplicates_when_no_paginator(
    tmp_path: Path,
) -> None:
    settings = _make_settings(tmp_path)
    scraper = ChronicleScraper(settings, retry_delays=(0.0, 0.0), max_pages_per_category=50)
    base_url = "https://play-chronicle.com"

    page1 = (
        b'<table class="table table-striped table-hover"><tbody>'
        b"<tr><td>1</td><td><a>GuildA</a></td><td>5</td><td>20</td><td>5000</td></tr>"
        b"</tbody></table>"
    )
    page2 = (
        b'<table class="table table-striped table-hover"><tbody>'
        b"<tr><td>2</td><td><a>GuildB</a></td><td>4</td><td>15</td><td>3000</td></tr>"
        b"</tbody></table>"
    )
    requested_pages: list[int] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        page = int(str(request.url).rsplit("-", maxsplit=1)[-1].removesuffix(".html"))
        requested_pages.append(page)
        # Site clamps page >= 2 to last page (page 2)
        return httpx.Response(200, content=page1 if page == 1 else page2)

    async def execute() -> CategoryFetchResult:
        client = httpx.AsyncClient(http2=True, transport=httpx.MockTransport(handler))
        async with client:
            return await scraper.fetch_guild_pages(client, base_url)

    res = _run_async(execute())

    assert requested_pages == [1, 2, 3]
    assert len(res.raw_records) == 2
    assert [r["guild_name"] for r in res.raw_records] == ["GuildA", "GuildB"]


def test_feat001_chronicle_reconciles_trader_hunter_thief_into_snapshots(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    scraper = ChronicleScraper(settings, retry_delays=(0.0, 0.0))
    base_url = "https://play-chronicle.com"
    server = _make_target_server("chronicle", base_url, min_records_expected=100)
    session = FakeAsyncSession(server)

    async def execute() -> ScrapeRunSummary:
        with respx.mock(assert_all_called=True) as mock_router:
            mock_router.get(f"{base_url}/ranking/top-player-1.html").mock(
                return_value=httpx.Response(
                    200, content=_load_fixture_bytes("chronicle_player.html")
                )
            )
            mock_router.get(f"{base_url}/ranking/top-player-2.html").mock(
                return_value=httpx.Response(
                    200, content=_load_fixture_bytes("chronicle_empty_page.html")
                )
            )
            mock_router.get(f"{base_url}/ranking/job/top-trader-1.html").mock(
                return_value=httpx.Response(
                    200, content=_load_fixture_bytes("chronicle_trader_page1.html")
                )
            )
            mock_router.get(f"{base_url}/ranking/job/top-trader-2.html").mock(
                return_value=httpx.Response(
                    200, content=_load_fixture_bytes("chronicle_trader_page2.html")
                )
            )
            mock_router.get(f"{base_url}/ranking/job/top-hunter-1.html").mock(
                return_value=httpx.Response(
                    200, content=_load_fixture_bytes("chronicle_hunter_page1.html")
                )
            )
            mock_router.get(f"{base_url}/ranking/job/top-hunter-2.html").mock(
                return_value=httpx.Response(
                    200, content=_load_fixture_bytes("chronicle_hunter_page1.html")
                )
            )
            mock_router.get(f"{base_url}/ranking/job/top-thieves-1.html").mock(
                return_value=httpx.Response(
                    200, content=_load_fixture_bytes("chronicle_thieves_page1.html")
                )
            )
            mock_router.get(f"{base_url}/ranking/top-guild-1.html").mock(
                return_value=httpx.Response(
                    200, content=_load_fixture_bytes("chronicle_guild.html")
                )
            )
            mock_router.get(f"{base_url}/ranking/top-guild-2.html").mock(
                return_value=httpx.Response(
                    200, content=_load_fixture_bytes("chronicle_empty_page.html")
                )
            )
            return await scraper.run(cast(AsyncSession, session), trigger_type="manual")

    summary = _run_async(execute())

    assert summary.status == "SUCCESS"
    snaps_by_char_id = {row["character_id"]: row for row in session.char_snapshot_chunks[0]}

    trader_snap = snaps_by_char_id[session.char_ids["Chron_Hero_001"]]
    assert trader_snap["job_type"] == "TRADER"
    assert trader_snap["job_level"] == 7
    assert trader_snap["job_exp"] == 1250000

    hunter_snap = snaps_by_char_id[session.char_ids["Chron_Hero_005"]]
    assert hunter_snap["job_type"] == "HUNTER"
    assert hunter_snap["job_level"] == 7
    assert hunter_snap["job_exp"] == 910000

    thief_snap = snaps_by_char_id[session.char_ids["Chron_Hero_007"]]
    assert thief_snap["job_type"] == "THIEF"
    assert thief_snap["job_level"] == 7
    assert thief_snap["job_exp"] == 845000


def test_feat002_rsilkroad_parses_main_table_ignores_sidebar_and_runs_e2e(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    scraper = RSilkroadScraper(settings, retry_delays=(0.0, 0.0))
    base_url = "https://www.rsilkroad.com"
    server = _make_target_server("rsilkroad", base_url, min_records_expected=10)
    session = FakeAsyncSession(server)

    async def execute() -> ScrapeRunSummary:
        with respx.mock(assert_all_called=True) as mock_router:
            mock_router.get(f"{base_url}/Rank/Player?page=1&pageSize=100").mock(
                return_value=httpx.Response(
                    200, content=_load_fixture_bytes("rsilkroad_player_page1.html")
                )
            )
            mock_router.get(f"{base_url}/Rank/Player?page=2&pageSize=100").mock(
                return_value=httpx.Response(
                    200, content=_load_fixture_bytes("rsilkroad_player_page2.html")
                )
            )
            mock_router.get(f"{base_url}/Rank/Guild?page=1&pageSize=100").mock(
                return_value=httpx.Response(
                    200, content=_load_fixture_bytes("rsilkroad_guild_page1.html")
                )
            )
            return await scraper.run(cast(AsyncSession, session), trigger_type="manual")

    summary = _run_async(execute())

    assert summary.status == "SUCCESS"
    assert summary.records_extracted == 16
    assert summary.records_persisted == 16
    assert summary.validation_errors_count == 0
    assert "SidebarGhost_01" not in session.char_ids
    assert "SidebarGuildGhost" not in session.guild_ids
    assert "RSro_Hero_01" in session.char_ids
    assert "RSro_Hero_10" in session.char_ids

    snaps_by_char_id = {row["character_id"]: row for row in session.char_snapshot_chunks[0]}
    assert snaps_by_char_id[session.char_ids["RSro_Hero_01"]]["race"] == "CH"
    assert snaps_by_char_id[session.char_ids["RSro_Hero_02"]]["race"] == "EU"
    assert snaps_by_char_id[session.char_ids["RSro_Hero_03"]]["guild_id"] is None

    guild_snaps = session.guild_snapshot_chunks[0]
    assert len(guild_snaps) == 6
    assert guild_snaps[0]["members_count"] == 48
    assert guild_snaps[0]["total_item_points"] == Decimal("84500.00")
    assert guild_snaps[0]["fortress_held"] is None
    assert guild_snaps[0]["extra_metrics"] == {"guild_level": 5}


def test_feat002_rsilkroad_multipage_concurrent_skip_to_last_and_empty_fallback(
    tmp_path: Path,
) -> None:
    settings = _make_settings(tmp_path)
    scraper = RSilkroadScraper(
        settings,
        retry_delays=(0.0, 0.0),
        inter_batch_delay_range=(0.0, 0.0),
    )
    base_url = "https://www.rsilkroad.com"
    total_pages = 5
    requested_urls: list[str] = []

    def _build_rsro_page(page: int) -> bytes:
        next_href = f"/Rank/Player?page={page + 1}&amp;pageSize=100"
        last_href = f"/Rank/Player?page={total_pages}&amp;pageSize=100"
        next_li = (
            f'<li class="PagedList-skipToNext"><a href="{next_href}" rel="next">&raquo;</a></li>'
            if page < total_pages
            else '<li class="PagedList-skipToNext disabled"><a rel="next">&raquo;</a></li>'
        )
        return (
            '<table class="table"><tbody>'
            f'<tr><td>{page}</td><td><img src="/img/european.png">'
            f'<a href="/c/{page}">Char_{page}</a></td>'
            "<td>Anatolia</td><td>110</td><td>1500</td><td>Detay</td></tr>"
            "</tbody></table>"
            '<ul class="pagination">'
            f"{next_li}"
            f'<li class="PagedList-skipToLast"><a href="{last_href}">&raquo;&raquo;</a></li>'
            "</ul>"
        ).encode()

    async def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        requested_urls.append(url)
        if "/Rank/Guild" in url:
            if "page=1" in url:
                return httpx.Response(
                    200,
                    content=(
                        b'<table class="table"><tbody>'
                        b"<tr><td>1</td><td><a>GuildOne</a></td><td>5</td>"
                        b"<td>20</td><td>10000</td><td>Detay</td></tr>"
                        b"</tbody></table>"
                    ),
                )
            # Empty 200 without paginator stops sequential probing
            return httpx.Response(200, content=b'<table class="table"><tbody></tbody></table>')
        page = int(request.url.params["page"])
        return httpx.Response(200, content=_build_rsro_page(page))

    async def execute() -> tuple[CategoryFetchResult, CategoryFetchResult, CategoryFetchResult]:
        client = httpx.AsyncClient(http2=True, transport=httpx.MockTransport(handler))
        async with client:
            p_res = await scraper.fetch_player_pages(client, base_url)
            j_res = await scraper.fetch_job_pages(client, base_url)
            g_res = await scraper.fetch_guild_pages(client, base_url)
            return p_res, j_res, g_res

    player_res, job_res, guild_res = _run_async(execute())

    assert len(player_res.raw_records) == total_pages
    assert [r["rank"] for r in player_res.raw_records] == [
        str(i) for i in range(1, total_pages + 1)
    ]
    assert job_res.raw_records == []
    assert job_res.raw_payloads == []
    assert len(guild_res.raw_records) == 1
    assert guild_res.raw_records[0]["master_name"] is None
    assert len([u for u in requested_urls if "/Rank/Player" in u]) == total_pages
    assert len([u for u in requested_urls if "/Rank/Guild" in u]) == 2


def test_max_pages_per_category_settings_default_and_override(tmp_path: Path) -> None:
    default_settings = _make_settings(tmp_path)
    assert default_settings.max_pages_per_category == 400

    chronicle_default = get_scraper("chronicle", settings=default_settings)
    rsilkroad_default = get_scraper("rsilkroad", settings=default_settings)
    assert isinstance(chronicle_default, ChronicleScraper)
    assert isinstance(rsilkroad_default, RSilkroadScraper)
    assert chronicle_default._max_pages_per_category == 400
    assert rsilkroad_default._max_pages_per_category == 400

    custom_settings = Settings(
        _env_file=None,
        database_url="postgresql+asyncpg://user:pass@localhost:5432/sro_test",
        quarantine_dir=tmp_path / "quarantine",
        max_pages_per_category=125,
    )
    chronicle_custom = get_scraper("chronicle", settings=custom_settings)
    rsilkroad_custom = get_scraper("rsilkroad", settings=custom_settings)
    assert isinstance(chronicle_custom, ChronicleScraper)
    assert isinstance(rsilkroad_custom, RSilkroadScraper)
    assert chronicle_custom._max_pages_per_category == 125
    assert rsilkroad_custom._max_pages_per_category == 125


def test_rsilkroad_inter_batch_delay_jitter_applied_between_batches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _make_settings(tmp_path)
    scraper = RSilkroadScraper(
        settings,
        retry_delays=(0.0, 0.0),
        inter_batch_delay_range=(0.25, 0.50),
    )
    base_url = "https://www.rsilkroad.com"
    total_pages = 5
    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    monkeypatch.setattr("src.scrapers.rsilkroad.asyncio.sleep", fake_sleep)

    async def handler(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params["page"])
        last_href = f"/Rank/Player?page={total_pages}&amp;pageSize=100"
        return httpx.Response(
            200,
            content=(
                '<table class="table"><tbody>'
                f'<tr><td>{page}</td><td><img src="/img/european.png"><a>Char_{page}</a></td>'
                "<td>Anatolia</td><td>110</td><td>1500</td><td>Detay</td></tr>"
                "</tbody></table>"
                f'<ul class="pagination"><li class="PagedList-skipToLast">'
                f'<a href="{last_href}">&raquo;&raquo;</a></li></ul>'
            ).encode(),
        )

    async def execute() -> CategoryFetchResult:
        client = httpx.AsyncClient(http2=True, transport=httpx.MockTransport(handler))
        async with client:
            return await scraper.fetch_player_pages(client, base_url)

    res = _run_async(execute())

    assert len(res.raw_records) == total_pages
    # Pages 2..4 in batch 1 -> 1 inter-batch sleep -> page 5 in batch 2
    assert len(sleep_calls) == 1
    assert 0.25 <= sleep_calls[0] <= 0.50


def test_feat003_rsilkroad_settings_defaults_overrides_and_init_validation(
    tmp_path: Path,
) -> None:
    default_settings = _make_settings(tmp_path)
    default_scraper = RSilkroadScraper(default_settings)
    assert default_scraper._page_size == 100
    assert default_scraper._inter_batch_delay_range == (0.35, 0.75)
    assert default_scraper._cf_cookies() is None

    custom_settings = Settings(
        _env_file=None,
        database_url="postgresql+asyncpg://user:pass@localhost:5432/sro_test",
        quarantine_dir=tmp_path / "quarantine",
        rsilkroad_page_size=25,
        rsilkroad_inter_batch_delay_min=0.5,
        rsilkroad_inter_batch_delay_max=1.25,
        rsilkroad_cf_clearance="rsro-secret-cookie",
    )
    from_settings = RSilkroadScraper(custom_settings)
    assert from_settings._page_size == 25
    assert from_settings._inter_batch_delay_range == (0.5, 1.25)
    assert from_settings._cf_cookies() == {"cf_clearance": "rsro-secret-cookie"}

    overridden = RSilkroadScraper(
        custom_settings,
        page_size=50,
        inter_batch_delay_range=(0.1, 0.2),
    )
    assert overridden._page_size == 50
    assert overridden._inter_batch_delay_range == (0.1, 0.2)

    for bad_page_size in (0, -1, 101):
        with pytest.raises(ValueError, match="page_size"):
            RSilkroadScraper(default_settings, page_size=bad_page_size)

    for bad_delay_range in (
        (-0.1, 0.5),
        (1.0, 0.5),
        (float("nan"), 1.0),
        (0.0, float("inf")),
        (float("-inf"), 0.5),
    ):
        with pytest.raises(ValueError, match="inter_batch_delay_range"):
            RSilkroadScraper(default_settings, inter_batch_delay_range=bad_delay_range)


def test_feat003_rsilkroad_propagates_configured_page_size_in_query_string(
    tmp_path: Path,
) -> None:
    settings = Settings(
        _env_file=None,
        database_url="postgresql+asyncpg://user:pass@localhost:5432/sro_test",
        quarantine_dir=tmp_path / "quarantine",
        rsilkroad_page_size=40,
        rsilkroad_inter_batch_delay_min=0.0,
        rsilkroad_inter_batch_delay_max=0.0,
    )
    scraper = RSilkroadScraper(settings, retry_delays=(0.0, 0.0))
    base_url = "https://www.rsilkroad.com"
    requested_urls: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requested_urls.append(str(request.url))
        return httpx.Response(200, content=b'<table class="table"><tbody></tbody></table>')

    async def execute() -> None:
        client = httpx.AsyncClient(http2=True, transport=httpx.MockTransport(handler))
        async with client:
            await scraper.fetch_player_pages(client, base_url)
            await scraper.fetch_guild_pages(client, base_url)

    _run_async(execute())

    assert requested_urls == [
        f"{base_url}/Rank/Player?page=1&pageSize=40",
        f"{base_url}/Rank/Guild?page=1&pageSize=40",
    ]


@pytest.mark.parametrize(
    ("cf_token", "expected_present"),
    [
        ("rsro-cf-abc-123", True),
        (None, False),
        (" \t", False),
    ],
)
def test_feat003_rsilkroad_cf_clearance_cookie_injection_and_no_log_leak(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    cf_token: str | None,
    expected_present: bool,
) -> None:
    from src.core.logging import clear_run_context, configure_logging

    configure_logging("DEBUG")
    clear_run_context()

    settings = Settings(
        _env_file=None,
        database_url="postgresql+asyncpg://user:pass@localhost:5432/sro_test",
        quarantine_dir=tmp_path / "quarantine",
        rsilkroad_cf_clearance=cf_token,
    )
    scraper = RSilkroadScraper(settings, retry_delays=(0.0, 0.0))
    captured_cookies: list[str | None] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        captured_cookies.append(request.headers.get("cookie"))
        return httpx.Response(200, content=b'<table class="table"><tbody></tbody></table>')

    async def execute() -> CategoryFetchResult:
        client = httpx.AsyncClient(http2=True, transport=httpx.MockTransport(handler))
        async with client:
            return await scraper.fetch_player_pages(client, "https://www.rsilkroad.com")

    _run_async(execute())

    assert len(captured_cookies) == 1
    cookie_header = captured_cookies[0]
    if expected_present:
        assert cookie_header is not None
        assert "cf_clearance=rsro-cf-abc-123" in cookie_header
    else:
        assert cookie_header is None or "cf_clearance=" not in cookie_header

    captured_logs = capsys.readouterr()
    assert "rsro-cf-abc-123" not in captured_logs.out
    assert "rsro-cf-abc-123" not in captured_logs.err
