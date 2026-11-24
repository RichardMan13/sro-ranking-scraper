"""Unit tests for `src.dashboard.config` and `src.dashboard.db`."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError
from sqlalchemy.orm import sessionmaker
from src.dashboard.config import DashboardSettings, get_dashboard_settings
from src.dashboard.db import (
    LOCK_TIMEOUT_MS,
    POOL_RECYCLE_SECONDS,
    POOL_SIZE,
    POOL_TIMEOUT_SECONDS,
    STATEMENT_TIMEOUT_MS,
    WORK_MEM_MB,
    build_postgres_options,
    create_dashboard_engine,
    create_dashboard_session_factory,
)


@pytest.mark.parametrize(
    ("raw_url", "expected"),
    [
        (
            "postgres://sro_readonly:secret@postgres:5432/sro_rankings",
            "postgresql+psycopg://sro_readonly:secret@postgres:5432/sro_rankings",
        ),
        (
            "postgresql://sro_readonly:secret@postgres:5432/sro_rankings",
            "postgresql+psycopg://sro_readonly:secret@postgres:5432/sro_rankings",
        ),
        (
            "postgresql+psycopg://sro_readonly:secret@postgres:5432/sro_rankings",
            "postgresql+psycopg://sro_readonly:secret@postgres:5432/sro_rankings",
        ),
        (
            "POSTGRESQL://sro_readonly:secret@db.internal:5432/sro_rankings",
            "postgresql+psycopg://sro_readonly:secret@db.internal:5432/sro_rankings",
        ),
    ],
)
def test_dashboard_settings_normalizes_sync_postgres_schemes(
    monkeypatch: pytest.MonkeyPatch,
    raw_url: str,
    expected: str,
) -> None:
    monkeypatch.setenv("DASHBOARD_DATABASE_URL", raw_url)
    settings = get_dashboard_settings()
    assert settings.dashboard_database_url.get_secret_value() == expected
    assert settings.dashboard_cache_ttl_seconds == 600
    assert settings.log_level == "INFO"


@pytest.mark.parametrize(
    "invalid_url",
    [
        "",
        "   ",
        "not-a-url",
        "postgresql+asyncpg://sro_readonly:secret@postgres:5432/sro_rankings",
        "postgresql+psycopg2://sro_readonly:secret@postgres:5432/sro_rankings",
        "sqlite:///tmp/test.db",
        "http://example.com/db",
        "postgresql+psycopg://sro_readonly:secret@/sro_rankings",
        "postgresql+psycopg://sro_readonly:secret@postgres:notaport/sro_rankings",
    ],
)
def test_dashboard_settings_rejects_invalid_database_urls(
    monkeypatch: pytest.MonkeyPatch,
    invalid_url: str,
) -> None:
    monkeypatch.setenv("DASHBOARD_DATABASE_URL", invalid_url)
    with pytest.raises(ValidationError):
        get_dashboard_settings()


def test_dashboard_settings_masks_secret_and_hides_input_in_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret_token = "SuperSecretReadonlyPass999"
    monkeypatch.setenv(
        "DASHBOARD_DATABASE_URL",
        f"postgresql+psycopg://sro_readonly:{secret_token}@postgres:5432/sro_rankings",
    )
    settings = get_dashboard_settings()
    assert secret_token not in repr(settings)
    assert secret_token not in str(settings)
    assert secret_token not in str(settings.dashboard_database_url)

    monkeypatch.setenv(
        "DASHBOARD_DATABASE_URL",
        f"postgresql+asyncpg://sro_readonly:{secret_token}@postgres:5432/sro_rankings",
    )
    with pytest.raises(ValidationError) as exc_info:
        get_dashboard_settings()
    assert secret_token not in str(exc_info.value)


def test_dashboard_cache_ttl_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "DASHBOARD_DATABASE_URL",
        "postgresql+psycopg://sro_readonly:secret@postgres:5432/sro_rankings",
    )
    monkeypatch.setenv("DASHBOARD_CACHE_TTL_SECONDS", "29")
    with pytest.raises(ValidationError):
        get_dashboard_settings()

    monkeypatch.setenv("DASHBOARD_CACHE_TTL_SECONDS", "3601")
    with pytest.raises(ValidationError):
        get_dashboard_settings()

    monkeypatch.setenv("DASHBOARD_CACHE_TTL_SECONDS", "300")
    settings = get_dashboard_settings()
    assert settings.dashboard_cache_ttl_seconds == 300


def test_build_postgres_options_contains_all_guardrails() -> None:
    options = build_postgres_options()
    assert "-c default_transaction_read_only=on" in options
    assert f"-c statement_timeout={STATEMENT_TIMEOUT_MS}" in options
    assert f"-c lock_timeout={LOCK_TIMEOUT_MS}" in options
    assert f"-c work_mem={WORK_MEM_MB}MB" in options


def test_create_dashboard_engine_applies_pool_and_connect_args(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    class _FakeEngine:
        pass

    fake_engine: Any = _FakeEngine()

    def _fake_create_engine(url: str, **kwargs: Any) -> Any:
        captured["url"] = url
        captured["kwargs"] = kwargs
        return fake_engine

    monkeypatch.setattr("src.dashboard.db.create_engine", _fake_create_engine)
    settings = DashboardSettings(
        dashboard_database_url="postgresql://sro_readonly:pass@postgres:5432/sro_rankings"
    )
    engine: Any = create_dashboard_engine(settings)

    assert engine is fake_engine
    assert captured["url"] == "postgresql+psycopg://sro_readonly:pass@postgres:5432/sro_rankings"
    assert captured["kwargs"]["pool_size"] == POOL_SIZE == 2
    assert captured["kwargs"]["max_overflow"] == 0
    assert captured["kwargs"]["pool_timeout"] == POOL_TIMEOUT_SECONDS == 5
    assert captured["kwargs"]["pool_recycle"] == POOL_RECYCLE_SECONDS == 300
    assert captured["kwargs"]["pool_pre_ping"] is True
    assert captured["kwargs"]["connect_args"] == {"options": build_postgres_options()}


def test_create_dashboard_session_factory_configures_expire_on_commit_false() -> None:
    settings = DashboardSettings(
        dashboard_database_url="postgresql+psycopg://sro_readonly:pass@postgres:5432/sro_rankings"
    )
    engine = create_dashboard_engine(settings)
    try:
        factory = create_dashboard_session_factory(engine)
        assert isinstance(factory, sessionmaker)
        assert factory.kw.get("expire_on_commit") is False
    finally:
        engine.dispose()
