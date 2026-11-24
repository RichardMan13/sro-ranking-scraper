"""Isolated Pydantic v2 configuration for the read-only Streamlit dashboard."""

from __future__ import annotations

from typing import Final
from urllib.parse import urlsplit

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_ALLOWED_SYNC_SCHEMES: Final[frozenset[str]] = frozenset(
    {
        "postgres",
        "postgresql",
        "postgresql+psycopg",
    }
)


class DashboardSettings(BaseSettings):
    """Settings for the read-only analytical dashboard service.

    Intentionally isolated from `src.core.config.Settings` so the dashboard
    container never requires or receives the read-write `DATABASE_URL`.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        hide_input_in_errors=True,
    )

    dashboard_database_url: SecretStr = Field(...)
    dashboard_cache_ttl_seconds: int = Field(default=600, ge=30, le=3600)
    log_level: str = "INFO"

    @field_validator("dashboard_database_url", mode="after")
    @classmethod
    def validate_sync_postgres_url(cls, value: SecretStr) -> SecretStr:
        raw = value.get_secret_value().strip()
        if not raw:
            raise ValueError("DASHBOARD_DATABASE_URL must not be empty")
        if "://" not in raw:
            raise ValueError(
                "DASHBOARD_DATABASE_URL must use a synchronous PostgreSQL scheme "
                "(postgresql+psycopg://, postgresql://, or postgres://)"
            )

        scheme, rest = raw.split("://", maxsplit=1)
        scheme_lower = scheme.lower()
        if scheme_lower not in _ALLOWED_SYNC_SCHEMES:
            raise ValueError(
                f"DASHBOARD_DATABASE_URL scheme {scheme_lower!r} is not supported; "
                "expected synchronous PostgreSQL scheme "
                "(postgresql+psycopg://, postgresql://, or postgres://)"
            )

        normalized = f"postgresql+psycopg://{rest}"
        try:
            parsed = urlsplit(normalized)
            _ = parsed.port
        except ValueError:
            raise ValueError(
                "DASHBOARD_DATABASE_URL is malformed; ensure special characters "
                "in credentials are URL-encoded"
            ) from None

        if not parsed.hostname:
            raise ValueError("DASHBOARD_DATABASE_URL must include a database host")

        return SecretStr(normalized)


def get_dashboard_settings() -> DashboardSettings:
    """Create and return a fresh DashboardSettings instance from the environment."""
    return DashboardSettings()
