from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Final
from urllib.parse import urlsplit

from pydantic import Field, PostgresDsn, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_ALLOWED_DB_SCHEME_RE = re.compile(
    r"^(?:postgres|postgresql(?:\+[a-z0-9_]+)?|sqlite(?:\+[a-z0-9_]+)?)$"
)
_DISCORD_WEBHOOK_HOSTS: Final[frozenset[str]] = frozenset(
    {
        "discord.com",
        "discordapp.com",
        "ptb.discord.com",
        "canary.discord.com",
    }
)


class Settings(BaseSettings):
    """Application settings loaded from environment variables and `.env`."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        hide_input_in_errors=True,
    )

    database_url: PostgresDsn | str = Field(...)
    prefect_api_url: str = "http://localhost:4200/api"
    log_level: str = "INFO"
    quarantine_dir: Path = Path("./quarantine")
    quarantine_retention_days: int = Field(default=30, ge=1)
    circuit_breaker_cooldown_hours: int = Field(default=6, ge=1)
    electus_cf_clearance: str | None = None
    discord_ops_webhook_url: SecretStr | None = None
    discord_community_webhook_url: SecretStr | None = None

    @field_validator(
        "discord_ops_webhook_url",
        "discord_community_webhook_url",
        mode="before",
    )
    @classmethod
    def normalize_discord_webhook_url(cls, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, SecretStr):
            stripped = value.get_secret_value().strip()
            return SecretStr(stripped) if stripped else None
        if isinstance(value, str):
            stripped = value.strip()
            return stripped if stripped else None
        return value

    @field_validator(
        "discord_ops_webhook_url",
        "discord_community_webhook_url",
        mode="after",
    )
    @classmethod
    def validate_discord_webhook_url(cls, value: SecretStr | None) -> SecretStr | None:
        if value is None:
            return None
        raw = value.get_secret_value()
        try:
            parsed = urlsplit(raw)
            _ = parsed.port
        except ValueError:
            raise ValueError(
                "Invalid Discord webhook URL: expected "
                "https://<discord-host>/api/webhooks/<id>/<token>"
            ) from None

        hostname = parsed.hostname.lower() if parsed.hostname is not None else ""
        if parsed.scheme.lower() != "https" or hostname not in _DISCORD_WEBHOOK_HOSTS:
            raise ValueError(
                "Invalid Discord webhook URL: expected "
                "https://<discord-host>/api/webhooks/<id>/<token>"
            )

        trimmed_path = parsed.path.rstrip("/")
        segments = trimmed_path.lstrip("/").split("/") if trimmed_path.startswith("/") else []
        if (
            len(segments) != 4
            or segments[0] != "api"
            or segments[1] != "webhooks"
            or not segments[2]
            or not segments[3]
        ):
            raise ValueError(
                "Invalid Discord webhook URL: expected "
                "https://<discord-host>/api/webhooks/<id>/<token>"
            )
        return value

    @field_validator("database_url", mode="after")
    @classmethod
    def validate_non_empty_database_url(cls, value: PostgresDsn | str) -> PostgresDsn | str:
        raw = str(value).strip()
        if not raw:
            raise ValueError("DATABASE_URL must not be empty")
        if "://" not in raw:
            raise ValueError("DATABASE_URL must include a postgresql or sqlite scheme")
        scheme, rest = raw.split("://", maxsplit=1)
        scheme_lower = scheme.lower()
        if not _ALLOWED_DB_SCHEME_RE.fullmatch(scheme_lower):
            raise ValueError(
                f"DATABASE_URL scheme {scheme_lower!r} is unsupported; "
                "expected postgresql or sqlite"
            )
        raw = f"{scheme_lower}://{rest}"
        parsed = urlsplit(raw)
        if scheme_lower.startswith("sqlite"):
            if not parsed.path or parsed.path == "/":
                raise ValueError("DATABASE_URL with sqlite scheme must include a database path")
        elif not parsed.hostname:
            raise ValueError("DATABASE_URL with postgresql scheme must include a host")
        return raw


def get_settings() -> Settings:
    """Create and return a fresh Settings instance from the current environment."""
    return Settings()
