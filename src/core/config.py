from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import urlsplit

from pydantic import Field, PostgresDsn, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_ALLOWED_DB_SCHEME_RE = re.compile(
    r"^(?:postgres|postgresql(?:\+[a-z0-9_]+)?|sqlite(?:\+[a-z0-9_]+)?)$"
)


class Settings(BaseSettings):
    """Application settings loaded from environment variables and `.env`."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: PostgresDsn | str = Field(...)
    prefect_api_url: str = "http://localhost:4200/api"
    log_level: str = "INFO"
    quarantine_dir: Path = Path("./quarantine")
    quarantine_retention_days: int = Field(default=30, ge=1)
    circuit_breaker_cooldown_hours: int = Field(default=6, ge=1)
    electus_cf_clearance: str | None = None

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
