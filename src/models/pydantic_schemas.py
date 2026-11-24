from __future__ import annotations

import math
import re
from typing import Any, Final, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

MAX_ERROR_RATE: Final[float] = 0.05
MIN_GUILD_RECORDS: Final[int] = 5

ScrapeStatus = Literal[
    "SUCCESS",
    "PARTIAL_SUCCESS",
    "SCHEMA_DRIFT",
    "LOW_COUNT_QUARANTINE",
    "BLOCKED_CHALLENGE",
    "TARGET_MAINTENANCE",
    "HTTP_ERROR",
]

TriggerType = Literal[
    "scheduled_12h",
    "post_fw_extra",
    "manual",
]

CANONICAL_STATUSES: Final[frozenset[str]] = frozenset(
    {
        "SUCCESS",
        "PARTIAL_SUCCESS",
        "SCHEMA_DRIFT",
        "LOW_COUNT_QUARANTINE",
        "BLOCKED_CHALLENGE",
        "TARGET_MAINTENANCE",
        "HTTP_ERROR",
    }
)

CANONICAL_TRIGGER_TYPES: Final[frozenset[str]] = frozenset(
    {
        "scheduled_12h",
        "post_fw_extra",
        "manual",
    }
)

_EMPTY_PLACEHOLDERS: Final[frozenset[str]] = frozenset({"", "-", "None", "No Name", "XXXX", "N/A"})

_JOB_TYPE_MAPPING: Final[dict[str, str]] = {
    "TRADER": "TRADER",
    "MERCADOR": "TRADER",
    "THIEF": "THIEF",
    "LADRÃO": "THIEF",
    "HUNTER": "HUNTER",
    "CAÇADOR": "HUNTER",
}


def _normalize_empty_strings(v: Any) -> str | None:
    if v is None:
        return None
    cleaned = str(v).strip()
    if cleaned in _EMPTY_PLACEHOLDERS:
        return None
    return cleaned


def _normalize_race(v: Any) -> str | None:
    if not v:
        return None
    cleaned = str(v).strip().upper()
    tokens = set(re.findall(r"[A-Z0-9]+", cleaned))
    is_ch = "CHINESE" in cleaned or "CHINA" in cleaned or "CH" in tokens
    is_eu = "EUROPEAN" in cleaned or "EUROPE" in cleaned or "EU" in tokens
    if is_ch and is_eu:
        return None
    if is_ch:
        return "CH"
    if is_eu:
        return "EU"
    return None


def _reject_bool(v: Any) -> Any:
    if isinstance(v, bool):
        raise ValueError("Boolean is not a valid integer")
    return v


def _clean_required_int(v: Any) -> int:
    _reject_bool(v)
    parsed = _clean_int_numbers(v)
    if parsed is None:
        raise ValueError("Field cannot be empty or non-numeric")
    return parsed


def _normalize_job_type(v: Any) -> str | None:
    if not v:
        return None
    val = str(v).strip().upper()
    return _JOB_TYPE_MAPPING.get(val)


def _clean_int_numbers(v: Any) -> int | None:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        if math.isfinite(v) and v.is_integer():
            return int(v)
        raise ValueError("Non-integer float is not valid for integer field")
    raw = str(v).strip()
    if raw in _EMPTY_PLACEHOLDERS:
        return None
    if "/" in raw:
        raw = raw.split("/", maxsplit=1)[0].strip()
    raw = re.sub(r"(?<=\d)\s*[A-Za-z]+$", "", raw).strip()
    if raw in _EMPTY_PLACEHOLDERS or not any(ch.isdigit() for ch in raw):
        return None
    if re.fullmatch(r"-?\d+", raw):
        return int(raw)
    if re.fullmatch(r"-?\d{1,3}(?:,\d{3})+", raw):
        return int(raw.replace(",", ""))
    if re.fullmatch(r"-?\d{1,3}(?:\.\d{3})+", raw):
        return int(raw.replace(".", ""))
    if re.fullmatch(r"-?(?:\d+|\d{1,3}(?:,\d{3})+)\.0+", raw):
        return int(raw.split(".", 1)[0].replace(",", ""))
    raise ValueError(f"Invalid integer value: {raw!r}")


def _clean_float_numbers(v: Any) -> float | None:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        num = float(v)
        if not math.isfinite(num):
            raise ValueError("Float value must be finite")
        return num
    raw = str(v).strip()
    if not raw:
        return None
    if "," in raw and "." in raw:
        if raw.rfind(",") < raw.rfind("."):
            cleaned = raw.replace(",", "")
        else:
            cleaned = raw.replace(".", "").replace(",", ".")
    elif "," in raw:
        parts = raw.split(",")
        if len(parts) == 2 and len(parts[1]) in (1, 2):
            cleaned = raw.replace(",", ".")
        else:
            cleaned = raw.replace(",", "")
    else:
        cleaned = raw
    if re.fullmatch(r"-?\d{1,3}(?:\.\d{3}){2,}", cleaned):
        cleaned = cleaned.replace(".", "")
    try:
        num = float(cleaned)
    except ValueError:
        return None
    if not math.isfinite(num):
        return None
    return num


class ErrorDetails(BaseModel):
    """Structured error summary persisted in `scrape_runs.error_details`."""

    reason: str
    failed_category: str
    sample_errors: list[dict[str, Any]] = Field(default_factory=list)
    quarantine_sha256: str | None = None
    quarantine_size_bytes: int | None = None


class RawPlayerRankingItem(BaseModel):
    """Validated player ranking record extracted from a target server."""

    model_config = ConfigDict(strict=False, str_strip_whitespace=True)

    # Core required fields
    rank: int = Field(ge=1)
    char_name: str = Field(min_length=1, max_length=64)
    level: int = Field(ge=1, le=140)

    # Standardized optional fields
    guild_name: str | None = None
    race: Literal["CH", "EU"] | None = None
    item_points: float | None = Field(default=None, ge=0)
    strength: int | None = Field(default=None, ge=0)
    intellect: int | None = Field(default=None, ge=0)
    title: str | None = None

    # Job & honor ecosystem
    job_type: Literal["TRADER", "THIEF", "HUNTER"] | None = None
    job_level: int | None = Field(default=None, ge=0)
    job_points: int | None = Field(default=None, ge=0)
    job_exp: int | None = Field(default=None, ge=0)
    honor_points: int | None = Field(default=None, ge=0)

    # Server-specific attributes stored in JSONB
    extra_metrics: dict[str, Any] = Field(default_factory=dict)

    @field_validator("rank", "level", mode="before")
    @classmethod
    def clean_required_int(cls, v: Any) -> int:
        return _clean_required_int(v)

    @field_validator("char_name", "guild_name", "title", mode="before")
    @classmethod
    def normalize_empty_strings(cls, v: Any) -> str | None:
        return _normalize_empty_strings(v)

    @field_validator("race", mode="before")
    @classmethod
    def normalize_race(cls, v: Any) -> str | None:
        return _normalize_race(v)

    @field_validator("job_type", mode="before")
    @classmethod
    def normalize_job_type(cls, v: Any) -> str | None:
        return _normalize_job_type(v)

    @field_validator(
        "strength",
        "intellect",
        "job_level",
        "job_points",
        "job_exp",
        "honor_points",
        mode="before",
    )
    @classmethod
    def clean_int_numbers(cls, v: Any) -> int | None:
        return _clean_int_numbers(v)

    @field_validator("item_points", mode="before")
    @classmethod
    def clean_float_numbers(cls, v: Any) -> float | None:
        return _clean_float_numbers(v)


class RawJobRankingItem(BaseModel):
    """Validated job ranking record used to enrich player snapshots in memory."""

    model_config = ConfigDict(strict=False, str_strip_whitespace=True)

    rank: int = Field(ge=1)
    char_name: str = Field(min_length=1, max_length=64)
    job_type: Literal["TRADER", "THIEF", "HUNTER"] | None = None
    job_level: int | None = Field(default=None, ge=1, le=140)
    job_points: int | None = Field(default=None, ge=0)
    job_exp: int | None = Field(default=None, ge=0)
    guild_name: str | None = None
    extra_metrics: dict[str, Any] = Field(default_factory=dict)

    @field_validator("rank", mode="before")
    @classmethod
    def clean_required_int(cls, v: Any) -> int:
        return _clean_required_int(v)

    @field_validator("job_level", mode="before")
    @classmethod
    def clean_optional_job_level(cls, v: Any) -> int | None:
        if v is None:
            return None
        _reject_bool(v)
        return _clean_int_numbers(v)

    @field_validator("char_name", "guild_name", mode="before")
    @classmethod
    def normalize_empty_strings(cls, v: Any) -> str | None:
        return _normalize_empty_strings(v)

    @field_validator("job_type", mode="before")
    @classmethod
    def normalize_job_type(cls, v: Any) -> str | None:
        if v is None:
            return None
        cleaned = str(v).strip()
        if cleaned in _EMPTY_PLACEHOLDERS:
            return None
        mapped = _JOB_TYPE_MAPPING.get(cleaned.upper())
        if mapped is None:
            raise ValueError(f"Unknown job_type: {v!r}")
        return mapped

    @field_validator("job_points", "job_exp", mode="before")
    @classmethod
    def clean_int_numbers(cls, v: Any) -> int | None:
        return _clean_int_numbers(v)


class RawGuildRankingItem(BaseModel):
    """Validated guild ranking record extracted from a target server."""

    model_config = ConfigDict(strict=False, str_strip_whitespace=True)

    rank: int = Field(ge=1)
    guild_name: str = Field(min_length=1, max_length=64)
    master_name: str | None = None
    members_count: int | None = Field(default=None, ge=0)
    total_item_points: float | None = Field(default=None, ge=0)
    fortress_held: str | None = None
    extra_metrics: dict[str, Any] = Field(default_factory=dict)

    @field_validator("rank", mode="before")
    @classmethod
    def clean_required_int(cls, v: Any) -> int:
        return _clean_required_int(v)

    @field_validator("guild_name", "master_name", "fortress_held", mode="before")
    @classmethod
    def normalize_empty_strings(cls, v: Any) -> str | None:
        return _normalize_empty_strings(v)

    @field_validator("members_count", mode="before")
    @classmethod
    def clean_int_numbers(cls, v: Any) -> int | None:
        return _clean_int_numbers(v)

    @field_validator("total_item_points", mode="before")
    @classmethod
    def clean_float_numbers(cls, v: Any) -> float | None:
        return _clean_float_numbers(v)
