from __future__ import annotations

import logging
from typing import cast
from uuid import UUID

import structlog
from structlog.typing import FilteringBoundLogger

_LOG_LEVELS: dict[str, int] = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "WARN": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


def configure_logging(log_level: str = "INFO") -> None:
    """Configure structlog for structured JSON output to stdout."""
    level = _LOG_LEVELS.get(log_level.strip().upper(), logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )


def get_logger(name: str | None = None) -> FilteringBoundLogger:
    """Return a structured logger optionally bound with a logger name."""
    logger = cast(FilteringBoundLogger, structlog.get_logger())
    if name is not None:
        return logger.bind(logger=name)
    return logger


def bind_run_context(
    server_id: str | None = None,
    run_id: str | UUID | None = None,
) -> None:
    """Bind run-scoped metadata (`server_id`, `run_id`) to structlog contextvars."""
    ctx: dict[str, str] = {}
    if server_id is not None:
        ctx["server_id"] = server_id
    if run_id is not None:
        ctx["run_id"] = str(run_id)
    if ctx:
        structlog.contextvars.bind_contextvars(**ctx)


def clear_run_context() -> None:
    """Clear all bound structlog contextvars for the current execution context."""
    structlog.contextvars.clear_contextvars()
