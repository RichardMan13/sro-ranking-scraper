"""Synchronous SQLAlchemy 2.0 read-only engine factory for the dashboard."""

from __future__ import annotations

from typing import Final

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.dashboard.config import DashboardSettings

STATEMENT_TIMEOUT_MS: Final[int] = 15000
LOCK_TIMEOUT_MS: Final[int] = 3000
WORK_MEM_MB: Final[int] = 16
POOL_SIZE: Final[int] = 2
POOL_TIMEOUT_SECONDS: Final[int] = 5
POOL_RECYCLE_SECONDS: Final[int] = 300


def build_postgres_options() -> str:
    """Return PostgreSQL startup session options enforcing read-only guardrails."""
    return (
        f"-c default_transaction_read_only=on "
        f"-c statement_timeout={STATEMENT_TIMEOUT_MS} "
        f"-c lock_timeout={LOCK_TIMEOUT_MS} "
        f"-c work_mem={WORK_MEM_MB}MB"
    )


def create_dashboard_engine(settings: DashboardSettings) -> Engine:
    """Create a constrained synchronous SQLAlchemy engine for read-only analytics."""
    return create_engine(
        settings.dashboard_database_url.get_secret_value(),
        pool_size=POOL_SIZE,
        max_overflow=0,
        pool_timeout=POOL_TIMEOUT_SECONDS,
        pool_recycle=POOL_RECYCLE_SECONDS,
        pool_pre_ping=True,
        connect_args={"options": build_postgres_options()},
    )


def create_dashboard_session_factory(engine: Engine) -> sessionmaker[Session]:
    """Create a synchronous SQLAlchemy sessionmaker bound to the dashboard engine."""
    return sessionmaker(bind=engine, expire_on_commit=False)
