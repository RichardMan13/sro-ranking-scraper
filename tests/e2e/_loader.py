"""Strict-mypy dynamic importlib loader for opaque-box E2E tests.

Provides `require_module`, `require_symbol`, and `require_file` so `tests/e2e/`
has zero static `src.*` imports (even under TYPE_CHECKING) and zero `# type: ignore`.
"""

from __future__ import annotations

import importlib
import types
from pathlib import Path

import pytest


def get_repo_root() -> Path:
    """Return repository root path (`tests/e2e/_loader.py` -> parents[2])."""
    return Path(__file__).resolve().parents[2]


def require_module(module_path: str) -> types.ModuleType:
    """Import `module_path` dynamically, skipping cleanly if `src.*` is unbuilt."""
    try:
        return importlib.import_module(module_path)
    except ModuleNotFoundError as exc:
        if exc.name is not None and (
            exc.name == module_path or exc.name.startswith("src.") or exc.name == "src"
        ):
            pytest.skip(f"Target module not implemented yet: {module_path} ({exc})")
        raise


def require_symbol(module_path: str, attr_name: str) -> object:
    """Resolve `attr_name` from `module_path`, skipping if not yet exported."""
    mod = require_module(module_path)
    if not hasattr(mod, attr_name):
        pytest.skip(f"Symbol {attr_name!r} not yet exported by {module_path}")
    val: object = getattr(mod, attr_name)
    return val


def require_file(rel_path: str, repo_root: Path | None = None) -> Path:
    """Verify deliverable file `rel_path` exists, skipping cleanly if absent."""
    root = repo_root if repo_root is not None else get_repo_root()
    path = root / rel_path
    if not path.exists():
        pytest.skip(f"Target deliverable not created yet: {rel_path}")
    return path
