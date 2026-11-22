"""Synchronous filesystem helpers for use from async E2E test functions (Ruff ASYNC240 safe)."""

from __future__ import annotations

import gzip
import os
from pathlib import Path


def path_exists(path: str | Path) -> bool:
    """Return whether `path` exists."""
    return Path(path).exists()


def read_bytes(path: str | Path) -> bytes:
    """Read raw bytes from `path`."""
    return Path(path).read_bytes()


def decompress_gz(path: str | Path) -> bytes:
    """Read and gzip-decompress `path`."""
    return gzip.decompress(Path(path).read_bytes())


def write_text(path: str | Path, text: str) -> None:
    """Write UTF-8 text to `path`."""
    Path(path).write_text(text, encoding="utf-8")


def read_text(path: str | Path) -> str:
    """Read UTF-8 text from `path`."""
    return Path(path).read_text(encoding="utf-8")


def write_gz(path: str | Path, data: bytes) -> None:
    """Gzip-compress and write `data` to `path`."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(gzip.compress(data))


def rglob_gz(root: Path) -> list[Path]:
    """Return sorted list of `*.html.gz` files under `root`."""
    return sorted(root.rglob("*.html.gz"))


def rglob_py(root: Path) -> list[Path]:
    """Return sorted list of `*.py` files under `root`."""
    return sorted(root.rglob("*.py"))


def set_mtime_age_days(path: Path, days_ago: float) -> None:
    """Set atime/mtime of `path` to `days_ago` days in the past."""
    import time

    target = time.time() - days_ago * 86400
    os.utime(path, (target, target))
