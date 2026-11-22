"""Stdout `structlog` JSON capture and parsing helpers."""

from __future__ import annotations

from typing import Any

import orjson


def parse_json_log_lines(stdout_text: str) -> list[dict[str, Any]]:
    """Parse non-empty JSON lines from captured stdout.

    Raises `AssertionError` if any non-empty line is not a JSON object.
    """
    records: list[dict[str, Any]] = []
    for raw_line in stdout_text.splitlines():
        stripped = raw_line.strip()
        if not stripped:
            continue
        parsed = orjson.loads(stripped)
        if not isinstance(parsed, dict):
            msg = f"Expected JSON object log line, got {type(parsed)!r}: {stripped!r}"
            raise AssertionError(msg)
        records.append(parsed)
    return records
