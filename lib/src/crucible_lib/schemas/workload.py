"""Generic workload file format — parsing and validation.

Workload files use a text-based annotation format that is query-language
agnostic (SQL, CQL, etc.).  Both the control plane (on upload) and the
worker (before execution) import this module to validate content.

File format
-----------
The first non-blank line must declare the workload type::

    -- @type: sql

Each query block is preceded by a ``-- @name:`` annotation::

    -- @name: TopSellingProducts
    SELECT product_id, SUM(revenue) FROM orders GROUP BY 1 ORDER BY 2 DESC LIMIT 10;

    -- @name: DailyActiveUsers
    SELECT DATE(created_at), COUNT(DISTINCT user_id) FROM events GROUP BY 1;

Validation rules
----------------
* ``-- @type:`` header is required and must appear before any ``-- @name:`` block.
* The type value must be non-empty.
* At least one ``-- @name:`` block must be present.
* Query names must match ``^[A-Za-z_][A-Za-z0-9_]*$``.
* Query names must be unique within the file.
* Query bodies must be non-empty (after stripping whitespace).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

_QUERY_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TYPE_HEADER_RE = re.compile(r"^--\s*@type:\s*(.*)$")
_NAME_HEADER_RE = re.compile(r"^--\s*@name:\s*(.+)$")


@dataclass
class WorkloadQuery:
    name: str
    body: str


@dataclass
class Workload:
    type: str
    queries: list[WorkloadQuery] = field(default_factory=list)


def validate_workload(content: str) -> list[str]:
    """Return a list of validation error strings.  An empty list means valid."""
    errors: list[str] = []
    lines = content.splitlines()

    workload_type: str | None = None
    type_line_seen = False
    name_blocks_seen = False

    # ── Pass 1: find @type header ─────────────────────────────────────────────
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        m = _TYPE_HEADER_RE.match(stripped)
        if m:
            workload_type = m.group(1).strip()
            type_line_seen = True
            if not workload_type:
                errors.append("'-- @type:' value must not be empty.")
            break
        # First non-blank line is not a @type header
        if not stripped.startswith("--"):
            errors.append(
                "Missing '-- @type: <type>' header; "
                "the first non-blank line must declare the workload type."
            )
            break
        # It's a comment but not @type
        errors.append(
            "Missing '-- @type: <type>' header; "
            "the first non-blank comment must be '-- @type: <type>'."
        )
        break
    else:
        # Loop completed without break — content is empty or whitespace-only
        if not type_line_seen:
            errors.append("Workload file is empty.")


    # ── Pass 2: parse @name blocks ────────────────────────────────────────────
    seen_names: set[str] = set()
    current_name: str | None = None
    current_body_lines: list[str] = []
    found_type_line = False

    def _flush(name: str, body_lines: list[str]) -> None:
        nonlocal name_blocks_seen
        name_blocks_seen = True
        body = "\n".join(body_lines).strip()
        if not _QUERY_NAME_RE.match(name):
            errors.append(
                f"Query name '{name}' is invalid; "
                "names must match ^[A-Za-z_][A-Za-z0-9_]*$."
            )
        if name in seen_names:
            errors.append(f"Duplicate query name: '{name}'.")
        else:
            seen_names.add(name)
        if not body:
            errors.append(f"Query '{name}' has an empty body.")

    for line in lines:
        stripped = line.strip()

        # Skip the @type header itself
        if not found_type_line and _TYPE_HEADER_RE.match(stripped):
            found_type_line = True
            continue

        m = _NAME_HEADER_RE.match(stripped)
        if m:
            if current_name is not None:
                _flush(current_name, current_body_lines)
            current_name = m.group(1).strip()
            current_body_lines = []
        elif current_name is not None:
            current_body_lines.append(line)

    # Flush last block
    if current_name is not None:
        _flush(current_name, current_body_lines)

    if type_line_seen and not name_blocks_seen:
        errors.append(
            "No '-- @name: <QueryName>' blocks found; "
            "at least one named query is required."
        )

    return errors


def parse_workload(content: str) -> Workload:
    """Parse workload file content into a :class:`Workload`.

    Raises :class:`ValueError` if the content fails validation.
    """
    errors = validate_workload(content)
    if errors:
        raise ValueError("Invalid workload file:\n" + "\n".join(f"  - {e}" for e in errors))

    lines = content.splitlines()
    workload_type = ""
    queries: list[WorkloadQuery] = []
    found_type_line = False
    current_name: str | None = None
    current_body_lines: list[str] = []

    def _flush_query(name: str, body_lines: list[str]) -> None:
        body = "\n".join(body_lines).strip()
        queries.append(WorkloadQuery(name=name, body=body))

    for line in lines:
        stripped = line.strip()

        if not found_type_line:
            m = _TYPE_HEADER_RE.match(stripped)
            if m:
                workload_type = m.group(1).strip()
                found_type_line = True
            continue

        m = _NAME_HEADER_RE.match(stripped)
        if m:
            if current_name is not None:
                _flush_query(current_name, current_body_lines)
            current_name = m.group(1).strip()
            current_body_lines = []
        elif current_name is not None:
            current_body_lines.append(line)

    if current_name is not None:
        _flush_query(current_name, current_body_lines)

    return Workload(type=workload_type, queries=queries)
