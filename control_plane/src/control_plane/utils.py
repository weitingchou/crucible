"""Shared utilities for the Control Plane."""

from __future__ import annotations

import re


def sanitize_plan_name(name: str) -> str:
    """Replace characters unsafe for S3 keys / run-IDs with hyphens.

    Keeps only ``[a-zA-Z0-9_\\-.]``, collapses consecutive hyphens, and
    strips leading/trailing hyphens.
    """
    sanitized = re.sub(r"[^a-zA-Z0-9_\-.]", "-", name)
    sanitized = re.sub(r"-{2,}", "-", sanitized)
    return sanitized.strip("-")
