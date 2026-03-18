"""Map httpx HTTP errors to structured MCP-friendly messages."""

from __future__ import annotations

import httpx


class CrucibleError(Exception):
    """Raised when the Control Plane returns a non-2xx response."""

    def __init__(self, status_code: int, detail: str) -> None:
        self.status_code = status_code
        self.detail = detail
        super().__init__(detail)


def raise_for_response(resp: httpx.Response) -> None:
    """Inspect an httpx response and raise CrucibleError with a structured message."""
    if resp.is_success:
        return

    try:
        body = resp.json()
    except Exception:
        body = {}

    detail = body.get("detail", resp.text or f"HTTP {resp.status_code}")

    # Detect FAILED_INSUFFICIENT_CAPACITY from body text
    raw = str(detail).lower()
    if "insufficient_capacity" in raw or "insufficient capacity" in raw:
        raise CrucibleError(503, f"ResourceExhausted: {detail}")

    # Pydantic validation errors — format field paths
    if resp.status_code == 422 and isinstance(detail, list):
        msgs = []
        for err in detail:
            loc = " -> ".join(str(p) for p in err.get("loc", []))
            msgs.append(f"{loc}: {err.get('msg', '')}")
        raise CrucibleError(422, "InvalidParams: " + "; ".join(msgs))

    raise CrucibleError(resp.status_code, str(detail))
