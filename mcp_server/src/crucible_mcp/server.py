"""Crucible MCP Server entrypoint.

Run locally (stdio):
    python -m crucible_mcp.server

Run as SSE server:
    TRANSPORT=sse python -m crucible_mcp.server
"""

import logging

from mcp.server.fastmcp import FastMCP
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from .config import settings
from .resources import register_resources
from .tools import register_tools

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("crucible-mcp")

mcp = FastMCP(
    "Crucible",
    instructions=(
        "Orchestration layer for the Crucible Data-Testing-as-a-Service platform. "
        "Translates natural language intent into structured load test operations: "
        "validate plans, submit runs, monitor progress, and stop workers."
    ),
)

register_tools(mcp)
register_resources(mcp)


class BearerAuthMiddleware(BaseHTTPMiddleware):
    """Reject SSE requests that lack a valid Bearer token."""

    def __init__(self, app, token: str) -> None:
        super().__init__(app)
        self.token = token

    async def dispatch(self, request: Request, call_next):
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Bearer ") or auth[7:] != self.token:
            return JSONResponse({"detail": "Unauthorized"}, status_code=401)
        return await call_next(request)


def main() -> None:
    if settings.transport == "sse":
        logger.info("Starting Crucible MCP (streamable-http) on %s:%d", settings.sse_host, settings.sse_port)
        import uvicorn
        starlette_app = mcp.streamable_http_app()
        if settings.crucible_api_token:
            starlette_app.add_middleware(BearerAuthMiddleware, token=settings.crucible_api_token)
        uvicorn.run(starlette_app, host=settings.sse_host, port=settings.sse_port)
    else:
        logger.info("Starting Crucible MCP in stdio mode")
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
