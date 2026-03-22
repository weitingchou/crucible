"""Tests for crucible_mcp.server — BearerAuthMiddleware and FastMCP config."""

import pytest
from starlette.testclient import TestClient
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route

from crucible_mcp.server import BearerAuthMiddleware, mcp


def _make_app(token: str) -> Starlette:
    """Build a minimal Starlette app with BearerAuthMiddleware."""
    async def homepage(request):
        return PlainTextResponse("ok")

    app = Starlette(routes=[Route("/", homepage)])
    app.add_middleware(BearerAuthMiddleware, token=token)
    return app


# ---------------------------------------------------------------------------
# Auth middleware tests
# ---------------------------------------------------------------------------

def test_request_with_valid_token_succeeds():
    app = _make_app("secret-token")
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.get("/", headers={"Authorization": "Bearer secret-token"})
    assert resp.status_code == 200
    assert resp.text == "ok"


def test_request_with_wrong_token_returns_401():
    app = _make_app("secret-token")
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.get("/", headers={"Authorization": "Bearer wrong-token"})
    assert resp.status_code == 401
    assert "Unauthorized" in resp.json()["detail"]


def test_request_without_auth_header_returns_401():
    app = _make_app("secret-token")
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.get("/")
    assert resp.status_code == 401


def test_request_with_non_bearer_scheme_returns_401():
    app = _make_app("secret-token")
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.get("/", headers={"Authorization": "Basic dXNlcjpwYXNz"})
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# FastMCP configuration tests
# ---------------------------------------------------------------------------

def test_json_response_enabled():
    """Server must have json_response=True so clients sending only
    Accept: application/json (without text/event-stream) get a plain
    JSON response instead of a 406 Not Acceptable."""
    assert mcp.settings.json_response is True


def test_streamable_http_app_accepts_json_only():
    """POST to /mcp with Accept: application/json should not return 406."""
    from mcp.server.fastmcp import FastMCP

    # Use a separate FastMCP instance with json_response and DNS rebinding
    # protection configured to allow the test client's Host header.
    test_mcp = FastMCP("test", json_response=True, host="0.0.0.0")
    app = test_mcp.streamable_http_app()

    with TestClient(app, raise_server_exceptions=False) as client:
        resp = client.post(
            "/mcp",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            json={"jsonrpc": "2.0", "method": "initialize", "id": 1, "params": {
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "0.1.0"},
            }},
        )
    # Should get a successful JSON response, not 406
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/json")
