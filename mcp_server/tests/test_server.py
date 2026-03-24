"""Tests for crucible_mcp.server — middleware and FastMCP config."""

import pytest
from starlette.testclient import TestClient
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route

from crucible_mcp.server import AcceptHeaderMiddleware, BearerAuthMiddleware, mcp


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
    Accept: application/json get a plain JSON response instead of a 406."""
    assert mcp.settings.json_response is True


# ---------------------------------------------------------------------------
# AcceptHeaderMiddleware tests
# ---------------------------------------------------------------------------

def _make_accept_app() -> Starlette:
    """Build a minimal app that echoes the Accept header back."""
    async def echo_accept(request):
        return PlainTextResponse(request.headers.get("accept", ""))

    app = Starlette(routes=[Route("/", echo_accept)])
    app.add_middleware(AcceptHeaderMiddleware)
    return app


def test_accept_header_injected_when_missing():
    """Requests without an Accept header get a default injected."""
    app = _make_accept_app()
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.get("/", headers={})
    assert "application/json" in resp.text
    assert "text/event-stream" in resp.text


def test_accept_header_preserved_when_present():
    """Requests that already have an Accept header are not modified."""
    app = _make_accept_app()
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.get("/", headers={"Accept": "text/html"})
    assert resp.text == "text/html"
