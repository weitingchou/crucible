"""Tests for crucible_mcp.server — BearerAuthMiddleware."""

import pytest
from starlette.testclient import TestClient
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route

from crucible_mcp.server import BearerAuthMiddleware


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
