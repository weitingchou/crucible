"""Tests for crucible_mcp.client — HTTP client wrapper with error handling."""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock

import httpx

from crucible_mcp.errors import CrucibleError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_response(status_code: int = 200, json_body=None, text: str = ""):
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.is_success = 200 <= status_code < 300
    resp.text = text
    if json_body is not None:
        resp.json.return_value = json_body
    else:
        resp.json.side_effect = ValueError("no json")
    return resp


class _FakeClient:
    """Stand-in for httpx.AsyncClient that returns a predetermined response."""

    def __init__(self, resp):
        self._resp = resp

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def get(self, url, **kw):
        return self._resp

    async def post(self, url, **kw):
        return self._resp

    async def put(self, url, **kw):
        return self._resp


# ---------------------------------------------------------------------------
# list_sut_types
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_sut_types_returns_list():
    resp = _mock_response(200, {"sut_types": ["doris", "trino"]})
    with patch("crucible_mcp.client._client", return_value=_FakeClient(resp)):
        from crucible_mcp.client import list_sut_types
        result = await list_sut_types()
    assert result == ["doris", "trino"]


@pytest.mark.asyncio
async def test_list_sut_types_raises_on_error():
    resp = _mock_response(500, {"detail": "Internal error"})
    with patch("crucible_mcp.client._client", return_value=_FakeClient(resp)):
        from crucible_mcp.client import list_sut_types
        with pytest.raises(CrucibleError) as exc_info:
            await list_sut_types()
        assert exc_info.value.status_code == 500


# ---------------------------------------------------------------------------
# submit_run
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_submit_run_posts_json():
    resp = _mock_response(200, {"run_id": "abc", "plan_key": "plans/smoke", "strategy": "intra_node"})
    with patch("crucible_mcp.client._client", return_value=_FakeClient(resp)):
        from crucible_mcp.client import submit_run
        result = await submit_run("plan_yaml: true", "smoke", "test-label")
    assert result["run_id"] == "abc"


@pytest.mark.asyncio
async def test_submit_run_raises_on_422():
    detail = [{"loc": ["body", "plan_yaml"], "msg": "field required"}]
    resp = _mock_response(422, {"detail": detail})
    with patch("crucible_mcp.client._client", return_value=_FakeClient(resp)):
        from crucible_mcp.client import submit_run
        with pytest.raises(CrucibleError) as exc_info:
            await submit_run("bad", "name")
        assert exc_info.value.status_code == 422
        assert "InvalidParams" in exc_info.value.detail


# ---------------------------------------------------------------------------
# get_run_status
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_run_status_returns_dict():
    body = {"run_id": "r1", "status": "EXECUTING", "run_label": "test",
            "sut_type": "doris", "scaling_mode": "intra_node",
            "submitted_at": "2025-01-01T00:00:00"}
    resp = _mock_response(200, body)
    with patch("crucible_mcp.client._client", return_value=_FakeClient(resp)):
        from crucible_mcp.client import get_run_status
        result = await get_run_status("r1")
    assert result["status"] == "EXECUTING"


@pytest.mark.asyncio
async def test_get_run_status_raises_on_404():
    resp = _mock_response(404, {"detail": "Run 'bad' not found."})
    with patch("crucible_mcp.client._client", return_value=_FakeClient(resp)):
        from crucible_mcp.client import get_run_status
        with pytest.raises(CrucibleError):
            await get_run_status("bad")


# ---------------------------------------------------------------------------
# stop_run
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stop_run_returns_action():
    resp = _mock_response(200, {"run_id": "r1", "action": "sigterm_sent"})
    with patch("crucible_mcp.client._client", return_value=_FakeClient(resp)):
        from crucible_mcp.client import stop_run
        result = await stop_run("r1")
    assert result["action"] == "sigterm_sent"


# ---------------------------------------------------------------------------
# list_fixtures / get_fixture_files
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_fixtures():
    resp = _mock_response(200, {"fixture_ids": ["tpch", "ssb"]})
    with patch("crucible_mcp.client._client", return_value=_FakeClient(resp)):
        from crucible_mcp.client import list_fixtures
        result = await list_fixtures()
    assert result["fixture_ids"] == ["tpch", "ssb"]


@pytest.mark.asyncio
async def test_get_fixture_files():
    resp = _mock_response(200, {"fixture_id": "tpch", "files": [{"name": "a.parquet", "size": 100}]})
    with patch("crucible_mcp.client._client", return_value=_FakeClient(resp)):
        from crucible_mcp.client import get_fixture_files
        result = await get_fixture_files("tpch")
    assert result["files"][0]["name"] == "a.parquet"


# ---------------------------------------------------------------------------
# get_recent_stats / get_run_artifacts
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_recent_stats():
    resp = _mock_response(200, {"runs": []})
    with patch("crucible_mcp.client._client", return_value=_FakeClient(resp)):
        from crucible_mcp.client import get_recent_stats
        result = await get_recent_stats()
    assert result["runs"] == []


@pytest.mark.asyncio
async def test_get_run_artifacts():
    resp = _mock_response(200, {"run_id": "r1", "artifacts": [{"key": "results/r1/k6.csv", "size": 1024}]})
    with patch("crucible_mcp.client._client", return_value=_FakeClient(resp)):
        from crucible_mcp.client import get_run_artifacts
        result = await get_run_artifacts("r1")
    assert len(result["artifacts"]) == 1


# ---------------------------------------------------------------------------
# upload_plan
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_upload_plan_returns_key():
    resp = _mock_response(200, {"key": "plans/my-plan"})
    with patch("crucible_mcp.client._client", return_value=_FakeClient(resp)):
        from crucible_mcp.client import upload_plan
        result = await upload_plan("my-plan", {"test_metadata": {"run_label": "my-plan"}})
    assert result["key"] == "plans/my-plan"


@pytest.mark.asyncio
async def test_upload_plan_raises_on_422():
    detail = [{"loc": ["body", "test_metadata"], "msg": "field required"}]
    resp = _mock_response(422, {"detail": detail})
    with patch("crucible_mcp.client._client", return_value=_FakeClient(resp)):
        from crucible_mcp.client import upload_plan
        with pytest.raises(CrucibleError) as exc_info:
            await upload_plan("name", {"bad": "data"})
        assert exc_info.value.status_code == 422


# ---------------------------------------------------------------------------
# trigger_run
# ---------------------------------------------------------------------------

class _CapturingClient(_FakeClient):
    """Captures the last POST call arguments for inspection."""
    def __init__(self, resp):
        super().__init__(resp)
        self.last_post_url = None
        self.last_post_kwargs = {}

    async def post(self, url, **kw):
        self.last_post_url = url
        self.last_post_kwargs = kw
        return self._resp


@pytest.mark.asyncio
async def test_trigger_run_returns_run_id():
    resp = _mock_response(200, {"run_id": "r1", "plan_key": "plans/bench", "strategy": "intra_node"})
    with patch("crucible_mcp.client._client", return_value=_FakeClient(resp)):
        from crucible_mcp.client import trigger_run
        result = await trigger_run("bench")
    assert result["run_id"] == "r1"


@pytest.mark.asyncio
async def test_trigger_run_with_cluster_spec_posts_spec():
    resp = _mock_response(200, {"run_id": "r2", "plan_key": "plans/bench", "strategy": "intra_node"})
    spec = {"type": "doris", "backend_node": {"replica": 3}}
    capturing = _CapturingClient(resp)
    with patch("crucible_mcp.client._client", return_value=capturing):
        from crucible_mcp.client import trigger_run
        await trigger_run("bench", cluster_spec=spec)
    assert capturing.last_post_kwargs["json"]["cluster_spec"] == spec


@pytest.mark.asyncio
async def test_trigger_run_with_label_posts_label():
    resp = _mock_response(200, {"run_id": "r3", "plan_key": "plans/bench", "strategy": "intra_node"})
    capturing = _CapturingClient(resp)
    with patch("crucible_mcp.client._client", return_value=capturing):
        from crucible_mcp.client import trigger_run
        await trigger_run("bench", label="my-run")
    assert capturing.last_post_kwargs["json"]["label"] == "my-run"


@pytest.mark.asyncio
async def test_trigger_run_no_label_no_spec_sends_empty_payload():
    """With no label or cluster_spec, payload should be an empty dict (not None)."""
    resp = _mock_response(200, {"run_id": "r4", "plan_key": "plans/bench", "strategy": "intra_node"})
    capturing = _CapturingClient(resp)
    with patch("crucible_mcp.client._client", return_value=capturing):
        from crucible_mcp.client import trigger_run
        await trigger_run("bench")
    # json key is present with an empty dict — not None
    assert "json" in capturing.last_post_kwargs
    assert capturing.last_post_kwargs["json"] == {}


@pytest.mark.asyncio
async def test_trigger_run_posts_to_correct_url():
    resp = _mock_response(200, {"run_id": "r5", "plan_key": "plans/my-plan", "strategy": "intra_node"})
    capturing = _CapturingClient(resp)
    with patch("crucible_mcp.client._client", return_value=capturing):
        from crucible_mcp.client import trigger_run
        await trigger_run("my-plan")
    assert capturing.last_post_url == "/v1/test-runs/my-plan"


@pytest.mark.asyncio
async def test_trigger_run_raises_on_404():
    resp = _mock_response(404, {"detail": "Plan 'bad' not found."})
    with patch("crucible_mcp.client._client", return_value=_FakeClient(resp)):
        from crucible_mcp.client import trigger_run
        with pytest.raises(CrucibleError) as exc_info:
            await trigger_run("bad")
        assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_trigger_run_raises_on_422():
    detail = [{"loc": ["body", "cluster_spec"], "msg": "type mismatch"}]
    resp = _mock_response(422, {"detail": detail})
    with patch("crucible_mcp.client._client", return_value=_FakeClient(resp)):
        from crucible_mcp.client import trigger_run
        with pytest.raises(CrucibleError) as exc_info:
            await trigger_run("bench", cluster_spec={"type": "cassandra"})
        assert exc_info.value.status_code == 422


# ---------------------------------------------------------------------------
# Auth header injection
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_headers_includes_token_when_set():
    with patch("crucible_mcp.client.settings") as mock_settings:
        mock_settings.crucible_api_token = "my-secret"
        mock_settings.crucible_api_url = "http://localhost:8000"
        from crucible_mcp.client import _headers
        h = _headers()
    assert h["Authorization"] == "Bearer my-secret"


@pytest.mark.asyncio
async def test_headers_empty_when_no_token():
    with patch("crucible_mcp.client.settings") as mock_settings:
        mock_settings.crucible_api_token = ""
        from crucible_mcp.client import _headers
        h = _headers()
    assert h == {}
