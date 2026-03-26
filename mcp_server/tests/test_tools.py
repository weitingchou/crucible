"""Tests for crucible_mcp.tools — all 8 MCP tools."""

import pytest
import yaml
from unittest.mock import AsyncMock, patch

from mcp.server.fastmcp import FastMCP

from crucible_mcp.errors import CrucibleError
from crucible_mcp.tools import register_tools


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_VALID_PLAN = {
    "test_metadata": {"run_label": "smoke-test"},
    "test_environment": {
        "env_type": "long-lived",
        "target_db": "tpch",
        "component_spec": {
            "type": "doris",
            "cluster_info": {"host": "doris-fe:9030", "username": "root", "password": ""},
        },
        "fixtures": [],
    },
    "execution": {
        "executor": "k6",
        "scaling_mode": "intra_node",
        "concurrency": 10,
        "ramp_up": "30s",
        "hold_for": "2m",
        "workload": [],
    },
}

_VALID_PLAN_YAML = yaml.dump(_VALID_PLAN)


@pytest.fixture
def mcp_app():
    """Create a FastMCP instance with tools registered."""
    app = FastMCP("test")
    register_tools(app)
    return app


def _get_tool(mcp_app: FastMCP, name: str):
    """Retrieve a registered tool function by name."""
    for tool in mcp_app._tool_manager._tools.values():
        if tool.name == name:
            return tool.fn
    raise KeyError(f"Tool {name!r} not found")


# ---------------------------------------------------------------------------
# list_supported_suts
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_supported_suts(mcp_app):
    fn = _get_tool(mcp_app, "list_supported_suts")
    with patch("crucible_mcp.tools.client.list_sut_types", new_callable=AsyncMock) as mock:
        mock.return_value = ["doris", "trino", "cassandra"]
        result = await fn()
    assert result == ["doris", "trino", "cassandra"]
    mock.assert_awaited_once()


# ---------------------------------------------------------------------------
# get_db_inventory
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_db_inventory(mcp_app):
    fn = _get_tool(mcp_app, "get_db_inventory")
    with patch("crucible_mcp.tools.client.get_sut_inventory", new_callable=AsyncMock) as mock:
        mock.return_value = {"active": [{"run_id": "r1", "sut_type": "doris", "status": "EXECUTING"}]}
        result = await fn()
    assert result["active"][0]["sut_type"] == "doris"


# ---------------------------------------------------------------------------
# validate_test_plan
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_validate_valid_plan(mcp_app):
    fn = _get_tool(mcp_app, "validate_test_plan")
    result = await fn(_VALID_PLAN_YAML)
    assert result == {"valid": True}


@pytest.mark.asyncio
async def test_validate_invalid_yaml(mcp_app):
    fn = _get_tool(mcp_app, "validate_test_plan")
    result = await fn("{{not: valid: yaml:")
    assert result["valid"] is False
    assert result["errors"][0]["path"] == "<yaml>"


@pytest.mark.asyncio
async def test_validate_missing_required_field(mcp_app):
    fn = _get_tool(mcp_app, "validate_test_plan")
    # Missing test_metadata
    plan = {
        "test_environment": _VALID_PLAN["test_environment"],
        "execution": _VALID_PLAN["execution"],
    }
    result = await fn(yaml.dump(plan))
    assert result["valid"] is False
    assert any("test_metadata" in e["path"] for e in result["errors"])


@pytest.mark.asyncio
async def test_validate_bad_concurrency(mcp_app):
    fn = _get_tool(mcp_app, "validate_test_plan")
    plan = {**_VALID_PLAN, "execution": {**_VALID_PLAN["execution"], "concurrency": 0}}
    result = await fn(yaml.dump(plan))
    assert result["valid"] is False


@pytest.mark.asyncio
async def test_validate_invalid_scaling_mode(mcp_app):
    fn = _get_tool(mcp_app, "validate_test_plan")
    plan = {**_VALID_PLAN, "execution": {**_VALID_PLAN["execution"], "scaling_mode": "invalid"}}
    result = await fn(yaml.dump(plan))
    assert result["valid"] is False


@pytest.mark.asyncio
async def test_validate_long_lived_without_cluster_info_fails(mcp_app):
    fn = _get_tool(mcp_app, "validate_test_plan")
    plan = {
        **_VALID_PLAN,
        "test_environment": {
            **_VALID_PLAN["test_environment"],
            "component_spec": {"type": "doris"},
        },
    }
    result = await fn(yaml.dump(plan))
    assert result["valid"] is False
    assert any("cluster_info" in e["message"] for e in result["errors"])


@pytest.mark.asyncio
async def test_validate_disposable_without_cluster_info_succeeds(mcp_app):
    fn = _get_tool(mcp_app, "validate_test_plan")
    plan = {
        **_VALID_PLAN,
        "test_environment": {
            **_VALID_PLAN["test_environment"],
            "env_type": "disposable",
            "component_spec": {"type": "doris"},
        },
    }
    result = await fn(yaml.dump(plan))
    assert result["valid"] is True


# ---------------------------------------------------------------------------
# upload_test_plan
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_upload_test_plan_succeeds(mcp_app):
    fn = _get_tool(mcp_app, "upload_test_plan")
    with patch("crucible_mcp.tools.client.upload_plan", new_callable=AsyncMock) as mock:
        mock.return_value = {"key": "plans/my-plan"}
        result = await fn(_VALID_PLAN_YAML, "my-plan")
    assert result["success"] is True
    assert result["key"] == "plans/my-plan"
    mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_upload_test_plan_invalid_yaml(mcp_app):
    fn = _get_tool(mcp_app, "upload_test_plan")
    result = await fn("not: valid: plan:", "name")
    assert result["success"] is False
    assert "errors" in result


@pytest.mark.asyncio
async def test_upload_test_plan_catches_crucible_error(mcp_app):
    fn = _get_tool(mcp_app, "upload_test_plan")
    with patch("crucible_mcp.tools.client.upload_plan", new_callable=AsyncMock) as mock:
        mock.side_effect = CrucibleError(500, "S3 unavailable")
        result = await fn(_VALID_PLAN_YAML, "name")
    assert result["success"] is False
    assert "S3 unavailable" in result["error"]


@pytest.mark.asyncio
async def test_upload_test_plan_does_not_call_api_on_invalid(mcp_app):
    fn = _get_tool(mcp_app, "upload_test_plan")
    with patch("crucible_mcp.tools.client.upload_plan", new_callable=AsyncMock) as mock:
        await fn("{{bad yaml", "name")
    mock.assert_not_called()


# ---------------------------------------------------------------------------
# submit_test_run
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_submit_valid_plan(mcp_app):
    fn = _get_tool(mcp_app, "submit_test_run")
    with patch("crucible_mcp.tools.client.submit_run", new_callable=AsyncMock) as mock:
        mock.return_value = {"run_id": "r1", "plan_key": "plans/smoke", "strategy": "intra_node"}
        result = await fn(_VALID_PLAN_YAML, "smoke", "test-label")
    assert result["success"] is True
    assert result["run_id"] == "r1"
    mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_submit_invalid_plan_returns_errors(mcp_app):
    fn = _get_tool(mcp_app, "submit_test_run")
    result = await fn("not: valid: plan:", "name")
    assert result["success"] is False
    assert "errors" in result


@pytest.mark.asyncio
async def test_submit_catches_crucible_error(mcp_app):
    fn = _get_tool(mcp_app, "submit_test_run")
    with patch("crucible_mcp.tools.client.submit_run", new_callable=AsyncMock) as mock:
        mock.side_effect = CrucibleError(503, "ResourceExhausted: no capacity")
        result = await fn(_VALID_PLAN_YAML, "smoke")
    assert result["success"] is False
    assert "ResourceExhausted" in result["error"]


@pytest.mark.asyncio
async def test_submit_injects_prometheus_url(mcp_app):
    fn = _get_tool(mcp_app, "submit_test_run")
    with patch("crucible_mcp.tools.settings") as mock_settings, \
         patch("crucible_mcp.tools.client.submit_run", new_callable=AsyncMock) as mock_submit:
        mock_settings.k6_prometheus_rw_url = "http://prom:9090/write"
        mock_submit.return_value = {"run_id": "r1", "plan_key": "p", "strategy": "intra_node"}
        await fn(_VALID_PLAN_YAML, "smoke")
        # Verify the submitted YAML now contains the injected URL
        submitted_yaml = mock_submit.call_args[0][0]
        parsed = yaml.safe_load(submitted_yaml)
        assert parsed["k6_prometheus_rw_server_url"] == "http://prom:9090/write"


@pytest.mark.asyncio
async def test_submit_does_not_overwrite_existing_prometheus_url(mcp_app):
    fn = _get_tool(mcp_app, "submit_test_run")
    plan_with_prom = {**_VALID_PLAN, "k6_prometheus_rw_server_url": "http://existing:9090"}
    plan_yaml = yaml.dump(plan_with_prom)
    with patch("crucible_mcp.tools.settings") as mock_settings, \
         patch("crucible_mcp.tools.client.submit_run", new_callable=AsyncMock) as mock_submit:
        mock_settings.k6_prometheus_rw_url = "http://new:9090"
        mock_submit.return_value = {"run_id": "r1", "plan_key": "p", "strategy": "intra_node"}
        await fn(plan_yaml, "smoke")
        submitted_yaml = mock_submit.call_args[0][0]
        parsed = yaml.safe_load(submitted_yaml)
        assert parsed["k6_prometheus_rw_server_url"] == "http://existing:9090"


# ---------------------------------------------------------------------------
# monitor_test_progress
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_monitor_returns_status(mcp_app):
    fn = _get_tool(mcp_app, "monitor_test_progress")
    with patch("crucible_mcp.tools.client.get_run_status", new_callable=AsyncMock) as mock:
        mock.return_value = {"run_id": "r1", "status": "EXECUTING"}
        result = await fn("r1")
    assert result["status"] == "EXECUTING"


@pytest.mark.asyncio
async def test_monitor_not_found_returns_error(mcp_app):
    fn = _get_tool(mcp_app, "monitor_test_progress")
    with patch("crucible_mcp.tools.client.get_run_status", new_callable=AsyncMock) as mock:
        mock.side_effect = CrucibleError(404, "Run 'bad' not found.")
        result = await fn("bad")
    assert "not found" in result["error"]


@pytest.mark.asyncio
async def test_monitor_server_error_returns_error(mcp_app):
    fn = _get_tool(mcp_app, "monitor_test_progress")
    with patch("crucible_mcp.tools.client.get_run_status", new_callable=AsyncMock) as mock:
        mock.side_effect = CrucibleError(500, "Internal error")
        result = await fn("r1")
    assert "error" in result


# ---------------------------------------------------------------------------
# emergency_stop
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_emergency_stop_sends_sigterm(mcp_app):
    fn = _get_tool(mcp_app, "emergency_stop")
    with patch("crucible_mcp.tools.client.stop_run", new_callable=AsyncMock) as mock:
        mock.return_value = {"run_id": "r1", "action": "sigterm_sent"}
        result = await fn("r1")
    assert result["action"] == "sigterm_sent"


@pytest.mark.asyncio
async def test_emergency_stop_already_stopped(mcp_app):
    fn = _get_tool(mcp_app, "emergency_stop")
    with patch("crucible_mcp.tools.client.stop_run", new_callable=AsyncMock) as mock:
        mock.return_value = {"run_id": "r1", "action": "already_stopped"}
        result = await fn("r1")
    assert result["action"] == "already_stopped"


@pytest.mark.asyncio
async def test_emergency_stop_catches_error(mcp_app):
    fn = _get_tool(mcp_app, "emergency_stop")
    with patch("crucible_mcp.tools.client.stop_run", new_callable=AsyncMock) as mock:
        mock.side_effect = CrucibleError(500, "Internal error")
        result = await fn("r1")
    assert "error" in result


# ---------------------------------------------------------------------------
# upload_workload_sql
# ---------------------------------------------------------------------------

_VALID_WORKLOAD = """\
-- @type: sql
-- @name: TopProducts
SELECT product_id, SUM(revenue) FROM orders GROUP BY 1 ORDER BY 2 DESC LIMIT 10;

-- @name: DailyUsers
SELECT DATE(created_at), COUNT(DISTINCT user_id) FROM events GROUP BY 1;
"""


@pytest.mark.asyncio
async def test_upload_workload_succeeds(mcp_app):
    fn = _get_tool(mcp_app, "upload_workload_sql")
    with patch("crucible_mcp.tools.client.upload_workload", new_callable=AsyncMock) as mock:
        mock.return_value = {"workload_id": "wl-1", "s3_key": "workloads/wl-1.sql"}
        result = await fn("wl-1", _VALID_WORKLOAD)
    assert result["success"] is True
    assert result["workload_id"] == "wl-1"
    assert result["s3_key"] == "workloads/wl-1.sql"


@pytest.mark.asyncio
async def test_upload_workload_invalid_content_returns_errors(mcp_app):
    fn = _get_tool(mcp_app, "upload_workload_sql")
    bad_content = "-- @name: Q1\nSELECT 1;"  # missing @type header
    result = await fn("wl-1", bad_content)
    assert result["success"] is False
    assert "errors" in result
    assert len(result["errors"]) > 0


@pytest.mark.asyncio
async def test_upload_workload_does_not_call_api_on_invalid(mcp_app):
    fn = _get_tool(mcp_app, "upload_workload_sql")
    with patch("crucible_mcp.tools.client.upload_workload", new_callable=AsyncMock) as mock:
        await fn("wl-1", "bad content no type header")
    mock.assert_not_called()


@pytest.mark.asyncio
async def test_upload_workload_catches_crucible_error(mcp_app):
    fn = _get_tool(mcp_app, "upload_workload_sql")
    with patch("crucible_mcp.tools.client.upload_workload", new_callable=AsyncMock) as mock:
        mock.side_effect = CrucibleError(500, "S3 unavailable")
        result = await fn("wl-1", _VALID_WORKLOAD)
    assert result["success"] is False
    assert "S3 unavailable" in result["error"]


# ---------------------------------------------------------------------------
# submit_test_run — cluster_spec parameter
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_submit_passes_cluster_spec_to_client(mcp_app):
    fn = _get_tool(mcp_app, "submit_test_run")
    spec = {"type": "doris", "backend_node": {"replica": 3}}
    with patch("crucible_mcp.tools.client.submit_run", new_callable=AsyncMock) as mock:
        mock.return_value = {"run_id": "r1", "plan_key": "plans/smoke", "strategy": "intra_node"}
        await fn(_VALID_PLAN_YAML, "smoke", "", spec)
    # cluster_spec should be forwarded as the 4th positional arg
    mock.assert_awaited_once()
    call_args = mock.call_args[0]
    assert call_args[3] == spec


@pytest.mark.asyncio
async def test_submit_rejects_invalid_cluster_spec(mcp_app):
    fn = _get_tool(mcp_app, "submit_test_run")
    spec = {"type": "unknown_db"}
    result = await fn(_VALID_PLAN_YAML, "smoke", "", spec)
    assert result["success"] is False
    assert "errors" in result


@pytest.mark.asyncio
async def test_submit_rejects_cluster_spec_with_invalid_replica(mcp_app):
    fn = _get_tool(mcp_app, "submit_test_run")
    spec = {"type": "doris", "backend_node": {"replica": 0}}
    result = await fn(_VALID_PLAN_YAML, "smoke", "", spec)
    assert result["success"] is False
    assert "errors" in result


# ---------------------------------------------------------------------------
# trigger_run_by_plan
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_trigger_run_by_plan_succeeds(mcp_app):
    fn = _get_tool(mcp_app, "trigger_run_by_plan")
    with patch("crucible_mcp.tools.client.trigger_run", new_callable=AsyncMock) as mock:
        mock.return_value = {"run_id": "r1", "plan_key": "plans/bench", "strategy": "intra_node"}
        result = await fn("bench")
    assert result["success"] is True
    assert result["run_id"] == "r1"


@pytest.mark.asyncio
async def test_trigger_run_by_plan_with_cluster_spec(mcp_app):
    fn = _get_tool(mcp_app, "trigger_run_by_plan")
    spec = {"type": "doris", "backend_node": {"replica": 5}}
    with patch("crucible_mcp.tools.client.trigger_run", new_callable=AsyncMock) as mock:
        mock.return_value = {"run_id": "r2", "plan_key": "plans/bench", "strategy": "intra_node"}
        result = await fn("bench", "5-be-run", spec)
    assert result["success"] is True
    mock.assert_awaited_once_with("bench", "5-be-run", spec, None)


@pytest.mark.asyncio
async def test_trigger_run_by_plan_rejects_invalid_cluster_spec(mcp_app):
    fn = _get_tool(mcp_app, "trigger_run_by_plan")
    spec = {"type": "unknown_db"}
    result = await fn("bench", "", spec)
    assert result["success"] is False
    assert "errors" in result


@pytest.mark.asyncio
async def test_trigger_run_by_plan_catches_error(mcp_app):
    fn = _get_tool(mcp_app, "trigger_run_by_plan")
    with patch("crucible_mcp.tools.client.trigger_run", new_callable=AsyncMock) as mock:
        mock.side_effect = CrucibleError(404, "Plan not found")
        result = await fn("nonexistent")
    assert result["success"] is False
    assert "Plan not found" in result["error"]


# ---------------------------------------------------------------------------
# cluster_settings parameter
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_submit_passes_cluster_settings_to_client(mcp_app):
    fn = _get_tool(mcp_app, "submit_test_run")
    with patch("crucible_mcp.tools.client.submit_run", new_callable=AsyncMock) as mock:
        mock.return_value = {"run_id": "r1", "plan_key": "plans/smoke", "strategy": "intra_node"}
        await fn(_VALID_PLAN_YAML, "smoke", "", None, "concurrency=20")
    call_args = mock.call_args[0]
    assert call_args[4] == "concurrency=20"


@pytest.mark.asyncio
async def test_trigger_passes_cluster_settings_to_client(mcp_app):
    fn = _get_tool(mcp_app, "trigger_run_by_plan")
    with patch("crucible_mcp.tools.client.trigger_run", new_callable=AsyncMock) as mock:
        mock.return_value = {"run_id": "r1", "plan_key": "plans/bench", "strategy": "intra_node"}
        await fn("bench", "", None, "concurrency=50")
    mock.assert_awaited_once_with("bench", "", None, "concurrency=50")


# ---------------------------------------------------------------------------
# list_test_runs
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_test_runs_no_filter(mcp_app):
    fn = _get_tool(mcp_app, "list_test_runs")
    with patch("crucible_mcp.tools.client.list_runs", new_callable=AsyncMock) as mock:
        mock.return_value = {"runs": [{"run_id": "r1"}]}
        result = await fn("")
    assert result["runs"][0]["run_id"] == "r1"
    mock.assert_awaited_once_with(None)


@pytest.mark.asyncio
async def test_list_test_runs_with_label(mcp_app):
    fn = _get_tool(mcp_app, "list_test_runs")
    with patch("crucible_mcp.tools.client.list_runs", new_callable=AsyncMock) as mock:
        mock.return_value = {"runs": [{"run_id": "r1", "run_label": "bench"}]}
        result = await fn("bench")
    assert len(result["runs"]) == 1
    mock.assert_awaited_once_with("bench")


@pytest.mark.asyncio
async def test_list_test_runs_catches_error(mcp_app):
    fn = _get_tool(mcp_app, "list_test_runs")
    with patch("crucible_mcp.tools.client.list_runs", new_callable=AsyncMock) as mock:
        mock.side_effect = CrucibleError(500, "Internal error")
        result = await fn("")
    assert "error" in result
