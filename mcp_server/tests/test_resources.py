"""Tests for crucible_mcp.resources — all 3 MCP resources."""

import pytest
from unittest.mock import AsyncMock, patch

from mcp.server.fastmcp import FastMCP

from crucible_mcp.resources import register_resources


@pytest.fixture
def mcp_app():
    app = FastMCP("test")
    register_resources(app)
    return app


def _get_resource_fn(mcp_app: FastMCP, uri: str):
    """Retrieve a registered resource or template function by URI."""
    rm = mcp_app._resource_manager
    # Static resources
    for resource in rm._resources.values():
        if str(resource.uri) == uri:
            return resource.fn
    # Parameterized templates
    for tmpl in rm._templates.values():
        if str(tmpl.uri_template) == uri:
            return tmpl.fn
    raise KeyError(f"Resource {uri!r} not found")


# ---------------------------------------------------------------------------
# crucible://fixtures/registry
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fixture_registry_no_fixtures(mcp_app):
    fn = _get_resource_fn(mcp_app, "crucible://fixtures/registry")
    with patch("crucible_mcp.resources.client.list_fixtures", new_callable=AsyncMock) as mock:
        mock.return_value = {"fixture_ids": []}
        result = await fn()
    assert result == "No fixtures registered."


@pytest.mark.asyncio
async def test_fixture_registry_formats_output(mcp_app):
    fn = _get_resource_fn(mcp_app, "crucible://fixtures/registry")
    with patch("crucible_mcp.resources.client.list_fixtures", new_callable=AsyncMock) as mock_list, \
         patch("crucible_mcp.resources.client.get_fixture_files", new_callable=AsyncMock) as mock_files:
        mock_list.return_value = {"fixture_ids": ["tpch"]}
        mock_files.return_value = {
            "fixture_id": "tpch",
            "files": [
                {"name": "lineitem.parquet", "size": 1048576, "last_modified": "2025-01-01"},
                {"name": "orders.parquet", "size": 524288, "last_modified": "2025-01-01"},
            ],
        }
        result = await fn()
    assert "# Crucible Fixture Registry" in result
    assert "tpch" in result
    assert "lineitem.parquet" in result
    assert "1.0 MB" in result  # 1048576 bytes
    assert "0.5 MB" in result  # 524288 bytes
    assert "2 file(s)" in result


@pytest.mark.asyncio
async def test_fixture_registry_handles_fetch_error(mcp_app):
    fn = _get_resource_fn(mcp_app, "crucible://fixtures/registry")
    with patch("crucible_mcp.resources.client.list_fixtures", new_callable=AsyncMock) as mock_list, \
         patch("crucible_mcp.resources.client.get_fixture_files", new_callable=AsyncMock) as mock_files:
        mock_list.return_value = {"fixture_ids": ["broken"]}
        mock_files.side_effect = RuntimeError("connection refused")
        result = await fn()
    assert "broken" in result
    assert "error fetching details" in result


# ---------------------------------------------------------------------------
# crucible://telemetry/recent-stats
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_recent_stats_no_runs(mcp_app):
    fn = _get_resource_fn(mcp_app, "crucible://telemetry/recent-stats")
    with patch("crucible_mcp.resources.client.list_runs", new_callable=AsyncMock) as mock:
        mock.return_value = {"runs": []}
        result = await fn()
    assert result == "No test runs recorded yet."


@pytest.mark.asyncio
async def test_recent_stats_with_completed_run(mcp_app):
    fn = _get_resource_fn(mcp_app, "crucible://telemetry/recent-stats")
    with patch("crucible_mcp.resources.client.list_runs", new_callable=AsyncMock) as mock:
        mock.return_value = {
            "runs": [
                {
                    "run_id": "abcdef12-3456-7890-abcd-ef1234567890",
                    "run_label": "smoke-test",
                    "sut_type": "doris",
                    "status": "COMPLETED",
                    "scaling_mode": "intra_node",
                    "submitted_at": "2025-01-01T00:00:00",
                    "completed_at": "2025-01-01T00:05:00",
                }
            ]
        }
        result = await fn()
    assert "# Recent Crucible Test Runs" in result
    assert "smoke-test" in result
    assert "COMPLETED" in result
    assert "Completed" in result  # completed_at line present


@pytest.mark.asyncio
async def test_recent_stats_with_pending_run(mcp_app):
    fn = _get_resource_fn(mcp_app, "crucible://telemetry/recent-stats")
    with patch("crucible_mcp.resources.client.list_runs", new_callable=AsyncMock) as mock:
        mock.return_value = {
            "runs": [
                {
                    "run_id": "abcdef12-3456-7890-abcd-ef1234567890",
                    "run_label": "pending-run",
                    "sut_type": "trino",
                    "status": "PENDING",
                    "scaling_mode": "inter_node",
                    "submitted_at": "2025-01-01T00:00:00",
                    "completed_at": None,
                }
            ]
        }
        result = await fn()
    assert "pending-run" in result
    assert "PENDING" in result
    # The Completed line should NOT be in the output for a pending run
    assert "Completed" not in result


# ---------------------------------------------------------------------------
# crucible://logs/{run_id}
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_logs_no_artifacts(mcp_app):
    fn = _get_resource_fn(mcp_app, "crucible://logs/{run_id}")
    with patch("crucible_mcp.resources.client.get_run_artifacts", new_callable=AsyncMock) as mock:
        mock.return_value = {"run_id": "r1", "artifacts": []}
        result = await fn("r1")
    assert "No artifacts found" in result
    assert "r1" in result


@pytest.mark.asyncio
async def test_run_logs_formats_artifacts(mcp_app):
    fn = _get_resource_fn(mcp_app, "crucible://logs/{run_id}")
    with patch("crucible_mcp.resources.client.get_run_artifacts", new_callable=AsyncMock) as mock:
        mock.return_value = {
            "run_id": "r1",
            "artifacts": [
                {"key": "results/r1/k6_raw_0.csv", "size": 10240},
                {"key": "results/r1/k6_raw_1.csv", "size": 20480},
            ],
        }
        result = await fn("r1")
    assert "# Artifacts for Run r1" in result
    assert "k6_raw_0.csv" in result
    assert "10.0 KB" in result
    assert "k6_raw_1.csv" in result
    assert "20.0 KB" in result
