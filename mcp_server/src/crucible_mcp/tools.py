"""MCP tool definitions for Crucible."""

from __future__ import annotations

import yaml
from mcp.server.fastmcp import FastMCP
from pydantic import ValidationError

from crucible_lib.schemas.test_plan import TestPlan

from . import client
from .config import settings
from .errors import CrucibleError


def register_tools(mcp: FastMCP) -> None:

    @mcp.tool()
    async def list_supported_suts() -> list[str]:
        """Returns a list of supported System Under Test engine types (e.g. doris, trino, cassandra)."""
        return await client.list_sut_types()

    @mcp.tool()
    async def get_db_inventory() -> dict:
        """Lists SUT instances currently held by an active test run (WAITING_ROOM or EXECUTING status)."""
        return await client.get_sut_inventory()

    @mcp.tool()
    async def validate_test_plan(plan_yaml: str) -> dict:
        """Validates a YAML string against the Crucible V1 test plan schema.

        Returns {"valid": true} on success, or {"valid": false, "errors": [...]} with
        field paths and messages on failure. Does not make any network calls.
        """
        try:
            raw = yaml.safe_load(plan_yaml)
        except yaml.YAMLError as exc:
            return {"valid": False, "errors": [{"path": "<yaml>", "message": str(exc)}]}

        try:
            TestPlan.model_validate(raw)
        except ValidationError as exc:
            errors = [
                {
                    "path": " -> ".join(str(p) for p in e["loc"]),
                    "message": e["msg"],
                }
                for e in exc.errors()
            ]
            return {"valid": False, "errors": errors}

        return {"valid": True}

    @mcp.tool()
    async def submit_test_run(plan_yaml: str, label: str = "ai-generated-test") -> dict:
        """Validates and submits a YAML test plan to the Crucible dispatcher.

        Validates the plan locally first. If valid, automatically injects
        K6_PROMETHEUS_RW_SERVER_URL into the plan environment if configured.
        Returns the run_id on success.
        """
        # Validate locally first
        validation = await validate_test_plan(plan_yaml)
        if not validation["valid"]:
            return {"success": False, "errors": validation["errors"]}

        # Auto-inject K6_PROMETHEUS_RW_SERVER_URL if configured and plan lacks it
        if settings.k6_prometheus_rw_url:
            try:
                raw = yaml.safe_load(plan_yaml)
                if not raw.get("k6_prometheus_rw_server_url"):
                    raw["k6_prometheus_rw_server_url"] = settings.k6_prometheus_rw_url
                    plan_yaml = yaml.dump(raw, default_flow_style=False)
            except Exception:
                pass  # best-effort injection

        try:
            result = await client.submit_run(plan_yaml, label)
        except CrucibleError as exc:
            return {"success": False, "error": exc.detail}
        return {"success": True, **result}

    @mcp.tool()
    async def monitor_test_progress(run_id: str) -> dict:
        """Returns the real-time status of a test run: PENDING, WAITING_ROOM, EXECUTING, COMPLETED, or FAILED."""
        try:
            return await client.get_run_status(run_id)
        except CrucibleError as exc:
            if exc.status_code == 404:
                return {"error": f"Run '{run_id}' not found."}
            return {"error": exc.detail}

    @mcp.tool()
    async def emergency_stop(run_id: str) -> dict:
        """Triggers the SIGTERM → SIGKILL escalation flow to stop a running test.

        Sends SIGTERM to the Celery worker task. The worker's teardown path
        will escalate to SIGKILL if k6 subprocesses don't exit within the grace period.
        """
        try:
            return await client.stop_run(run_id)
        except CrucibleError as exc:
            return {"error": exc.detail}
