1. Vision & Role
The Crucible MCP Server acts as a semantic orchestration layer for the Data-Testing-as-a-Service platform. It translates natural language intent into structured calls to the Crucible Control Plane REST API. It must support both stdio (for local CLI debugging via Claude Code) and SSE (for cloud-native production access).

2. Technical Stack
• Framework: Python with FastMCP (high-level, ergonomic wrapper).
• Integration: Thin wrapper over the existing FastAPI Control Plane.
• Transport: Dual-mode (stdio/SSE).
• Security: Bearer Token authentication for SSE connections.

3. Tool Definitions (Capabilities)
Claude should implement the following tools within the MCP server:
• list_supported_suts: Returns a list of supported System Under Test types (Doris, Trino, Cassandra).
• get_db_inventory: Lists currently available/leased database instances from the Metadata Store.
• validate_test_plan: Validates a YAML string against the V1 Locked Schema.
• submit_test_run: Submits the plan to the Dispatcher. Returns a run_id.
• monitor_test_progress: Returns real-time status: "Waiting Room," "Executing," or "Completed."
• emergency_stop: Triggers the SIGTERM -> SIGKILL escalation flow to kill worker processes.

4. Resource Definitions (Context)
Resources allow Claude to "read" the state of Crucible:
• crucible://fixtures/registry: Metadata about uploaded datasets in S3/MinIO (size, format, target tables).
• crucible://telemetry/recent-stats: A summary of the last 5 test runs to provide context for baseline comparisons.
• crucible://logs/{run_id}: Streaming logs from the Celery workers during execution.

5. Implementation Requirements
• Async First: All network calls to the Control Plane or Metadata DB must be non-blocking.
• Progress Notifications: Use MCP's notify/progress feature for long-running upload_fixture or waiting_room phases so the client doesn't time out.
• Error Mapping: Map FAILED_INSUFFICIENT_CAPACITY to a "ResourceExhausted" MCP error. Map YAML validation errors to a structured "InvalidParams" error with line numbers.
• Environment Injection: Automatically inject K6_PROMETHEUS_RW_SERVER_URL into test plans if missing.

6. Deployment Strategy
• Local: Executable via python server.py for use with the MCP Inspector.
• K8s/EKS: A dedicated deployment in the Crucible namespace, exposed via a LoadBalancer/Ingress with Nginx buffering disabled (proxy_buffering off;) to support SSE streaming.

7. Starter Implementation (Python/FastMCP)
```python
import os
import httpx from mcp.server.fastmcp
import FastMCP from typing
import Optional, List
import logging

mcp = FastMCP(
    "Crucible",
    description="Orchestration layer for Crucible Data-Testing-as-a-Service"
)

# Configuration
CONTROL_PLANE_URL = os.getenv("CRUCIBLE_API_URL", "http://localhost:8000")
API_TOKEN = os.getenv("CRUCIBLE_API_TOKEN")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("crucible-mcp")

# --- TOOLS (Capabilities) ---

@mcp.tool()
async def list_supported_suts() -> List[str]:
    """Returns supported Systems Under Test (Doris, Trino, Cassandra)."""
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{CONTROL_PLANE_URL}/v1/sut/types")
        return response.json()

@mcp.tool()
async def submit_test_run(plan_yaml: str, label: str = "ai-generated-test") -> str:
    """Submits a YAML test plan to the Dispatcher. Returns run_id."""
    payload = {"plan": plan_yaml, "label": label}
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{CONTROL_PLANE_URL}/v1/test-runs",
            json=payload,
            headers=headers
        )
        data = response.json()
        return f"Successfully submitted. Run ID: {data.get('id')}"

@mcp.tool()
async def monitor_test_progress(run_id: str) -> dict:
    """Checks the status of a specific test run in the Celery queue."""
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{CONTROL_PLANE_URL}/v1/test-runs/{run_id}/status")
            return response.json()

# --- RESOURCES (Context) ---

@mcp.resource("crucible://fixtures/registry")
async def get_fixture_registry() -> str:
    """Returns metadata about uploaded datasets in MinIO/S3."""
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{CONTROL_PLANE_URL}/v1/fixtures")
        fixtures = response.json()
        return "\n".join([f"- {f['name']} ({f['size_gb']}GB) - {f['format']}" for f in fixtures])

if name == "main":
    mcp.run()
```
