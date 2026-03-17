# MCP Server Implementation Plan

This document is the authoritative implementation plan for the Crucible MCP Server.
Reference: `DESIGN_CONTEXT_MCP.md` for requirements.

---

## Gap Analysis

| MCP Capability | Existing Backend | Gap |
|---|---|---|
| `list_supported_suts` | Nothing | Need `GET /v1/sut/types` or hardcode in MCP |
| `get_db_inventory` | Nothing | Need `test_runs` table + `GET /v1/sut/inventory` |
| `validate_test_plan` | `crucible_lib.schemas.test_plan.TestPlan` | **No gap** — validate locally |
| `submit_test_run` | `POST /test-runs/{filename}` (S3 key required) | Need `POST /v1/test-runs` accepting raw YAML |
| `monitor_test_progress` | Nothing — no run state tracking | Need `test_runs` table + `GET /v1/test-runs/{id}/status` |
| `emergency_stop` | `wait_and_teardown()` handles SIGTERM/SIGKILL locally | Need `POST /v1/test-runs/{id}/stop` + Celery `revoke` |
| `crucible://fixtures/registry` | `GET /fixtures` + `GET /fixtures/{id}` | **No gap** — compose from existing endpoints |
| `crucible://telemetry/recent-stats` | Nothing | Need `test_runs` table + `GET /v1/telemetry/recent-stats` |
| `crucible://logs/{run_id}` | Results in S3 at `results/{run_id}/` | Need `GET /v1/test-runs/{id}/artifacts` |

The biggest prerequisite is **run lifecycle tracking** — the system currently fires-and-forgets into Celery with no persistent status record.

---

## Step 1: PostgreSQL — `test_runs` Table

**Files:**
- `infrastructure/config/postgres/init.sql`
- `helm/crucible/templates/configmap-postgres-init.yaml`

Add to both:

```sql
CREATE TABLE IF NOT EXISTS test_runs (
    run_id        TEXT        PRIMARY KEY,
    task_id       TEXT,                           -- Celery AsyncResult ID
    plan_key      TEXT        NOT NULL,           -- S3 key, e.g. "plans/foo.yaml"
    run_label     TEXT        NOT NULL DEFAULT '',
    sut_type      TEXT        NOT NULL DEFAULT '',
    scaling_mode  TEXT        NOT NULL DEFAULT 'intra_node',
    status        TEXT        NOT NULL DEFAULT 'PENDING',
    error_detail  TEXT,
    submitted_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at    TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_test_runs_status ON test_runs(status);
CREATE INDEX IF NOT EXISTS idx_test_runs_submitted_at ON test_runs(submitted_at DESC);
```

Status lifecycle: `PENDING → WAITING_ROOM → EXECUTING → COMPLETED | FAILED | STOPPING`

---

## Step 2: Control Plane — Add Database Layer

**Files:**
- `control_plane/src/control_plane/config.py` — add `database_url` field
- `control_plane/src/control_plane/services/db.py` — **new file**
- `control_plane/src/control_plane/main.py` — add `lifespan` for asyncpg pool
- `control_plane/pyproject.toml` — add `asyncpg` dependency

### `config.py` addition

```python
database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/crucible"
```

### `services/db.py`

An `asyncpg` connection pool managed via FastAPI's `lifespan` context manager. Provides:

```python
async def insert_run(run_id, task_id, plan_key, run_label, sut_type, scaling_mode) -> None
async def get_run(run_id) -> dict | None
async def update_run_status(run_id, status, **fields) -> None
async def list_recent_runs(limit=5) -> list[dict]
async def get_active_runs_by_sut() -> list[dict]
```

### `main.py` lifespan

```python
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init_pool()
    yield
    await db.close_pool()

app = FastAPI(..., lifespan=lifespan)
```

---

## Step 3: Control Plane — Three New Routers

All under `/v1/` prefix to coexist with existing unversioned routes.

**Files:**
- `control_plane/src/control_plane/routers/sut.py` — **new**
- `control_plane/src/control_plane/routers/test_runs_v1.py` — **new**
- `control_plane/src/control_plane/routers/telemetry.py` — **new**
- `control_plane/src/control_plane/main.py` — register routers

### Router: `sut.py`

| Endpoint | Impl |
|---|---|
| `GET /v1/sut/types` | Returns `["doris", "trino", "cassandra"]` — static |
| `GET /v1/sut/inventory` | Queries `test_runs` for active runs (status IN `WAITING_ROOM`, `EXECUTING`), returns which SUT types are in use |

### Router: `test_runs_v1.py`

| Endpoint | Impl |
|---|---|
| `POST /v1/test-runs` | Accepts `{plan_yaml, label}` JSON. Parses YAML → validates via `TestPlan.model_validate()` → saves to S3 via `s3_broker.save_plan()` → inserts `test_runs` row (status=`PENDING`) → dispatches via Celery `send_task` → returns `{run_id, plan_key, strategy}` |
| `GET /v1/test-runs/{run_id}/status` | Queries `test_runs` row. If status is `WAITING_ROOM`, also queries `waiting_room` for `ready_count/target_count` |
| `POST /v1/test-runs/{run_id}/stop` | Sets `status = 'STOPPING'`, calls `celery.control.revoke(task_id, terminate=True, signal='SIGTERM')`. Worker's existing `wait_and_teardown()` handles SIGTERM→SIGKILL for k6 subprocesses. |
| `GET /v1/test-runs/{run_id}/artifacts` | Lists S3 objects under `results/{run_id}/` — returns keys and sizes |

### Router: `telemetry.py`

| Endpoint | Impl |
|---|---|
| `GET /v1/telemetry/recent-stats` | Queries `test_runs ORDER BY submitted_at DESC LIMIT 5` |

---

## Step 4: New Response Models

**File:** `control_plane/src/control_plane/models.py`

Add:

```python
class SutTypesResponse(BaseModel):
    sut_types: list[str]

class SutInstance(BaseModel):
    sut_type: str
    run_id: str
    run_label: str
    status: str

class SutInventoryResponse(BaseModel):
    active: list[SutInstance]

class SubmitRunRequest(BaseModel):
    plan_yaml: str
    label: str = "ai-generated-test"

class SubmitRunResponse(BaseModel):
    run_id: str
    plan_key: str
    strategy: str

class WaitingRoomInfo(BaseModel):
    ready_count: int
    target_count: int

class RunStatusResponse(BaseModel):
    run_id: str
    status: str
    run_label: str
    sut_type: str
    scaling_mode: str
    submitted_at: str
    started_at: str | None = None
    completed_at: str | None = None
    error_detail: str | None = None
    waiting_room: WaitingRoomInfo | None = None

class StopRunResponse(BaseModel):
    run_id: str
    action: str   # "sigterm_sent" | "already_stopped" | "not_found"

class ArtifactEntry(BaseModel):
    key: str
    size: int

class ArtifactsResponse(BaseModel):
    run_id: str
    artifacts: list[ArtifactEntry]

class RunSummary(BaseModel):
    run_id: str
    run_label: str
    sut_type: str
    status: str
    scaling_mode: str
    submitted_at: str
    completed_at: str | None = None

class RecentStatsResponse(BaseModel):
    runs: list[RunSummary]
```

---

## Step 5: Worker Status Writes

**Files:**
- `worker/src/worker/tasks/dispatcher.py`
- `worker/src/worker/tasks/executor.py`

Add helper (in both, or factor into a shared module under `worker/src/worker/db.py`):

```python
def _update_run_status(run_id: str, status: str, **kwargs) -> None:
    """Write lifecycle transition to test_runs table."""
    # Uses existing _db_conn() pattern with psycopg2
    # UPDATE test_runs SET status = %s, ... WHERE run_id = %s
```

### Insertion points

| Location | Status | Extra fields |
|---|---|---|
| `dispatcher_task` — intra_node branch, after fixture load | `EXECUTING` | `started_at = now()` |
| `dispatcher_task` — inter_node branch, after `_init_waiting_room()` | `WAITING_ROOM` | — |
| `dispatcher_task` — `_set_start_signal(run_id, "START")` | `EXECUTING` | `started_at = now()` |
| `dispatcher_task` — capacity failure | `FAILED` | `error_detail = "..."` |
| `dispatcher_task` — timeout | `FAILED` | `error_detail = "Worker check-in timeout"` |
| `k6_executor_task` — final return | `COMPLETED` | `completed_at = now()` |

**Important:** The Control Plane inserts the initial `test_runs` row (status=`PENDING`) at dispatch time via `POST /v1/test-runs`. The worker only UPDATEs — never INSERTs. This avoids race conditions.

---

## Step 6: MCP Package Scaffold

**Files:**
- `mcp_server/pyproject.toml` — **new**
- `mcp_server/src/crucible_mcp/__init__.py` — **new**
- `pyproject.toml` — add `"mcp_server"` to workspace members

### `mcp_server/pyproject.toml`

```toml
[project]
name = "crucible-mcp"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "mcp>=1.0",
    "httpx>=0.27",
    "pydantic-settings>=2.6",
    "pyyaml>=6.0",
    "crucible-lib",
]

[tool.uv.sources]
crucible-lib = { workspace = true }

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/crucible_mcp"]
```

Run `uv sync` to verify workspace resolves.

---

## Step 7: MCP Core

**Files:**
- `mcp_server/src/crucible_mcp/config.py` — **new**
- `mcp_server/src/crucible_mcp/client.py` — **new**
- `mcp_server/src/crucible_mcp/errors.py` — **new**

### `config.py`

```python
class Settings(BaseSettings):
    crucible_api_url: str = "http://localhost:8000"
    crucible_api_token: str = ""
    transport: Literal["stdio", "sse"] = "stdio"
    sse_host: str = "0.0.0.0"
    sse_port: int = 8001
    k6_prometheus_rw_url: str = ""  # auto-injected into plans if missing
```

### `client.py`

Single `CrucibleClient` class wrapping `httpx.AsyncClient`.
- Injects `Authorization: Bearer {token}` when token is non-empty.
- Methods map 1:1 to Control Plane endpoints:

```python
async def list_sut_types() -> list[str]
async def get_sut_inventory() -> dict
async def submit_run(plan_yaml: str, label: str) -> dict
async def get_run_status(run_id: str) -> dict
async def stop_run(run_id: str) -> dict
async def list_fixtures() -> dict
async def get_fixture_files(fixture_id: str) -> dict
async def get_recent_stats() -> dict
async def get_run_artifacts(run_id: str) -> dict
```

### `errors.py`

Maps HTTP responses to MCP errors:
- `422` with Pydantic detail → `InvalidParams` with field paths + messages
- `404` → tool returns "not found" message (not a protocol error)
- `503` or body containing `failed_insufficient_capacity` → `ResourceExhausted`
- `5xx` → `InternalError`

---

## Step 8: MCP Tools + Resources

**Files:**
- `mcp_server/src/crucible_mcp/tools.py` — **new**
- `mcp_server/src/crucible_mcp/resources.py` — **new**

### `tools.py` — 6 tools

1. **`list_supported_suts()`** — Calls `client.list_sut_types()`.

2. **`get_db_inventory()`** — Calls `client.get_sut_inventory()`.

3. **`validate_test_plan(plan_yaml: str)`** — **Local only, no HTTP call.** `yaml.safe_load()` → `TestPlan.model_validate()`. Returns `{valid: true}` or `{valid: false, errors: [{path, message}]}`.

4. **`submit_test_run(plan_yaml: str, label: str = "ai-generated-test")`** — Validates locally first. If invalid, returns errors immediately. If valid, auto-injects `K6_PROMETHEUS_RW_SERVER_URL` if `config.k6_prometheus_rw_url` is set. Calls `client.submit_run()`.

5. **`monitor_test_progress(run_id: str)`** — Calls `client.get_run_status()`.

6. **`emergency_stop(run_id: str)`** — Calls `client.stop_run()`.

### `resources.py` — 3 resources

1. **`crucible://fixtures/registry`** — Calls `GET /fixtures` to list IDs, then `GET /fixtures/{id}` for each. Formats as readable text.

2. **`crucible://telemetry/recent-stats`** — Calls `GET /v1/telemetry/recent-stats`. Formats last 5 runs as summary.

3. **`crucible://logs/{run_id}`** — Calls `GET /v1/test-runs/{run_id}/artifacts`. Lists CSV artifact files from S3. (V1 = artifact metadata, not live streaming. Live logs would require Loki — out of scope.)

---

## Step 9: MCP Entrypoint

**File:** `mcp_server/src/crucible_mcp/server.py` — **new**

```python
mcp = FastMCP("Crucible", description="Orchestration layer for Crucible DTaaS")

# Register tools from tools.py
# Register resources from resources.py

if __name__ == "__main__":
    if settings.transport == "sse":
        mcp.run(transport="sse", host=settings.sse_host, port=settings.sse_port)
    else:
        mcp.run(transport="stdio")
```

For SSE mode: add `BearerAuthMiddleware` that rejects requests without a valid token. Only active when `transport=sse` and `crucible_api_token` is non-empty.

Test locally: `python -m crucible_mcp.server` (stdio mode) with MCP Inspector.

---

## Step 10: Helm Chart

**New files:**
- `helm/crucible/templates/deployment-mcp.yaml`
- `helm/crucible/templates/service-mcp.yaml`
- `helm/crucible/templates/ingress-mcp.yaml`

**Modified files:**
- `helm/crucible/templates/secret.yaml` — add `mcp-api-token` key
- `helm/crucible/templates/deployment-control-plane.yaml` — add `DATABASE_URL` env from `postgres-url` secret
- `helm/crucible/values.yaml` — add `mcp:` stanza
- `helm/crucible/values-eks.yaml` — add MCP overrides
- `helm/crucible/templates/_helpers.tpl` — (optional) add `crucible.mcpControlPlaneUrl` helper

### `deployment-mcp.yaml`

Mirrors `deployment-control-plane.yaml` pattern:
- Component label: `mcp-server`
- Container port: 8001
- Env vars:
  - `CRUCIBLE_API_URL` = `http://<fullname>-control-plane:8000`
  - `CRUCIBLE_API_TOKEN` from Secret
  - `TRANSPORT` = `sse`
  - `K6_PROMETHEUS_RW_URL` from `crucible.prometheusRwUrl` helper
- Liveness probe: TCP on 8001 (no /health endpoint in FastMCP by default)

### `ingress-mcp.yaml`

Critical SSE annotations:
```yaml
nginx.ingress.kubernetes.io/proxy-buffering: "off"
nginx.ingress.kubernetes.io/proxy-read-timeout: "3600"
nginx.ingress.kubernetes.io/proxy-send-timeout: "3600"
```

### `values.yaml` addition

```yaml
mcp:
  enabled: true
  image:
    repository: crucible/mcp
    tag: latest
    pullPolicy: IfNotPresent
  replicaCount: 1
  apiToken: ""
  service:
    type: ClusterIP
    port: 8001
  ingress:
    enabled: false
    className: ""
    annotations: {}
    hosts: []
    tls: []
  resources: {}
  nodeSelector: {}
  tolerations: []
  affinity: {}
```

---

## Step 11: Wire `DATABASE_URL` into Control Plane Deployment

**File:** `helm/crucible/templates/deployment-control-plane.yaml`

Add env entry:
```yaml
- name: DATABASE_URL
  valueFrom:
    secretKeyRef:
      name: {{ include "crucible.secretName" . }}
      key: postgres-url
```

The `postgres-url` key already exists in `secret.yaml`.

---

## Dockerfile (`mcp_server/Dockerfile`)

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY . .
RUN pip install uv && uv sync --package crucible-mcp
ENV TRANSPORT=sse
ENV SSE_HOST=0.0.0.0
ENV SSE_PORT=8001
EXPOSE 8001
CMD ["uv", "run", "--package", "crucible-mcp", "python", "-m", "crucible_mcp.server"]
```

---

## File Change Summary

### New files (13)
| File | Description |
|---|---|
| `mcp_server/pyproject.toml` | Package definition |
| `mcp_server/Dockerfile` | Container build |
| `mcp_server/src/crucible_mcp/__init__.py` | Package init |
| `mcp_server/src/crucible_mcp/server.py` | FastMCP entrypoint |
| `mcp_server/src/crucible_mcp/config.py` | Settings |
| `mcp_server/src/crucible_mcp/client.py` | HTTP client wrapper |
| `mcp_server/src/crucible_mcp/errors.py` | Error mapping |
| `mcp_server/src/crucible_mcp/tools.py` | 6 MCP tools |
| `mcp_server/src/crucible_mcp/resources.py` | 3 MCP resources |
| `control_plane/src/control_plane/services/db.py` | asyncpg pool + queries |
| `control_plane/src/control_plane/routers/sut.py` | SUT endpoints |
| `control_plane/src/control_plane/routers/test_runs_v1.py` | V1 test run endpoints |
| `control_plane/src/control_plane/routers/telemetry.py` | Telemetry endpoint |

### Modified files (10)
| File | Change |
|---|---|
| `pyproject.toml` | Add `"mcp_server"` to workspace members |
| `infrastructure/config/postgres/init.sql` | Add `test_runs` table DDL |
| `control_plane/pyproject.toml` | Add `asyncpg` dependency |
| `control_plane/src/control_plane/config.py` | Add `database_url` field |
| `control_plane/src/control_plane/main.py` | Add `lifespan`, register 3 new routers |
| `control_plane/src/control_plane/models.py` | Add v1 response models |
| `worker/src/worker/tasks/dispatcher.py` | Add `_update_run_status()` calls |
| `worker/src/worker/tasks/executor.py` | Add `_update_run_status()` call on completion |
| `helm/crucible/values.yaml` | Add `mcp:` stanza |
| `helm/crucible/templates/secret.yaml` | Add `mcp-api-token` key |
| `helm/crucible/templates/deployment-control-plane.yaml` | Add `DATABASE_URL` env |
| `helm/crucible/templates/configmap-postgres-init.yaml` | Add `test_runs` DDL |

### New Helm templates (3)
| File | Description |
|---|---|
| `helm/crucible/templates/deployment-mcp.yaml` | MCP Deployment |
| `helm/crucible/templates/service-mcp.yaml` | MCP Service |
| `helm/crucible/templates/ingress-mcp.yaml` | MCP Ingress (SSE-friendly) |
