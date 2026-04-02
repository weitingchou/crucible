# Crucible

**Data-Testing-as-a-Service (DTaaS)** platform for load testing massive datasets (100GB+) against distributed database systems (Doris, Trino, Cassandra, etc.). Users define tests entirely through YAML configs and SQL files — no custom code required.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Repository Structure](#repository-structure)
- [1. Building the Project](#1-building-the-project)
  - [Prerequisites](#prerequisites)
  - [Install Python dependencies](#install-python-dependencies)
  - [Build Docker images](#build-docker-images)
- [2. Running the Project](#2-running-the-project)
  - [Start all services with Docker Compose](#start-all-services-with-docker-compose)
  - [Verify the stack is healthy](#verify-the-stack-is-healthy)
  - [Quick start: run a test](#quick-start-run-a-test)
  - [SQL workload file format](#sql-workload-file-format)
  - [Environment variables](#environment-variables)
- [3. MCP Server (AI Agent Interface)](#3-mcp-server-ai-agent-interface)
  - [Tools](#tools)
  - [Resources](#resources)
  - [Run locally (stdio mode)](#run-locally-stdio-mode)
  - [Run as SSE server](#run-as-sse-server)
  - [Connect from Claude Desktop](#connect-from-claude-desktop)
  - [Configuration](#configuration)
- [4. Testing the Project](#4-testing-the-project)
- [5. End-to-End Testing with Apache Doris](#5-end-to-end-testing-with-apache-doris)

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│  AI Agent / Client                                      │
│  • Uploads test plans (YAML) and SQL workload files     │
│  • Uploads fixture datasets directly to S3/MinIO        │
└──────────┬──────────────────────────────────────────────┘
           │ MCP (stdio / SSE)
┌──────────▼──────────────────────────────────────────────┐
│  MCP Server (FastMCP · port 8001)                       │
│  • 8 tools: validate, upload plan, submit, monitor, etc │
│  • 3 resources: fixtures, telemetry, logs               │
│  • Bearer auth middleware (SSE mode)                    │
└──────────┬──────────────────────────────────────────────┘
           │ REST API
┌──────────▼──────────────────────────────────────────────┐
│  Control Plane (FastAPI · port 8000)                    │
│  • Validates test plans · brokers S3 multipart uploads  │
│  • V1 API: run lifecycle tracking in PostgreSQL         │
│  • Dispatches Celery tasks based on scaling_mode        │
└──────────┬──────────────────────────────────────────────┘
           │ RabbitMQ
┌──────────▼──────────────────────────────────────────────┐
│  Execution Worker (Celery)                              │
│  • Lease Manager → Fixture Loader → Driver Mgr → k6    │
│  • Pushes metrics to Prometheus remote-write            │
│  • Writes lifecycle status to PostgreSQL                │
└─────────────────────────────────────────────────────────┘

Infrastructure: RabbitMQ · PostgreSQL · MinIO · Prometheus
```

## Repository Structure

```
crucible/
├── control_plane/          # FastAPI service
│   ├── src/control_plane/
│   │   ├── main.py
│   │   ├── routers/        # test_plans, test_runs, test_runs_v1, fixtures, sut, telemetry
│   │   └── services/       # dispatcher, s3_broker, db (asyncpg)
│   ├── tests/
│   ├── pyproject.toml
│   └── Dockerfile
├── mcp_server/             # MCP server (FastMCP)
│   ├── src/crucible_mcp/
│   │   ├── server.py       # Entrypoint (stdio / SSE)
│   │   ├── tools.py        # 8 MCP tools
│   │   ├── resources.py    # 3 MCP resources
│   │   ├── client.py       # HTTP client for Control Plane
│   │   ├── errors.py       # HTTP → MCP error mapping
│   │   └── config.py       # Settings (pydantic-settings)
│   ├── tests/
│   ├── pyproject.toml
│   └── Dockerfile
├── worker/                 # Celery worker
│   ├── src/worker/
│   │   ├── celery_app.py
│   │   ├── db.py           # Shared PostgreSQL helpers
│   │   ├── tasks/          # dispatcher, executor
│   │   ├── fixture_loader/ # zero_download, streaming, standard
│   │   └── drivers/        # generic_sql_driver.js (k6)
│   ├── tests/
│   ├── pyproject.toml
│   └── Dockerfile
├── lib/                    # Shared library (crucible-lib)
│   ├── src/crucible_lib/
│   │   ├── schemas/        # TestPlan, ExecutionConfig, etc.
│   │   └── lease_manager/
│   └── pyproject.toml
├── helm/crucible/          # Helm chart (EKS deployment)
├── infrastructure/
│   ├── docker-compose.yml
│   └── config/
│       ├── postgres/       # init.sql (waiting_room, test_runs)
│       ├── prometheus.yml
│       └── rabbitmq/
└── pyproject.toml          # uv workspace root
```

---

## 1. Building the Project

### Prerequisites

| Tool | Version | Purpose |
|---|---|---|
| [uv](https://docs.astral.sh/uv/) | latest | Python package manager |
| [Docker](https://www.docker.com/) | 24+ | Container runtime |
| [Docker Compose](https://docs.docker.com/compose/) | v2+ | Multi-service orchestration |
| Python | 3.12 | Runtime (managed by uv) |

### Install Python dependencies

This is a `uv` workspace. Install all packages and their dependencies from the repo root:

```bash
uv sync
```

To install a specific package only (e.g. for CI):

```bash
uv sync --package crucible-control-plane --no-dev
uv sync --package crucible-worker --no-dev
uv sync --package crucible-mcp --no-dev
```

### Build Docker images

Build service images via Docker Compose (the build context is the repo root, as required by the Dockerfiles):

```bash
# Build all images
docker compose -f infrastructure/docker-compose.yml build

# Build a specific service image
docker compose -f infrastructure/docker-compose.yml build control_plane
docker compose -f infrastructure/docker-compose.yml build worker
```

> **Note:** The worker Dockerfile performs a multi-stage build. Stage 1 compiles a custom `k6` binary with the `xk6-sql` and `xk6-sql-driver-mysql` extensions using Go 1.22. This step requires internet access on first build and may take a few minutes.

---

## 2. Running the Project

### Start all services with Docker Compose

```bash
cd infrastructure
docker compose up
```

This starts the full stack:

| Service | URL / Port | Purpose |
|---|---|---|
| Control Plane API | http://localhost:8000 | REST API |
| API Docs (Swagger) | http://localhost:8000/docs | Interactive API docs |
| MCP Server | stdio (default) or http://localhost:8001 (SSE) | AI agent interface |
| RabbitMQ Management | http://localhost:15672 | Queue monitoring (guest/guest) |
| MinIO Console | http://localhost:9001 | S3-compatible object store UI (minioadmin/minioadmin) |
| MinIO API | http://localhost:9000 | S3 endpoint for boto3/clients |
| Prometheus | http://localhost:9090 | Metrics scraping UI |
| Pushgateway | http://localhost:9091 | Worker metrics ingestion |
| PostgreSQL | localhost:5432 | Metadata / lease store (postgres/postgres) |

To run in detached mode:

```bash
docker compose up -d
```

To stop and remove containers:

```bash
docker compose down
```

### Verify the stack is healthy

```bash
curl http://localhost:8000/health
# {"status": "ok"}
```

### Quick start: run a test

**Step 1 — Upload a test plan (YAML)**

Create a `doris-smoke-test.yaml` file:

```yaml
test_metadata:
  run_label: "doris-smoke-test"

test_environment:
  env_type: long-lived
  target_db: benchmark_db
  component_spec:
    type: doris
    cluster_info:
      host: localhost:9060
      username: root
      password: ""
  fixtures:
    - fixture_id: my-fixture-001
      table: sales

execution:
  executor: k6
  scaling_mode: intra_node
  concurrency: 50
  ramp_up: 30s
  hold_for: 5m
  workload:
    - workload_id: my-workload-001
```

Upload it (POST creates, fails with 409 if the plan already exists):

```bash
curl -X POST http://localhost:8000/test-plans/upload \
  -F "file=@doris-smoke-test.yaml"
# {"key": "plans/doris-smoke-test"}
```

To replace an existing plan, use PUT:

```bash
curl -X PUT http://localhost:8000/test-plans/doris-smoke-test/upload \
  -F "file=@doris-smoke-test.yaml"
```

**Step 2 — Upload a fixture dataset (for files > 5 GB, use multipart)**

```bash
# Initiate a multipart upload
curl -X POST http://localhost:8000/fixtures/my-fixture-001/sales.parquet/multipart/init

# Get a pre-signed URL for each part
curl http://localhost:8000/fixtures/my-fixture-001/sales.parquet/multipart/{upload_id}/part/1

# Upload the part directly to MinIO/S3 (bypasses the API server)
curl -X PUT "<presigned_url>" --upload-file sales_part1.parquet
```

**Step 3 — Trigger a test run**

Trigger by plan name (the plan must already be uploaded):

```bash
curl -X POST http://localhost:8000/v1/test-runs/doris-smoke-test
# {"run_id": "doris-smoke-test_20260325-1207_a1b2c3d4", "plan_key": "plans/doris-smoke-test", "strategy": "intra_node"}
```

Or use the combined upload + run shortcut (upserts the plan, then dispatches):

```bash
curl -X POST http://localhost:8000/v1/test-runs \
  -H "Content-Type: application/json" \
  -d '{"plan_yaml": "...", "plan_name": "doris-smoke-test"}'
```

You can trigger multiple runs against the same plan — each gets a unique `run_id` in the format `{plan_name}_{YYYYMMDD-HHmm}_{8-hex}`.

### Chaos / resilience testing

Test plans can include a `chaos_spec` section to inject infrastructure faults (pod kills, network delays, I/O errors) during the load test using [Chaos Mesh](https://chaos-mesh.org/) (K8s) or chaosd (EC2). Chaos events are recorded with inject/recover timestamps and included in the test results alongside k6 metrics, so you can correlate fault timing with performance impact in a single `GET /v1/test-runs/{run_id}/results` call.

### SQL workload file format

SQL files use `-- @name:` annotations to label individual queries. The k6 driver parses these and tracks per-query latency as separate Prometheus metrics.

```sql
-- @name: GetTopSellers
SELECT seller_id, SUM(revenue) AS total
FROM sales
WHERE region = 'US'
GROUP BY seller_id
ORDER BY total DESC
LIMIT 10;

-- @name: GetDailyVolume
SELECT DATE(order_date) AS day, COUNT(*) AS orders
FROM sales
GROUP BY day
ORDER BY day DESC;
```

### Environment variables

The worker injects DB credentials and infrastructure endpoints automatically at runtime. The following variables are configured via `docker-compose.yml` and are not exposed to end users:

| Variable | Default | Description |
|---|---|---|
| `CELERY_BROKER_URL` | `amqp://guest:guest@rabbitmq:5672//` | RabbitMQ connection |
| `S3_BUCKET` | `project-crucible-storage` | MinIO/S3 bucket name |
| `AWS_ENDPOINT_URL` | `http://minio:9000` | MinIO endpoint |
| `DATABASE_URL` | `postgresql+asyncpg://...` | PostgreSQL for lease management and run lifecycle |
| `PUSHGATEWAY_URL` | `http://pushgateway:9091` | Prometheus Pushgateway |
| `RUNNER_IP` | `127.0.0.1` | Worker's IP (set by scheduler for inter-node scaling) |

---

## 3. MCP Server (AI Agent Interface)

The MCP server exposes Crucible's capabilities to AI agents via the [Model Context Protocol](https://modelcontextprotocol.io). It supports stdio (local) and SSE (networked) transports.

### Tools

| Tool | Description |
|---|---|
| `list_supported_suts` | Returns supported database engine types (doris, trino, cassandra) |
| `get_db_inventory` | Lists SUT instances held by active test runs |
| `validate_test_plan` | Validates a YAML test plan locally (no network call) |
| `upload_test_plan` | Validates and uploads a test plan without starting a run (PUT upsert) |
| `submit_test_run` | Validates, auto-injects Prometheus URL, and submits a test run |
| `trigger_run_by_plan` | Triggers a new run using an existing plan (with optional cluster_spec/settings) |
| `list_test_runs` | Lists test runs, optionally filtered by run_label |
| `monitor_test_progress` | Returns real-time lifecycle status of a test run |
| `get_test_results` | Returns k6 metrics, observability data, and chaos event log for a completed run |
| `emergency_stop` | Sends SIGTERM → SIGKILL escalation to stop a running test |
| `upload_workload_sql` | Validates and uploads an annotated SQL/CQL workload file |

### Resources

| URI | Description |
|---|---|
| `crucible://fixtures/registry` | Metadata about uploaded datasets in S3/MinIO |
| `crucible://logs/{run_id}` | S3 artifact files produced by a test run |

### Run locally (stdio mode)

```bash
uv run python -m crucible_mcp
```

### Run as SSE server

```bash
TRANSPORT=sse CRUCIBLE_API_URL=http://localhost:8000 uv run python -m crucible_mcp
```

### Connect from Claude Desktop

Add to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "crucible": {
      "command": "uv",
      "args": ["run", "--directory", "/path/to/crucible", "python", "-m", "crucible_mcp"],
      "env": {
        "CRUCIBLE_API_URL": "http://localhost:8000"
      }
    }
  }
}
```

### Configuration

| Variable | Default | Description |
|---|---|---|
| `CRUCIBLE_API_URL` | `http://localhost:8000` | Control Plane base URL |
| `CRUCIBLE_API_TOKEN` | (empty) | Bearer token for auth (also enables SSE auth middleware) |
| `TRANSPORT` | `stdio` | Transport mode: `stdio` or `sse` |
| `SSE_HOST` | `0.0.0.0` | SSE server bind address |
| `SSE_PORT` | `8001` | SSE server port |
| `K6_PROMETHEUS_RW_URL` | (empty) | Auto-injected into submitted test plans if set |

---

## 4. Testing the Project

### Unit tests

Run all unit tests (no infrastructure required):

```bash
uv run pytest control_plane/          # Control plane (original + V1 routers)
uv run pytest mcp_server/tests/       # MCP server (tools, resources, client, errors, auth)
cd worker && uv run pytest            # Worker (shared DB module)
uv run pytest lib/                    # Shared library (schemas, utilities)
```

Run a specific test file or test case:

```bash
uv run pytest mcp_server/tests/test_tools.py
uv run pytest mcp_server/tests/test_tools.py::test_validate_valid_plan
```

### End-to-end tests

E2e tests hit the real Control Plane API, PostgreSQL, and MinIO with no mocks. A setup script handles starting Docker services, applying the DB schema, and launching the control plane.

```bash
# Start the e2e environment (Docker + control plane)
./tests/start_e2e_env.sh

# Run the tests
uv run python -m pytest tests/test_e2e_api.py -v

# Tear down everything
./tests/start_e2e_env.sh --stop
```

The tests auto-skip if the infrastructure is not running, so `uv run pytest tests/` is always safe to run.

### Type checking

```bash
uv run mypy control_plane/src worker/src lib/src mcp_server/src
```

### Linting

```bash
uv run ruff check .
uv run ruff format --check .
```

---

## 5. End-to-End Testing with Apache Doris

The Doris FE, BE, and init services are gated behind the `doris` Compose profile so they don't start during normal development.

### Start the full stack including Doris

```bash
docker compose -f infrastructure/docker-compose.yml --profile doris up
```

To start Doris alone (without the Crucible services):

```bash
docker compose -f infrastructure/docker-compose.yml --profile doris up doris-fe doris-be doris-init
```

### Verify the Doris cluster is healthy

Wait ~30 seconds for FE to initialise, then confirm the BE is registered and alive:

```bash
mysql -h 127.0.0.1 -P 9030 -u root -e "SHOW BACKENDS\G"
# Expect: Alive: true
```

### Apple Silicon (M-series) note

The default images are `x86_64` and run under Rosetta emulation. If startup is slow or fails, switch to the native ARM images in `docker-compose.yml`:

```
apache/doris:2.1.7-fe-arm64
apache/doris:2.1.7-be-arm64
```

### Resource requirements

Allocate at least **8 GB RAM** to Docker Desktop (Settings → Resources → Memory) before starting Doris. The BE will OOM-kill with less.

### Run the TPC-H e2e test plan

```bash
# Upload the test plan (use PUT to create or replace)
curl -X PUT http://localhost:8000/test-plans/tpch-doris/upload \
  -F "file=@tests/plans/tpch_doris.yaml"

# Trigger a test run against the uploaded plan
curl -X POST http://localhost:8000/v1/test-runs/tpch-doris
```
