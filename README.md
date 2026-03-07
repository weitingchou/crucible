# Crucible

**Data-Testing-as-a-Service (DTaaS)** platform for load testing massive datasets (100GB+) against distributed database systems (Doris, Trino, Cassandra, etc.). Users define tests entirely through YAML configs and SQL files — no custom code required.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│  Client                                                 │
│  • Uploads test plans (YAML) and SQL workload files     │
│  • Uploads fixture datasets directly to S3/MinIO        │
└─────────────────────┬───────────────────────────────────┘
                      │ REST API
┌─────────────────────▼───────────────────────────────────┐
│  Control Plane (FastAPI · port 8000)                    │
│  • Validates test plans · brokers S3 multipart uploads  │
│  • Dispatches Celery tasks based on scaling_mode        │
└─────────────────────┬───────────────────────────────────┘
                      │ RabbitMQ
┌─────────────────────▼───────────────────────────────────┐
│  Execution Worker (Celery)                              │
│  • Lease Manager → Fixture Loader → Taurus → k6 (xk6)  │
│  • Pushes metrics to Prometheus Pushgateway             │
└─────────────────────────────────────────────────────────┘

Infrastructure: RabbitMQ · PostgreSQL · MinIO · Prometheus
```

## Repository Structure

```
crucible/
├── control_plane/          # FastAPI service
│   ├── src/control_plane/
│   │   ├── main.py
│   │   ├── routers/        # fixtures, test_runs
│   │   └── services/       # dispatcher, s3_broker
│   ├── pyproject.toml
│   └── Dockerfile
├── worker/                 # Celery worker
│   ├── src/worker/
│   │   ├── celery_app.py
│   │   ├── tasks/          # master, runner
│   │   ├── fixture_loader/ # zero_download, streaming, standard
│   │   ├── orchestrator/   # taurus
│   │   └── drivers/        # generic_sql_driver.js (k6)
│   ├── pyproject.toml
│   └── Dockerfile
├── lib/                    # Shared library (crucible-lib)
│   ├── src/crucible_lib/
│   │   ├── schemas/        # TestPlan, ExecutionConfig, etc.
│   │   └── lease_manager/
│   └── pyproject.toml
├── infrastructure/
│   ├── docker-compose.yml
│   └── config/
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

Upload it:

```bash
curl -X POST http://localhost:8000/test-plans/upload \
  -F "file=@doris-smoke-test.yaml"
# {"key": "plans/doris-smoke-test"}
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

Pass the S3 key returned in Step 1:

```bash
curl -X POST http://localhost:8000/test-runs/doris-smoke-test
# {"run_id": "...", "strategy": "distributed_vertical"}
```

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
| `DATABASE_URL` | `postgresql+asyncpg://...` | PostgreSQL for lease management |
| `PUSHGATEWAY_URL` | `http://pushgateway:9091` | Prometheus Pushgateway |
| `RUNNER_IP` | `127.0.0.1` | Worker's IP (set by scheduler for inter-node scaling) |

---

## 3. Testing the Project

### Run all tests

```bash
uv run pytest
```

### Run tests for a specific package

```bash
uv run pytest control_plane/
uv run pytest worker/
uv run pytest lib/
```

### Run tests with verbose output

```bash
uv run pytest -v
```

### Run a specific test file or test case

```bash
uv run pytest worker/tests/test_fixture_loader.py
uv run pytest worker/tests/test_fixture_loader.py::test_zero_download_strategy
```

### Type checking

```bash
uv run mypy control_plane/src worker/src lib/src
```

### Linting

```bash
uv run ruff check .
uv run ruff format --check .
```

---

## 4. End-to-End Testing with Apache Doris

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
# Upload the test plan
curl -X POST http://localhost:8000/test-plans/upload \
  -F "file=@tests/plans/tpch_doris.yaml"

# Trigger the test run
curl -X POST http://localhost:8000/test-runs/tpch_doris
```
