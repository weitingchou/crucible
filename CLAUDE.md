# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## AWS CLI

Always use `--profile claude-bot` with all `aws` CLI commands.

## Project Overview

**Crucible** is a Data-Testing-as-a-Service (DTaaS) platform for load testing massive datasets (100GB+) against distributed database systems (Doris, Trino, Cassandra, etc.). Users define tests entirely through YAML configs and SQL files — no custom code required.

The project is currently in the **design/architecture phase**. The authoritative reference is `DESIGN_CONTEXT.md`.

## Architecture

### Component Stack

| Component | Technology | Role |
|---|---|---|
| Control Plane | Python (FastAPI) | REST API: test plan management, asset brokering, job dispatch |
| Job Queue | RabbitMQ | Async task distribution via Celery |
| Artifact Storage | S3 / MinIO | Stores test plans, fixture datasets, results |
| Metadata Store | PostgreSQL | Test metadata, resource leasing/locking |
| Execution Worker | Python (Celery) + Docker | Autonomous test lifecycle runner |
| Load Driver | k6 (Go) + custom `xk6-sql` binary | High-concurrency SQL execution via Goroutines |
| Telemetry | Prometheus (remote-write) | Real-time metrics streamed directly from k6 |

### Execution Worker Sub-components

The Celery worker contains four sub-components that run in sequence:

1. **Lease Manager** — Acquires atomic locks on shared SUTs (long-lived) or provisions ephemeral clusters (disposable)
2. **Fixture Loader** — Hydrates the SUT using one of three strategies (see below)
3. **Driver Manager (`driver_manager/k6_manager.py`)** — Spawns and supervises k6 OS processes directly via Python `subprocess`; sends SIGTERM then SIGKILL on teardown
4. **Generic Workload Driver** — Custom `xk6-sql` binary that parses annotated SQL files and executes randomized queries as concurrent Goroutines

### Key Design Decisions

**Fixture loading uses a Strategy Pattern based on database type:**
- **Zero-Download (MPP):** Issues SQL commands to pull Parquet files from S3 directly into Doris/Trino via S3 TVF — no data touches the worker disk
- **Streaming (NoSQL):** Pipes CSV rows from S3 stream into Cassandra via async driver — near-zero worker memory footprint
- **Standard:** Traditional ingestion path

**Scaling uses two modes determined from the YAML `scaling_mode` field:**
- **Intra-node (vertical):** `dispatcher_task` fans out a single `k6_executor_task` instructed to spawn N local k6 processes
- **Inter-node (horizontal):** `dispatcher_task` fans out N `k6_executor_task`s across the fleet; workers check into a PostgreSQL waiting room and start simultaneously on a global START signal

**Asset upload flow for 100GB+ datasets:**
- Client calls `/fixtures/{id}/{file}/multipart/init` → gets `upload_id`
- Client calls `/fixtures/{id}/{file}/multipart/{upload_id}/part/{n}` → gets pre-signed S3 URL
- Client uploads directly to S3 (bypasses API server entirely)

**SQL workload files use `-- @name: QueryName` annotations** to allow the k6 driver to parse, name, and track per-query latency as individual Prometheus `Trend` metrics.

**Environment injection:** The worker silently injects DB credentials, endpoints, and S3 URIs at runtime. End users never see infrastructure details.

## Local Development Environment Setup

Run these steps on any new machine to get a fully working local environment.

### Prerequisites

- Docker + Docker Compose v2 (`docker compose`)
- `curl` (for installing uv)

### 1. Install uv and Python 3.12

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"   # or add permanently to ~/.zshrc / ~/.bashrc
uv python install 3.12
```

### 2. Install Python dependencies

From the repo root:

```bash
uv sync
```

This creates `.venv` with all workspace packages (`crucible-lib`, `crucible-control-plane`, `crucible-worker`) and their dependencies installed under Python 3.12.

Activate when needed:
```bash
source .venv/bin/activate
```

### 3. Start infrastructure services

```bash
cd infrastructure
docker compose up -d rabbitmq postgres minio pushgateway prometheus
```

Wait for health checks to pass (postgres and rabbitmq report `healthy`):
```bash
docker compose ps
```

### Service Endpoints (local)

| Service | URL | Credentials |
|---|---|---|
| Control Plane API | http://localhost:8000 | — |
| RabbitMQ UI | http://localhost:15672 | guest / guest |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| MinIO S3 API | http://localhost:9000 | minioadmin / minioadmin |
| PostgreSQL | localhost:5432 | postgres / postgres, db: crucible |
| Prometheus | http://localhost:9090 | — |
| Pushgateway | http://localhost:9091 | — |

### Optional: Start Doris (e2e testing only)

```bash
docker compose --profile doris up -d
```

### Notes

- The `infrastructure/` directory is the Docker Compose root — always run `docker compose` from there.
- `RUNNER_IP` defaults to `127.0.0.1`; override it if running worker on a different host.
- MinIO acts as a local S3 replacement; `AWS_ENDPOINT_URL` points services at it.

## Kubernetes / EKS

- EKS cluster: `richard-claude-playground` (ap-southeast-1)
- Always deploy to the `crucible` namespace.
