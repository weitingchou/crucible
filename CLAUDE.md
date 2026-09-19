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
| Telemetry | Prometheus (remote-write) | Real-time metrics streamed directly from k6 (optional — see below) |

### Bring-your-own metrics (`prometheus.enabled`)

The bundled Prometheus is only a remote-write sink for k6 — its scrape config
is empty and nothing in Crucible scrapes or queries it. `prometheus.enabled`
defaults to `true`; set it to `false` when the consumer supplies its own
metrics stack.

With it off:

- The Prometheus Deployment, Service, PVC and ConfigMap are not created.
- `PROMETHEUS_RW_URL` is unset on the workers, so `spawn_k6` omits the
  `experimental-prometheus-rw` output. Runs still complete and results are
  unchanged — `results.json` is built from k6's CSV output, which is never
  optional. The only loss is live time series for the load driver.
- The MCP server stops injecting `k6_prometheus_rw_server_url` into plans.

Unaffected, because the worker queries those URLs directly and they are the
caller's to supply: `test_environment.observability.prometheus_sources`,
including its `tls.ca_bundle_pem` for endpoints behind a private CA.

Note that the plan-level `k6_prometheus_rw_server_url` field is currently
injected by the MCP server but read by nothing — `spawn_k6` uses the
`PROMETHEUS_RW_URL` env var instead. Setting it in a plan has no effect.

### Celery Queues

The worker runs as **two deployments consuming separate queues**, never one
shared pool:

| Queue | Task | Deployment |
|---|---|---|
| `dispatch` | `dispatcher_task` | `crucible-worker-dispatch` |
| `execute` | `k6_executor_task` | `crucible-worker-execute` |

A dispatcher holds its slot for the whole run while waiting on its executor, so
on a shared pool dispatchers fill every slot and starve the executors they are
waiting for — the run sits `EXECUTING` with no k6 process, forever (issue #2).
Queue names live in `lib/src/crucible_lib/queues.py`; Celery resolves routing
**sender-side**, so any new `send_task` call must name its queue explicitly.

`task_acks_late` is deliberately off. Turning it on without first raising
RabbitMQ's `consumer_timeout` (unset, so the 30-minute default) would make the
broker redeliver any run longer than 30 minutes.

Contention for a SUT is handled by retrying the dispatcher off-slot
(`pg_try_advisory_lock` + `self.retry`), not by blocking on the lock.

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

### Building and pushing images

```bash
./scripts/build_push.sh all        # or: worker | control-plane | mcp
```

Tags images with the short commit SHA (and `latest`), builds `--platform
linux/amd64`, and pushes with `docker push` — not `buildx --push`, which needs
`ecr:BatchGetImage` that `claude-bot` lacks. It refuses a dirty tree unless
given `--allow-dirty`, and prints the matching `helm upgrade --set` flags.
Deploy the SHA tag, not `latest`, so running pods map to a commit.

### Helm Deploy Procedure

```bash
# 1. Update kubeconfig (written to ~/.kube/claude-config)
aws eks update-kubeconfig --name richard-claude-playground --profile claude-bot

# 2. Create namespace only if it does not already exist
kubectl get namespace crucible --kubeconfig ~/.kube/claude-config 2>/dev/null \
  || kubectl create namespace crucible --kubeconfig ~/.kube/claude-config

# 3. Get AWS account ID and install
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --profile claude-bot)
helm install crucible ./helm/crucible \
  -f helm/crucible/values-eks.yaml \
  --set awsAccountId=$AWS_ACCOUNT_ID \
  --set rabbitmq.auth.password=<strong-password> \
  --set postgresql.auth.password=<strong-password> \
  --namespace crucible \
  --kubeconfig ~/.kube/claude-config
```

**Always pass explicit passwords** for RabbitMQ and PostgreSQL via `--set` — `values-eks.yaml` leaves them empty.

Worker sizing is per role: `worker.dispatch.{replicaCount,concurrency}` and
`worker.execute.{replicaCount,concurrency}`. The old flat `worker.replicaCount`
/ `worker.resources` keys are gone and the chart fails the install if they are
still set. Keep `execute.concurrency` at or below the pod's CPU limit — an
oversubscribed k6 makes the worker, not the SUT, the thing being measured.

RabbitMQ's `RABBITMQ_DEFAULT_PASS` only applies on first initialization; the chart uses a definitions file (`RABBITMQ_DEFAULT_DEFINITIONS_FILE`) mounted from the Secret so the password is synced on every pod start, including after `helm upgrade`.
