# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

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
| Orchestrator | Taurus | Process tree lifecycle management for k6 |
| Telemetry | Prometheus Pushgateway | Push-based real-time metrics aggregation |

### Execution Worker Sub-components

The Celery worker contains four sub-components that run in sequence:

1. **Lease Manager** — Acquires atomic locks on shared SUTs (long-lived) or provisions ephemeral clusters (disposable)
2. **Fixture Loader** — Hydrates the SUT using one of three strategies (see below)
3. **Taurus Orchestrator** — Manages k6 process lifecycle: `prepare → startup → execution → shutdown`
4. **Generic Workload Driver** — Custom `xk6-sql` binary that parses annotated SQL files and executes randomized queries as concurrent Goroutines

### Key Design Decisions

**Fixture loading uses a Strategy Pattern based on database type:**
- **Zero-Download (MPP):** Issues SQL commands to pull Parquet files from S3 directly into Doris/Trino via S3 TVF — no data touches the worker disk
- **Streaming (NoSQL):** Pipes CSV rows from S3 stream into Cassandra via async driver — near-zero worker memory footprint
- **Standard:** Traditional ingestion path

**Scaling uses two modes determined from the YAML `scaling_mode` field:**
- **Intra-node (vertical):** Single worker spawns multiple local k6 instances
- **Inter-node (horizontal):** Celery Chain: master task retrieves its IP, then spawns N-1 worker tasks across the fleet

**Asset upload flow for 100GB+ datasets:**
- Client calls `/fixtures/{id}/{file}/multipart/init` → gets `upload_id`
- Client calls `/fixtures/{id}/{file}/multipart/{upload_id}/part/{n}` → gets pre-signed S3 URL
- Client uploads directly to S3 (bypasses API server entirely)

**SQL workload files use `-- @name: QueryName` annotations** to allow the k6 driver to parse, name, and track per-query latency as individual Prometheus `Trend` metrics.

**Environment injection:** The worker silently injects DB credentials, endpoints, and S3 URIs at runtime. End users never see infrastructure details.
