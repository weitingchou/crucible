# ── Stage 1: build the custom xk6-sql binary ─────────────────────────────────
FROM golang:1.25-alpine AS k6-builder

RUN apk add --no-cache git
RUN go install go.k6.io/xk6/cmd/xk6@latest

# xk6-sql provides the generic SQL extension; xk6-sql-driver-mysql covers
# MySQL-wire-compatible targets (Doris, TiDB, etc.). Add more drivers as needed.
RUN xk6 build \
    --with github.com/grafana/xk6-sql@latest \
    --with github.com/grafana/xk6-sql-driver-mysql@latest \
    --output /k6

# ── Stage 2: install Python dependencies ─────────────────────────────────────
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS py-builder

WORKDIR /app

# cassandra-driver requires a C compiler at build time.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libssl-dev libev-dev \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
COPY lib/ lib/
COPY worker/ worker/

RUN uv sync --package crucible-worker --no-dev

# ── Stage 3: lean runtime image ───────────────────────────────────────────────
FROM python:3.12-slim

WORKDIR /app

# Runtime shared libraries required by C-extension wheels.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 libev4 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=py-builder /app/.venv /app/.venv
COPY --from=py-builder /app/lib /app/lib
COPY --from=py-builder /app/worker /app/worker

# Place the custom k6 binary on PATH.
COPY --from=k6-builder /k6 /usr/local/bin/k6

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

# The JS driver script path; consumed by the k6 manager as an env var.
ENV SQL_DRIVER_PATH="/app/worker/src/worker/drivers/generic_sql_driver.js"

# Both worker roles run from this image; the role is the -Q flag, set by the
# deployment (see helm/crucible/templates/deployment-worker.yaml and
# infrastructure/docker-compose.yml).  The default names the dispatch queue
# explicitly rather than relying on task_default_queue, so it is obvious that a
# container started this way is only half of a working fleet — without an
# execute worker alongside it, k6_executor_task has no consumer and runs hang.
CMD ["celery", "-A", "worker.celery_app", "worker", "-Q", "dispatch", "--loglevel=info"]
