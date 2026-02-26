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

# cassandra-driver and some Taurus deps require a C compiler at build time.
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

# The JS driver script path; consumed by the Taurus orchestrator as an env var.
ENV SQL_DRIVER_PATH="/app/worker/src/worker/drivers/generic_sql_driver.js"

CMD ["celery", "-A", "worker.celery_app", "worker", "--loglevel=info"]
