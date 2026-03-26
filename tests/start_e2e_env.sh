#!/usr/bin/env bash
#
# Start the local e2e test environment for Crucible.
#
# This script:
#   1. Starts MinIO, PostgreSQL, and RabbitMQ via docker compose
#   2. Waits for all services to become healthy
#   3. Applies the DB schema (idempotent — safe to re-run)
#   4. Starts the Control Plane API on localhost:8000
#
# Usage:
#   ./tests/start_e2e_env.sh          # start everything
#   ./tests/start_e2e_env.sh --stop   # tear down
#
# After the environment is up, run the tests:
#   uv run python -m pytest tests/test_e2e_api.py -v

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
INFRA_DIR="$REPO_ROOT/infrastructure"
API_PID_FILE="$REPO_ROOT/.e2e-api.pid"

# ---------------------------------------------------------------------------
# Tear down
# ---------------------------------------------------------------------------
if [[ "${1:-}" == "--stop" ]]; then
    echo "Stopping e2e environment..."

    if [[ -f "$API_PID_FILE" ]]; then
        pid=$(cat "$API_PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            echo "  Stopping control plane (PID $pid)..."
            kill "$pid" 2>/dev/null || true
            # Wait up to 5s for graceful shutdown
            for _ in $(seq 1 10); do
                kill -0 "$pid" 2>/dev/null || break
                sleep 0.5
            done
            kill -9 "$pid" 2>/dev/null || true
        fi
        rm -f "$API_PID_FILE"
    fi

    echo "  Stopping docker services..."
    cd "$INFRA_DIR"
    docker compose down 2>/dev/null || true

    echo "Done."
    exit 0
fi

# ---------------------------------------------------------------------------
# 1. Start docker services
# ---------------------------------------------------------------------------
echo "==> Starting docker services..."
cd "$INFRA_DIR"
docker compose up -d rabbitmq postgres minio

# ---------------------------------------------------------------------------
# 2. Wait for health checks
# ---------------------------------------------------------------------------
echo "==> Waiting for services to become healthy..."
max_wait=60
elapsed=0
while true; do
    healthy=0
    total=3
    for svc in rabbitmq postgres minio; do
        status=$(docker compose ps --format '{{.Health}}' "$svc" 2>/dev/null || echo "unknown")
        if [[ "$status" == "healthy" ]]; then
            ((healthy++))
        fi
    done

    if [[ $healthy -eq $total ]]; then
        echo "  All services healthy."
        break
    fi

    if [[ $elapsed -ge $max_wait ]]; then
        echo "ERROR: Services did not become healthy within ${max_wait}s."
        docker compose ps
        exit 1
    fi

    sleep 2
    ((elapsed+=2))
    echo "  Waiting... ($healthy/$total healthy, ${elapsed}s elapsed)"
done

# Initialize MinIO bucket
echo "==> Initializing MinIO bucket..."
docker compose up -d minio-init
# Wait for minio-init to complete
for _ in $(seq 1 15); do
    status=$(docker compose ps --format '{{.State}}' minio-init 2>/dev/null || echo "running")
    if [[ "$status" == "exited" ]]; then
        break
    fi
    sleep 1
done

# ---------------------------------------------------------------------------
# 3. Apply DB schema
# ---------------------------------------------------------------------------
echo "==> Applying database schema..."
cd "$REPO_ROOT"
uv run python -c "
import psycopg2
conn = psycopg2.connect('host=localhost port=5432 dbname=crucible user=postgres password=postgres')
conn.autocommit = True
conn.cursor().execute(open('infrastructure/config/postgres/init.sql').read())
conn.close()
print('  Schema applied.')
"

# ---------------------------------------------------------------------------
# 4. Start the Control Plane API
# ---------------------------------------------------------------------------
if [[ -f "$API_PID_FILE" ]]; then
    old_pid=$(cat "$API_PID_FILE")
    if kill -0 "$old_pid" 2>/dev/null; then
        echo "==> Control plane already running (PID $old_pid), restarting..."
        kill "$old_pid" 2>/dev/null || true
        sleep 1
        kill -9 "$old_pid" 2>/dev/null || true
    fi
    rm -f "$API_PID_FILE"
fi

echo "==> Starting control plane on http://localhost:8000..."
cd "$REPO_ROOT"
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
AWS_ENDPOINT_URL=http://localhost:9000 \
nohup uv run uvicorn control_plane.main:app \
    --host 0.0.0.0 --port 8000 \
    > "$REPO_ROOT/.e2e-api.log" 2>&1 &
echo $! > "$API_PID_FILE"

# Wait for API to be ready
echo "  Waiting for API..."
for i in $(seq 1 20); do
    if curl -sf http://localhost:8000/health > /dev/null 2>&1; then
        echo "  Control plane ready (PID $(cat "$API_PID_FILE"))."
        break
    fi
    if [[ $i -eq 20 ]]; then
        echo "ERROR: Control plane did not start. Check .e2e-api.log"
        cat "$REPO_ROOT/.e2e-api.log"
        exit 1
    fi
    sleep 1
done

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
echo ""
echo "============================================"
echo "  E2E environment is ready!"
echo ""
echo "  Run tests:"
echo "    uv run python -m pytest tests/test_e2e_api.py -v"
echo ""
echo "  Tear down:"
echo "    ./tests/start_e2e_env.sh --stop"
echo "============================================"
