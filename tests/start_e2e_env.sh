#!/usr/bin/env bash
#
# Start the local e2e test environment for Crucible.
#
# Two modes:
#   Default — API-only tests (no SUT, no worker):
#     ./tests/start_e2e_env.sh
#     uv run python -m pytest tests/test_e2e_api.py -v
#
#   Full — includes MySQL SUT + Celery worker for full-pipeline tests:
#     ./tests/start_e2e_env.sh --full
#     uv run python -m pytest tests/test_e2e_api.py -v
#
#   Tear down (works for both modes):
#     ./tests/start_e2e_env.sh --stop
#
# Full mode additionally:
#   - Starts a MySQL 8.0 container as the test SUT
#   - Seeds it with a small test_data table
#   - Builds and starts the Celery worker container (which includes the k6 binary)
#   - Uploads the e2e workload SQL to MinIO

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
INFRA_DIR="$REPO_ROOT/infrastructure"
API_PID_FILE="$REPO_ROOT/.e2e-api.pid"
FULL_MODE=false
if [[ "${1:-}" == "--full" ]]; then
    FULL_MODE=true
fi

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
    docker compose --profile e2e --profile doris down 2>/dev/null || true

    echo "Done."
    exit 0
fi

# ---------------------------------------------------------------------------
# 1. Start docker services
# ---------------------------------------------------------------------------
echo "==> Starting docker services..."
cd "$INFRA_DIR"
docker compose up -d rabbitmq postgres minio prometheus

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
            healthy=$((healthy + 1))
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
    elapsed=$((elapsed + 2))
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

# Wait for Prometheus to be ready
echo "==> Waiting for Prometheus..."
for i in $(seq 1 20); do
    if curl -sf http://localhost:9090/-/healthy > /dev/null 2>&1; then
        echo "  Prometheus ready."
        break
    fi
    if [[ $i -eq 20 ]]; then
        echo "WARNING: Prometheus did not become healthy. Prometheus e2e tests will be skipped."
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
# 5. Full mode: MySQL SUT + Celery worker
# ---------------------------------------------------------------------------
if [[ "$FULL_MODE" == "true" ]]; then
    echo "==> Starting MySQL SUT..."
    cd "$INFRA_DIR"
    docker compose --profile e2e up -d mysql

    echo "  Waiting for MySQL to become healthy..."
    mysql_wait=0
    while true; do
        status=$(docker compose --profile e2e ps --format '{{.Health}}' mysql 2>/dev/null || echo "unknown")
        if [[ "$status" == "healthy" ]]; then
            echo "  MySQL healthy."
            break
        fi
        if [[ $mysql_wait -ge 60 ]]; then
            echo "ERROR: MySQL did not become healthy within 60s."
            exit 1
        fi
        sleep 2
        ((mysql_wait+=2))
    done

    echo "==> Seeding MySQL test data..."
    docker compose --profile e2e up mysql-init 2>/dev/null
    echo "  MySQL seeded."

    echo "==> Uploading e2e workload to MinIO..."
    cd "$REPO_ROOT"
    uv run python -c "
import boto3
s3 = boto3.client('s3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    region_name='us-east-1')
with open('tests/workloads/e2e_simple.sql') as f:
    content = f.read()
s3.put_object(Bucket='project-crucible-storage', Key='workloads/e2e-simple', Body=content.encode())
print('  Workload uploaded: workloads/e2e-simple')
"

    echo "==> Building and starting Celery worker..."
    cd "$INFRA_DIR"
    docker compose --profile e2e up -d --build worker
    echo "  Waiting for worker to connect..."
    for i in $(seq 1 30); do
        if docker compose --profile e2e logs worker 2>/dev/null | grep -q "celery.*ready"; then
            echo "  Worker ready."
            break
        fi
        if [[ $i -eq 30 ]]; then
            echo "WARNING: Worker may not be ready yet. Check logs if full-pipeline tests fail."
        fi
        sleep 2
    done
fi

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
echo ""
echo "============================================"
echo "  E2E environment is ready!"
if [[ "$FULL_MODE" == "true" ]]; then
echo "  Mode: FULL (MySQL SUT + worker)"
else
echo "  Mode: DEFAULT (API-only)"
fi
echo ""
echo "  Run tests:"
echo "    uv run python -m pytest tests/test_e2e_api.py -v"
echo ""
echo "  Tear down:"
echo "    ./tests/start_e2e_env.sh --stop"
echo "============================================"
