#!/usr/bin/env python3
"""
Local end-to-end tests for the Crucible Control Plane API.

These tests hit the real running services — Control Plane API, MinIO, and
PostgreSQL — with no mocks. They verify that data flows correctly through
the full stack.

Quick start:
    ./tests/start_e2e_env.sh
    uv run python -m pytest tests/test_e2e_api.py -v
    ./tests/start_e2e_env.sh --stop

The tests use a unique label per session to avoid collisions and clean up
after themselves.
"""

from __future__ import annotations

import pathlib
import uuid

import boto3
import httpx
import psycopg2
import psycopg2.extras
import pytest

# ---------------------------------------------------------------------------
# Config — matches docker-compose defaults
# ---------------------------------------------------------------------------

API_BASE = "http://localhost:8000"
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
S3_BUCKET = "project-crucible-storage"
PG_DSN = "host=localhost port=5432 dbname=crucible user=postgres password=postgres"

_PLANS_DIR = pathlib.Path(__file__).parent / "plans"
_SESSION_TAG = uuid.uuid4().hex[:8]


# ---------------------------------------------------------------------------
# Connectivity checks — skip entire module if infra is not running
# ---------------------------------------------------------------------------

def _api_reachable() -> bool:
    try:
        r = httpx.get(f"{API_BASE}/health", timeout=2)
        return r.status_code == 200
    except httpx.ConnectError:
        return False


def _pg_reachable() -> bool:
    try:
        conn = psycopg2.connect(PG_DSN, connect_timeout=2)
        conn.close()
        return True
    except Exception:
        return False


def _minio_reachable() -> bool:
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name="us-east-1",
        )
        s3.head_bucket(Bucket=S3_BUCKET)
        return True
    except Exception:
        return False


_infra_ok = _api_reachable() and _pg_reachable() and _minio_reachable()
pytestmark = pytest.mark.skipif(
    not _infra_ok,
    reason="Local infrastructure not running (API / PostgreSQL / MinIO)",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_plan_yaml() -> str:
    return (_PLANS_DIR / "tpch_doris.yaml").read_text()


def _s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def _pg_query(sql: str, params: tuple = ()) -> list[dict]:
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    conn.close()
    return rows


def _pg_execute(sql: str, params: tuple = ()) -> None:
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(sql, params)
    conn.close()


def _s3_key_exists(key: str) -> bool:
    try:
        _s3().head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except Exception:
        return False


def _cleanup_run(run_id: str) -> None:
    """Remove a test run from PostgreSQL and its plan from MinIO."""
    _pg_execute("DELETE FROM test_runs WHERE run_id = %s", (run_id,))


def _cleanup_plan(plan_key: str) -> None:
    try:
        _s3().delete_object(Bucket=S3_BUCKET, Key=plan_key)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# POST /v1/test-runs — submit a test run
# ---------------------------------------------------------------------------

class TestSubmitRun:
    """Submit flow: plan is validated, uploaded to MinIO, run is recorded in PostgreSQL."""

    def test_submit_creates_run_and_uploads_plan(self):
        plan_name = f"e2e-submit-{_SESSION_TAG}"
        plan_key = f"plans/{plan_name}"

        resp = httpx.post(f"{API_BASE}/v1/test-runs", json={
            "plan_yaml": _load_plan_yaml(),
            "plan_name": plan_name,
            "label": f"e2e-label-{_SESSION_TAG}",
        }, timeout=10)

        assert resp.status_code == 200
        body = resp.json()
        run_id = body["run_id"]
        assert plan_name in run_id
        assert body["plan_key"] == plan_key
        assert body["strategy"] == "intra_node"

        # Verify plan was uploaded to MinIO
        assert _s3_key_exists(plan_key)

        # Verify run was recorded in PostgreSQL
        rows = _pg_query("SELECT * FROM test_runs WHERE run_id = %s", (run_id,))
        assert len(rows) == 1
        row = rows[0]
        assert row["plan_name"] == plan_name
        assert row["run_label"] == f"e2e-label-{_SESSION_TAG}"
        assert row["sut_type"] == "doris"
        assert row["scaling_mode"] == "intra_node"
        assert row["status"] == "PENDING"

        # Cleanup
        _cleanup_run(run_id)
        _cleanup_plan(plan_key)

    def test_submit_with_cluster_settings_persists_to_db(self):
        plan_name = f"e2e-settings-{_SESSION_TAG}"

        resp = httpx.post(f"{API_BASE}/v1/test-runs", json={
            "plan_yaml": _load_plan_yaml(),
            "plan_name": plan_name,
            "label": f"e2e-settings-{_SESSION_TAG}",
            "cluster_settings": "concurrency=20",
        }, timeout=10)

        assert resp.status_code == 200
        run_id = resp.json()["run_id"]

        rows = _pg_query("SELECT cluster_settings FROM test_runs WHERE run_id = %s", (run_id,))
        assert rows[0]["cluster_settings"] == "concurrency=20"

        _cleanup_run(run_id)
        _cleanup_plan(f"plans/{plan_name}")

    def test_submit_rejects_invalid_yaml(self):
        resp = httpx.post(f"{API_BASE}/v1/test-runs", json={
            "plan_yaml": "not: [valid: yaml: {{",
            "plan_name": "bad",
        }, timeout=10)
        assert resp.status_code == 422

    def test_submit_rejects_invalid_plan_schema(self):
        resp = httpx.post(f"{API_BASE}/v1/test-runs", json={
            "plan_yaml": "foo: bar\n",
            "plan_name": "bad",
        }, timeout=10)
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# GET /v1/test-runs — list test runs
# ---------------------------------------------------------------------------

class TestListRuns:
    """List flow: runs are queryable, filterable by run_label, and include all metadata."""

    @pytest.fixture(autouse=True)
    def _setup_runs(self):
        """Insert test rows directly into PostgreSQL and clean up after."""
        self.run_ids = []
        label = f"e2e-list-{_SESSION_TAG}"
        for i in range(3):
            run_id = f"e2e-list-{_SESSION_TAG}-{i}"
            self.run_ids.append(run_id)
            _pg_execute(
                """INSERT INTO test_runs
                   (run_id, task_id, plan_name, plan_key, run_label, sut_type,
                    scaling_mode, cluster_spec, cluster_settings, status)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    run_id, f"task-{i}", "e2e-plan", "plans/e2e-plan",
                    label if i < 2 else "other-label",
                    "doris", "intra_node",
                    psycopg2.extras.Json({"type": "doris"}) if i == 0 else None,
                    "concurrency=10" if i == 0 else None,
                    "COMPLETED",
                ),
            )
        yield
        for rid in self.run_ids:
            _cleanup_run(rid)

    def test_list_all_runs(self):
        resp = httpx.get(f"{API_BASE}/v1/test-runs", timeout=10)
        assert resp.status_code == 200
        body = resp.json()
        returned_ids = {r["run_id"] for r in body["runs"]}
        # Our 3 inserted runs should be in the result
        for rid in self.run_ids:
            assert rid in returned_ids

    def test_list_filter_by_run_label(self):
        label = f"e2e-list-{_SESSION_TAG}"
        resp = httpx.get(f"{API_BASE}/v1/test-runs", params={"run_label": label}, timeout=10)
        assert resp.status_code == 200
        runs = resp.json()["runs"]
        # Exactly the 2 runs with our label
        matching = [r for r in runs if r["run_id"] in self.run_ids]
        assert len(matching) == 2
        for r in matching:
            assert r["run_label"] == label

    def test_list_filter_empty_result(self):
        resp = httpx.get(
            f"{API_BASE}/v1/test-runs",
            params={"run_label": f"nonexistent-{_SESSION_TAG}"},
            timeout=10,
        )
        assert resp.status_code == 200
        assert resp.json()["runs"] == []

    def test_list_returns_all_metadata_fields(self):
        label = f"e2e-list-{_SESSION_TAG}"
        resp = httpx.get(f"{API_BASE}/v1/test-runs", params={"run_label": label}, timeout=10)
        # Find the run that has cluster_spec and cluster_settings
        run = next(r for r in resp.json()["runs"] if r["run_id"] == self.run_ids[0])
        assert run["plan_name"] == "e2e-plan"
        assert run["run_label"] == label
        assert run["sut_type"] == "doris"
        assert run["status"] == "COMPLETED"
        assert run["scaling_mode"] == "intra_node"
        assert run["cluster_spec"] == {"type": "doris"}
        assert run["cluster_settings"] == "concurrency=10"
        assert run["submitted_at"] is not None
        # These fields should be present (even if None)
        assert "started_at" in run
        assert "completed_at" in run
        assert "error_detail" in run


# ---------------------------------------------------------------------------
# POST /v1/test-runs/{plan_name} — trigger run by existing plan
# ---------------------------------------------------------------------------

class TestTriggerRun:
    """Trigger flow: fetches plan from MinIO, creates a new run."""

    @pytest.fixture(autouse=True)
    def _upload_plan(self):
        """Upload a plan to MinIO so trigger can find it."""
        self.plan_name = f"e2e-trigger-{_SESSION_TAG}"
        self.plan_key = f"plans/{self.plan_name}"
        _s3().put_object(
            Bucket=S3_BUCKET,
            Key=self.plan_key,
            Body=_load_plan_yaml().encode(),
        )
        self.run_ids = []
        yield
        for rid in self.run_ids:
            _cleanup_run(rid)
        _cleanup_plan(self.plan_key)

    def test_trigger_creates_run(self):
        resp = httpx.post(f"{API_BASE}/v1/test-runs/{self.plan_name}", timeout=10)
        assert resp.status_code == 200
        body = resp.json()
        run_id = body["run_id"]
        self.run_ids.append(run_id)
        assert self.plan_name in run_id
        assert body["plan_key"] == self.plan_key

        # Verify run in PostgreSQL
        rows = _pg_query("SELECT * FROM test_runs WHERE run_id = %s", (run_id,))
        assert len(rows) == 1
        assert rows[0]["plan_name"] == self.plan_name

    def test_trigger_with_cluster_settings(self):
        resp = httpx.post(
            f"{API_BASE}/v1/test-runs/{self.plan_name}",
            json={"cluster_settings": "parallel_fragment_exec_instance_num=8"},
            timeout=10,
        )
        assert resp.status_code == 200
        run_id = resp.json()["run_id"]
        self.run_ids.append(run_id)

        rows = _pg_query("SELECT cluster_settings FROM test_runs WHERE run_id = %s", (run_id,))
        assert rows[0]["cluster_settings"] == "parallel_fragment_exec_instance_num=8"

    def test_trigger_plan_not_found(self):
        resp = httpx.post(
            f"{API_BASE}/v1/test-runs/nonexistent-plan-{_SESSION_TAG}",
            timeout=10,
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /v1/test-runs/{run_id}/status
# ---------------------------------------------------------------------------

class TestRunStatus:
    """Status endpoint reads directly from PostgreSQL."""

    @pytest.fixture(autouse=True)
    def _setup_run(self):
        self.run_id = f"e2e-status-{_SESSION_TAG}"
        _pg_execute(
            """INSERT INTO test_runs
               (run_id, task_id, plan_name, plan_key, run_label, sut_type,
                scaling_mode, cluster_spec, cluster_settings, status)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                self.run_id, "task-status", "e2e-plan", "plans/e2e-plan",
                f"e2e-status-{_SESSION_TAG}", "doris", "intra_node",
                psycopg2.extras.Json({"type": "doris"}), "concurrency=10", "EXECUTING",
            ),
        )
        yield
        _cleanup_run(self.run_id)

    def test_status_returns_full_metadata(self):
        resp = httpx.get(f"{API_BASE}/v1/test-runs/{self.run_id}/status", timeout=10)
        assert resp.status_code == 200
        body = resp.json()
        assert body["run_id"] == self.run_id
        assert body["status"] == "EXECUTING"
        assert body["sut_type"] == "doris"
        assert body["scaling_mode"] == "intra_node"
        assert body["cluster_spec"] == {"type": "doris"}
        assert body["cluster_settings"] == "concurrency=10"

    def test_status_not_found(self):
        resp = httpx.get(
            f"{API_BASE}/v1/test-runs/nonexistent-{_SESSION_TAG}/status",
            timeout=10,
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST /v1/test-runs/{run_id}/stop
# ---------------------------------------------------------------------------

class TestStopRun:

    @pytest.fixture(autouse=True)
    def _setup_run(self):
        self.run_id = f"e2e-stop-{_SESSION_TAG}"
        _pg_execute(
            """INSERT INTO test_runs
               (run_id, task_id, plan_name, plan_key, run_label, sut_type,
                scaling_mode, status)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                self.run_id, "task-stop", "e2e-plan", "plans/e2e-plan",
                "stop-test", "doris", "intra_node", "EXECUTING",
            ),
        )
        yield
        _cleanup_run(self.run_id)

    def test_stop_marks_as_stopping(self):
        resp = httpx.post(f"{API_BASE}/v1/test-runs/{self.run_id}/stop", timeout=10)
        assert resp.status_code == 200
        assert resp.json()["action"] == "sigterm_sent"

        # Verify status changed in PostgreSQL
        rows = _pg_query("SELECT status FROM test_runs WHERE run_id = %s", (self.run_id,))
        assert rows[0]["status"] == "STOPPING"

    def test_stop_not_found(self):
        resp = httpx.post(
            f"{API_BASE}/v1/test-runs/nonexistent-{_SESSION_TAG}/stop",
            timeout=10,
        )
        assert resp.status_code == 200
        assert resp.json()["action"] == "not_found"


# ---------------------------------------------------------------------------
# GET /v1/test-runs/{run_id}/artifacts
# ---------------------------------------------------------------------------

class TestArtifacts:

    def test_list_artifacts_empty_run(self):
        resp = httpx.get(
            f"{API_BASE}/v1/test-runs/nonexistent-{_SESSION_TAG}/artifacts",
            timeout=10,
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["artifacts"] == []


# ===========================================================================
# FAILURE PATH TESTS
# ===========================================================================


# ---------------------------------------------------------------------------
# Submit validation failures
# ---------------------------------------------------------------------------

class TestSubmitValidationFailures:
    """Submit endpoint rejects invalid inputs with 422."""

    def test_submit_invalid_cluster_spec_unknown_type(self):
        resp = httpx.post(f"{API_BASE}/v1/test-runs", json={
            "plan_yaml": _load_plan_yaml(),
            "plan_name": f"e2e-bad-spec-{_SESSION_TAG}",
            "cluster_spec": {"type": "unknown_db"},
        }, timeout=10)
        assert resp.status_code == 422

    def test_submit_invalid_cluster_spec_bad_replica(self):
        resp = httpx.post(f"{API_BASE}/v1/test-runs", json={
            "plan_yaml": _load_plan_yaml(),
            "plan_name": f"e2e-bad-replica-{_SESSION_TAG}",
            "cluster_spec": {"type": "doris", "backend_node": {"replica": 0}},
        }, timeout=10)
        assert resp.status_code == 422

    def test_submit_cluster_spec_type_mismatch(self):
        """cluster_spec.type must match the plan's component_spec.type."""
        resp = httpx.post(f"{API_BASE}/v1/test-runs", json={
            "plan_yaml": _load_plan_yaml(),
            "plan_name": f"e2e-mismatch-{_SESSION_TAG}",
            "cluster_spec": {"type": "cassandra"},  # plan uses doris
        }, timeout=10)
        assert resp.status_code == 422
        assert "does not match" in str(resp.json()["detail"])

    def test_submit_disposable_plan_without_cluster_spec(self):
        """Disposable plans require cluster_spec."""
        import yaml
        disposable_plan = {
            "test_metadata": {"run_label": "bench"},
            "test_environment": {
                "env_type": "disposable",
                "target_db": "tpch",
                "component_spec": {"type": "doris"},
                "fixtures": [],
            },
            "execution": {
                "executor": "k6",
                "scaling_mode": "intra_node",
                "concurrency": 10,
                "ramp_up": "30s",
                "hold_for": "2m",
                "workload": [],
            },
        }
        resp = httpx.post(f"{API_BASE}/v1/test-runs", json={
            "plan_yaml": yaml.dump(disposable_plan),
            "plan_name": f"e2e-disposable-{_SESSION_TAG}",
        }, timeout=10)
        assert resp.status_code == 422
        assert "cluster_spec is required" in str(resp.json()["detail"])

    def test_submit_empty_cluster_spec(self):
        """Empty dict {} is not a valid cluster_spec (missing 'type')."""
        resp = httpx.post(f"{API_BASE}/v1/test-runs", json={
            "plan_yaml": _load_plan_yaml(),
            "plan_name": f"e2e-empty-spec-{_SESSION_TAG}",
            "cluster_spec": {},
        }, timeout=10)
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Trigger validation failures
# ---------------------------------------------------------------------------

class TestTriggerValidationFailures:
    """Trigger endpoint rejects invalid inputs."""

    @pytest.fixture(autouse=True)
    def _upload_plan(self):
        """Upload a plan to MinIO so trigger can find it."""
        self.plan_name = f"e2e-trigger-fail-{_SESSION_TAG}"
        self.plan_key = f"plans/{self.plan_name}"
        _s3().put_object(
            Bucket=S3_BUCKET,
            Key=self.plan_key,
            Body=_load_plan_yaml().encode(),
        )
        yield
        _cleanup_plan(self.plan_key)

    def test_trigger_with_type_mismatch_cluster_spec(self):
        resp = httpx.post(
            f"{API_BASE}/v1/test-runs/{self.plan_name}",
            json={"cluster_spec": {"type": "cassandra"}},
            timeout=10,
        )
        assert resp.status_code == 422
        assert "does not match" in str(resp.json()["detail"])

    def test_trigger_with_invalid_cluster_spec(self):
        resp = httpx.post(
            f"{API_BASE}/v1/test-runs/{self.plan_name}",
            json={"cluster_spec": {"type": "unknown_db"}},
            timeout=10,
        )
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# FAILED run status — error_detail visibility
# ---------------------------------------------------------------------------

class TestFailedRunStatus:
    """Verify that FAILED runs expose error_detail through all read endpoints."""

    @pytest.fixture(autouse=True)
    def _setup_failed_run(self):
        self.run_id = f"e2e-failed-{_SESSION_TAG}"
        self.error_msg = "k6 process failure:\ninstance 0 exited with code 1\n  stderr: ERRO connection refused"
        _pg_execute(
            """INSERT INTO test_runs
               (run_id, task_id, plan_name, plan_key, run_label, sut_type,
                scaling_mode, cluster_spec, cluster_settings, status,
                error_detail, completed_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())""",
            (
                self.run_id, "task-fail", "e2e-plan", "plans/e2e-plan",
                f"e2e-failed-{_SESSION_TAG}", "doris", "intra_node",
                psycopg2.extras.Json({"type": "doris"}), "concurrency=10",
                "FAILED", self.error_msg,
            ),
        )
        yield
        _cleanup_run(self.run_id)

    def test_status_shows_error_detail(self):
        resp = httpx.get(f"{API_BASE}/v1/test-runs/{self.run_id}/status", timeout=10)
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "FAILED"
        assert body["error_detail"] == self.error_msg
        assert "connection refused" in body["error_detail"]
        assert body["completed_at"] is not None

    def test_list_shows_error_detail(self):
        resp = httpx.get(
            f"{API_BASE}/v1/test-runs",
            params={"run_label": f"e2e-failed-{_SESSION_TAG}"},
            timeout=10,
        )
        assert resp.status_code == 200
        runs = resp.json()["runs"]
        assert len(runs) == 1
        run = runs[0]
        assert run["status"] == "FAILED"
        assert run["error_detail"] == self.error_msg

    def test_stop_failed_run_returns_already_stopped(self):
        resp = httpx.post(f"{API_BASE}/v1/test-runs/{self.run_id}/stop", timeout=10)
        assert resp.status_code == 200
        assert resp.json()["action"] == "already_stopped"


# ---------------------------------------------------------------------------
# COMPLETED run — stop is a no-op
# ---------------------------------------------------------------------------

class TestStopCompletedRun:

    @pytest.fixture(autouse=True)
    def _setup_completed_run(self):
        self.run_id = f"e2e-completed-{_SESSION_TAG}"
        _pg_execute(
            """INSERT INTO test_runs
               (run_id, task_id, plan_name, plan_key, run_label, sut_type,
                scaling_mode, status, completed_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())""",
            (
                self.run_id, "task-done", "e2e-plan", "plans/e2e-plan",
                "completed-test", "doris", "intra_node", "COMPLETED",
            ),
        )
        yield
        _cleanup_run(self.run_id)

    def test_stop_completed_run_returns_already_stopped(self):
        resp = httpx.post(f"{API_BASE}/v1/test-runs/{self.run_id}/stop", timeout=10)
        assert resp.status_code == 200
        assert resp.json()["action"] == "already_stopped"

    def test_completed_run_status_unchanged_after_stop(self):
        """Stop on a completed run should NOT change its status."""
        httpx.post(f"{API_BASE}/v1/test-runs/{self.run_id}/stop", timeout=10)
        rows = _pg_query("SELECT status FROM test_runs WHERE run_id = %s", (self.run_id,))
        assert rows[0]["status"] == "COMPLETED"


# ---------------------------------------------------------------------------
# STOPPING run — stop is a no-op
# ---------------------------------------------------------------------------

class TestStopStoppingRun:

    @pytest.fixture(autouse=True)
    def _setup_stopping_run(self):
        self.run_id = f"e2e-stopping-{_SESSION_TAG}"
        _pg_execute(
            """INSERT INTO test_runs
               (run_id, task_id, plan_name, plan_key, run_label, sut_type,
                scaling_mode, status)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                self.run_id, "task-stopping", "e2e-plan", "plans/e2e-plan",
                "stopping-test", "doris", "intra_node", "STOPPING",
            ),
        )
        yield
        _cleanup_run(self.run_id)

    def test_stop_stopping_run_returns_already_stopped(self):
        resp = httpx.post(f"{API_BASE}/v1/test-runs/{self.run_id}/stop", timeout=10)
        assert resp.status_code == 200
        assert resp.json()["action"] == "already_stopped"
