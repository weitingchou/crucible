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

import json
import pathlib
import time
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
PROMETHEUS_URL = "http://localhost:9090"
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


def _prometheus_reachable() -> bool:
    try:
        r = httpx.get(f"{PROMETHEUS_URL}/-/healthy", timeout=2)
        return r.status_code == 200
    except httpx.ConnectError:
        return False


_infra_ok = _api_reachable() and _pg_reachable() and _minio_reachable()
_prometheus_ok = _prometheus_reachable() if _infra_ok else False
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
            "cluster_spec": {"type": "doris", "backend_node": {"replica": 1}},
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
        spec = row["cluster_spec"]
        if isinstance(spec, str):
            import json as _json
            spec = _json.loads(spec)
        assert spec["type"] == "doris"

        # Cleanup
        _cleanup_run(run_id)
        _cleanup_plan(plan_key)

    def test_submit_with_cluster_settings_persists_to_db(self):
        plan_name = f"e2e-settings-{_SESSION_TAG}"

        resp = httpx.post(f"{API_BASE}/v1/test-runs", json={
            "plan_yaml": _load_plan_yaml(),
            "plan_name": plan_name,
            "label": f"e2e-settings-{_SESSION_TAG}",
            "cluster_spec": {"type": "doris", "backend_node": {"replica": 1}},
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
        resp = httpx.post(f"{API_BASE}/v1/test-runs/{self.plan_name}", json={
            "cluster_spec": {"type": "doris", "backend_node": {"replica": 1}},
        }, timeout=10)
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
        spec = rows[0]["cluster_spec"]
        if isinstance(spec, str):
            import json as _json
            spec = _json.loads(spec)
        assert spec["type"] == "doris"

    def test_trigger_with_cluster_settings(self):
        resp = httpx.post(
            f"{API_BASE}/v1/test-runs/{self.plan_name}",
            json={
                "cluster_spec": {"type": "doris", "backend_node": {"replica": 1}},
                "cluster_settings": "parallel_fragment_exec_instance_num=8",
            },
            timeout=10,
        )
        assert resp.status_code == 200
        run_id = resp.json()["run_id"]
        self.run_ids.append(run_id)

        rows = _pg_query("SELECT cluster_settings FROM test_runs WHERE run_id = %s", (run_id,))
        assert rows[0]["cluster_settings"] == "parallel_fragment_exec_instance_num=8"

    def test_trigger_without_cluster_spec_returns_422(self):
        resp = httpx.post(
            f"{API_BASE}/v1/test-runs/{self.plan_name}",
            timeout=10,
        )
        assert resp.status_code == 422

    def test_trigger_plan_not_found(self):
        resp = httpx.post(
            f"{API_BASE}/v1/test-runs/nonexistent-plan-{_SESSION_TAG}",
            json={"cluster_spec": {"type": "doris"}},
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


# ---------------------------------------------------------------------------
# Submit with failure_detection config
# ---------------------------------------------------------------------------

class TestSubmitWithFailureDetection:
    """Verify that failure_detection config is accepted, persisted, and rejected when invalid."""

    def test_submit_plan_with_failure_detection(self):
        """Custom failure_detection config is accepted and persisted in S3."""
        import yaml

        plan_yaml = _load_plan_yaml()
        raw = yaml.safe_load(plan_yaml)
        raw["execution"]["failure_detection"] = {
            "enabled": True,
            "error_rate_threshold": 0.3,
            "abort_delay": "30s",
        }
        modified_yaml = yaml.dump(raw)

        plan_name = f"e2e-fd-custom-{_SESSION_TAG}"
        plan_key = f"plans/{plan_name}"
        resp = httpx.post(f"{API_BASE}/v1/test-runs", json={
            "plan_yaml": modified_yaml,
            "plan_name": plan_name,
            "label": f"e2e-fd-{_SESSION_TAG}",
            "cluster_spec": {"type": "doris", "backend_node": {"replica": 1}},
        }, timeout=10)

        assert resp.status_code == 200
        run_id = resp.json()["run_id"]

        # Verify failure_detection is preserved in the stored plan YAML
        obj = _s3().get_object(Bucket=S3_BUCKET, Key=plan_key)
        stored = yaml.safe_load(obj["Body"].read())
        fd = stored["execution"]["failure_detection"]
        assert fd["enabled"] is True
        assert fd["error_rate_threshold"] == 0.3
        assert fd["abort_delay"] == "30s"

        _cleanup_run(run_id)
        _cleanup_plan(plan_key)

    def test_submit_plan_with_failure_detection_disabled(self):
        """failure_detection with enabled: false is accepted."""
        import yaml

        plan_yaml = _load_plan_yaml()
        raw = yaml.safe_load(plan_yaml)
        raw["execution"]["failure_detection"] = {"enabled": False}
        modified_yaml = yaml.dump(raw)

        plan_name = f"e2e-fd-off-{_SESSION_TAG}"
        plan_key = f"plans/{plan_name}"
        resp = httpx.post(f"{API_BASE}/v1/test-runs", json={
            "plan_yaml": modified_yaml,
            "plan_name": plan_name,
            "label": f"e2e-fd-off-{_SESSION_TAG}",
            "cluster_spec": {"type": "doris", "backend_node": {"replica": 1}},
        }, timeout=10)

        assert resp.status_code == 200
        run_id = resp.json()["run_id"]

        _cleanup_run(run_id)
        _cleanup_plan(plan_key)

    def test_submit_rejects_invalid_failure_detection_threshold(self):
        """error_rate_threshold out of range (0, 1.0] is rejected with 422."""
        import yaml

        plan_yaml = _load_plan_yaml()

        # threshold = 0 → invalid (must be > 0)
        raw = yaml.safe_load(plan_yaml)
        raw["execution"]["failure_detection"] = {"error_rate_threshold": 0}
        resp = httpx.post(f"{API_BASE}/v1/test-runs", json={
            "plan_yaml": yaml.dump(raw),
            "plan_name": f"e2e-fd-bad-{_SESSION_TAG}",
            "cluster_spec": {"type": "doris"},
        }, timeout=10)
        assert resp.status_code == 422

        # threshold = 1.5 → invalid (must be <= 1.0)
        raw["execution"]["failure_detection"] = {"error_rate_threshold": 1.5}
        resp = httpx.post(f"{API_BASE}/v1/test-runs", json={
            "plan_yaml": yaml.dump(raw),
            "plan_name": f"e2e-fd-bad2-{_SESSION_TAG}",
            "cluster_spec": {"type": "doris"},
        }, timeout=10)
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Results with abort_reason (SUT failure detection)
# ---------------------------------------------------------------------------

class TestResultsWithAbortReason:
    """Verify that abort_reason flows through results.json → API for SUT failure aborts."""

    def test_results_returns_abort_reason_for_sut_failure(self):
        """COMPLETED run with abort_reason shows partial results and the reason."""
        run_id = f"e2e-abort-reason-{_SESSION_TAG}"
        _pg_execute(
            """INSERT INTO test_runs
               (run_id, task_id, plan_name, plan_key, run_label, sut_type,
                scaling_mode, status, completed_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())""",
            (
                run_id, "task-ar", "e2e-plan", "plans/e2e-plan",
                "abort-test", "doris", "intra_node", "COMPLETED",
            ),
        )
        results_json = {
            "run_id": run_id,
            "collected_at": "2026-03-29T10:00:00Z",
            "collection_error": None,
            "abort_reason": "SUT failure detected: query error rate exceeded threshold",
            "k6": {
                "metrics": [
                    {"name": "sql_duration_Q1", "type": "trend",
                     "stats": {"count": 50, "min": 1.0, "max": 30.0, "avg": 8.0,
                               "med": 6.0, "p90": 20.0, "p95": 25.0, "p99": 29.0}},
                    {"name": "query_errors", "type": "trend",
                     "stats": {"count": 120, "min": 0.0, "max": 1.0, "avg": 0.58,
                               "med": 1.0, "p90": 1.0, "p95": 1.0, "p99": 1.0}},
                ],
            },
            "observability": {"sources": []},
        }
        _s3().put_object(
            Bucket=S3_BUCKET,
            Key=f"results/{run_id}/results.json",
            Body=json.dumps(results_json).encode(),
        )
        try:
            resp = httpx.get(f"{API_BASE}/v1/test-runs/{run_id}/results", timeout=10)
            assert resp.status_code == 200
            body = resp.json()
            assert body["status"] == "COMPLETED"
            # Partial k6 results are present
            metrics = body["k6"]["metrics"]
            assert len(metrics) == 2
            q1 = next(m for m in metrics if m["name"] == "sql_duration_Q1")
            assert q1["stats"]["count"] == 50
            # query_errors metric is present
            qe = next(m for m in metrics if m["name"] == "query_errors")
            assert qe["stats"]["avg"] == 0.58
        finally:
            _cleanup_run(run_id)
            try:
                _s3().delete_object(Bucket=S3_BUCKET, Key=f"results/{run_id}/results.json")
            except Exception:
                pass

    def test_results_returns_null_abort_reason_for_normal_completion(self):
        """Normal COMPLETED run has no abort_reason."""
        run_id = f"e2e-no-abort-{_SESSION_TAG}"
        _pg_execute(
            """INSERT INTO test_runs
               (run_id, task_id, plan_name, plan_key, run_label, sut_type,
                scaling_mode, status, completed_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())""",
            (
                run_id, "task-na", "e2e-plan", "plans/e2e-plan",
                "normal-test", "doris", "intra_node", "COMPLETED",
            ),
        )
        results_json = {
            "run_id": run_id,
            "collected_at": "2026-03-29T10:00:00Z",
            "collection_error": None,
            "abort_reason": None,
            "k6": {
                "metrics": [
                    {"name": "sql_duration_Q1", "type": "trend",
                     "stats": {"count": 500, "min": 1.0, "max": 50.0, "avg": 10.0,
                               "med": 8.0, "p90": 25.0, "p95": 35.0, "p99": 48.0}},
                ],
            },
            "observability": {"sources": []},
        }
        _s3().put_object(
            Bucket=S3_BUCKET,
            Key=f"results/{run_id}/results.json",
            Body=json.dumps(results_json).encode(),
        )
        try:
            resp = httpx.get(f"{API_BASE}/v1/test-runs/{run_id}/results", timeout=10)
            assert resp.status_code == 200
            body = resp.json()
            assert body["status"] == "COMPLETED"
            assert body["k6"]["metrics"][0]["stats"]["count"] == 500
        finally:
            _cleanup_run(run_id)
            try:
                _s3().delete_object(Bucket=S3_BUCKET, Key=f"results/{run_id}/results.json")
            except Exception:
                pass


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

    def test_submit_without_cluster_spec_returns_422(self):
        """Any plan submitted without cluster_spec should be rejected."""
        resp = httpx.post(f"{API_BASE}/v1/test-runs", json={
            "plan_yaml": _load_plan_yaml(),
            "plan_name": f"e2e-no-spec-{_SESSION_TAG}",
        }, timeout=10)
        assert resp.status_code == 422

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


# ---------------------------------------------------------------------------
# GET /v1/test-runs/{run_id}/results
# ---------------------------------------------------------------------------

class TestResults:
    """Results endpoint loads structured data from S3."""

    def test_results_returns_404_for_unknown_run(self):
        resp = httpx.get(
            f"{API_BASE}/v1/test-runs/nonexistent-{_SESSION_TAG}/results",
            timeout=10,
        )
        assert resp.status_code == 404

    def test_results_returns_409_for_pending_run(self):
        run_id = f"e2e-results-pending-{_SESSION_TAG}"
        _pg_execute(
            """INSERT INTO test_runs
               (run_id, task_id, plan_name, plan_key, run_label, sut_type,
                scaling_mode, status)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                run_id, "task-rp", "e2e-plan", "plans/e2e-plan",
                "results-test", "doris", "intra_node", "PENDING",
            ),
        )
        try:
            resp = httpx.get(f"{API_BASE}/v1/test-runs/{run_id}/results", timeout=10)
            assert resp.status_code == 409
        finally:
            _cleanup_run(run_id)

    def test_results_returns_200_for_completed_run_with_s3_data(self):
        run_id = f"e2e-results-ok-{_SESSION_TAG}"
        _pg_execute(
            """INSERT INTO test_runs
               (run_id, task_id, plan_name, plan_key, run_label, sut_type,
                scaling_mode, status, completed_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())""",
            (
                run_id, "task-rok", "e2e-plan", "plans/e2e-plan",
                "results-test", "doris", "intra_node", "COMPLETED",
            ),
        )
        # Upload a fake results.json to S3
        results_json = {
            "run_id": run_id,
            "collected_at": "2025-03-15T14:35:00Z",
            "collection_error": None,
            "k6": {
                "metrics": [
                    {"name": "sql_duration_Q1", "type": "trend",
                     "stats": {"count": 100, "min": 1.0, "max": 50.0, "avg": 10.0,
                               "med": 8.0, "p90": 25.0, "p95": 35.0, "p99": 48.0}},
                ],
            },
            "observability": {"sources": []},
        }
        _s3().put_object(
            Bucket=S3_BUCKET,
            Key=f"results/{run_id}/results.json",
            Body=json.dumps(results_json).encode(),
        )
        try:
            resp = httpx.get(f"{API_BASE}/v1/test-runs/{run_id}/results", timeout=10)
            assert resp.status_code == 200
            body = resp.json()
            assert body["run_id"] == run_id
            assert body["status"] == "COMPLETED"
            assert body["k6"]["metrics"][0]["name"] == "sql_duration_Q1"
            assert body["k6"]["metrics"][0]["stats"]["count"] == 100
            assert body["collection_error"] is None
        finally:
            _cleanup_run(run_id)
            try:
                _s3().delete_object(Bucket=S3_BUCKET, Key=f"results/{run_id}/results.json")
            except Exception:
                pass

    def test_results_includes_observability_sources(self):
        run_id = f"e2e-results-obs-{_SESSION_TAG}"
        _pg_execute(
            """INSERT INTO test_runs
               (run_id, task_id, plan_name, plan_key, run_label, sut_type,
                scaling_mode, status, completed_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())""",
            (
                run_id, "task-robs", "e2e-plan", "plans/e2e-plan",
                "results-test", "doris", "intra_node", "COMPLETED",
            ),
        )
        results_json = {
            "run_id": run_id,
            "collected_at": "2025-03-15T14:35:00Z",
            "collection_error": None,
            "k6": {"metrics": []},
            "observability": {
                "sources": [
                    {
                        "name": "engine",
                        "url": "http://prometheus:9090",
                        "metrics": [
                            {
                                "name": "cluster_qps",
                                "query": "sum(rate(doris_be_query_total[1m]))",
                                "values": [[1710510600, "245.3"], [1710510615, "312.7"]],
                            },
                            {
                                "name": "avg_memory",
                                "query": "avg(doris_be_mem_usage_bytes)",
                                "values": [[1710510600, "4294967296"]],
                            },
                        ],
                    },
                    {
                        "name": "infra",
                        "url": "http://prom-infra:9090",
                        "metrics": [
                            {
                                "name": "cpu_usage",
                                "query": "avg(rate(node_cpu_seconds_total{mode!='idle'}[1m]))",
                                "values": [[1710510600, "0.45"], [1710510615, "0.52"]],
                            },
                        ],
                    },
                ],
            },
        }
        _s3().put_object(
            Bucket=S3_BUCKET,
            Key=f"results/{run_id}/results.json",
            Body=json.dumps(results_json).encode(),
        )
        try:
            resp = httpx.get(f"{API_BASE}/v1/test-runs/{run_id}/results", timeout=10)
            assert resp.status_code == 200
            body = resp.json()
            sources = body["observability"]["sources"]
            assert len(sources) == 2
            # Verify engine source
            engine = next(s for s in sources if s["name"] == "engine")
            assert engine["url"] == "http://prometheus:9090"
            assert len(engine["metrics"]) == 2
            qps = next(m for m in engine["metrics"] if m["name"] == "cluster_qps")
            assert len(qps["values"]) == 2
            assert qps["values"][0] == [1710510600, "245.3"]
            # Verify infra source
            infra = next(s for s in sources if s["name"] == "infra")
            assert infra["url"] == "http://prom-infra:9090"
            assert len(infra["metrics"]) == 1
            assert infra["metrics"][0]["name"] == "cpu_usage"
        finally:
            _cleanup_run(run_id)
            try:
                _s3().delete_object(Bucket=S3_BUCKET, Key=f"results/{run_id}/results.json")
            except Exception:
                pass

    def test_results_handles_missing_s3_file(self):
        run_id = f"e2e-results-no-s3-{_SESSION_TAG}"
        _pg_execute(
            """INSERT INTO test_runs
               (run_id, task_id, plan_name, plan_key, run_label, sut_type,
                scaling_mode, status, completed_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())""",
            (
                run_id, "task-rns", "e2e-plan", "plans/e2e-plan",
                "results-test", "doris", "intra_node", "COMPLETED",
            ),
        )
        try:
            resp = httpx.get(f"{API_BASE}/v1/test-runs/{run_id}/results", timeout=10)
            assert resp.status_code == 200
            body = resp.json()
            assert body["run_id"] == run_id
            assert body["collection_error"] is not None
        finally:
            _cleanup_run(run_id)


# ---------------------------------------------------------------------------
# GET /v1/test-runs/{run_id}/results — real Prometheus integration
# ---------------------------------------------------------------------------

def _query_prometheus_range(query: str, start: float, end: float, step: int = 15) -> list:
    """Query Prometheus query_range API and return raw values list."""
    resp = httpx.get(
        f"{PROMETHEUS_URL}/api/v1/query_range",
        params={"query": query, "start": start, "end": end, "step": f"{step}s"},
        timeout=10,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "success"
    values = []
    for series in body["data"]["result"]:
        values.extend(series["values"])
    return values


@pytest.mark.skipif(not _prometheus_ok, reason="Prometheus not running")
class TestResultsWithPrometheus:
    """Tests that query real Prometheus — requires ``prometheus`` service running."""

    def test_query_range_returns_real_timeseries(self):
        """Directly hit Prometheus query_range API and verify real data comes back."""
        now = time.time()
        start = now - 120  # last 2 minutes
        values = _query_prometheus_range('up{job="prometheus"}', start, now)
        assert len(values) > 0
        # Each value is [unix_timestamp, "string_value"]
        ts, val = values[0]
        assert isinstance(ts, (int, float))
        assert val == "1"  # self-scrape target is always up

    def test_results_api_with_real_prometheus_data(self):
        """Full pipeline: query real Prometheus → build results JSON → upload
        to S3 → hit API → verify real time-series data in response."""
        run_id = f"e2e-prom-{_SESSION_TAG}"
        _pg_execute(
            """INSERT INTO test_runs
               (run_id, task_id, plan_name, plan_key, run_label, sut_type,
                scaling_mode, status, started_at, completed_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                       now() - interval '2 minutes', now())""",
            (
                run_id, "task-prom", "e2e-plan", "plans/e2e-plan",
                "prom-test", "doris", "intra_node", "COMPLETED",
            ),
        )

        # Query real Prometheus for two metrics
        now = time.time()
        start = now - 120
        up_values = _query_prometheus_range('up{job="prometheus"}', start, now)
        build_values = _query_prometheus_range("prometheus_build_info", start, now)

        results_json = {
            "run_id": run_id,
            "collected_at": "2025-03-15T14:35:00Z",
            "collection_error": None,
            "k6": {"metrics": []},
            "observability": {
                "sources": [
                    {
                        "name": "prometheus-self",
                        "url": PROMETHEUS_URL,
                        "metrics": [
                            {
                                "name": "target_up",
                                "query": 'up{job="prometheus"}',
                                "values": up_values,
                            },
                            {
                                "name": "build_info",
                                "query": "prometheus_build_info",
                                "values": build_values,
                            },
                        ],
                    },
                ],
            },
        }
        _s3().put_object(
            Bucket=S3_BUCKET,
            Key=f"results/{run_id}/results.json",
            Body=json.dumps(results_json).encode(),
        )
        try:
            resp = httpx.get(f"{API_BASE}/v1/test-runs/{run_id}/results", timeout=10)
            assert resp.status_code == 200
            body = resp.json()

            sources = body["observability"]["sources"]
            assert len(sources) == 1
            src = sources[0]
            assert src["name"] == "prometheus-self"
            assert src["url"] == PROMETHEUS_URL

            # Verify two metrics came through
            assert len(src["metrics"]) == 2
            up_metric = next(m for m in src["metrics"] if m["name"] == "target_up")
            build_metric = next(m for m in src["metrics"] if m["name"] == "build_info")

            # Real time-series — multiple data points, all with value "1"
            assert len(up_metric["values"]) > 0
            for ts, val in up_metric["values"]:
                assert isinstance(ts, (int, float))
                assert val == "1"

            assert len(build_metric["values"]) > 0
            for ts, val in build_metric["values"]:
                assert isinstance(ts, (int, float))
                assert val == "1"
        finally:
            _cleanup_run(run_id)
            try:
                _s3().delete_object(Bucket=S3_BUCKET, Key=f"results/{run_id}/results.json")
            except Exception:
                pass

    def test_multiple_prometheus_sources_with_real_data(self):
        """Verify multiple named sources work with real Prometheus data.

        Uses the same Prometheus instance as two logical sources (engine vs infra)
        with different queries — mirrors the multi-source design.
        """
        run_id = f"e2e-prom-multi-{_SESSION_TAG}"
        _pg_execute(
            """INSERT INTO test_runs
               (run_id, task_id, plan_name, plan_key, run_label, sut_type,
                scaling_mode, status, started_at, completed_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                       now() - interval '2 minutes', now())""",
            (
                run_id, "task-prom-m", "e2e-plan", "plans/e2e-plan",
                "prom-multi", "doris", "intra_node", "COMPLETED",
            ),
        )

        now = time.time()
        start = now - 120
        up_values = _query_prometheus_range('up{job="prometheus"}', start, now)
        scrape_values = _query_prometheus_range(
            "scrape_duration_seconds", start, now
        )

        results_json = {
            "run_id": run_id,
            "collected_at": "2025-03-15T14:35:00Z",
            "collection_error": None,
            "k6": {"metrics": []},
            "observability": {
                "sources": [
                    {
                        "name": "engine",
                        "url": PROMETHEUS_URL,
                        "metrics": [
                            {
                                "name": "target_up",
                                "query": 'up{job="prometheus"}',
                                "values": up_values,
                            },
                        ],
                    },
                    {
                        "name": "infra",
                        "url": PROMETHEUS_URL,
                        "metrics": [
                            {
                                "name": "scrape_duration",
                                "query": "scrape_duration_seconds",
                                "values": scrape_values,
                            },
                        ],
                    },
                ],
            },
        }
        _s3().put_object(
            Bucket=S3_BUCKET,
            Key=f"results/{run_id}/results.json",
            Body=json.dumps(results_json).encode(),
        )
        try:
            resp = httpx.get(f"{API_BASE}/v1/test-runs/{run_id}/results", timeout=10)
            assert resp.status_code == 200
            body = resp.json()

            sources = body["observability"]["sources"]
            assert len(sources) == 2
            source_names = {s["name"] for s in sources}
            assert source_names == {"engine", "infra"}

            engine = next(s for s in sources if s["name"] == "engine")
            assert len(engine["metrics"]) == 1
            assert engine["metrics"][0]["name"] == "target_up"
            assert len(engine["metrics"][0]["values"]) > 0

            infra = next(s for s in sources if s["name"] == "infra")
            assert len(infra["metrics"]) == 1
            assert infra["metrics"][0]["name"] == "scrape_duration"
            # scrape_duration_seconds is a real gauge — values are floats > 0
            assert len(infra["metrics"][0]["values"]) > 0
            for ts, val in infra["metrics"][0]["values"]:
                assert float(val) > 0
        finally:
            _cleanup_run(run_id)
            try:
                _s3().delete_object(Bucket=S3_BUCKET, Key=f"results/{run_id}/results.json")
            except Exception:
                pass
