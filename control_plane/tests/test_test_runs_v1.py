"""Tests for control_plane.routers.test_runs_v1 — V1 test run endpoints."""

from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone

import pytest
import yaml
from fastapi.testclient import TestClient

from control_plane.main import app

client = TestClient(app)

_VALID_PLAN = {
    "test_metadata": {"run_label": "smoke-test"},
    "test_environment": {
        "env_type": "long-lived",
        "target_db": "tpch",
        "component_spec": {
            "type": "doris",
            "cluster_info": {"host": "doris-fe:9030", "username": "root", "password": ""},
        },
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

_VALID_PLAN_YAML = yaml.dump(_VALID_PLAN)


def _mock_celery_result(task_id: str = "task-123"):
    m = MagicMock()
    m.id = task_id
    return m


# ---------------------------------------------------------------------------
# POST /v1/test-runs — submit
# ---------------------------------------------------------------------------

def test_submit_valid_plan_returns_200():
    mock_s3 = MagicMock()
    mock_celery_result = _mock_celery_result()
    with patch("control_plane.routers.test_runs_v1._s3", return_value=mock_s3), \
         patch("control_plane.routers.test_runs_v1._celery") as mock_celery, \
         patch("control_plane.routers.test_runs_v1.db.insert_run", new_callable=AsyncMock), \
         patch("control_plane.routers.test_runs_v1.asyncio") as mock_asyncio:
        # Make asyncio.to_thread just call the function
        mock_asyncio.to_thread = AsyncMock(side_effect=lambda fn, *a, **kw: fn(*a, **kw))
        mock_celery.send_task.return_value = mock_celery_result
        response = client.post("/v1/test-runs", json={"plan_yaml": _VALID_PLAN_YAML, "label": "test"})
    assert response.status_code == 200
    body = response.json()
    assert "run_id" in body
    assert body["strategy"] == "intra_node"


def test_submit_returns_422_for_invalid_yaml():
    response = client.post("/v1/test-runs", json={"plan_yaml": "{{bad", "label": "test"})
    assert response.status_code == 422


def test_submit_returns_422_for_invalid_plan():
    # Valid YAML but missing required fields
    response = client.post("/v1/test-runs", json={"plan_yaml": "foo: bar", "label": "test"})
    assert response.status_code == 422


def test_submit_inserts_run_into_db():
    mock_s3 = MagicMock()
    with patch("control_plane.routers.test_runs_v1._s3", return_value=mock_s3), \
         patch("control_plane.routers.test_runs_v1._celery") as mock_celery, \
         patch("control_plane.routers.test_runs_v1.db.insert_run", new_callable=AsyncMock) as mock_insert, \
         patch("control_plane.routers.test_runs_v1.asyncio") as mock_asyncio:
        mock_asyncio.to_thread = AsyncMock(side_effect=lambda fn, *a, **kw: fn(*a, **kw))
        mock_celery.send_task.return_value = _mock_celery_result()
        client.post("/v1/test-runs", json={"plan_yaml": _VALID_PLAN_YAML, "label": "my-label"})
    mock_insert.assert_awaited_once()
    call_kwargs = mock_insert.call_args[1]
    assert call_kwargs["run_label"] == "my-label"
    assert call_kwargs["sut_type"] == "doris"
    assert call_kwargs["scaling_mode"] == "intra_node"


# ---------------------------------------------------------------------------
# GET /v1/test-runs/{run_id}/status
# ---------------------------------------------------------------------------

def test_get_status_returns_run():
    ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
    with patch("control_plane.routers.test_runs_v1.db.get_run", new_callable=AsyncMock) as mock:
        mock.return_value = {
            "run_id": "r1", "status": "EXECUTING", "run_label": "test",
            "sut_type": "doris", "scaling_mode": "intra_node",
            "submitted_at": ts, "started_at": ts, "completed_at": None,
            "error_detail": None,
        }
        response = client.get("/v1/test-runs/r1/status")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "EXECUTING"
    assert body["waiting_room"] is None


def test_get_status_returns_404_for_missing_run():
    with patch("control_plane.routers.test_runs_v1.db.get_run", new_callable=AsyncMock) as mock:
        mock.return_value = None
        response = client.get("/v1/test-runs/nonexistent/status")
    assert response.status_code == 404


def test_get_status_enriches_waiting_room():
    ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
    with patch("control_plane.routers.test_runs_v1.db.get_run", new_callable=AsyncMock) as mock_run, \
         patch("control_plane.routers.test_runs_v1.db.get_waiting_room_info", new_callable=AsyncMock) as mock_wr:
        mock_run.return_value = {
            "run_id": "r1", "status": "WAITING_ROOM", "run_label": "test",
            "sut_type": "doris", "scaling_mode": "inter_node",
            "submitted_at": ts, "started_at": None, "completed_at": None,
            "error_detail": None,
        }
        mock_wr.return_value = {"ready_count": 2, "target_count": 4}
        response = client.get("/v1/test-runs/r1/status")
    body = response.json()
    assert body["waiting_room"]["ready_count"] == 2
    assert body["waiting_room"]["target_count"] == 4


# ---------------------------------------------------------------------------
# POST /v1/test-runs/{run_id}/stop
# ---------------------------------------------------------------------------

def test_stop_run_sends_sigterm():
    with patch("control_plane.routers.test_runs_v1.db.get_run", new_callable=AsyncMock) as mock_run, \
         patch("control_plane.routers.test_runs_v1.db.update_run_status", new_callable=AsyncMock), \
         patch("control_plane.routers.test_runs_v1._celery") as mock_celery, \
         patch("control_plane.routers.test_runs_v1.asyncio") as mock_asyncio:
        mock_asyncio.to_thread = AsyncMock(side_effect=lambda fn, *a, **kw: fn(*a, **kw))
        mock_run.return_value = {"run_id": "r1", "status": "EXECUTING", "task_id": "t1"}
        response = client.post("/v1/test-runs/r1/stop")
    assert response.status_code == 200
    assert response.json()["action"] == "sigterm_sent"
    mock_celery.control.revoke.assert_called_once_with("t1", terminate=True, signal="SIGTERM")


def test_stop_run_returns_not_found_for_missing():
    with patch("control_plane.routers.test_runs_v1.db.get_run", new_callable=AsyncMock) as mock:
        mock.return_value = None
        response = client.post("/v1/test-runs/missing/stop")
    assert response.json()["action"] == "not_found"


def test_stop_run_returns_already_stopped():
    with patch("control_plane.routers.test_runs_v1.db.get_run", new_callable=AsyncMock) as mock:
        mock.return_value = {"run_id": "r1", "status": "COMPLETED", "task_id": "t1"}
        response = client.post("/v1/test-runs/r1/stop")
    assert response.json()["action"] == "already_stopped"


def test_stop_run_handles_no_task_id():
    """If task_id is None (edge case), stop should not call revoke."""
    with patch("control_plane.routers.test_runs_v1.db.get_run", new_callable=AsyncMock) as mock_run, \
         patch("control_plane.routers.test_runs_v1.db.update_run_status", new_callable=AsyncMock), \
         patch("control_plane.routers.test_runs_v1._celery") as mock_celery:
        mock_run.return_value = {"run_id": "r1", "status": "EXECUTING", "task_id": None}
        response = client.post("/v1/test-runs/r1/stop")
    assert response.json()["action"] == "sigterm_sent"
    mock_celery.control.revoke.assert_not_called()


# ---------------------------------------------------------------------------
# GET /v1/test-runs/{run_id}/artifacts
# ---------------------------------------------------------------------------

def test_list_artifacts_returns_empty():
    mock_s3 = MagicMock()
    paginator = MagicMock()
    paginator.paginate.return_value = [{"Contents": []}]
    mock_s3.get_paginator.return_value = paginator
    with patch("control_plane.routers.test_runs_v1._s3", return_value=mock_s3), \
         patch("control_plane.routers.test_runs_v1.asyncio") as mock_asyncio:
        mock_asyncio.to_thread = AsyncMock(side_effect=lambda fn, *a, **kw: fn(*a, **kw))
        response = client.get("/v1/test-runs/r1/artifacts")
    assert response.status_code == 200
    assert response.json()["artifacts"] == []


def test_list_artifacts_returns_files():
    mock_s3 = MagicMock()
    paginator = MagicMock()
    paginator.paginate.return_value = [
        {"Contents": [
            {"Key": "results/r1/k6_raw_0.csv", "Size": 1024},
            {"Key": "results/r1/k6_raw_1.csv", "Size": 2048},
        ]}
    ]
    mock_s3.get_paginator.return_value = paginator
    with patch("control_plane.routers.test_runs_v1._s3", return_value=mock_s3), \
         patch("control_plane.routers.test_runs_v1.asyncio") as mock_asyncio:
        mock_asyncio.to_thread = AsyncMock(side_effect=lambda fn, *a, **kw: fn(*a, **kw))
        response = client.get("/v1/test-runs/r1/artifacts")
    body = response.json()
    assert len(body["artifacts"]) == 2
    assert body["artifacts"][0]["key"] == "results/r1/k6_raw_0.csv"
