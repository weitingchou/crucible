"""Tests for control_plane.routers.test_runs_v1 — V1 test run endpoints."""

import re
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone

import pytest
import yaml
from botocore.exceptions import ClientError
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
        response = client.post("/v1/test-runs", json={"plan_yaml": _VALID_PLAN_YAML, "plan_name": "test"})
    assert response.status_code == 200
    body = response.json()
    assert "run_id" in body
    assert body["run_id"].startswith("test_")
    assert body["plan_key"] == "plans/test"
    assert body["strategy"] == "intra_node"


def test_submit_returns_422_for_invalid_yaml():
    response = client.post("/v1/test-runs", json={"plan_yaml": "{{bad", "plan_name": "test"})
    assert response.status_code == 422


def test_submit_returns_422_for_invalid_plan():
    # Valid YAML but missing required fields
    response = client.post("/v1/test-runs", json={"plan_yaml": "foo: bar", "plan_name": "test"})
    assert response.status_code == 422


def test_submit_inserts_run_into_db():
    mock_s3 = MagicMock()
    with patch("control_plane.routers.test_runs_v1._s3", return_value=mock_s3), \
         patch("control_plane.routers.test_runs_v1._celery") as mock_celery, \
         patch("control_plane.routers.test_runs_v1.db.insert_run", new_callable=AsyncMock) as mock_insert, \
         patch("control_plane.routers.test_runs_v1.asyncio") as mock_asyncio:
        mock_asyncio.to_thread = AsyncMock(side_effect=lambda fn, *a, **kw: fn(*a, **kw))
        mock_celery.send_task.return_value = _mock_celery_result()
        client.post("/v1/test-runs", json={"plan_yaml": _VALID_PLAN_YAML, "plan_name": "my-label"})
    mock_insert.assert_awaited_once()
    call_kwargs = mock_insert.call_args[1]
    assert call_kwargs["plan_name"] == "my-label"
    assert call_kwargs["run_label"] == "my-label"
    assert call_kwargs["plan_key"] == "plans/my-label"
    assert call_kwargs["sut_type"] == "doris"
    assert call_kwargs["scaling_mode"] == "intra_node"


def test_run_id_format():
    """run_id should match ``{label}_{YYYYMMDD-HHmm}_{8hex}``."""
    mock_s3 = MagicMock()
    with patch("control_plane.routers.test_runs_v1._s3", return_value=mock_s3), \
         patch("control_plane.routers.test_runs_v1._celery") as mock_celery, \
         patch("control_plane.routers.test_runs_v1.db.insert_run", new_callable=AsyncMock), \
         patch("control_plane.routers.test_runs_v1.asyncio") as mock_asyncio:
        mock_asyncio.to_thread = AsyncMock(side_effect=lambda fn, *a, **kw: fn(*a, **kw))
        mock_celery.send_task.return_value = _mock_celery_result()
        response = client.post("/v1/test-runs", json={"plan_yaml": _VALID_PLAN_YAML, "plan_name": "smoke"})
    run_id = response.json()["run_id"]
    assert re.fullmatch(r"smoke_\d{8}-\d{4}_[0-9a-f]{8}", run_id), f"run_id format unexpected: {run_id}"


def test_same_label_produces_same_plan_key():
    """Two submits with the same label produce the same plan_key but different run_ids."""
    run_ids = []
    plan_keys = []
    for _ in range(2):
        mock_s3 = MagicMock()
        with patch("control_plane.routers.test_runs_v1._s3", return_value=mock_s3), \
             patch("control_plane.routers.test_runs_v1._celery") as mock_celery, \
             patch("control_plane.routers.test_runs_v1.db.insert_run", new_callable=AsyncMock), \
             patch("control_plane.routers.test_runs_v1.asyncio") as mock_asyncio:
            mock_asyncio.to_thread = AsyncMock(side_effect=lambda fn, *a, **kw: fn(*a, **kw))
            mock_celery.send_task.return_value = _mock_celery_result()
            resp = client.post("/v1/test-runs", json={"plan_yaml": _VALID_PLAN_YAML, "plan_name": "repeat"})
        body = resp.json()
        run_ids.append(body["run_id"])
        plan_keys.append(body["plan_key"])

    assert plan_keys[0] == plan_keys[1] == "plans/repeat"
    assert run_ids[0] != run_ids[1]


# ---------------------------------------------------------------------------
# POST /v1/test-runs/{plan_name} — trigger by plan name
# ---------------------------------------------------------------------------

def test_trigger_by_plan_name_returns_200():
    plan_bytes = _VALID_PLAN_YAML.encode()
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=plan_bytes)),
    }
    with patch("control_plane.routers.test_runs_v1._s3", return_value=mock_s3), \
         patch("control_plane.routers.test_runs_v1._celery") as mock_celery, \
         patch("control_plane.routers.test_runs_v1.db.insert_run", new_callable=AsyncMock), \
         patch("control_plane.routers.test_runs_v1.asyncio") as mock_asyncio:
        mock_asyncio.to_thread = AsyncMock(side_effect=lambda fn, *a, **kw: fn(*a, **kw))
        mock_celery.send_task.return_value = _mock_celery_result()
        response = client.post("/v1/test-runs/smoke-test")
    assert response.status_code == 200
    body = response.json()
    assert body["run_id"].startswith("smoke-test_")
    assert body["plan_key"] == "plans/smoke-test"
    assert body["strategy"] == "intra_node"


def test_trigger_by_plan_name_run_id_format():
    """run_id for trigger-by-plan should match ``{plan_name}_{YYYYMMDD-HHmm}_{8hex}``."""
    plan_bytes = _VALID_PLAN_YAML.encode()
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=plan_bytes)),
    }
    with patch("control_plane.routers.test_runs_v1._s3", return_value=mock_s3), \
         patch("control_plane.routers.test_runs_v1._celery") as mock_celery, \
         patch("control_plane.routers.test_runs_v1.db.insert_run", new_callable=AsyncMock), \
         patch("control_plane.routers.test_runs_v1.asyncio") as mock_asyncio:
        mock_asyncio.to_thread = AsyncMock(side_effect=lambda fn, *a, **kw: fn(*a, **kw))
        mock_celery.send_task.return_value = _mock_celery_result()
        response = client.post("/v1/test-runs/smoke-test")
    run_id = response.json()["run_id"]
    assert re.fullmatch(r"smoke-test_\d{8}-\d{4}_[0-9a-f]{8}", run_id), f"run_id format unexpected: {run_id}"


def test_trigger_by_plan_name_inserts_plan_name_in_db():
    plan_bytes = _VALID_PLAN_YAML.encode()
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=plan_bytes)),
    }
    with patch("control_plane.routers.test_runs_v1._s3", return_value=mock_s3), \
         patch("control_plane.routers.test_runs_v1._celery") as mock_celery, \
         patch("control_plane.routers.test_runs_v1.db.insert_run", new_callable=AsyncMock) as mock_insert, \
         patch("control_plane.routers.test_runs_v1.asyncio") as mock_asyncio:
        mock_asyncio.to_thread = AsyncMock(side_effect=lambda fn, *a, **kw: fn(*a, **kw))
        mock_celery.send_task.return_value = _mock_celery_result()
        client.post("/v1/test-runs/smoke-test")
    mock_insert.assert_awaited_once()
    call_kwargs = mock_insert.call_args[1]
    assert call_kwargs["plan_name"] == "smoke-test"
    assert call_kwargs["plan_key"] == "plans/smoke-test"


def test_submit_explicit_label_overrides_plan_name_in_db():
    """When label is provided, run_label in DB should be the label, not plan_name."""
    mock_s3 = MagicMock()
    with patch("control_plane.routers.test_runs_v1._s3", return_value=mock_s3), \
         patch("control_plane.routers.test_runs_v1._celery") as mock_celery, \
         patch("control_plane.routers.test_runs_v1.db.insert_run", new_callable=AsyncMock) as mock_insert, \
         patch("control_plane.routers.test_runs_v1.asyncio") as mock_asyncio:
        mock_asyncio.to_thread = AsyncMock(side_effect=lambda fn, *a, **kw: fn(*a, **kw))
        mock_celery.send_task.return_value = _mock_celery_result()
        client.post("/v1/test-runs", json={
            "plan_yaml": _VALID_PLAN_YAML,
            "plan_name": "smoke",
            "label": "My custom label",
        })
    call_kwargs = mock_insert.call_args[1]
    assert call_kwargs["run_label"] == "My custom label"
    assert call_kwargs["plan_name"] == "smoke"


def test_trigger_by_plan_name_returns_404_when_missing():
    mock_s3 = MagicMock()
    error_response = {"Error": {"Code": "NoSuchKey", "Message": "Not found"}}
    mock_s3.get_object.side_effect = ClientError(error_response, "GetObject")
    with patch("control_plane.routers.test_runs_v1._s3", return_value=mock_s3), \
         patch("control_plane.routers.test_runs_v1.asyncio") as mock_asyncio:
        mock_asyncio.to_thread = AsyncMock(side_effect=lambda fn, *a, **kw: fn(*a, **kw))
        response = client.post("/v1/test-runs/nonexistent")
    assert response.status_code == 404


# ---------------------------------------------------------------------------
# GET /v1/test-runs/{run_id}/status
# ---------------------------------------------------------------------------

def test_get_status_returns_run():
    ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
    with patch("control_plane.routers.test_runs_v1.db.get_run", new_callable=AsyncMock) as mock:
        mock.return_value = {
            "run_id": "r1", "status": "EXECUTING", "plan_name": "smoke",
            "run_label": "test", "sut_type": "doris", "scaling_mode": "intra_node",
            "submitted_at": ts, "started_at": ts, "completed_at": None,
            "error_detail": None,
        }
        response = client.get("/v1/test-runs/r1/status")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "EXECUTING"
    assert body["plan_name"] == "smoke"
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
            "run_id": "r1", "status": "WAITING_ROOM", "plan_name": "smoke",
            "run_label": "test", "sut_type": "doris", "scaling_mode": "inter_node",
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
