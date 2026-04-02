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

_DISPOSABLE_PLAN = {
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

_DISPOSABLE_PLAN_YAML = yaml.dump(_DISPOSABLE_PLAN)

_DORIS_CLUSTER_SPEC = {
    "type": "doris",
    "version": "3.0",
    "frontend_node": {"replica": 1},
    "backend_node": {"replica": 3},
}


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
        response = client.post("/v1/test-runs", json={
            "plan_yaml": _VALID_PLAN_YAML, "plan_name": "test",
            "cluster_spec": _DORIS_CLUSTER_SPEC,
        })
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
        client.post("/v1/test-runs", json={
            "plan_yaml": _VALID_PLAN_YAML, "plan_name": "my-label",
            "cluster_spec": _DORIS_CLUSTER_SPEC,
        })
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
        response = client.post("/v1/test-runs", json={
            "plan_yaml": _VALID_PLAN_YAML, "plan_name": "smoke",
            "cluster_spec": _DORIS_CLUSTER_SPEC,
        })
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
            resp = client.post("/v1/test-runs", json={
                "plan_yaml": _VALID_PLAN_YAML, "plan_name": "repeat",
                "cluster_spec": _DORIS_CLUSTER_SPEC,
            })
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
        response = client.post("/v1/test-runs/smoke-test", json={
            "cluster_spec": _DORIS_CLUSTER_SPEC,
        })
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
        response = client.post("/v1/test-runs/smoke-test", json={
            "cluster_spec": _DORIS_CLUSTER_SPEC,
        })
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
        client.post("/v1/test-runs/smoke-test", json={
            "cluster_spec": _DORIS_CLUSTER_SPEC,
        })
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
            "cluster_spec": _DORIS_CLUSTER_SPEC,
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
        response = client.post("/v1/test-runs/nonexistent", json={
            "cluster_spec": _DORIS_CLUSTER_SPEC,
        })
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
            "cluster_spec": None,
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
            "cluster_spec": None,
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


# ---------------------------------------------------------------------------
# cluster_spec — submit with cluster_spec
# ---------------------------------------------------------------------------

def test_submit_disposable_plan_with_cluster_spec():
    """Disposable plan + cluster_spec should succeed and store spec in DB."""
    mock_s3 = MagicMock()
    with patch("control_plane.routers.test_runs_v1._s3", return_value=mock_s3), \
         patch("control_plane.routers.test_runs_v1._celery") as mock_celery, \
         patch("control_plane.routers.test_runs_v1.db.insert_run", new_callable=AsyncMock) as mock_insert, \
         patch("control_plane.routers.test_runs_v1.asyncio") as mock_asyncio:
        mock_asyncio.to_thread = AsyncMock(side_effect=lambda fn, *a, **kw: fn(*a, **kw))
        mock_celery.send_task.return_value = _mock_celery_result()
        response = client.post("/v1/test-runs", json={
            "plan_yaml": _DISPOSABLE_PLAN_YAML,
            "plan_name": "bench",
            "cluster_spec": _DORIS_CLUSTER_SPEC,
        })
    assert response.status_code == 200
    call_kwargs = mock_insert.call_args[1]
    assert call_kwargs["cluster_spec"] == _DORIS_CLUSTER_SPEC


def test_submit_without_cluster_spec_returns_422():
    """Any plan submitted without cluster_spec should be rejected."""
    response = client.post("/v1/test-runs", json={
        "plan_yaml": _VALID_PLAN_YAML,
        "plan_name": "bench",
    })
    assert response.status_code == 422


def test_submit_cluster_spec_type_mismatch_returns_422():
    """cluster_spec.type must match component_spec.type."""
    bad_spec = {**_DORIS_CLUSTER_SPEC, "type": "cassandra"}
    response = client.post("/v1/test-runs", json={
        "plan_yaml": _DISPOSABLE_PLAN_YAML,
        "plan_name": "bench",
        "cluster_spec": bad_spec,
    })
    assert response.status_code == 422


def test_submit_invalid_cluster_spec_returns_422():
    """Invalid cluster_spec (unknown type) should be rejected."""
    response = client.post("/v1/test-runs", json={
        "plan_yaml": _DISPOSABLE_PLAN_YAML,
        "plan_name": "bench",
        "cluster_spec": {"type": "unknown_db"},
    })
    assert response.status_code == 422


def test_submit_long_lived_plan_without_cluster_spec_returns_422():
    """Long-lived plan without cluster_spec is now also rejected."""
    response = client.post("/v1/test-runs", json={
        "plan_yaml": _VALID_PLAN_YAML,
        "plan_name": "smoke",
    })
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# cluster_spec — trigger by plan name with cluster_spec
# ---------------------------------------------------------------------------

def test_trigger_with_cluster_spec_dispatches_correctly():
    """Trigger an existing disposable plan with cluster_spec in the body."""
    plan_bytes = _DISPOSABLE_PLAN_YAML.encode()
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
        response = client.post("/v1/test-runs/bench", json={
            "cluster_spec": _DORIS_CLUSTER_SPEC,
        })
    assert response.status_code == 200
    # Celery should receive cluster_spec as third positional arg
    celery_args = mock_celery.send_task.call_args
    assert celery_args[1]["args"][2] == _DORIS_CLUSTER_SPEC
    # DB should store cluster_spec
    call_kwargs = mock_insert.call_args[1]
    assert call_kwargs["cluster_spec"] == _DORIS_CLUSTER_SPEC


def test_trigger_with_cluster_spec_stores_label():
    """Trigger with label in body should use that label."""
    plan_bytes = _DISPOSABLE_PLAN_YAML.encode()
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
        response = client.post("/v1/test-runs/bench", json={
            "label": "3-be-nodes",
            "cluster_spec": _DORIS_CLUSTER_SPEC,
        })
    assert response.status_code == 200
    call_kwargs = mock_insert.call_args[1]
    assert call_kwargs["run_label"] == "3-be-nodes"


def test_trigger_without_cluster_spec_returns_422():
    """Triggering any plan without cluster_spec should fail."""
    response = client.post("/v1/test-runs/bench")
    assert response.status_code == 422


def test_trigger_cluster_spec_type_mismatch_returns_422():
    """cluster_spec.type must match the plan's component_spec.type."""
    plan_bytes = _DISPOSABLE_PLAN_YAML.encode()
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=plan_bytes)),
    }
    with patch("control_plane.routers.test_runs_v1._s3", return_value=mock_s3), \
         patch("control_plane.routers.test_runs_v1.asyncio") as mock_asyncio:
        mock_asyncio.to_thread = AsyncMock(side_effect=lambda fn, *a, **kw: fn(*a, **kw))
        response = client.post("/v1/test-runs/bench", json={
            "cluster_spec": {"type": "cassandra"},
        })
    assert response.status_code == 422


def test_submit_empty_dict_cluster_spec_returns_422():
    """An empty dict {} is not a valid cluster_spec (missing required 'type')."""
    response = client.post("/v1/test-runs", json={
        "plan_yaml": _DISPOSABLE_PLAN_YAML,
        "plan_name": "bench",
        "cluster_spec": {},
    })
    assert response.status_code == 422


def test_submit_long_lived_plan_with_cluster_spec_stores_spec():
    """Long-lived plans can optionally accept cluster_spec (for inter-node scaling)."""
    mock_s3 = MagicMock()
    with patch("control_plane.routers.test_runs_v1._s3", return_value=mock_s3), \
         patch("control_plane.routers.test_runs_v1._celery") as mock_celery, \
         patch("control_plane.routers.test_runs_v1.db.insert_run", new_callable=AsyncMock) as mock_insert, \
         patch("control_plane.routers.test_runs_v1.asyncio") as mock_asyncio:
        mock_asyncio.to_thread = AsyncMock(side_effect=lambda fn, *a, **kw: fn(*a, **kw))
        mock_celery.send_task.return_value = _mock_celery_result()
        response = client.post("/v1/test-runs", json={
            "plan_yaml": _VALID_PLAN_YAML,
            "plan_name": "smoke",
            "cluster_spec": _DORIS_CLUSTER_SPEC,
        })
    assert response.status_code == 200
    call_kwargs = mock_insert.call_args[1]
    assert call_kwargs["cluster_spec"] == _DORIS_CLUSTER_SPEC


def test_submit_cluster_spec_passed_as_celery_third_arg():
    """cluster_spec is forwarded to Celery as the third positional argument."""
    mock_s3 = MagicMock()
    with patch("control_plane.routers.test_runs_v1._s3", return_value=mock_s3), \
         patch("control_plane.routers.test_runs_v1._celery") as mock_celery, \
         patch("control_plane.routers.test_runs_v1.db.insert_run", new_callable=AsyncMock), \
         patch("control_plane.routers.test_runs_v1.asyncio") as mock_asyncio:
        mock_asyncio.to_thread = AsyncMock(side_effect=lambda fn, *a, **kw: fn(*a, **kw))
        mock_celery.send_task.return_value = _mock_celery_result()
        client.post("/v1/test-runs", json={
            "plan_yaml": _DISPOSABLE_PLAN_YAML,
            "plan_name": "bench",
            "cluster_spec": _DORIS_CLUSTER_SPEC,
        })
    celery_call = mock_celery.send_task.call_args
    assert celery_call[1]["args"][2] == _DORIS_CLUSTER_SPEC


def test_get_status_includes_cluster_spec_in_response():
    """Run status response should include cluster_spec when present."""
    ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
    spec = {"type": "doris", "backend_node": {"replica": 3}}
    with patch("control_plane.routers.test_runs_v1.db.get_run", new_callable=AsyncMock) as mock:
        mock.return_value = {
            "run_id": "r1", "status": "EXECUTING", "plan_name": "bench",
            "run_label": "test", "sut_type": "doris", "scaling_mode": "intra_node",
            "cluster_spec": spec,
            "submitted_at": ts, "started_at": ts, "completed_at": None,
            "error_detail": None,
        }
        response = client.get("/v1/test-runs/r1/status")
    assert response.status_code == 200
    assert response.json()["cluster_spec"] == spec


# ---------------------------------------------------------------------------
# cluster_settings
# ---------------------------------------------------------------------------

def test_submit_stores_cluster_settings_in_db():
    """cluster_settings string should be persisted via db.insert_run."""
    mock_s3 = MagicMock()
    with patch("control_plane.routers.test_runs_v1._s3", return_value=mock_s3), \
         patch("control_plane.routers.test_runs_v1._celery") as mock_celery, \
         patch("control_plane.routers.test_runs_v1.db.insert_run", new_callable=AsyncMock) as mock_insert, \
         patch("control_plane.routers.test_runs_v1.asyncio") as mock_asyncio:
        mock_asyncio.to_thread = AsyncMock(side_effect=lambda fn, *a, **kw: fn(*a, **kw))
        mock_celery.send_task.return_value = _mock_celery_result()
        client.post("/v1/test-runs", json={
            "plan_yaml": _DISPOSABLE_PLAN_YAML,
            "plan_name": "bench",
            "cluster_spec": _DORIS_CLUSTER_SPEC,
            "cluster_settings": "concurrency=20",
        })
    call_kwargs = mock_insert.call_args[1]
    assert call_kwargs["cluster_settings"] == "concurrency=20"


def test_trigger_stores_cluster_settings_in_db():
    """cluster_settings in trigger body should be persisted via db.insert_run."""
    plan_bytes = _DISPOSABLE_PLAN_YAML.encode()
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
        client.post("/v1/test-runs/bench", json={
            "cluster_spec": _DORIS_CLUSTER_SPEC,
            "cluster_settings": "replicas=5",
        })
    call_kwargs = mock_insert.call_args[1]
    assert call_kwargs["cluster_settings"] == "replicas=5"


def test_get_status_includes_cluster_settings_in_response():
    """Run status response should include cluster_settings when present."""
    ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
    with patch("control_plane.routers.test_runs_v1.db.get_run", new_callable=AsyncMock) as mock:
        mock.return_value = {
            "run_id": "r1", "status": "EXECUTING", "plan_name": "bench",
            "run_label": "test", "sut_type": "doris", "scaling_mode": "intra_node",
            "cluster_spec": None, "cluster_settings": "concurrency=10",
            "submitted_at": ts, "started_at": ts, "completed_at": None,
            "error_detail": None,
        }
        response = client.get("/v1/test-runs/r1/status")
    assert response.status_code == 200
    assert response.json()["cluster_settings"] == "concurrency=10"


# ---------------------------------------------------------------------------
# GET /v1/test-runs — list test runs
# ---------------------------------------------------------------------------

def _make_run_row(run_id, run_label="bench", status="COMPLETED"):
    ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
    return {
        "run_id": run_id, "plan_name": "smoke", "run_label": run_label,
        "sut_type": "doris", "status": status, "scaling_mode": "intra_node",
        "cluster_spec": None, "cluster_settings": None,
        "submitted_at": ts, "started_at": ts, "completed_at": ts,
        "error_detail": None,
    }


def test_list_runs_returns_all():
    """GET /v1/test-runs with no filter returns all runs."""
    rows = [_make_run_row("r1"), _make_run_row("r2")]
    with patch("control_plane.routers.test_runs_v1.db.list_runs", new_callable=AsyncMock) as mock:
        mock.return_value = rows
        response = client.get("/v1/test-runs")
    assert response.status_code == 200
    data = response.json()
    assert len(data["runs"]) == 2
    mock.assert_awaited_once_with(run_label=None)


def test_list_runs_filters_by_run_label():
    """GET /v1/test-runs?run_label=bench filters by label."""
    rows = [_make_run_row("r1", run_label="bench")]
    with patch("control_plane.routers.test_runs_v1.db.list_runs", new_callable=AsyncMock) as mock:
        mock.return_value = rows
        response = client.get("/v1/test-runs", params={"run_label": "bench"})
    assert response.status_code == 200
    data = response.json()
    assert len(data["runs"]) == 1
    assert data["runs"][0]["run_label"] == "bench"
    mock.assert_awaited_once_with(run_label="bench")


def test_list_runs_empty_result():
    """GET /v1/test-runs returns empty list when no runs match."""
    with patch("control_plane.routers.test_runs_v1.db.list_runs", new_callable=AsyncMock) as mock:
        mock.return_value = []
        response = client.get("/v1/test-runs", params={"run_label": "nonexistent"})
    assert response.status_code == 200
    assert response.json()["runs"] == []


def test_list_runs_includes_all_metadata_fields():
    """Each run in the list should include all metadata fields."""
    ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
    row = {
        "run_id": "r1", "plan_name": "smoke", "run_label": "bench",
        "sut_type": "doris", "status": "COMPLETED", "scaling_mode": "intra_node",
        "cluster_spec": {"type": "doris", "backend_node": {"replica": 3}},
        "cluster_settings": "concurrency=20",
        "submitted_at": ts, "started_at": ts, "completed_at": ts,
        "error_detail": None,
    }
    with patch("control_plane.routers.test_runs_v1.db.list_runs", new_callable=AsyncMock) as mock:
        mock.return_value = [row]
        response = client.get("/v1/test-runs")
    run = response.json()["runs"][0]
    assert run["run_id"] == "r1"
    assert run["cluster_spec"] == {"type": "doris", "backend_node": {"replica": 3}}
    assert run["cluster_settings"] == "concurrency=20"
    assert run["started_at"] is not None
    assert run["completed_at"] is not None
    assert run["error_detail"] is None


# ---------------------------------------------------------------------------
# GET /v1/test-runs/{run_id}/results
# ---------------------------------------------------------------------------

_SAMPLE_RESULTS_JSON = {
    "run_id": "r1",
    "collected_at": "2025-01-01T00:05:00+00:00",
    "collection_error": None,
    "k6": {
        "metrics": [
            {
                "name": "sql_duration_Q1",
                "type": "trend",
                "stats": {
                    "count": 100, "min": 1.0, "max": 50.0,
                    "avg": 10.0, "med": 8.0, "p90": 30.0,
                    "p95": 40.0, "p99": 48.0, "rate": None,
                },
            },
        ],
    },
    "observability": {
        "sources": [
            {
                "name": "engine",
                "url": "http://prom:9090",
                "metrics": [
                    {"name": "cluster_qps", "query": "sum(rate(x[1m]))", "values": [[1710510600, "245.3"]]},
                ],
            },
        ],
    },
}


def _mock_s3_get_results(results_json, chaos_events=None):
    """Create a mock S3 client that returns results_json and optional chaos_events.

    ``results_json`` is returned for keys ending in ``results.json``.
    ``chaos_events`` (list) is returned for ``chaos_events.json``; when
    ``None`` the mock raises ``NoSuchKey`` (no chaos was configured).
    """
    import json as _json
    mock_s3 = MagicMock()

    def _get_object(**kwargs):
        key = kwargs.get("Key", "")
        if key.endswith("results.json"):
            body_bytes = _json.dumps(results_json).encode()
            return {"Body": MagicMock(read=MagicMock(return_value=body_bytes))}
        if key.endswith("chaos_events.json"):
            if chaos_events is not None:
                body_bytes = _json.dumps(chaos_events).encode()
                return {"Body": MagicMock(read=MagicMock(return_value=body_bytes))}
            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "Not found"}},
                "GetObject",
            )
        raise ValueError(f"Unexpected S3 key: {key}")

    mock_s3.get_object.side_effect = _get_object
    return mock_s3


def _patch_asyncio():
    """Return a mock asyncio that supports both to_thread and gather."""
    mock = MagicMock()
    mock.to_thread = AsyncMock(side_effect=lambda fn, *a, **kw: fn(*a, **kw))

    async def _fake_gather(*coros):
        return tuple([await c for c in coros])
    mock.gather = _fake_gather
    return mock


def test_results_returns_200_for_completed_run():
    mock_s3 = _mock_s3_get_results(_SAMPLE_RESULTS_JSON)
    with patch("control_plane.routers.test_runs_v1.db.get_run", new_callable=AsyncMock) as mock_db, \
         patch("control_plane.routers.test_runs_v1._s3", return_value=mock_s3), \
         patch("control_plane.routers.test_runs_v1.asyncio", _patch_asyncio()):
        mock_db.return_value = _make_run_row("r1", status="COMPLETED")
        response = client.get("/v1/test-runs/r1/results")
    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == "r1"
    assert body["status"] == "COMPLETED"
    assert body["collection_error"] is None
    assert len(body["k6"]["metrics"]) == 1
    assert body["k6"]["metrics"][0]["name"] == "sql_duration_Q1"
    assert len(body["observability"]["sources"]) == 1
    assert body["chaos"]["events"] == []


def test_results_returns_404_for_unknown_run():
    with patch("control_plane.routers.test_runs_v1.db.get_run", new_callable=AsyncMock) as mock_db:
        mock_db.return_value = None
        response = client.get("/v1/test-runs/nonexistent/results")
    assert response.status_code == 404


def test_results_returns_409_for_pending_run():
    with patch("control_plane.routers.test_runs_v1.db.get_run", new_callable=AsyncMock) as mock_db:
        mock_db.return_value = _make_run_row("r1", status="PENDING")
        response = client.get("/v1/test-runs/r1/results")
    assert response.status_code == 409


def test_results_returns_409_for_executing_run():
    with patch("control_plane.routers.test_runs_v1.db.get_run", new_callable=AsyncMock) as mock_db:
        mock_db.return_value = _make_run_row("r1", status="EXECUTING")
        response = client.get("/v1/test-runs/r1/results")
    assert response.status_code == 409


def test_results_returns_200_for_failed_run():
    """FAILED is a terminal status — results should still be returned."""
    mock_s3 = _mock_s3_get_results(_SAMPLE_RESULTS_JSON)
    with patch("control_plane.routers.test_runs_v1.db.get_run", new_callable=AsyncMock) as mock_db, \
         patch("control_plane.routers.test_runs_v1._s3", return_value=mock_s3), \
         patch("control_plane.routers.test_runs_v1.asyncio", _patch_asyncio()):
        mock_db.return_value = _make_run_row("r1", status="FAILED")
        response = client.get("/v1/test-runs/r1/results")
    assert response.status_code == 200
    assert response.json()["status"] == "FAILED"


def test_results_returns_409_for_waiting_room_run():
    with patch("control_plane.routers.test_runs_v1.db.get_run", new_callable=AsyncMock) as mock_db:
        mock_db.return_value = _make_run_row("r1", status="WAITING_ROOM")
        response = client.get("/v1/test-runs/r1/results")
    assert response.status_code == 409


def test_results_handles_missing_s3_file():
    """COLLECTING run with no results.json yet → empty results with error message."""
    mock_s3 = MagicMock()
    error_response = {"Error": {"Code": "NoSuchKey", "Message": "Not found"}}
    mock_s3.get_object.side_effect = ClientError(error_response, "GetObject")
    with patch("control_plane.routers.test_runs_v1.db.get_run", new_callable=AsyncMock) as mock_db, \
         patch("control_plane.routers.test_runs_v1._s3", return_value=mock_s3), \
         patch("control_plane.routers.test_runs_v1.asyncio", _patch_asyncio()):
        mock_db.return_value = _make_run_row("r1", status="COLLECTING")
        response = client.get("/v1/test-runs/r1/results")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "COLLECTING"
    assert "not yet available" in body["collection_error"]
    assert body["k6"]["metrics"] == []
    assert body["observability"]["sources"] == []
    assert body["chaos"]["events"] == []


def test_results_includes_collection_error():
    """Results with collection_error should surface it in the response."""
    results_with_error = {
        **_SAMPLE_RESULTS_JSON,
        "collection_error": "Prometheus source 'infra' unreachable",
    }
    mock_s3 = _mock_s3_get_results(results_with_error)
    with patch("control_plane.routers.test_runs_v1.db.get_run", new_callable=AsyncMock) as mock_db, \
         patch("control_plane.routers.test_runs_v1._s3", return_value=mock_s3), \
         patch("control_plane.routers.test_runs_v1.asyncio", _patch_asyncio()):
        mock_db.return_value = _make_run_row("r1", status="COMPLETED")
        response = client.get("/v1/test-runs/r1/results")
    assert response.status_code == 200
    body = response.json()
    assert "unreachable" in body["collection_error"]
    # k6 metrics should still be present
    assert len(body["k6"]["metrics"]) == 1


# ---------------------------------------------------------------------------
# GET /v1/test-runs/{run_id}/results — chaos events
# ---------------------------------------------------------------------------

_SAMPLE_CHAOS_EVENTS = [
    {
        "experiment": "kill-doris-be",
        "fault_type": "PodChaos",
        "target": {"env_type": "k8s", "namespace": "doris", "selector": {"app": "doris-be"}},
        "injected_at": "2025-01-01T00:01:00+00:00",
        "recovered_at": "2025-01-01T00:03:00+00:00",
        "duration_seconds": 120.0,
        "engine": "k8s",
        "engine_status": {
            "status": "recovered",
            "created_at": "2025-01-01T00:01:00Z",
            "updated_at": "2025-01-01T00:03:00Z",
            "targets": [{"id": "doris-be-0", "inject_time": "2025-01-01T00:01:00Z", "recover_time": "2025-01-01T00:03:00Z"}],
        },
        "raw_engine_status": {"conditions": [{"type": "AllInjected", "status": "True"}]},
    },
]


def test_results_includes_chaos_events():
    """Chaos events from S3 are merged into the response."""
    mock_s3 = _mock_s3_get_results(_SAMPLE_RESULTS_JSON, chaos_events=_SAMPLE_CHAOS_EVENTS)
    with patch("control_plane.routers.test_runs_v1.db.get_run", new_callable=AsyncMock) as mock_db, \
         patch("control_plane.routers.test_runs_v1._s3", return_value=mock_s3), \
         patch("control_plane.routers.test_runs_v1.asyncio", _patch_asyncio()):
        mock_db.return_value = _make_run_row("r1", status="COMPLETED")
        response = client.get("/v1/test-runs/r1/results")
    assert response.status_code == 200
    body = response.json()
    assert len(body["chaos"]["events"]) == 1
    evt = body["chaos"]["events"][0]
    assert evt["experiment"] == "kill-doris-be"
    assert evt["fault_type"] == "PodChaos"
    assert evt["duration_seconds"] == 120.0
    assert evt["engine_status"]["status"] == "recovered"
    assert evt["engine_status"]["targets"][0]["id"] == "doris-be-0"
    assert evt["raw_engine_status"]["conditions"][0]["type"] == "AllInjected"
    # k6 still present
    assert len(body["k6"]["metrics"]) == 1


def test_results_no_chaos_file_returns_empty_events():
    """When chaos_events.json does not exist, chaos.events is an empty list."""
    mock_s3 = _mock_s3_get_results(_SAMPLE_RESULTS_JSON, chaos_events=None)
    with patch("control_plane.routers.test_runs_v1.db.get_run", new_callable=AsyncMock) as mock_db, \
         patch("control_plane.routers.test_runs_v1._s3", return_value=mock_s3), \
         patch("control_plane.routers.test_runs_v1.asyncio", _patch_asyncio()):
        mock_db.return_value = _make_run_row("r1", status="COMPLETED")
        response = client.get("/v1/test-runs/r1/results")
    assert response.status_code == 200
    assert response.json()["chaos"]["events"] == []


def test_results_chaos_events_present_when_results_missing():
    """Chaos events should appear even when results.json is not yet available."""
    import json as _json

    mock_s3 = MagicMock()

    def _get_object(**kwargs):
        key = kwargs.get("Key", "")
        if key.endswith("results.json"):
            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "Not found"}},
                "GetObject",
            )
        if key.endswith("chaos_events.json"):
            body_bytes = _json.dumps(_SAMPLE_CHAOS_EVENTS).encode()
            return {"Body": MagicMock(read=MagicMock(return_value=body_bytes))}
        raise ValueError(f"Unexpected S3 key: {key}")

    mock_s3.get_object.side_effect = _get_object
    with patch("control_plane.routers.test_runs_v1.db.get_run", new_callable=AsyncMock) as mock_db, \
         patch("control_plane.routers.test_runs_v1._s3", return_value=mock_s3), \
         patch("control_plane.routers.test_runs_v1.asyncio", _patch_asyncio()):
        mock_db.return_value = _make_run_row("r1", status="COLLECTING")
        response = client.get("/v1/test-runs/r1/results")
    assert response.status_code == 200
    body = response.json()
    assert "not yet available" in body["collection_error"]
    assert len(body["chaos"]["events"]) == 1
    assert body["chaos"]["events"][0]["experiment"] == "kill-doris-be"


def test_results_chaos_s3_error_propagates():
    """Non-NoSuchKey S3 errors on chaos_events.json should propagate."""
    mock_s3 = MagicMock()

    def _get_object(**kwargs):
        key = kwargs.get("Key", "")
        if key.endswith("results.json"):
            import json as _json
            body_bytes = _json.dumps(_SAMPLE_RESULTS_JSON).encode()
            return {"Body": MagicMock(read=MagicMock(return_value=body_bytes))}
        if key.endswith("chaos_events.json"):
            raise ClientError(
                {"Error": {"Code": "AccessDenied", "Message": "Forbidden"}},
                "GetObject",
            )
        raise ValueError(f"Unexpected S3 key: {key}")

    mock_s3.get_object.side_effect = _get_object
    with patch("control_plane.routers.test_runs_v1.db.get_run", new_callable=AsyncMock) as mock_db, \
         patch("control_plane.routers.test_runs_v1._s3", return_value=mock_s3), \
         patch("control_plane.routers.test_runs_v1.asyncio", _patch_asyncio()):
        mock_db.return_value = _make_run_row("r1", status="COMPLETED")
        with pytest.raises(ClientError):
            client.get("/v1/test-runs/r1/results")


# ---------------------------------------------------------------------------
# ChaosEvent Pydantic model — engine_status / raw_engine_status
# ---------------------------------------------------------------------------


def test_chaos_event_model_parses_engine_status():
    """ChaosEvent model correctly deserializes engine_status into ChaosEngineStatus."""
    from control_plane.models import ChaosEvent

    evt = ChaosEvent(
        experiment="test",
        fault_type="PodChaos",
        target={"env_type": "k8s"},
        engine="k8s",
        engine_status={
            "status": "recovered",
            "created_at": "2025-01-01T00:01:00Z",
            "updated_at": "2025-01-01T00:03:00Z",
            "targets": [
                {"id": "pod-0", "inject_time": "2025-01-01T00:01:00Z", "recover_time": "2025-01-01T00:03:00Z"},
            ],
        },
        raw_engine_status={"conditions": []},
    )
    assert evt.engine_status.status == "recovered"
    assert len(evt.engine_status.targets) == 1
    assert evt.engine_status.targets[0].id == "pod-0"
    assert evt.raw_engine_status == {"conditions": []}


def test_chaos_event_model_accepts_null_statuses():
    """ChaosEvent allows both engine_status and raw_engine_status to be None."""
    from control_plane.models import ChaosEvent

    evt = ChaosEvent(
        experiment="test",
        fault_type="PodChaos",
        target={"env_type": "ec2"},
        engine="ec2",
        engine_status=None,
        raw_engine_status=None,
    )
    assert evt.engine_status is None
    assert evt.raw_engine_status is None
