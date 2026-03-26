"""Tests for worker.tasks.dispatcher — cluster_size extraction from cluster_spec."""

import pytest
from unittest.mock import MagicMock, patch

from worker.tasks.dispatcher import dispatcher_task


def _make_plan(scaling_mode="intra_node"):
    return {
        "test_environment": {
            "component_spec": {
                "type": "doris",
                "cluster_info": {"host": "h:9030", "username": "root", "password": ""},
            },
            "target_db": "tpch",
            "fixtures": [],
        },
        "execution": {
            "executor": "k6",
            "scaling_mode": scaling_mode,
            "concurrency": 1,
            "ramp_up": "1s",
            "hold_for": "1s",
            "workload": [],
        },
    }


# ---------------------------------------------------------------------------
# cluster_size extraction from cluster_spec
# ---------------------------------------------------------------------------

@patch("worker.tasks.dispatcher.update_run_status")
@patch("worker.tasks.dispatcher.k6_executor_task")
@patch("worker.tasks.dispatcher.FixtureLoader")
def test_cluster_size_defaults_to_1_when_no_cluster_spec(mock_loader, mock_exec, mock_status):
    """No cluster_spec → cluster_size defaults to 1."""
    mock_loader.return_value.load.return_value = None
    mock_exec.delay.return_value = None
    mock_status.return_value = None

    # dispatcher_task.run is a bound method; Python binds the task instance as
    # self automatically — do not pass a mock self explicitly.
    dispatcher_task.run(_make_plan(), "run-1", cluster_spec=None)

    mock_exec.delay.assert_called_once()
    _, kwargs = mock_exec.delay.call_args
    assert kwargs["local_instances"] == 1


@patch("worker.tasks.dispatcher.update_run_status")
@patch("worker.tasks.dispatcher.k6_executor_task")
@patch("worker.tasks.dispatcher.FixtureLoader")
def test_cluster_size_from_backend_node_count(mock_loader, mock_exec, mock_status):
    """cluster_spec with backend_node.count=4 → cluster_size=4."""
    mock_loader.return_value.load.return_value = None
    mock_exec.delay.return_value = None
    mock_status.return_value = None

    cluster_spec = {"type": "doris", "backend_node": {"count": 4}}
    dispatcher_task.run(_make_plan(), "run-2", cluster_spec=cluster_spec)

    mock_exec.delay.assert_called_once()
    _, kwargs = mock_exec.delay.call_args
    assert kwargs["local_instances"] == 4


@patch("worker.tasks.dispatcher.update_run_status")
@patch("worker.tasks.dispatcher.k6_executor_task")
@patch("worker.tasks.dispatcher.FixtureLoader")
def test_cluster_size_defaults_to_1_when_backend_node_absent(mock_loader, mock_exec, mock_status):
    """cluster_spec present but no backend_node key → cluster_size=1."""
    mock_loader.return_value.load.return_value = None
    mock_exec.delay.return_value = None
    mock_status.return_value = None

    cluster_spec = {"type": "doris", "frontend_node": {"count": 2}}
    dispatcher_task.run(_make_plan(), "run-3", cluster_spec=cluster_spec)

    _, kwargs = mock_exec.delay.call_args
    assert kwargs["local_instances"] == 1


@patch("worker.tasks.dispatcher.update_run_status")
@patch("worker.tasks.dispatcher.k6_executor_task")
@patch("worker.tasks.dispatcher.FixtureLoader")
def test_cluster_size_defaults_to_1_when_backend_node_is_null(mock_loader, mock_exec, mock_status):
    """cluster_spec with backend_node=None (explicit null) → cluster_size=1, no AttributeError."""
    mock_loader.return_value.load.return_value = None
    mock_exec.delay.return_value = None
    mock_status.return_value = None

    # This is the critical regression test: backend_node key exists but value is None.
    # Previously triggered AttributeError: 'NoneType' object has no attribute 'get'
    cluster_spec = {"type": "doris", "backend_node": None}
    dispatcher_task.run(_make_plan(), "run-4", cluster_spec=cluster_spec)

    _, kwargs = mock_exec.delay.call_args
    assert kwargs["local_instances"] == 1


@patch("worker.tasks.dispatcher.update_run_status")
@patch("worker.tasks.dispatcher.k6_executor_task")
@patch("worker.tasks.dispatcher.FixtureLoader")
def test_cluster_spec_none_and_inter_node_uses_size_1(mock_loader, mock_exec, mock_status):
    """No cluster_spec with inter_node → cluster_size=1, checked against active workers."""
    mock_loader.return_value.load.return_value = None
    mock_exec.delay.return_value = None
    mock_status.return_value = None

    inspector = MagicMock()
    inspector.active_queues.return_value = {"worker1": [], "worker2": []}

    with patch("worker.tasks.dispatcher.Inspect", return_value=inspector), \
         patch("worker.tasks.dispatcher.init_waiting_room"), \
         patch("worker.tasks.dispatcher.get_ready_count", return_value=1), \
         patch("worker.tasks.dispatcher.set_start_signal"):
        dispatcher_task.run(_make_plan("inter_node"), "run-5", cluster_spec=None)

    # cluster_size=1: one executor task dispatched
    assert mock_exec.delay.call_count == 1
