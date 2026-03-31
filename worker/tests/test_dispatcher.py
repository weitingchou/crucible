"""Tests for worker.tasks.dispatcher — lease management, cluster_size, error handling."""

import pytest
from unittest.mock import MagicMock, patch

from worker.tasks.dispatcher import _sut_lock_key, dispatcher_task


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


def _run_dispatcher(plan=None, run_id="run-1", cluster_spec=None, **extra_patches):
    """Run dispatcher_task with all infrastructure mocked out."""
    if plan is None:
        plan = _make_plan()
    with patch("worker.tasks.dispatcher.acquire_sut_lock") as m_acquire, \
         patch("worker.tasks.dispatcher.release_sut_lock") as m_release, \
         patch("worker.tasks.dispatcher._wait_for_completion") as m_wait, \
         patch("worker.tasks.dispatcher.update_run_status") as m_status, \
         patch("worker.tasks.dispatcher.k6_executor_task") as m_exec, \
         patch("worker.tasks.dispatcher.FixtureLoader") as m_loader:
        m_loader.return_value.load.return_value = None
        m_exec.delay.return_value = None
        # Apply any extra patches
        for attr, val in extra_patches.items():
            if attr == "loader_side_effect":
                m_loader.return_value.load.side_effect = val
        result = dispatcher_task.run(plan, run_id, cluster_spec=cluster_spec)
        return result, {
            "acquire": m_acquire, "release": m_release, "wait": m_wait,
            "status": m_status, "exec": m_exec, "loader": m_loader,
        }


# ---------------------------------------------------------------------------
# _sut_lock_key
# ---------------------------------------------------------------------------

def test_sut_lock_key_same_host_same_key():
    """Same host → same lock key."""
    assert _sut_lock_key(_make_plan()) == _sut_lock_key(_make_plan())


def test_sut_lock_key_different_host_different_key():
    """Different hosts → different lock keys."""
    plan1 = _make_plan()
    plan2 = _make_plan()
    plan2["test_environment"]["component_spec"]["cluster_info"]["host"] = "other:9030"
    assert _sut_lock_key(plan1) != _sut_lock_key(plan2)


def test_sut_lock_key_no_cluster_info_falls_back_to_type():
    """No cluster_info → uses component_spec.type."""
    plan = _make_plan()
    del plan["test_environment"]["component_spec"]["cluster_info"]
    key = _sut_lock_key(plan)
    assert isinstance(key, int)
    assert key >= 0


# ---------------------------------------------------------------------------
# Lease management — QUEUED status and lock ordering
# ---------------------------------------------------------------------------

def test_dispatcher_transitions_to_queued_before_lock():
    """Run transitions to QUEUED before acquire_sut_lock is called."""
    call_order = []
    with patch("worker.tasks.dispatcher.acquire_sut_lock") as m_acquire, \
         patch("worker.tasks.dispatcher.release_sut_lock"), \
         patch("worker.tasks.dispatcher._wait_for_completion"), \
         patch("worker.tasks.dispatcher.update_run_status") as m_status, \
         patch("worker.tasks.dispatcher.k6_executor_task") as m_exec, \
         patch("worker.tasks.dispatcher.FixtureLoader") as m_loader:
        m_loader.return_value.load.return_value = None
        m_exec.delay.return_value = None
        m_status.side_effect = lambda *a, **kw: call_order.append(("status", a[1] if len(a) > 1 else None))
        m_acquire.side_effect = lambda *a: call_order.append(("acquire",))

        dispatcher_task.run(_make_plan(), "run-q", cluster_spec=None)

    queued_idx = next(i for i, c in enumerate(call_order) if c == ("status", "QUEUED"))
    acquire_idx = next(i for i, c in enumerate(call_order) if c == ("acquire",))
    assert queued_idx < acquire_idx


def test_dispatcher_releases_lock_on_success():
    """Lock is released after successful completion."""
    _, mocks = _run_dispatcher()
    mocks["acquire"].assert_called_once()
    mocks["release"].assert_called_once()
    assert mocks["acquire"].call_args[0][0] == mocks["release"].call_args[0][0]


def test_dispatcher_releases_lock_on_fixture_failure():
    """Lock is released even when fixture loading fails."""
    with patch("worker.tasks.dispatcher.acquire_sut_lock"), \
         patch("worker.tasks.dispatcher.release_sut_lock") as m_release, \
         patch("worker.tasks.dispatcher._wait_for_completion"), \
         patch("worker.tasks.dispatcher.update_run_status"), \
         patch("worker.tasks.dispatcher.k6_executor_task"), \
         patch("worker.tasks.dispatcher.FixtureLoader") as m_loader:
        m_loader.return_value.load.side_effect = RuntimeError("Connection refused")
        with pytest.raises(RuntimeError):
            dispatcher_task.run(_make_plan(), "run-err", cluster_spec=None)
    m_release.assert_called_once()


def test_dispatcher_waits_for_completion_intra_node():
    """Intra-node: dispatcher waits for executor completion before releasing lock."""
    call_order = []
    with patch("worker.tasks.dispatcher.acquire_sut_lock"), \
         patch("worker.tasks.dispatcher.release_sut_lock") as m_release, \
         patch("worker.tasks.dispatcher._wait_for_completion") as m_wait, \
         patch("worker.tasks.dispatcher.update_run_status"), \
         patch("worker.tasks.dispatcher.k6_executor_task") as m_exec, \
         patch("worker.tasks.dispatcher.FixtureLoader") as m_loader:
        m_loader.return_value.load.return_value = None
        m_exec.delay.return_value = None
        m_wait.side_effect = lambda *a, **kw: call_order.append("wait")
        m_release.side_effect = lambda *a: call_order.append("release")

        dispatcher_task.run(_make_plan(), "run-wait", cluster_spec=None)

    assert call_order.index("wait") < call_order.index("release")


# ---------------------------------------------------------------------------
# cluster_size extraction from cluster_spec
# ---------------------------------------------------------------------------

def test_cluster_size_defaults_to_1_when_no_cluster_spec():
    _, mocks = _run_dispatcher(run_id="run-1")
    mocks["exec"].delay.assert_called_once()
    _, kwargs = mocks["exec"].delay.call_args
    assert kwargs["local_instances"] == 1
    assert kwargs["segment_index"] == 0


def test_cluster_size_from_backend_node_replica():
    _, mocks = _run_dispatcher(
        run_id="run-2",
        cluster_spec={"type": "doris", "backend_node": {"replica": 4}},
    )
    _, kwargs = mocks["exec"].delay.call_args
    assert kwargs["local_instances"] == 4


def test_cluster_size_defaults_to_1_when_backend_node_absent():
    _, mocks = _run_dispatcher(
        run_id="run-3",
        cluster_spec={"type": "doris", "frontend_node": {"replica": 2}},
    )
    _, kwargs = mocks["exec"].delay.call_args
    assert kwargs["local_instances"] == 1


def test_cluster_size_defaults_to_1_when_backend_node_is_null():
    _, mocks = _run_dispatcher(
        run_id="run-4",
        cluster_spec={"type": "doris", "backend_node": None},
    )
    _, kwargs = mocks["exec"].delay.call_args
    assert kwargs["local_instances"] == 1


def test_cluster_spec_none_and_inter_node_uses_size_1():
    """No cluster_spec with inter_node → cluster_size=1."""
    with patch("worker.tasks.dispatcher.acquire_sut_lock"), \
         patch("worker.tasks.dispatcher.release_sut_lock"), \
         patch("worker.tasks.dispatcher._wait_for_completion"), \
         patch("worker.tasks.dispatcher.update_run_status"), \
         patch("worker.tasks.dispatcher.k6_executor_task") as m_exec, \
         patch("worker.tasks.dispatcher.FixtureLoader") as m_loader, \
         patch("worker.tasks.dispatcher.Inspect") as m_inspect, \
         patch("worker.tasks.dispatcher.init_waiting_room"), \
         patch("worker.tasks.dispatcher.get_ready_count", return_value=1), \
         patch("worker.tasks.dispatcher.set_start_signal"):
        m_loader.return_value.load.return_value = None
        m_exec.delay.return_value = None
        m_inspect.return_value.active_queues.return_value = {"w1": [], "w2": []}

        dispatcher_task.run(_make_plan("inter_node"), "run-5", cluster_spec=None)

    assert m_exec.delay.call_count == 1


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

def test_fixture_loader_failure_marks_run_as_failed():
    with patch("worker.tasks.dispatcher.acquire_sut_lock"), \
         patch("worker.tasks.dispatcher.release_sut_lock"), \
         patch("worker.tasks.dispatcher._wait_for_completion"), \
         patch("worker.tasks.dispatcher.update_run_status") as m_status, \
         patch("worker.tasks.dispatcher.k6_executor_task"), \
         patch("worker.tasks.dispatcher.FixtureLoader") as m_loader:
        m_loader.return_value.load.side_effect = RuntimeError("Connection refused")
        with pytest.raises(RuntimeError):
            dispatcher_task.run(_make_plan(), "run-err-1", cluster_spec=None)

    failed_calls = [c for c in m_status.call_args_list if len(c[0]) > 1 and c[0][1] == "FAILED"]
    assert len(failed_calls) == 1
    assert "Fixture loading failed" in failed_calls[0][1]["error_detail"]
    assert "Connection refused" in failed_calls[0][1]["error_detail"]
