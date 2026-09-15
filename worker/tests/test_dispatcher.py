"""Tests for worker.tasks.dispatcher — lease management, cluster_size, error handling."""

import pytest
from unittest.mock import MagicMock, patch

from celery.exceptions import Retry

from crucible_lib.queues import DISPATCH_QUEUE, EXECUTE_QUEUE
from worker.celery_app import app
from worker.tasks.dispatcher import (
    _LOCK_RETRY_CEILING,
    _execute_slot_capacity,
    _lock_retry_countdown,
    _sut_lock_key,
    dispatcher_task,
)


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
    with patch("worker.tasks.dispatcher.try_acquire_sut_lock", return_value=True) as m_acquire, \
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

def _run_with_lease_held(run_id="run-q"):
    """Run the dispatcher against a SUT whose lease another run already holds."""
    with patch("worker.tasks.dispatcher.try_acquire_sut_lock", return_value=False), \
         patch("worker.tasks.dispatcher.release_sut_lock") as m_release, \
         patch("worker.tasks.dispatcher.update_run_status") as m_status, \
         patch("worker.tasks.dispatcher.k6_executor_task") as m_exec, \
         patch("worker.tasks.dispatcher.FixtureLoader") as m_loader:
        try:
            result = dispatcher_task.run(_make_plan(), run_id, cluster_spec=None)
        except Retry as exc:
            result = exc
        return result, {"release": m_release, "status": m_status,
                        "exec": m_exec, "loader": m_loader}


def test_dispatcher_retries_instead_of_blocking_when_lease_is_held():
    """Contention must release the pool slot, not park on the lock.

    A blocking acquire holds a Celery slot for the whole wait, which is how
    dispatchers end up occupying the pool and starving their own executors.
    """
    result, _ = _run_with_lease_held()
    assert isinstance(result, Retry)


def test_dispatcher_marks_queued_when_lease_is_held():
    _, mocks = _run_with_lease_held()
    statuses = [c.args[1] for c in mocks["status"].call_args_list]
    assert "QUEUED" in statuses


def test_dispatcher_does_no_work_when_lease_is_held():
    """No fixture load, no fan-out, and nothing to release — it never had the lock."""
    _, mocks = _run_with_lease_held()
    mocks["loader"].assert_not_called()
    mocks["exec"].delay.assert_not_called()
    mocks["release"].assert_not_called()


def test_dispatcher_skips_queued_when_lease_is_free():
    """An uncontended run goes straight to EXECUTING; QUEUED means contention."""
    _, mocks = _run_dispatcher(run_id="run-free")
    statuses = [c.args[1] for c in mocks["status"].call_args_list]
    assert "QUEUED" not in statuses
    assert "EXECUTING" in statuses


def test_dispatcher_fails_run_when_lease_wait_is_exhausted():
    """Past the retry budget the run is failed, not left silently QUEUED."""
    # Celery's retry() short-circuits to a plain Retry when called_directly is
    # set, so the max-retries branch is only reachable with a worker-like request.
    dispatcher_task.push_request(retries=10**6, called_directly=False, id="run-exhausted")
    try:
        with patch("worker.tasks.dispatcher.try_acquire_sut_lock", return_value=False), \
             patch("worker.tasks.dispatcher.release_sut_lock"), \
             patch("worker.tasks.dispatcher.update_run_status") as m_status, \
             patch("worker.tasks.dispatcher.k6_executor_task"), \
             patch("worker.tasks.dispatcher.FixtureLoader"):
            result = dispatcher_task.run(_make_plan(), "run-exhausted", cluster_spec=None)
    finally:
        dispatcher_task.pop_request()

    assert result["status"] == "failed_sut_lease_timeout"
    assert m_status.call_args_list[-1].args[1] == "FAILED"


def test_dispatcher_releases_lock_on_success():
    """Lock is released after successful completion."""
    _, mocks = _run_dispatcher()
    mocks["acquire"].assert_called_once()
    mocks["release"].assert_called_once()
    assert mocks["acquire"].call_args[0][0] == mocks["release"].call_args[0][0]


def test_dispatcher_releases_lock_on_fixture_failure():
    """Lock is released even when fixture loading fails."""
    with patch("worker.tasks.dispatcher.try_acquire_sut_lock"), \
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
    with patch("worker.tasks.dispatcher.try_acquire_sut_lock"), \
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
    with patch("worker.tasks.dispatcher.try_acquire_sut_lock"), \
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
        m_inspect.return_value.active_queues.return_value = {
            "execute@w1": [{"name": EXECUTE_QUEUE}],
            "execute@w2": [{"name": EXECUTE_QUEUE}],
        }
        m_inspect.return_value.stats.return_value = {
            "execute@w1": {"pool": {"max-concurrency": 2}},
            "execute@w2": {"pool": {"max-concurrency": 2}},
        }

        dispatcher_task.run(_make_plan("inter_node"), "run-5", cluster_spec=None)

    assert m_exec.delay.call_count == 1


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

def test_fixture_loader_failure_marks_run_as_failed():
    with patch("worker.tasks.dispatcher.try_acquire_sut_lock"), \
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


# ---------------------------------------------------------------------------
# Queue routing — dispatchers and executors must not share a pool
# ---------------------------------------------------------------------------

def test_dispatcher_and_executor_route_to_separate_queues():
    """A shared queue lets dispatchers fill the pool and starve their executors."""
    routes = app.conf.task_routes
    assert routes["worker.tasks.dispatcher.*"]["queue"] == DISPATCH_QUEUE
    assert routes["worker.tasks.executor.*"]["queue"] == EXECUTE_QUEUE
    assert DISPATCH_QUEUE != EXECUTE_QUEUE


def test_prefetch_is_one_so_busy_workers_do_not_hoard_tasks():
    assert app.conf.worker_prefetch_multiplier == 1


def test_acks_late_stays_off_until_consumer_timeout_is_raised():
    """RabbitMQ's 30-min consumer_timeout would redeliver any longer run."""
    assert app.conf.task_acks_late is False


# ---------------------------------------------------------------------------
# _execute_slot_capacity — count slots, not worker nodes
# ---------------------------------------------------------------------------

def _capacity_with(active_queues, stats):
    with patch("worker.tasks.dispatcher.Inspect") as m_inspect:
        m_inspect.return_value.active_queues.return_value = active_queues
        m_inspect.return_value.stats.return_value = stats
        return _execute_slot_capacity(MagicMock())


def test_capacity_sums_pool_sizes_not_node_count():
    """Two nodes with 4 slots each is 8 executors, not 2."""
    capacity = _capacity_with(
        {"execute@a": [{"name": EXECUTE_QUEUE}], "execute@b": [{"name": EXECUTE_QUEUE}]},
        {"execute@a": {"pool": {"max-concurrency": 4}},
         "execute@b": {"pool": {"max-concurrency": 4}}},
    )
    assert capacity == 8


def test_capacity_ignores_dispatch_only_workers():
    """Dispatch workers can't run executors, so they add no capacity."""
    capacity = _capacity_with(
        {"dispatch@a": [{"name": DISPATCH_QUEUE}], "execute@b": [{"name": EXECUTE_QUEUE}]},
        {"dispatch@a": {"pool": {"max-concurrency": 16}},
         "execute@b": {"pool": {"max-concurrency": 2}}},
    )
    assert capacity == 2


def test_capacity_is_zero_with_no_executor_workers():
    capacity = _capacity_with({"dispatch@a": [{"name": DISPATCH_QUEUE}]}, {})
    assert capacity == 0


def test_capacity_survives_missing_stats():
    """inspect() is best-effort; a node that answers one call but not the other
    still counts for at least one slot rather than crashing the dispatcher."""
    capacity = _capacity_with({"execute@a": [{"name": EXECUTE_QUEUE}]}, {})
    assert capacity == 1


def test_inter_node_fails_when_executor_slots_are_short():
    plan = _make_plan("inter_node")
    with patch("worker.tasks.dispatcher.try_acquire_sut_lock", return_value=True), \
         patch("worker.tasks.dispatcher.release_sut_lock"), \
         patch("worker.tasks.dispatcher.update_run_status") as m_status, \
         patch("worker.tasks.dispatcher.k6_executor_task") as m_exec, \
         patch("worker.tasks.dispatcher.FixtureLoader"), \
         patch("worker.tasks.dispatcher._execute_slot_capacity", return_value=1):
        result = dispatcher_task.run(
            plan, "run-short", cluster_spec={"backend_node": {"replica": 4}}
        )

    assert result["status"] == "failed_insufficient_capacity"
    assert "only 1 executor slots" in result["error"]
    assert m_status.call_args_list[-1].args[1] == "FAILED"
    m_exec.delay.assert_not_called()


def test_capacity_is_checked_before_the_lease_and_the_fixture_load():
    """An impossible fan-out must not first hydrate the SUT or take the lease.

    Hydration is the expensive step (100GB+), and holding the lease blocks every
    other run against that SUT — both wasted on a run that cannot be scheduled.
    """
    with patch("worker.tasks.dispatcher.try_acquire_sut_lock") as m_lock, \
         patch("worker.tasks.dispatcher.release_sut_lock"), \
         patch("worker.tasks.dispatcher.update_run_status"), \
         patch("worker.tasks.dispatcher.k6_executor_task"), \
         patch("worker.tasks.dispatcher.FixtureLoader") as m_loader, \
         patch("worker.tasks.dispatcher._execute_slot_capacity", return_value=1):
        result = dispatcher_task.run(
            _make_plan("inter_node"), "run-early",
            cluster_spec={"backend_node": {"replica": 4}},
        )

    assert result["status"] == "failed_insufficient_capacity"
    m_lock.assert_not_called()
    m_loader.assert_not_called()


def test_capacity_failure_records_a_completion_time():
    """A terminal run with a NULL completed_at breaks any duration the API reports."""
    with patch("worker.tasks.dispatcher.try_acquire_sut_lock", return_value=True), \
         patch("worker.tasks.dispatcher.release_sut_lock"), \
         patch("worker.tasks.dispatcher.update_run_status") as m_status, \
         patch("worker.tasks.dispatcher.k6_executor_task"), \
         patch("worker.tasks.dispatcher.FixtureLoader"), \
         patch("worker.tasks.dispatcher._execute_slot_capacity", return_value=0):
        dispatcher_task.run(
            _make_plan("inter_node"), "run-done",
            cluster_spec={"backend_node": {"replica": 2}},
        )

    assert m_status.call_args_list[-1].kwargs["set_completed_at"] is True


# ---------------------------------------------------------------------------
# _lock_retry_countdown
# ---------------------------------------------------------------------------

def test_retry_countdown_stays_near_the_ceiling_once_backed_off():
    """Jitter is centred on the ceiling, not below it — the retry budget is
    derived from the ceiling, so a downward-only jitter would silently make the
    real timeout shorter than sut_lock_max_wait_seconds advertises."""
    samples = [_lock_retry_countdown(20) for _ in range(200)]
    assert min(samples) >= _LOCK_RETRY_CEILING * 0.7
    assert max(samples) <= _LOCK_RETRY_CEILING * 1.3
    assert abs(sum(samples) / len(samples) - _LOCK_RETRY_CEILING) < 2


def test_retry_countdown_does_not_exponentiate_without_bound():
    """The exponent is clamped, so a large retry count can't build a bignum
    only to discard it for the ceiling."""
    assert _lock_retry_countdown(10**6) <= _LOCK_RETRY_CEILING * 1.3


def test_retry_countdown_starts_small():
    assert _lock_retry_countdown(0) <= 2
