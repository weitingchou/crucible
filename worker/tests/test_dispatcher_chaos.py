"""Tests for chaos integration in the dispatcher task."""

from unittest.mock import MagicMock, patch, call


def _make_plan(chaos_spec=None, scaling_mode="intra_node"):
    plan = {
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
    if chaos_spec:
        plan["chaos_spec"] = chaos_spec
    return plan


def _chaos_spec():
    return {
        "engine": "chaos-mesh",
        "experiments": [
            {
                "name": "test-fault",
                "fault_type": "networkchaos",
                "target": {
                    "env_type": "k8s",
                    "namespace": "default",
                    "selector": {"app": "test"},
                },
                "parameters": {"action": "delay"},
                "schedule": {"start_after": "1s", "duration": "2s"},
            },
        ],
    }


def test_dispatcher_starts_chaos_scheduler_when_spec_present():
    """When chaos_spec is present, _start_chaos is called."""
    with patch("worker.tasks.dispatcher.acquire_sut_lock"), \
         patch("worker.tasks.dispatcher.release_sut_lock"), \
         patch("worker.tasks.dispatcher._wait_for_completion"), \
         patch("worker.tasks.dispatcher.update_run_status"), \
         patch("worker.tasks.dispatcher.k6_executor_task") as m_exec, \
         patch("worker.tasks.dispatcher.FixtureLoader") as m_loader, \
         patch("worker.tasks.dispatcher._start_chaos") as m_start, \
         patch("worker.tasks.dispatcher._stop_chaos") as m_stop:
        m_loader.return_value.load.return_value = None
        m_exec.delay.return_value = None
        m_start.return_value = MagicMock()

        from worker.tasks.dispatcher import dispatcher_task
        dispatcher_task.run(_make_plan(_chaos_spec()), "run-chaos-1", cluster_spec=None)

    m_start.assert_called_once()
    m_stop.assert_called_once()


def test_dispatcher_skips_chaos_when_no_spec():
    """When chaos_spec is absent, _start_chaos returns None."""
    with patch("worker.tasks.dispatcher.acquire_sut_lock"), \
         patch("worker.tasks.dispatcher.release_sut_lock"), \
         patch("worker.tasks.dispatcher._wait_for_completion"), \
         patch("worker.tasks.dispatcher.update_run_status"), \
         patch("worker.tasks.dispatcher.k6_executor_task") as m_exec, \
         patch("worker.tasks.dispatcher.FixtureLoader") as m_loader, \
         patch("worker.tasks.dispatcher._start_chaos") as m_start, \
         patch("worker.tasks.dispatcher._stop_chaos") as m_stop:
        m_loader.return_value.load.return_value = None
        m_exec.delay.return_value = None
        m_start.return_value = None

        from worker.tasks.dispatcher import dispatcher_task
        dispatcher_task.run(_make_plan(), "run-no-chaos", cluster_spec=None)

    m_start.assert_called_once()
    # _stop_chaos still called (with None) — it's a no-op
    m_stop.assert_called_once_with(None)


def test_dispatcher_stops_chaos_on_failure():
    """Chaos is stopped even when _wait_for_completion raises."""
    with patch("worker.tasks.dispatcher.acquire_sut_lock"), \
         patch("worker.tasks.dispatcher.release_sut_lock"), \
         patch("worker.tasks.dispatcher._wait_for_completion") as m_wait, \
         patch("worker.tasks.dispatcher.update_run_status"), \
         patch("worker.tasks.dispatcher.k6_executor_task") as m_exec, \
         patch("worker.tasks.dispatcher.FixtureLoader") as m_loader, \
         patch("worker.tasks.dispatcher._start_chaos") as m_start, \
         patch("worker.tasks.dispatcher._stop_chaos") as m_stop:
        m_loader.return_value.load.return_value = None
        m_exec.delay.return_value = None
        m_start.return_value = MagicMock()
        m_wait.side_effect = RuntimeError("boom")

        from worker.tasks.dispatcher import dispatcher_task
        try:
            dispatcher_task.run(_make_plan(_chaos_spec()), "run-chaos-err", cluster_spec=None)
        except RuntimeError:
            pass

    m_stop.assert_called_once()


def test_total_chaos_duration_single_experiment():
    """_total_chaos_duration sums start_after + duration for one experiment."""
    from worker.tasks.dispatcher import _total_chaos_duration

    spec = _chaos_spec()
    # start_after=1s + duration=2s = 3s
    assert _total_chaos_duration(spec) == 3


def test_total_chaos_duration_empty_spec():
    from worker.tasks.dispatcher import _total_chaos_duration

    assert _total_chaos_duration({}) == 0
    assert _total_chaos_duration({"experiments": []}) == 0


def test_total_chaos_duration_multiple_experiments_sums():
    """Sequential execution means total is the sum, not the max."""
    from worker.tasks.dispatcher import _total_chaos_duration

    spec = {
        "experiments": [
            {"schedule": {"start_after": "10s", "duration": "30s"}},
            {"schedule": {"start_after": "5s", "duration": "20s"}},
        ],
    }
    # (10+30) + (5+20) = 65
    assert _total_chaos_duration(spec) == 65


def test_dispatcher_inter_node_starts_chaos_after_checkin():
    """In inter-node mode, chaos starts after workers check in."""
    with patch("worker.tasks.dispatcher.acquire_sut_lock"), \
         patch("worker.tasks.dispatcher.release_sut_lock"), \
         patch("worker.tasks.dispatcher._wait_for_completion"), \
         patch("worker.tasks.dispatcher.update_run_status"), \
         patch("worker.tasks.dispatcher.k6_executor_task") as m_exec, \
         patch("worker.tasks.dispatcher.FixtureLoader") as m_loader, \
         patch("worker.tasks.dispatcher._start_chaos") as m_start, \
         patch("worker.tasks.dispatcher._stop_chaos") as m_stop, \
         patch("worker.tasks.dispatcher.init_waiting_room"), \
         patch("worker.tasks.dispatcher.get_ready_count", return_value=1), \
         patch("worker.tasks.dispatcher.set_start_signal"), \
         patch("worker.tasks.dispatcher.Inspect") as m_inspect:
        m_loader.return_value.load.return_value = None
        m_exec.delay.return_value = None
        m_start.return_value = MagicMock()
        m_inspect.return_value.active_queues.return_value = {"w1": []}

        from worker.tasks.dispatcher import dispatcher_task
        result = dispatcher_task.run(
            _make_plan(_chaos_spec(), scaling_mode="inter_node"),
            "run-inter-chaos",
            cluster_spec={"backend_node": {"replica": 1}},
        )

    assert result["status"] == "inter_node_completed"
    m_start.assert_called_once()
    m_stop.assert_called_once()


def test_stop_chaos_noop_for_none():
    """_stop_chaos(None) is a no-op — no exception."""
    from worker.tasks.dispatcher import _stop_chaos

    _stop_chaos(None)  # should not raise


def test_stop_chaos_cancels_alive_scheduler():
    """_stop_chaos calls cancel + join on an alive scheduler."""
    from worker.tasks.dispatcher import _stop_chaos

    mock_sched = MagicMock()
    mock_sched.is_alive.return_value = True
    _stop_chaos(mock_sched)

    mock_sched.cancel.assert_called_once()
    mock_sched.join.assert_called_once_with(timeout=30)


def test_stop_chaos_skips_cancel_for_finished_scheduler():
    """_stop_chaos does not cancel a scheduler that already finished."""
    from worker.tasks.dispatcher import _stop_chaos

    mock_sched = MagicMock()
    mock_sched.is_alive.return_value = False
    _stop_chaos(mock_sched)

    mock_sched.cancel.assert_not_called()
