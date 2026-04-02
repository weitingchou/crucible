"""Tests for ChaosScheduler — timing, cancel, and cleanup."""

import threading
import time
from unittest.mock import MagicMock, patch, call

import pytest


def _make_chaos_spec(start_after="0s", duration="1s"):
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
                "schedule": {"start_after": start_after, "duration": duration},
            },
        ],
    }


@patch("worker.chaos_injector.scheduler._get_engine")
def test_scheduler_injects_and_recovers(mock_get_engine):
    from worker.chaos_injector.scheduler import ChaosScheduler

    mock_engine = MagicMock()
    mock_engine.inject.return_value = "handle-1"
    mock_get_engine.return_value = mock_engine

    scheduler = ChaosScheduler(_make_chaos_spec(start_after="0s", duration="0s"), "run-1")
    scheduler.start()
    scheduler.join(timeout=5)

    mock_engine.inject.assert_called_once()
    mock_engine.recover.assert_called_once()
    assert mock_engine.recover.call_args[0][2] == "handle-1"


@patch("worker.chaos_injector.scheduler._get_engine")
def test_scheduler_cancel_triggers_recovery(mock_get_engine):
    from worker.chaos_injector.scheduler import ChaosScheduler

    mock_engine = MagicMock()
    mock_engine.inject.return_value = "handle-2"
    mock_get_engine.return_value = mock_engine

    # Use a long duration so we can cancel during it
    scheduler = ChaosScheduler(_make_chaos_spec(start_after="0s", duration="60s"), "run-2")
    scheduler.start()
    time.sleep(0.1)  # Let inject happen
    scheduler.cancel()
    scheduler.join(timeout=5)

    mock_engine.inject.assert_called_once()
    # Should recover via _recover_all
    mock_engine.recover.assert_called_once()


@patch("worker.chaos_injector.scheduler._get_engine")
def test_scheduler_cancel_during_start_after_skips_inject(mock_get_engine):
    from worker.chaos_injector.scheduler import ChaosScheduler

    mock_engine = MagicMock()
    mock_get_engine.return_value = mock_engine

    scheduler = ChaosScheduler(_make_chaos_spec(start_after="60s", duration="1s"), "run-3")
    scheduler.start()
    time.sleep(0.05)
    scheduler.cancel()
    scheduler.join(timeout=5)

    mock_engine.inject.assert_not_called()


@patch("worker.chaos_injector.scheduler._get_engine")
def test_scheduler_inject_failure_continues(mock_get_engine):
    from worker.chaos_injector.scheduler import ChaosScheduler

    mock_engine = MagicMock()
    mock_engine.inject.side_effect = RuntimeError("connection refused")
    mock_get_engine.return_value = mock_engine

    scheduler = ChaosScheduler(_make_chaos_spec(start_after="0s", duration="0s"), "run-4")
    scheduler.start()
    scheduler.join(timeout=5)

    mock_engine.inject.assert_called_once()
    # recover should not be called since inject failed
    mock_engine.recover.assert_not_called()


@patch("worker.chaos_injector.scheduler._get_engine")
def test_scheduler_multiple_experiments(mock_get_engine):
    from worker.chaos_injector.scheduler import ChaosScheduler

    mock_engine = MagicMock()
    mock_engine.inject.side_effect = ["h1", "h2"]
    mock_get_engine.return_value = mock_engine

    spec = {
        "engine": "chaos-mesh",
        "experiments": [
            {
                "name": "exp-1",
                "fault_type": "networkchaos",
                "target": {"env_type": "k8s", "namespace": "ns", "selector": {"a": "b"}},
                "parameters": {},
                "schedule": {"start_after": "0s", "duration": "0s"},
            },
            {
                "name": "exp-2",
                "fault_type": "podchaos",
                "target": {"env_type": "k8s", "namespace": "ns", "selector": {"a": "b"}},
                "parameters": {},
                "schedule": {"start_after": "0s", "duration": "0s"},
            },
        ],
    }

    scheduler = ChaosScheduler(spec, "run-5")
    scheduler.start()
    scheduler.join(timeout=5)

    assert mock_engine.inject.call_count == 2
    assert mock_engine.recover.call_count == 2


@patch("worker.chaos_injector.scheduler._get_engine")
def test_scheduler_cancel_between_experiments(mock_get_engine):
    """Cancel after the first experiment succeeds but before the second starts."""
    from worker.chaos_injector.scheduler import ChaosScheduler

    mock_engine = MagicMock()
    inject_count = 0

    def inject_side_effect(exp, run_id):
        nonlocal inject_count
        inject_count += 1
        return f"h{inject_count}"

    mock_engine.inject.side_effect = inject_side_effect
    mock_get_engine.return_value = mock_engine

    spec = {
        "engine": "chaos-mesh",
        "experiments": [
            {
                "name": "exp-1",
                "fault_type": "networkchaos",
                "target": {"env_type": "k8s", "namespace": "ns", "selector": {"a": "b"}},
                "parameters": {},
                "schedule": {"start_after": "0s", "duration": "0s"},
            },
            {
                "name": "exp-2",
                "fault_type": "podchaos",
                "target": {"env_type": "k8s", "namespace": "ns", "selector": {"a": "b"}},
                "parameters": {},
                "schedule": {"start_after": "60s", "duration": "1s"},
            },
        ],
    }

    scheduler = ChaosScheduler(spec, "run-6")
    scheduler.start()
    time.sleep(0.1)  # Let first experiment complete
    scheduler.cancel()
    scheduler.join(timeout=5)

    # First experiment injected and recovered normally, second never started
    assert mock_engine.inject.call_count == 1


@patch("worker.chaos_injector.scheduler._get_engine")
def test_scheduler_recover_all_handles_recovery_failure(mock_get_engine):
    """_recover_all should not raise even if individual recovery fails."""
    from worker.chaos_injector.scheduler import ChaosScheduler

    mock_engine = MagicMock()
    mock_engine.inject.return_value = "handle-fail"
    # Recover raises on first call (normal recovery), and also in _recover_all
    mock_engine.recover.side_effect = RuntimeError("cleanup failed")
    mock_get_engine.return_value = mock_engine

    scheduler = ChaosScheduler(_make_chaos_spec(start_after="0s", duration="0s"), "run-7")
    scheduler.start()
    scheduler.join(timeout=5)

    # Should complete without raising
    assert not scheduler.is_alive()
    mock_engine.inject.assert_called_once()


@patch("worker.chaos_injector.scheduler._get_engine")
def test_scheduler_is_daemon_thread(mock_get_engine):
    from worker.chaos_injector.scheduler import ChaosScheduler

    mock_get_engine.return_value = MagicMock()

    scheduler = ChaosScheduler(_make_chaos_spec(), "run-8")
    assert scheduler.daemon is True
    assert scheduler.name == "chaos-run-8"


@patch("worker.chaos_injector.scheduler._get_engine")
def test_scheduler_empty_experiments_completes(mock_get_engine):
    """A chaos spec with empty experiments list completes immediately."""
    from worker.chaos_injector.scheduler import ChaosScheduler

    mock_engine = MagicMock()
    mock_get_engine.return_value = mock_engine

    scheduler = ChaosScheduler({"experiments": []}, "run-9")
    scheduler.start()
    scheduler.join(timeout=5)

    assert not scheduler.is_alive()
    mock_engine.inject.assert_not_called()


# ---------------------------------------------------------------------------
# Event tracking (get_events)
# ---------------------------------------------------------------------------

@patch("worker.chaos_injector.scheduler._get_engine")
def test_scheduler_records_events_on_normal_recovery(mock_get_engine):
    from worker.chaos_injector.scheduler import ChaosScheduler

    mock_engine = MagicMock()
    mock_engine.inject.return_value = "handle-evt"
    mock_engine.collect_status.return_value = {"conditions": [{"type": "AllInjected"}]}
    mock_get_engine.return_value = mock_engine

    scheduler = ChaosScheduler(_make_chaos_spec(start_after="0s", duration="0s"), "run-ev1")
    scheduler.start()
    scheduler.join(timeout=5)

    events = scheduler.get_events()
    assert len(events) == 1
    evt = events[0]
    assert evt["experiment"] == "test-fault"
    assert evt["fault_type"] == "networkchaos"
    assert evt["engine"] == "k8s"
    assert evt["injected_at"] is not None
    assert evt["recovered_at"] is not None
    assert evt["duration_seconds"] is not None
    assert evt["duration_seconds"] >= 0
    assert evt["crd_status"] == {"conditions": [{"type": "AllInjected"}]}
    mock_engine.collect_status.assert_called_once()


@patch("worker.chaos_injector.scheduler._get_engine")
def test_scheduler_records_events_on_cancel(mock_get_engine):
    """Events are recorded even when cancel triggers _recover_all."""
    from worker.chaos_injector.scheduler import ChaosScheduler

    mock_engine = MagicMock()
    mock_engine.inject.return_value = "handle-cancel"
    mock_engine.collect_status.return_value = None
    mock_get_engine.return_value = mock_engine

    scheduler = ChaosScheduler(_make_chaos_spec(start_after="0s", duration="60s"), "run-ev2")
    scheduler.start()
    time.sleep(0.1)
    scheduler.cancel()
    scheduler.join(timeout=5)

    events = scheduler.get_events()
    assert len(events) == 1
    assert events[0]["experiment"] == "test-fault"
    assert events[0]["recovered_at"] is not None


@patch("worker.chaos_injector.scheduler._get_engine")
def test_scheduler_no_event_when_inject_fails(mock_get_engine):
    """Failed injection does not produce an event."""
    from worker.chaos_injector.scheduler import ChaosScheduler

    mock_engine = MagicMock()
    mock_engine.inject.side_effect = RuntimeError("boom")
    mock_get_engine.return_value = mock_engine

    scheduler = ChaosScheduler(_make_chaos_spec(start_after="0s", duration="0s"), "run-ev3")
    scheduler.start()
    scheduler.join(timeout=5)

    assert scheduler.get_events() == []


@patch("worker.chaos_injector.scheduler._get_engine")
def test_scheduler_multiple_experiments_all_recorded(mock_get_engine):
    from worker.chaos_injector.scheduler import ChaosScheduler

    mock_engine = MagicMock()
    mock_engine.inject.side_effect = ["h1", "h2"]
    mock_engine.collect_status.return_value = None
    mock_get_engine.return_value = mock_engine

    spec = {
        "engine": "chaos-mesh",
        "experiments": [
            {
                "name": "exp-a",
                "fault_type": "networkchaos",
                "target": {"env_type": "k8s", "namespace": "ns", "selector": {"a": "b"}},
                "parameters": {},
                "schedule": {"start_after": "0s", "duration": "0s"},
            },
            {
                "name": "exp-b",
                "fault_type": "podchaos",
                "target": {"env_type": "k8s", "namespace": "ns", "selector": {"a": "b"}},
                "parameters": {},
                "schedule": {"start_after": "0s", "duration": "0s"},
            },
        ],
    }

    scheduler = ChaosScheduler(spec, "run-ev4")
    scheduler.start()
    scheduler.join(timeout=5)

    events = scheduler.get_events()
    assert len(events) == 2
    assert events[0]["experiment"] == "exp-a"
    assert events[1]["experiment"] == "exp-b"


@patch("worker.chaos_injector.scheduler._get_engine")
def test_scheduler_get_events_returns_copy(mock_get_engine):
    """get_events returns a copy, not a reference to internal list."""
    from worker.chaos_injector.scheduler import ChaosScheduler

    mock_engine = MagicMock()
    mock_engine.inject.return_value = "h1"
    mock_engine.collect_status.return_value = None
    mock_get_engine.return_value = mock_engine

    scheduler = ChaosScheduler(_make_chaos_spec(start_after="0s", duration="0s"), "run-ev5")
    scheduler.start()
    scheduler.join(timeout=5)

    events1 = scheduler.get_events()
    events2 = scheduler.get_events()
    assert events1 == events2
    assert events1 is not events2


@patch("worker.chaos_injector.scheduler._get_engine")
def test_scheduler_records_event_on_recovery_failure(mock_get_engine):
    """When _recover_all fails to recover, event still records with no recovered_at."""
    from worker.chaos_injector.scheduler import ChaosScheduler

    mock_engine = MagicMock()
    mock_engine.inject.return_value = "handle-fail"
    mock_engine.collect_status.return_value = None
    mock_engine.recover.side_effect = RuntimeError("cleanup failed")
    mock_get_engine.return_value = mock_engine

    # Use long duration so recovery happens via _recover_all after cancel
    scheduler = ChaosScheduler(_make_chaos_spec(start_after="0s", duration="60s"), "run-ev6")
    scheduler.start()
    time.sleep(0.1)
    scheduler.cancel()
    scheduler.join(timeout=5)

    events = scheduler.get_events()
    assert len(events) == 1
    assert events[0]["recovered_at"] is None


@patch("worker.chaos_injector.scheduler._get_engine")
def test_scheduler_normal_recovery_failure_no_event(mock_get_engine):
    """When recover() fails during normal (non-cancel) path, no event is recorded."""
    from worker.chaos_injector.scheduler import ChaosScheduler

    mock_engine = MagicMock()
    mock_engine.inject.return_value = "handle-nrf"
    mock_engine.collect_status.return_value = {"conditions": []}
    mock_engine.recover.side_effect = RuntimeError("recover failed")
    mock_get_engine.return_value = mock_engine

    # duration=0s means normal recovery path (not cancel/_recover_all)
    scheduler = ChaosScheduler(_make_chaos_spec(start_after="0s", duration="0s"), "run-ev7")
    scheduler.start()
    scheduler.join(timeout=5)

    # _run_experiment logs the error but does NOT append an event
    # The experiment remains in _active and _recover_all runs on thread exit,
    # which will also fail (same mock), recording the event with no recovered_at
    events = scheduler.get_events()
    assert len(events) == 1
    assert events[0]["recovered_at"] is None
    assert events[0]["crd_status"] == {"conditions": []}


@patch("worker.chaos_injector.scheduler._get_engine")
def test_scheduler_collect_status_failure_still_recovers(mock_get_engine):
    """When collect_status raises during normal recovery, recovery still proceeds."""
    from worker.chaos_injector.scheduler import ChaosScheduler

    mock_engine = MagicMock()
    mock_engine.inject.return_value = "handle-csf"
    mock_engine.collect_status.side_effect = RuntimeError("status read failed")
    mock_get_engine.return_value = mock_engine

    scheduler = ChaosScheduler(_make_chaos_spec(start_after="0s", duration="0s"), "run-ev8")
    scheduler.start()
    scheduler.join(timeout=5)

    # recover should still be called
    mock_engine.recover.assert_called_once()
    events = scheduler.get_events()
    assert len(events) == 1
    assert events[0]["crd_status"] is None
    assert events[0]["recovered_at"] is not None


# ---------------------------------------------------------------------------
# _build_event (unit tests — no threading)
# ---------------------------------------------------------------------------

def test_build_event_calculates_duration():
    from worker.chaos_injector.scheduler import ChaosScheduler

    exp = {
        "name": "test-exp",
        "fault_type": "PodChaos",
        "target": {"env_type": "k8s", "namespace": "ns"},
    }
    evt = ChaosScheduler._build_event(
        exp,
        injected_at="2025-01-01T00:00:00+00:00",
        recovered_at="2025-01-01T00:02:00+00:00",
        crd_status={"conditions": []},
    )
    assert evt["experiment"] == "test-exp"
    assert evt["fault_type"] == "PodChaos"
    assert evt["engine"] == "k8s"
    assert evt["duration_seconds"] == 120.0
    assert evt["crd_status"] == {"conditions": []}
    assert evt["target"] == {"env_type": "k8s", "namespace": "ns"}


def test_build_event_no_recovery():
    from worker.chaos_injector.scheduler import ChaosScheduler

    exp = {
        "name": "no-recover",
        "fault_type": "networkchaos",
        "target": {"env_type": "ec2", "address": "10.0.0.1"},
    }
    evt = ChaosScheduler._build_event(
        exp,
        injected_at="2025-01-01T00:00:00+00:00",
        recovered_at=None,
        crd_status=None,
    )
    assert evt["recovered_at"] is None
    assert evt["duration_seconds"] is None
    assert evt["engine"] == "ec2"
