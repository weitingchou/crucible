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
