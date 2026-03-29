import subprocess
from unittest.mock import patch, MagicMock

import pytest

from worker.driver_manager.k6_manager import spawn_k6, wait_and_teardown


def _make_plan(host, port=None):
    """Build a minimal plan dict with the given cluster_info host/port layout."""
    cluster_info = {"host": host, "username": "root", "password": ""}
    if port is not None:
        cluster_info["port"] = port
    return {
        "test_environment": {
            "component_spec": {"cluster_info": cluster_info},
            "target_db": "testdb",
        },
        "execution": {"concurrency": 1, "hold_for": "10s"},
    }


@patch("worker.driver_manager.k6_manager.subprocess.Popen")
def test_spawn_k6_with_embedded_port(mock_popen):
    """host: 'doris-fe:9030' — port parsed from host string."""
    plan = _make_plan("doris-fe:9030")
    spawn_k6("run-1", "0%:100%", 0, plan)

    env = mock_popen.call_args.kwargs["env"]
    assert env["DB_HOST"] == "doris-fe"
    assert env["DB_PORT"] == "9030"


@patch("worker.driver_manager.k6_manager.subprocess.Popen")
def test_spawn_k6_with_separate_port(mock_popen):
    """host + port as separate fields in cluster_info."""
    plan = _make_plan("doris-doris-fe.crucible-benchmarks.svc.cluster.local", port=9030)
    spawn_k6("run-1", "0%:100%", 0, plan)

    env = mock_popen.call_args.kwargs["env"]
    assert env["DB_HOST"] == "doris-doris-fe.crucible-benchmarks.svc.cluster.local"
    assert env["DB_PORT"] == "9030"


def test_spawn_k6_no_port_raises():
    """Neither embedded nor separate port — should raise ValueError."""
    plan = _make_plan("doris-doris-fe.crucible-benchmarks.svc.cluster.local")
    with pytest.raises(ValueError, match="host must include a port"):
        spawn_k6("run-1", "0%:100%", 0, plan)


# ---------------------------------------------------------------------------
# wait_and_teardown — exit code collection
# ---------------------------------------------------------------------------

def _mock_process(returncode=0, wait_side_effect=None, stderr_data=b""):
    p = MagicMock(spec=subprocess.Popen)
    p.returncode = returncode
    p.stderr = MagicMock()
    p.stderr.read.return_value = stderr_data
    if wait_side_effect:
        p.wait.side_effect = wait_side_effect
    else:
        p.wait.return_value = returncode
    return p


def test_wait_and_teardown_returns_k6_results():
    """Normal completion → returns K6Result list with exit codes."""
    procs = [_mock_process(0), _mock_process(0)]
    results = wait_and_teardown(procs, timeout=10)
    assert [r.returncode for r in results] == [0, 0]
    assert all(not r.timed_out for r in results)


def test_wait_and_teardown_captures_stderr():
    """stderr from k6 is captured in K6Result."""
    procs = [_mock_process(1, stderr_data=b"ERRO connection refused")]
    results = wait_and_teardown(procs, timeout=10)
    assert results[0].returncode == 1
    assert "connection refused" in results[0].stderr


def test_wait_and_teardown_returns_nonzero_exit_codes():
    """k6 exits with error → K6Result has non-zero returncode."""
    procs = [_mock_process(0), _mock_process(1)]
    results = wait_and_teardown(procs, timeout=10)
    assert [r.returncode for r in results] == [0, 1]


def test_wait_and_teardown_handles_timeout():
    """Process exceeds timeout → SIGTERM, K6Result.timed_out is True."""
    p = _mock_process(returncode=-15)
    p.wait.side_effect = [subprocess.TimeoutExpired("k6", 10), -15]
    results = wait_and_teardown([p], timeout=10)
    p.send_signal.assert_called_once()
    assert results[0].returncode == -15
    assert results[0].timed_out is True


# ---------------------------------------------------------------------------
# spawn_k6 — failure detection env vars
# ---------------------------------------------------------------------------

@patch("worker.driver_manager.k6_manager.subprocess.Popen")
def test_spawn_k6_default_failure_detection(mock_popen):
    """No failure_detection in plan → defaults: threshold=0.5, delay=10s."""
    plan = _make_plan("doris-fe:9030")
    spawn_k6("run-1", "0%:100%", 0, plan)

    env = mock_popen.call_args.kwargs["env"]
    assert env["K6_ERROR_RATE_THRESHOLD"] == "0.5"
    assert env["K6_ERROR_ABORT_DELAY"] == "10s"
    assert "K6_FAILURE_DETECTION_DISABLED" not in env


@patch("worker.driver_manager.k6_manager.subprocess.Popen")
def test_spawn_k6_custom_failure_detection(mock_popen):
    """Custom failure_detection values are passed through."""
    plan = _make_plan("doris-fe:9030")
    plan["execution"]["failure_detection"] = {
        "enabled": True,
        "error_rate_threshold": 0.3,
        "abort_delay": "30s",
    }
    spawn_k6("run-1", "0%:100%", 0, plan)

    env = mock_popen.call_args.kwargs["env"]
    assert env["K6_ERROR_RATE_THRESHOLD"] == "0.3"
    assert env["K6_ERROR_ABORT_DELAY"] == "30s"
    assert "K6_FAILURE_DETECTION_DISABLED" not in env


@patch("worker.driver_manager.k6_manager.subprocess.Popen")
def test_spawn_k6_failure_detection_disabled(mock_popen):
    """enabled: false → K6_FAILURE_DETECTION_DISABLED=true, no threshold env vars."""
    plan = _make_plan("doris-fe:9030")
    plan["execution"]["failure_detection"] = {"enabled": False}
    spawn_k6("run-1", "0%:100%", 0, plan)

    env = mock_popen.call_args.kwargs["env"]
    assert env["K6_FAILURE_DETECTION_DISABLED"] == "true"
    assert "K6_ERROR_RATE_THRESHOLD" not in env
    assert "K6_ERROR_ABORT_DELAY" not in env
