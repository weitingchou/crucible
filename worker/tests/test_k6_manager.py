from unittest.mock import patch, MagicMock

import pytest

from worker.driver_manager.k6_manager import spawn_k6


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
