"""Tests for worker.tasks.executor — error handling and k6 exit code detection."""

import pytest
from unittest.mock import MagicMock, patch

from worker.driver_manager.k6_manager import K6Result
from worker.tasks.executor import k6_executor_task


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
            "hold_for_seconds": 5,
            "workload": [{"workload_id": "test-wl"}],
        },
    }


def _mock_process(returncode=0):
    p = MagicMock()
    p.returncode = returncode
    p.kill.return_value = None
    p.wait.return_value = returncode
    return p


def _k6_result(returncode=0, stderr="", timed_out=False):
    return K6Result(returncode=returncode, stderr=stderr, timed_out=timed_out)


# ---------------------------------------------------------------------------
# S3 download failure → FAILED
# ---------------------------------------------------------------------------

@patch("worker.tasks.executor.update_run_status")
@patch("worker.tasks.executor._download_sql_fixtures")
def test_s3_download_failure_marks_run_as_failed(mock_download, mock_status):
    """S3 download failure raises → run transitions to FAILED."""
    mock_download.side_effect = Exception("NoSuchKey: workloads/test-wl")

    with pytest.raises(Exception):
        k6_executor_task.run(_make_plan(), "run-dl-err", "0%:100%", 1)

    mock_status.assert_called_once()
    call_args = mock_status.call_args
    assert call_args[0][0] == "run-dl-err"
    assert call_args[0][1] == "FAILED"
    assert "Workload download failed" in call_args[1]["error_detail"]
    assert "NoSuchKey" in call_args[1]["error_detail"]


# ---------------------------------------------------------------------------
# Workload validation failure → FAILED
# ---------------------------------------------------------------------------

@patch("worker.tasks.executor.update_run_status")
@patch("worker.tasks.executor._download_sql_fixtures")
def test_workload_validation_failure_marks_run_as_failed(mock_download, mock_status):
    """Workload validation raises ValueError → FAILED with detail."""
    mock_download.side_effect = ValueError("Workload 'test-wl' failed validation")

    with pytest.raises(ValueError):
        k6_executor_task.run(_make_plan(), "run-val-err", "0%:100%", 1)

    mock_status.assert_called_once()
    assert mock_status.call_args[0][1] == "FAILED"
    assert "Workload download failed" in mock_status.call_args[1]["error_detail"]
    assert "failed validation" in mock_status.call_args[1]["error_detail"]


# ---------------------------------------------------------------------------
# k6 non-zero exit code → FAILED with stderr
# ---------------------------------------------------------------------------

@patch("worker.tasks.executor.update_run_status")
@patch("worker.tasks.executor.wait_and_teardown")
@patch("worker.tasks.executor.spawn_k6")
@patch("worker.tasks.executor._download_sql_fixtures")
@patch("worker.tasks.executor._upload_to_s3")
@patch("worker.tasks.executor._cleanup")
def test_k6_nonzero_exit_marks_run_as_failed(
    mock_cleanup, mock_upload, mock_download, mock_spawn, mock_wait, mock_status
):
    """k6 exits with non-zero code → run transitions to FAILED with stderr."""
    mock_download.return_value = ["/tmp/test-wl"]
    mock_spawn.return_value = _mock_process(returncode=1)
    mock_wait.return_value = [
        _k6_result(returncode=1, stderr="ERRO[0002] connection refused to doris-fe:9030"),
    ]

    result = k6_executor_task.run(_make_plan(), "run-k6-err", "0%:100%", 1)

    assert result["status"] == "failed"
    assert "instance 0 exited with code 1" in result["error"]
    assert "connection refused" in result["error"]
    mock_status.assert_called_once()
    error_detail = mock_status.call_args[1]["error_detail"]
    assert "k6 process failure" in error_detail
    assert "connection refused" in error_detail


@patch("worker.tasks.executor.update_run_status")
@patch("worker.tasks.executor.wait_and_teardown")
@patch("worker.tasks.executor.spawn_k6")
@patch("worker.tasks.executor._download_sql_fixtures")
@patch("worker.tasks.executor._upload_to_s3")
@patch("worker.tasks.executor._cleanup")
def test_k6_partial_failure_reports_all_failed_instances(
    mock_cleanup, mock_upload, mock_download, mock_spawn, mock_wait, mock_status
):
    """Multiple k6 instances, some fail → error_detail lists all failed."""
    mock_download.return_value = ["/tmp/test-wl"]
    mock_spawn.side_effect = [_mock_process(0), _mock_process(137), _mock_process(0)]
    mock_wait.return_value = [
        _k6_result(0),
        _k6_result(137, stderr="Killed", timed_out=True),
        _k6_result(0),
    ]

    result = k6_executor_task.run(_make_plan(), "run-k6-partial", "0%:100%", 3)

    assert result["status"] == "failed"
    assert "instance 1 exited with code 137" in result["error"]
    assert "timed out, killed" in result["error"]
    assert "Killed" in result["error"]
    # Instance 0 and 2 (exit 0) should NOT appear in the error
    assert "instance 0" not in result["error"]
    assert "instance 2" not in result["error"]


@patch("worker.tasks.executor.update_run_status")
@patch("worker.tasks.executor.wait_and_teardown")
@patch("worker.tasks.executor.spawn_k6")
@patch("worker.tasks.executor._download_sql_fixtures")
@patch("worker.tasks.executor._upload_to_s3")
@patch("worker.tasks.executor._cleanup")
def test_k6_timeout_includes_timed_out_flag(
    mock_cleanup, mock_upload, mock_download, mock_spawn, mock_wait, mock_status
):
    """k6 exceeds hold_for timeout → error_detail says 'timed out'."""
    mock_download.return_value = ["/tmp/test-wl"]
    mock_spawn.return_value = _mock_process(returncode=-15)
    mock_wait.return_value = [
        _k6_result(returncode=-15, stderr="", timed_out=True),
    ]

    result = k6_executor_task.run(_make_plan(), "run-k6-timeout", "0%:100%", 1)

    assert result["status"] == "failed"
    assert "timed out, killed" in result["error"]


# ---------------------------------------------------------------------------
# k6 success → COMPLETED
# ---------------------------------------------------------------------------

@patch("worker.tasks.executor.update_run_status")
@patch("worker.tasks.executor.wait_and_teardown")
@patch("worker.tasks.executor.spawn_k6")
@patch("worker.tasks.executor._download_sql_fixtures")
@patch("worker.tasks.executor._upload_to_s3")
@patch("worker.tasks.executor._cleanup")
def test_k6_success_marks_run_as_completed(
    mock_cleanup, mock_upload, mock_download, mock_spawn, mock_wait, mock_status
):
    """All k6 instances exit 0 → run transitions to COMPLETED."""
    mock_download.return_value = ["/tmp/test-wl"]
    mock_spawn.return_value = _mock_process(returncode=0)
    mock_wait.return_value = [_k6_result(0)]

    result = k6_executor_task.run(_make_plan(), "run-ok", "0%:100%", 1)

    assert result["status"] == "completed"
    mock_status.assert_called_once()
    assert mock_status.call_args[0][1] == "COMPLETED"


# ---------------------------------------------------------------------------
# Spawn failure → FAILED
# ---------------------------------------------------------------------------

@patch("worker.tasks.executor.update_run_status")
@patch("worker.tasks.executor.spawn_k6")
@patch("worker.tasks.executor._download_sql_fixtures")
@patch("worker.tasks.executor._cleanup")
def test_spawn_failure_marks_run_as_failed(mock_cleanup, mock_download, mock_spawn, mock_status):
    """spawn_k6 raises (e.g. binary not found) → FAILED with detail."""
    mock_download.return_value = ["/tmp/test-wl"]
    mock_spawn.side_effect = FileNotFoundError("/usr/local/bin/k6: not found")

    with pytest.raises(FileNotFoundError):
        k6_executor_task.run(_make_plan(), "run-spawn-err", "0%:100%", 1)

    mock_status.assert_called_once()
    assert mock_status.call_args[0][1] == "FAILED"
    assert "k6 spawn failed" in mock_status.call_args[1]["error_detail"]
    assert "FileNotFoundError" in mock_status.call_args[1]["error_detail"]
