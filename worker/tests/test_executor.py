"""Tests for worker.tasks.executor — error handling and k6 exit code detection."""

import os
import pytest
from unittest.mock import MagicMock, call, patch

from worker.driver_manager.k6_manager import K6Result
from worker.tasks.executor import _sub_segment, k6_executor_task


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
# _sub_segment — fraction format for k6
# ---------------------------------------------------------------------------

def test_sub_segment_single_instance_passthrough():
    """total=1 returns the original segment unchanged."""
    assert _sub_segment("0%:100%", 0, 1) == "0%:100%"


def test_sub_segment_two_instances_from_percentage():
    """Percentage input split into 0–1 fractions."""
    assert _sub_segment("0%:100%", 0, 2) == "0:0.5"
    assert _sub_segment("0%:100%", 1, 2) == "0.5:1"


def test_sub_segment_three_instances():
    s0 = _sub_segment("0%:100%", 0, 3)
    s1 = _sub_segment("0%:100%", 1, 3)
    s2 = _sub_segment("0%:100%", 2, 3)
    # Verify start of each equals end of previous
    assert s0.split(":")[1] == s1.split(":")[0]
    assert s1.split(":")[1] == s2.split(":")[0]
    # First starts at 0, last ends at 1
    assert s0.startswith("0:")
    assert s2.endswith(":1")


def test_sub_segment_fraction_input():
    """Already in 0–1 fraction format."""
    assert _sub_segment("0:1", 0, 2) == "0:0.5"
    assert _sub_segment("0:1", 1, 2) == "0.5:1"


def test_sub_segment_no_trailing_zeros():
    """Output should not have unnecessary trailing zeros like 0.5000."""
    result = _sub_segment("0%:100%", 0, 2)
    assert "0.5000" not in result
    assert result == "0:0.5"


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
@patch("worker.tasks.executor._cleanup_files")
def test_k6_nonzero_exit_marks_run_as_failed(
    mock_cleanup_files, mock_upload, mock_download, mock_spawn, mock_wait, mock_status
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
@patch("worker.tasks.executor._cleanup_files")
def test_k6_partial_failure_reports_all_failed_instances(
    mock_cleanup_files, mock_upload, mock_download, mock_spawn, mock_wait, mock_status
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
@patch("worker.tasks.executor._cleanup_files")
def test_k6_timeout_includes_timed_out_flag(
    mock_cleanup_files, mock_upload, mock_download, mock_spawn, mock_wait, mock_status
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
# k6 success → collect_and_store → COMPLETED
# ---------------------------------------------------------------------------

@patch("worker.tasks.executor.collect_and_store")
@patch("worker.tasks.executor.update_run_status")
@patch("worker.tasks.executor.wait_and_teardown")
@patch("worker.tasks.executor.spawn_k6")
@patch("worker.tasks.executor._download_sql_fixtures")
@patch("worker.tasks.executor._upload_to_s3")
@patch("worker.tasks.executor._cleanup_files")
def test_k6_success_calls_collect_and_store(
    mock_cleanup_files, mock_upload, mock_download, mock_spawn, mock_wait,
    mock_status, mock_collect,
):
    """All k6 instances exit 0 → collect_and_store is called (handles COLLECTING → COMPLETED)."""
    mock_download.return_value = ["/tmp/test-wl"]
    mock_spawn.return_value = _mock_process(returncode=0)
    mock_wait.return_value = [_k6_result(0)]

    result = k6_executor_task.run(_make_plan(), "run-ok", "0%:100%", 1)

    assert result["status"] == "completed"
    mock_collect.assert_called_once_with("run-ok", _make_plan(), abort_reason=None)


# ---------------------------------------------------------------------------
# Timeout computed from ramp_up + hold_for + buffer
# ---------------------------------------------------------------------------

@patch("worker.tasks.executor.collect_and_store")
@patch("worker.tasks.executor.update_run_status")
@patch("worker.tasks.executor.wait_and_teardown")
@patch("worker.tasks.executor.spawn_k6")
@patch("worker.tasks.executor._download_sql_fixtures")
@patch("worker.tasks.executor._upload_to_s3")
@patch("worker.tasks.executor._cleanup_files")
def test_wait_timeout_computed_from_plan_durations(
    mock_cleanup_files, mock_upload, mock_download, mock_spawn, mock_wait,
    mock_status, mock_collect,
):
    """timeout = parse(ramp_up) + parse(hold_for) + 30s buffer."""
    mock_download.return_value = ["/tmp/test-wl"]
    mock_spawn.return_value = _mock_process(returncode=0)
    mock_wait.return_value = [_k6_result(0)]

    plan = _make_plan()
    plan["execution"]["ramp_up"] = "30s"
    plan["execution"]["hold_for"] = "5m"
    k6_executor_task.run(plan, "run-timeout", "0%:100%", 1)

    # 30 + 300 + 30 = 360
    mock_wait.assert_called_once()
    actual_timeout = mock_wait.call_args[1]["timeout"]
    assert actual_timeout == 360


# ---------------------------------------------------------------------------
# k6 threshold abort (exit code 99) → COMPLETED with partial results
# ---------------------------------------------------------------------------

@patch("worker.tasks.executor.collect_and_store")
@patch("worker.tasks.executor.update_run_status")
@patch("worker.tasks.executor.wait_and_teardown")
@patch("worker.tasks.executor.spawn_k6")
@patch("worker.tasks.executor._download_sql_fixtures")
@patch("worker.tasks.executor._upload_to_s3")
@patch("worker.tasks.executor._cleanup_files")
def test_k6_threshold_abort_collects_results_and_completes(
    mock_cleanup_files, mock_upload, mock_download, mock_spawn, mock_wait,
    mock_status, mock_collect,
):
    """k6 exits with code 99 (threshold violation) → collect_and_store with abort_reason, not FAILED."""
    mock_download.return_value = ["/tmp/test-wl"]
    mock_spawn.return_value = _mock_process(returncode=99)
    mock_wait.return_value = [
        _k6_result(returncode=99, stderr="threshold 'query_errors' crossed"),
    ]

    result = k6_executor_task.run(_make_plan(), "run-threshold", "0%:100%", 1)

    assert result["status"] == "completed"
    mock_collect.assert_called_once()
    assert mock_collect.call_args[1]["abort_reason"] == \
        "SUT failure detected: query error rate exceeded threshold"
    # update_run_status should NOT have been called with FAILED
    for call in mock_status.call_args_list:
        assert call[0][1] != "FAILED"


@patch("worker.tasks.executor.collect_and_store")
@patch("worker.tasks.executor.update_run_status")
@patch("worker.tasks.executor.wait_and_teardown")
@patch("worker.tasks.executor.spawn_k6")
@patch("worker.tasks.executor._download_sql_fixtures")
@patch("worker.tasks.executor._upload_to_s3")
@patch("worker.tasks.executor._cleanup_files")
def test_k6_mixed_threshold_and_real_failure_marks_failed(
    mock_cleanup_files, mock_upload, mock_download, mock_spawn, mock_wait,
    mock_status, mock_collect,
):
    """One instance exits 99 (threshold) + another exits 1 (real error) → FAILED."""
    mock_download.return_value = ["/tmp/test-wl"]
    mock_spawn.side_effect = [_mock_process(99), _mock_process(1)]
    mock_wait.return_value = [
        _k6_result(returncode=99, stderr="threshold crossed"),
        _k6_result(returncode=1, stderr="connection refused"),
    ]

    result = k6_executor_task.run(_make_plan(), "run-mixed", "0%:100%", 2)

    assert result["status"] == "failed"
    assert "instance 1 exited with code 1" in result["error"]
    # Instance 0 (code 99) should NOT appear in the error
    assert "instance 0" not in result["error"]
    mock_collect.assert_not_called()


@patch("worker.tasks.executor.collect_and_store")
@patch("worker.tasks.executor.update_run_status")
@patch("worker.tasks.executor.wait_and_teardown")
@patch("worker.tasks.executor.spawn_k6")
@patch("worker.tasks.executor._download_sql_fixtures")
@patch("worker.tasks.executor._upload_to_s3")
@patch("worker.tasks.executor._cleanup_files")
def test_k6_all_instances_threshold_abort_multi(
    mock_cleanup_files, mock_upload, mock_download, mock_spawn, mock_wait,
    mock_status, mock_collect,
):
    """Multiple instances all exit 99 → COMPLETED with abort_reason (not FAILED)."""
    mock_download.return_value = ["/tmp/test-wl"]
    mock_spawn.side_effect = [_mock_process(99), _mock_process(99)]
    mock_wait.return_value = [
        _k6_result(returncode=99, stderr="threshold crossed"),
        _k6_result(returncode=99, stderr="threshold crossed"),
    ]

    result = k6_executor_task.run(_make_plan(), "run-all-99", "0%:100%", 2)

    assert result["status"] == "completed"
    mock_collect.assert_called_once()
    assert mock_collect.call_args[1]["abort_reason"] == \
        "SUT failure detected: query error rate exceeded threshold"
    for call in mock_status.call_args_list:
        assert call[0][1] != "FAILED"


# ---------------------------------------------------------------------------
# Spawn failure → FAILED
# ---------------------------------------------------------------------------

@patch("worker.tasks.executor.update_run_status")
@patch("worker.tasks.executor.spawn_k6")
@patch("worker.tasks.executor._download_sql_fixtures")
@patch("worker.tasks.executor._cleanup_files")
def test_spawn_failure_marks_run_as_failed(mock_cleanup_files, mock_download, mock_spawn, mock_status):
    """spawn_k6 raises (e.g. binary not found) → FAILED with detail."""
    mock_download.return_value = ["/tmp/test-wl"]
    mock_spawn.side_effect = FileNotFoundError("/usr/local/bin/k6: not found")

    with pytest.raises(FileNotFoundError):
        k6_executor_task.run(_make_plan(), "run-spawn-err", "0%:100%", 1)

    mock_status.assert_called_once()
    assert mock_status.call_args[0][1] == "FAILED"
    assert "k6 spawn failed" in mock_status.call_args[1]["error_detail"]
    assert "FileNotFoundError" in mock_status.call_args[1]["error_detail"]


# ---------------------------------------------------------------------------
# Cleanup ordering — CSV files must survive until collect_and_store finishes
# ---------------------------------------------------------------------------

@patch("worker.tasks.executor.collect_and_store")
@patch("worker.tasks.executor.update_run_status")
@patch("worker.tasks.executor.wait_and_teardown")
@patch("worker.tasks.executor.spawn_k6")
@patch("worker.tasks.executor._download_sql_fixtures")
@patch("worker.tasks.executor._upload_to_s3")
@patch("worker.tasks.executor._cleanup_files")
def test_cleanup_called_after_collect_and_store_on_success(
    mock_cleanup_files, mock_upload, mock_download, mock_spawn, mock_wait,
    mock_status, mock_collect,
):
    """_cleanup must be called AFTER collect_and_store, not before it.

    Regression test for the bug where _cleanup deleted CSV files before
    collect_and_store could parse them, resulting in empty per-query metrics.
    """
    mock_download.return_value = ["/tmp/test-wl"]
    mock_spawn.return_value = _mock_process(returncode=0)
    mock_wait.return_value = [_k6_result(0)]

    # Track call order across both mocks
    call_order = []
    mock_collect.side_effect = lambda *a, **kw: call_order.append("collect_and_store")
    mock_cleanup_files.side_effect = lambda *a, **kw: call_order.append("cleanup_files")

    k6_executor_task.run(_make_plan(), "run-order", "0%:100%", 1)

    assert "collect_and_store" in call_order, "collect_and_store was not called"
    assert "cleanup_files" in call_order, "_cleanup_files was not called"
    assert call_order.index("collect_and_store") < call_order.index("cleanup_files"), (
        f"_cleanup_files was called before collect_and_store! Order: {call_order}"
    )


@patch("worker.tasks.executor.collect_and_store")
@patch("worker.tasks.executor.update_run_status")
@patch("worker.tasks.executor.wait_and_teardown")
@patch("worker.tasks.executor.spawn_k6")
@patch("worker.tasks.executor._download_sql_fixtures")
@patch("worker.tasks.executor._upload_to_s3")
@patch("worker.tasks.executor._cleanup_files")
def test_cleanup_called_after_collect_on_threshold_abort(
    mock_cleanup_files, mock_upload, mock_download, mock_spawn, mock_wait,
    mock_status, mock_collect,
):
    """On threshold abort (exit 99), cleanup must still happen after collect_and_store."""
    mock_download.return_value = ["/tmp/test-wl"]
    mock_spawn.return_value = _mock_process(returncode=99)
    mock_wait.return_value = [_k6_result(returncode=99, stderr="threshold crossed")]

    call_order = []
    mock_collect.side_effect = lambda *a, **kw: call_order.append("collect_and_store")
    mock_cleanup_files.side_effect = lambda *a, **kw: call_order.append("cleanup_files")

    k6_executor_task.run(_make_plan(), "run-99-order", "0%:100%", 1)

    assert call_order.index("collect_and_store") < call_order.index("cleanup_files")


@patch("worker.tasks.executor.update_run_status")
@patch("worker.tasks.executor.wait_and_teardown")
@patch("worker.tasks.executor.spawn_k6")
@patch("worker.tasks.executor._download_sql_fixtures")
@patch("worker.tasks.executor._upload_to_s3")
@patch("worker.tasks.executor._cleanup_files")
def test_cleanup_called_on_k6_failure_path(
    mock_cleanup_files, mock_upload, mock_download, mock_spawn, mock_wait, mock_status,
):
    """k6 failure (non-zero, non-99 exit) still cleans up local files."""
    mock_download.return_value = ["/tmp/test-wl"]
    mock_spawn.return_value = _mock_process(returncode=1)
    mock_wait.return_value = [_k6_result(returncode=1, stderr="error")]

    k6_executor_task.run(_make_plan(), "run-fail-cleanup", "0%:100%", 1)

    mock_cleanup_files.assert_called_once()


# ---------------------------------------------------------------------------
# Integration: CSV files are readable when collect_and_store runs
# ---------------------------------------------------------------------------

@patch("worker.tasks.executor.collect_and_store")
@patch("worker.tasks.executor.update_run_status")
@patch("worker.tasks.executor.wait_and_teardown")
@patch("worker.tasks.executor.spawn_k6")
@patch("worker.tasks.executor._download_sql_fixtures")
@patch("worker.tasks.executor._upload_to_s3")
def test_csv_files_cleaned_up_after_collect(
    mock_upload_s3, mock_download, mock_spawn, mock_wait,
    mock_status, mock_collect,
):
    """Local CSV and SQL files are cleaned up after collect_and_store."""
    run_id = "run-cleanup-integration"
    csv_path = f"/tmp/k6_raw_{run_id}_0_0.csv"
    sql_path = "/tmp/test-wl-cleanup"

    try:
        with open(csv_path, "w") as f:
            f.write("metric_name,timestamp,metric_value\n")
        with open(sql_path, "w") as f:
            f.write("SELECT 1;")

        mock_download.return_value = [sql_path]
        mock_spawn.return_value = _mock_process(returncode=0)
        mock_wait.return_value = [_k6_result(0)]

        k6_executor_task.run(_make_plan(), run_id, "0%:100%", 1)

        assert not os.path.exists(csv_path), "CSV file was not cleaned up"
        assert not os.path.exists(sql_path), "SQL fixture was not cleaned up"

    finally:
        for p in [csv_path, sql_path]:
            if os.path.exists(p):
                os.remove(p)


@patch("worker.tasks.executor.collect_and_store")
@patch("worker.tasks.executor.update_run_status")
@patch("worker.tasks.executor.wait_and_teardown")
@patch("worker.tasks.executor.spawn_k6")
@patch("worker.tasks.executor._download_sql_fixtures")
@patch("worker.tasks.executor._upload_to_s3")
def test_csv_files_use_segment_index_in_filename(
    mock_upload_s3, mock_download, mock_spawn, mock_wait,
    mock_status, mock_collect,
):
    """CSV filenames include segment_index for unique S3 keys."""
    run_id = "run-seg-naming"
    csv_path = f"/tmp/k6_raw_{run_id}_3_0.csv"  # segment_index=3
    sql_path = "/tmp/test-wl-seg"

    try:
        with open(csv_path, "w") as f:
            f.write("metric_name,timestamp,metric_value\n")
        with open(sql_path, "w") as f:
            f.write("SELECT 1;")

        mock_download.return_value = [sql_path]
        mock_spawn.return_value = _mock_process(returncode=0)
        mock_wait.return_value = [_k6_result(0)]

        k6_executor_task.run(_make_plan(), run_id, "0%:100%", 1, segment_index=3)

        # Verify the upload used the segment_index in the S3 key
        mock_upload_s3.assert_called_once()
        local_path_arg = mock_upload_s3.call_args[0][0]
        s3_key_arg = mock_upload_s3.call_args[0][1]
        assert "3_0" in local_path_arg
        assert "k6_raw_3_0.csv" in s3_key_arg

    finally:
        for p in [csv_path, sql_path]:
            if os.path.exists(p):
                os.remove(p)
