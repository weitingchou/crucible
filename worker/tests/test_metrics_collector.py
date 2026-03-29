"""Tests for worker.metrics_collector — k6 CSV parsing and Prometheus querying."""

import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from worker.metrics_collector import (
    _parse_k6_csvs,
    _percentile,
    _query_prometheus_sources,
    _summarize_metrics,
    collect_and_store,
)


# ---------------------------------------------------------------------------
# _percentile
# ---------------------------------------------------------------------------

def test_percentile_single_value():
    assert _percentile([5.0], 50) == 5.0
    assert _percentile([5.0], 99) == 5.0


def test_percentile_two_values():
    assert _percentile([1.0, 3.0], 50) == 2.0


def test_percentile_known_values():
    vals = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert _percentile(vals, 0) == 1.0
    assert _percentile(vals, 50) == 3.0
    assert _percentile(vals, 100) == 5.0


# ---------------------------------------------------------------------------
# _summarize_metrics
# ---------------------------------------------------------------------------

def test_summarize_metrics_trend():
    result = _summarize_metrics({"sql_duration_Q1": [10.0, 20.0, 30.0, 40.0, 50.0]})
    assert len(result) == 1
    m = result[0]
    assert m["name"] == "sql_duration_Q1"
    assert m["type"] == "trend"
    assert m["stats"]["count"] == 5
    assert m["stats"]["min"] == 10.0
    assert m["stats"]["max"] == 50.0
    assert m["stats"]["avg"] == 30.0
    assert m["stats"]["med"] == 30.0
    assert m["stats"]["rate"] is None


def test_summarize_metrics_empty():
    assert _summarize_metrics({}) == []
    assert _summarize_metrics({"x": []}) == []


def test_summarize_metrics_sorted_by_name():
    result = _summarize_metrics({"z_metric": [1.0], "a_metric": [2.0]})
    assert result[0]["name"] == "a_metric"
    assert result[1]["name"] == "z_metric"


# ---------------------------------------------------------------------------
# _parse_k6_csvs
# ---------------------------------------------------------------------------

def test_parse_k6_csvs_reads_csv_files():
    """Parse a synthetic k6 CSV and verify summary stats."""
    run_id = "test-parse-csv"
    csv_content = (
        "metric_name,timestamp,metric_value\n"
        "sql_duration_Q1,1710510600,10.5\n"
        "sql_duration_Q1,1710510601,20.5\n"
        "sql_duration_Q1,1710510602,30.5\n"
        "sql_duration_Q2,1710510600,5.0\n"
        "sql_duration_Q2,1710510601,15.0\n"
    )
    path = f"/tmp/k6_raw_{run_id}_0.csv"
    try:
        with open(path, "w") as f:
            f.write(csv_content)
        result = _parse_k6_csvs(run_id, 1)
        names = [m["name"] for m in result]
        assert "sql_duration_Q1" in names
        assert "sql_duration_Q2" in names
        q1 = next(m for m in result if m["name"] == "sql_duration_Q1")
        assert q1["stats"]["count"] == 3
        assert q1["stats"]["min"] == 10.5
        assert q1["stats"]["max"] == 30.5
    finally:
        if os.path.exists(path):
            os.remove(path)


def test_parse_k6_csvs_missing_file_returns_empty():
    result = _parse_k6_csvs("nonexistent-run", 1)
    assert result == []


def test_parse_k6_csvs_skips_rows_with_missing_fields():
    """Rows missing metric_name or metric_value should be silently skipped."""
    run_id = "test-bad-rows"
    csv_content = (
        "metric_name,timestamp,metric_value\n"
        ",1710510600,10.0\n"              # empty metric_name
        "sql_duration_Q1,1710510600,\n"   # empty metric_value
        "sql_duration_Q1,1710510601,20.0\n"  # valid
    )
    path = f"/tmp/k6_raw_{run_id}_0.csv"
    try:
        with open(path, "w") as f:
            f.write(csv_content)
        result = _parse_k6_csvs(run_id, 1)
        assert len(result) == 1
        assert result[0]["stats"]["count"] == 1
        assert result[0]["stats"]["min"] == 20.0
    finally:
        if os.path.exists(path):
            os.remove(path)


def test_parse_k6_csvs_skips_non_numeric_values():
    """Non-numeric metric_value should be skipped without raising."""
    run_id = "test-nonnumeric"
    csv_content = (
        "metric_name,timestamp,metric_value\n"
        "sql_duration_Q1,1710510600,not_a_number\n"
        "sql_duration_Q1,1710510601,5.0\n"
    )
    path = f"/tmp/k6_raw_{run_id}_0.csv"
    try:
        with open(path, "w") as f:
            f.write(csv_content)
        result = _parse_k6_csvs(run_id, 1)
        assert len(result) == 1
        assert result[0]["stats"]["count"] == 1
    finally:
        if os.path.exists(path):
            os.remove(path)


def test_parse_k6_csvs_multiple_instances():
    """Merge metrics from multiple k6 CSV files."""
    run_id = "test-multi-csv"
    paths = []
    try:
        for i in range(2):
            path = f"/tmp/k6_raw_{run_id}_{i}.csv"
            paths.append(path)
            with open(path, "w") as f:
                f.write("metric_name,timestamp,metric_value\n")
                f.write(f"sql_duration_Q1,1710510600,{10.0 + i * 10}\n")

        result = _parse_k6_csvs(run_id, 2)
        q1 = next(m for m in result if m["name"] == "sql_duration_Q1")
        assert q1["stats"]["count"] == 2
    finally:
        for p in paths:
            if os.path.exists(p):
                os.remove(p)


# ---------------------------------------------------------------------------
# collect_and_store — integration
# ---------------------------------------------------------------------------

@patch("worker.metrics_collector._upload_results_json")
@patch("worker.metrics_collector._parse_k6_csvs")
@patch("worker.metrics_collector.update_run_status")
def test_collect_and_store_no_observability(mock_status, mock_parse, mock_upload):
    """Plan without observability → k6 stats only, no Prometheus queries."""
    mock_parse.return_value = [
        {"name": "sql_duration_Q1", "type": "trend", "stats": {"count": 10}},
    ]

    plan = {"test_environment": {}, "execution": {}}
    collect_and_store("run-1", plan, 1)

    # Transitions: COLLECTING → COMPLETED
    assert mock_status.call_count == 2
    assert mock_status.call_args_list[0][0] == ("run-1", "COLLECTING")
    assert mock_status.call_args_list[1][0] == ("run-1", "COMPLETED")

    # Results uploaded
    mock_upload.assert_called_once()
    results = mock_upload.call_args[0][1]
    assert results["run_id"] == "run-1"
    assert results["collection_error"] is None
    assert results["abort_reason"] is None
    assert len(results["k6"]["metrics"]) == 1
    assert results["observability"]["sources"] == []


@patch("worker.metrics_collector._upload_results_json")
@patch("worker.metrics_collector._parse_k6_csvs")
@patch("worker.metrics_collector.update_run_status")
def test_collect_and_store_includes_abort_reason(mock_status, mock_parse, mock_upload):
    """abort_reason is included in results JSON when set."""
    mock_parse.return_value = [
        {"name": "sql_duration_Q1", "type": "trend", "stats": {"count": 5}},
    ]

    plan = {"test_environment": {}, "execution": {}}
    collect_and_store("run-abort", plan, 1, abort_reason="SUT failure detected: query error rate exceeded threshold")

    mock_upload.assert_called_once()
    results = mock_upload.call_args[0][1]
    assert results["abort_reason"] == "SUT failure detected: query error rate exceeded threshold"
    assert results["collection_error"] is None
    # Still transitions to COMPLETED
    assert mock_status.call_args_list[-1][0] == ("run-abort", "COMPLETED")


@patch("worker.metrics_collector._upload_results_json")
@patch("worker.metrics_collector._query_prometheus_sources")
@patch("worker.metrics_collector._parse_k6_csvs")
@patch("worker.metrics_collector.update_run_status")
def test_collect_and_store_with_observability(mock_status, mock_parse, mock_prom, mock_upload):
    """Plan with prometheus_sources → both k6 and Prometheus results."""
    mock_parse.return_value = []
    mock_prom.return_value = (
        [{"name": "engine", "url": "http://prom:9090", "metrics": []}],
        None,
    )

    plan = {
        "test_environment": {
            "observability": {
                "prometheus_sources": [
                    {"name": "engine", "url": "http://prom:9090", "metrics": [{"name": "qps", "query": "sum(x)"}]},
                ],
            },
        },
        "execution": {},
    }
    collect_and_store("run-2", plan, 1)

    mock_prom.assert_called_once()
    results = mock_upload.call_args[0][1]
    assert len(results["observability"]["sources"]) == 1
    assert results["collection_error"] is None


@patch("worker.metrics_collector._upload_results_json")
@patch("worker.metrics_collector._parse_k6_csvs")
@patch("worker.metrics_collector.update_run_status")
def test_collect_and_store_csv_error_sets_collection_error(mock_status, mock_parse, mock_upload):
    """CSV parse failure → collection_error set, run still COMPLETED."""
    mock_parse.side_effect = RuntimeError("corrupt CSV")

    plan = {"test_environment": {}, "execution": {}}
    collect_and_store("run-err", plan, 1)

    results = mock_upload.call_args[0][1]
    assert "corrupt CSV" in results["collection_error"]
    assert results["k6"]["metrics"] == []
    # Still transitions to COMPLETED
    assert mock_status.call_args_list[1][0] == ("run-err", "COMPLETED")


@patch("worker.metrics_collector._upload_results_json")
@patch("worker.metrics_collector._query_prometheus_sources")
@patch("worker.metrics_collector._parse_k6_csvs")
@patch("worker.metrics_collector.update_run_status")
def test_collect_and_store_prometheus_error_sets_collection_error(
    mock_status, mock_parse, mock_prom, mock_upload
):
    """Prometheus query failure → collection_error set, k6 stats still present."""
    mock_parse.return_value = [{"name": "Q1", "type": "trend", "stats": {"count": 5}}]
    mock_prom.return_value = ([], "Prometheus source 'infra' unreachable: Connection refused")

    plan = {
        "test_environment": {
            "observability": {
                "prometheus_sources": [
                    {"name": "infra", "url": "http://prom:9090", "metrics": [{"name": "cpu", "query": "avg(x)"}]},
                ],
            },
        },
        "execution": {},
    }
    collect_and_store("run-prom-err", plan, 1)

    results = mock_upload.call_args[0][1]
    assert "unreachable" in results["collection_error"]
    assert len(results["k6"]["metrics"]) == 1


@patch("worker.metrics_collector._upload_results_json")
@patch("worker.metrics_collector._parse_k6_csvs")
@patch("worker.metrics_collector.update_run_status")
def test_collect_and_store_s3_upload_failure_still_completes(
    mock_status, mock_parse, mock_upload
):
    """S3 upload failure → run still transitions to COMPLETED."""
    mock_parse.return_value = [{"name": "Q1", "type": "trend", "stats": {"count": 5}}]
    mock_upload.side_effect = RuntimeError("S3 unreachable")

    plan = {"test_environment": {}, "execution": {}}
    collect_and_store("run-s3-err", plan, 1)

    # Still transitions to COMPLETED despite upload failure
    assert mock_status.call_args_list[-1][0] == ("run-s3-err", "COMPLETED")


# ---------------------------------------------------------------------------
# _query_prometheus_sources — unit tests
# ---------------------------------------------------------------------------

@patch("worker.metrics_collector._prometheus_query_range")
@patch("worker.metrics_collector._get_run_time_window")
def test_query_prometheus_sources_returns_metrics(mock_window, mock_query):
    """Successful query returns source results with no error."""
    mock_window.return_value = (1000.0, 2000.0)
    mock_query.return_value = [[1500, "42.5"]]

    sources_cfg = [
        {
            "name": "engine",
            "url": "http://prom:9090",
            "resolution": 15,
            "max_data_points": 500,
            "metrics": [{"name": "qps", "query": "sum(rate(x[1m]))"}],
        },
    ]
    results, error = _query_prometheus_sources(sources_cfg, "run-1")
    assert error is None
    assert len(results) == 1
    assert results[0]["name"] == "engine"
    assert len(results[0]["metrics"]) == 1
    assert results[0]["metrics"][0]["values"] == [[1500, "42.5"]]


@patch("worker.metrics_collector._prometheus_query_range")
@patch("worker.metrics_collector._get_run_time_window")
def test_query_prometheus_sources_per_metric_error(mock_window, mock_query):
    """One metric fails → error string set, other metrics still collected."""
    mock_window.return_value = (1000.0, 2000.0)
    mock_query.side_effect = [
        [[1500, "42.5"]],           # first metric succeeds
        ConnectionError("refused"),  # second metric fails
    ]

    sources_cfg = [
        {
            "name": "engine",
            "url": "http://prom:9090",
            "metrics": [
                {"name": "qps", "query": "sum(rate(x[1m]))"},
                {"name": "errors", "query": "sum(rate(err[1m]))"},
            ],
        },
    ]
    results, error = _query_prometheus_sources(sources_cfg, "run-1")
    assert error is not None
    assert "refused" in error
    # The successful metric should still be in results
    assert len(results) == 1
    assert len(results[0]["metrics"]) == 1
    assert results[0]["metrics"][0]["name"] == "qps"


@patch("worker.metrics_collector._prometheus_query_range")
@patch("worker.metrics_collector._get_run_time_window")
def test_query_prometheus_sources_fallback_time_window(mock_window, mock_query):
    """When started_at is None, falls back to last 5 minutes."""
    mock_window.return_value = (None, None)
    mock_query.return_value = []

    sources_cfg = [
        {
            "name": "engine",
            "url": "http://prom:9090",
            "metrics": [{"name": "qps", "query": "sum(x)"}],
        },
    ]
    _query_prometheus_sources(sources_cfg, "run-1")
    # Verify query was called (fallback window was used)
    mock_query.assert_called_once()
    call_args = mock_query.call_args[0]
    # start should be ~300s before end
    assert call_args[3] - call_args[2] == pytest.approx(300, abs=1)
