"""Tests for worker.metrics_collector — k6 CSV parsing and Prometheus querying."""

import json
import os
from pathlib import Path
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from worker.metrics_collector import (
    _download_and_parse_csvs,
    _percentile,
    _prometheus_query_range,
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
# _download_and_parse_csvs — reads CSVs from S3
# ---------------------------------------------------------------------------

def _make_csv(rows_str: str) -> bytes:
    return ("metric_name,timestamp,metric_value\n" + rows_str).encode()


def _mock_s3_with_csvs(csv_dict: dict[str, bytes]):
    """Return a mock S3 client with list_objects_v2 + get_object for given key→body pairs."""
    mock_s3 = MagicMock()
    paginator = MagicMock()
    contents = [{"Key": k} for k in csv_dict]
    paginator.paginate.return_value = [{"Contents": contents}]
    mock_s3.get_paginator.return_value = paginator

    def get_object(Bucket, Key):
        body = MagicMock()
        body.read.return_value = csv_dict[Key]
        return {"Body": body}

    mock_s3.get_object.side_effect = get_object
    return mock_s3


@patch("worker.metrics_collector._s3_client")
def test_download_and_parse_single_csv(mock_s3_fn):
    """Single CSV in S3 → metrics parsed correctly."""
    csv_data = _make_csv(
        "sql_duration_Q1,1710510600,10.5\n"
        "sql_duration_Q1,1710510601,20.5\n"
        "sql_duration_Q2,1710510600,5.0\n"
    )
    mock_s3_fn.return_value = _mock_s3_with_csvs({
        "results/run-1/k6_raw_0_0.csv": csv_data,
    })
    result = _download_and_parse_csvs("run-1")
    names = [m["name"] for m in result]
    assert "sql_duration_Q1" in names
    assert "sql_duration_Q2" in names
    q1 = next(m for m in result if m["name"] == "sql_duration_Q1")
    assert q1["stats"]["count"] == 2
    assert q1["stats"]["min"] == 10.5
    assert q1["stats"]["max"] == 20.5


@patch("worker.metrics_collector._s3_client")
def test_download_and_parse_merges_multiple_csvs(mock_s3_fn):
    """Multiple CSVs from different segments → merged into one result."""
    csv_a = _make_csv("sql_duration_Q1,1710510600,10.0\nsql_duration_Q1,1710510601,20.0\n")
    csv_b = _make_csv("sql_duration_Q1,1710510600,30.0\nsql_duration_Q1,1710510601,40.0\n")
    mock_s3_fn.return_value = _mock_s3_with_csvs({
        "results/run-2/k6_raw_0_0.csv": csv_a,
        "results/run-2/k6_raw_1_0.csv": csv_b,
    })
    result = _download_and_parse_csvs("run-2")
    q1 = next(m for m in result if m["name"] == "sql_duration_Q1")
    assert q1["stats"]["count"] == 4
    assert q1["stats"]["min"] == 10.0
    assert q1["stats"]["max"] == 40.0


@patch("worker.metrics_collector._s3_client")
def test_download_and_parse_no_csvs_returns_empty(mock_s3_fn):
    """No CSV files in S3 prefix → empty metrics list."""
    mock_s3 = MagicMock()
    paginator = MagicMock()
    paginator.paginate.return_value = [{"Contents": []}]
    mock_s3.get_paginator.return_value = paginator
    mock_s3_fn.return_value = mock_s3
    result = _download_and_parse_csvs("nonexistent-run")
    assert result == []


@patch("worker.metrics_collector._s3_client")
def test_download_and_parse_skips_bad_rows(mock_s3_fn):
    """Rows with missing or non-numeric values are silently skipped."""
    csv_data = _make_csv(
        ",1710510600,10.0\n"
        "sql_duration_Q1,1710510600,\n"
        "sql_duration_Q1,1710510601,not_a_number\n"
        "sql_duration_Q1,1710510602,5.0\n"
    )
    mock_s3_fn.return_value = _mock_s3_with_csvs({
        "results/run-3/k6_raw_0_0.csv": csv_data,
    })
    result = _download_and_parse_csvs("run-3")
    assert len(result) == 1
    assert result[0]["stats"]["count"] == 1
    assert result[0]["stats"]["min"] == 5.0


# ---------------------------------------------------------------------------
# collect_and_store — integration
# ---------------------------------------------------------------------------

@patch("worker.metrics_collector._upload_results_json")
@patch("worker.metrics_collector._download_and_parse_csvs")
@patch("worker.metrics_collector.update_run_status")
def test_collect_and_store_no_observability(mock_status, mock_parse, mock_upload):
    """Plan without observability → k6 stats only, no Prometheus queries."""
    mock_parse.return_value = [
        {"name": "sql_duration_Q1", "type": "trend", "stats": {"count": 10}},
    ]

    plan = {"test_environment": {}, "execution": {}}
    collect_and_store("run-1", plan)

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
@patch("worker.metrics_collector._download_and_parse_csvs")
@patch("worker.metrics_collector.update_run_status")
def test_collect_and_store_includes_abort_reason(mock_status, mock_parse, mock_upload):
    """abort_reason is included in results JSON when set."""
    mock_parse.return_value = [
        {"name": "sql_duration_Q1", "type": "trend", "stats": {"count": 5}},
    ]

    plan = {"test_environment": {}, "execution": {}}
    collect_and_store("run-abort", plan, abort_reason="SUT failure detected: query error rate exceeded threshold")

    mock_upload.assert_called_once()
    results = mock_upload.call_args[0][1]
    assert results["abort_reason"] == "SUT failure detected: query error rate exceeded threshold"
    assert results["collection_error"] is None
    # Still transitions to COMPLETED
    assert mock_status.call_args_list[-1][0] == ("run-abort", "COMPLETED")


@patch("worker.metrics_collector._upload_results_json")
@patch("worker.metrics_collector._query_prometheus_sources")
@patch("worker.metrics_collector._download_and_parse_csvs")
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
    collect_and_store("run-2", plan)

    mock_prom.assert_called_once()
    results = mock_upload.call_args[0][1]
    assert len(results["observability"]["sources"]) == 1
    assert results["collection_error"] is None


@patch("worker.metrics_collector._upload_results_json")
@patch("worker.metrics_collector._download_and_parse_csvs")
@patch("worker.metrics_collector.update_run_status")
def test_collect_and_store_csv_error_sets_collection_error(mock_status, mock_parse, mock_upload):
    """CSV parse failure → collection_error set, run still COMPLETED."""
    mock_parse.side_effect = RuntimeError("corrupt CSV")

    plan = {"test_environment": {}, "execution": {}}
    collect_and_store("run-err", plan)

    results = mock_upload.call_args[0][1]
    assert "corrupt CSV" in results["collection_error"]
    assert results["k6"]["metrics"] == []
    # Still transitions to COMPLETED
    assert mock_status.call_args_list[1][0] == ("run-err", "COMPLETED")


@patch("worker.metrics_collector._upload_results_json")
@patch("worker.metrics_collector._query_prometheus_sources")
@patch("worker.metrics_collector._download_and_parse_csvs")
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
    collect_and_store("run-prom-err", plan)

    results = mock_upload.call_args[0][1]
    assert "unreachable" in results["collection_error"]
    assert len(results["k6"]["metrics"]) == 1


@patch("worker.metrics_collector._upload_results_json")
@patch("worker.metrics_collector._download_and_parse_csvs")
@patch("worker.metrics_collector.update_run_status")
def test_collect_and_store_s3_upload_failure_still_completes(
    mock_status, mock_parse, mock_upload
):
    """S3 upload failure → run still transitions to COMPLETED."""
    mock_parse.return_value = [{"name": "Q1", "type": "trend", "stats": {"count": 5}}]
    mock_upload.side_effect = RuntimeError("S3 unreachable")

    plan = {"test_environment": {}, "execution": {}}
    collect_and_store("run-s3-err", plan)

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


# ---------------------------------------------------------------------------
# Per-source TLS (tls.ca_bundle_pem)
# ---------------------------------------------------------------------------

@patch("worker.metrics_collector._prometheus_query_range")
@patch("worker.metrics_collector._get_run_time_window")
def test_source_without_tls_uses_system_trust(mock_window, mock_query):
    mock_window.return_value = (1000.0, 2000.0)
    mock_query.return_value = [[1500, "1"]]

    _query_prometheus_sources(
        [{"name": "s", "url": "http://prom:9090",
          "metrics": [{"name": "m", "query": "up"}]}],
        "run-1",
    )
    assert mock_query.call_args.kwargs["session"].verify is True


@patch("worker.metrics_collector._prometheus_query_range")
@patch("worker.metrics_collector._get_run_time_window")
def test_ca_bundle_is_written_to_a_file_and_used_by_the_session(mock_window, mock_query):
    """A private per-run CA must reach requests as a readable bundle path."""
    mock_window.return_value = (1000.0, 2000.0)
    seen = {}

    def _capture(*args, **kwargs):
        path = kwargs["session"].verify
        seen["path"] = path
        seen["contents"] = Path(path).read_text()
        return [[1500, "1"]]

    mock_query.side_effect = _capture

    _query_prometheus_sources(
        [{"name": "s", "url": "https://prom:8428",
          "tls": {"ca_bundle_pem": "-----BEGIN CERTIFICATE-----\nabc\n"},
          "metrics": [{"name": "m", "query": "up"}]}],
        "run-1",
    )
    assert seen["contents"] == "-----BEGIN CERTIFICATE-----\nabc\n"
    # ...and must not be left behind on disk afterwards.
    assert not os.path.exists(seen["path"])


@patch("worker.metrics_collector._prometheus_query_range")
@patch("worker.metrics_collector._get_run_time_window")
def test_ca_bundle_file_is_removed_even_when_the_query_fails(mock_window, mock_query):
    mock_window.return_value = (1000.0, 2000.0)
    seen = {}

    def _capture(*args, **kwargs):
        seen["path"] = kwargs["session"].verify
        raise ConnectionError("refused")

    mock_query.side_effect = _capture

    _query_prometheus_sources(
        [{"name": "s", "url": "https://prom:8428",
          "tls": {"ca_bundle_pem": "pem"},
          "metrics": [{"name": "m", "query": "up"}]}],
        "run-1",
    )
    assert not os.path.exists(seen["path"])


@patch("worker.metrics_collector._prometheus_query_range")
@patch("worker.metrics_collector._get_run_time_window")
def test_empty_tls_block_falls_back_to_system_trust(mock_window, mock_query):
    mock_window.return_value = (1000.0, 2000.0)
    mock_query.return_value = [[1500, "1"]]

    _query_prometheus_sources(
        [{"name": "s", "url": "https://prom:8428", "tls": {},
          "metrics": [{"name": "m", "query": "up"}]}],
        "run-1",
    )
    assert mock_query.call_args.kwargs["session"].verify is True


# ---------------------------------------------------------------------------
# Per-source error reporting
# ---------------------------------------------------------------------------

@patch("worker.metrics_collector._prometheus_query_range")
@patch("worker.metrics_collector._get_run_time_window")
def test_healthy_source_reports_no_error(mock_window, mock_query):
    mock_window.return_value = (1000.0, 2000.0)
    mock_query.return_value = [[1500, "1"]]

    results, _ = _query_prometheus_sources(
        [{"name": "s", "url": "http://prom:9090",
          "metrics": [{"name": "m", "query": "up"}]}],
        "run-1",
    )
    assert results[0]["error"] is None


@patch("worker.metrics_collector._prometheus_query_range")
@patch("worker.metrics_collector._get_run_time_window")
def test_partial_source_failure_is_reported_on_the_source(mock_window, mock_query):
    """Consumers gate completeness on the source's own error, not metric count."""
    mock_window.return_value = (1000.0, 2000.0)
    mock_query.side_effect = [[[1500, "1"]], ConnectionError("refused")]

    results, run_error = _query_prometheus_sources(
        [{"name": "engine", "url": "http://prom:9090",
          "metrics": [{"name": "ok", "query": "up"},
                      {"name": "bad", "query": "down"}]}],
        "run-1",
    )
    assert len(results[0]["metrics"]) == 1
    assert "bad" in results[0]["error"]
    assert "refused" in results[0]["error"]
    # The run-level string still aggregates it, named by source.
    assert "Prometheus source 'engine'" in run_error


@patch("worker.metrics_collector._prometheus_query_range")
@patch("worker.metrics_collector._get_run_time_window")
def test_fully_failed_source_is_still_listed(mock_window, mock_query):
    """A source that broke must not vanish — that reads as 'never configured'."""
    mock_window.return_value = (1000.0, 2000.0)
    mock_query.side_effect = ConnectionError("certificate verify failed")

    results, _ = _query_prometheus_sources(
        [{"name": "engine", "url": "https://prom:8428",
          "metrics": [{"name": "m", "query": "up"}]}],
        "run-1",
    )
    assert len(results) == 1
    assert results[0]["name"] == "engine"
    assert results[0]["metrics"] == []
    assert "certificate verify failed" in results[0]["error"]


@patch("worker.metrics_collector._prometheus_query_range")
@patch("worker.metrics_collector._get_run_time_window")
def test_source_with_no_metrics_configured_is_omitted(mock_window, mock_query):
    mock_window.return_value = (1000.0, 2000.0)
    results, _ = _query_prometheus_sources(
        [{"name": "s", "url": "http://prom:9090", "metrics": []}], "run-1"
    )
    assert results == []


# ---------------------------------------------------------------------------
# Collection must never strand a run in COLLECTING
# ---------------------------------------------------------------------------

@patch("worker.metrics_collector._upload_results_json")
@patch("worker.metrics_collector._query_prometheus_sources")
@patch("worker.metrics_collector._download_and_parse_csvs")
@patch("worker.metrics_collector.update_run_status")
def test_observability_crash_still_completes_the_run(
    mock_status, mock_parse, mock_prom, mock_upload
):
    """An exception from the Prometheus stage must not lose the whole run.

    It sits between two best-effort stages; letting it escape would skip both
    the results upload and the COMPLETED transition, stranding the run in
    COLLECTING with the already-parsed k6 data thrown away.
    """
    mock_parse.return_value = [{"name": "http_reqs", "type": "counter"}]
    mock_prom.side_effect = OSError("read-only file system")

    plan = {"test_environment": {"observability": {
        "prometheus_sources": [{"name": "s", "url": "https://p", "metrics": []}]}}}
    collect_and_store("run-x", plan)

    assert mock_status.call_args_list[-1].args[1] == "COMPLETED"
    results = mock_upload.call_args.args[1]
    assert "read-only file system" in results["collection_error"]
    # The k6 data survived.
    assert results["k6"]["metrics"] == [{"name": "http_reqs", "type": "counter"}]


@patch("worker.metrics_collector._upload_results_json")
@patch("worker.metrics_collector._download_and_parse_csvs")
@patch("worker.metrics_collector.update_run_status")
def test_explicit_observability_null_is_tolerated(mock_status, mock_parse, mock_upload):
    """Plans stored via the JSON endpoints round-trip as `observability: null`.

    model_dump() emits the key even when the section is absent, so a .get()
    default never fires and the value really is None.
    """
    mock_parse.return_value = []
    collect_and_store("run-y", {"test_environment": {"observability": None}})

    assert mock_status.call_args_list[-1].args[1] == "COMPLETED"
    assert mock_upload.call_args.args[1]["collection_error"] is None


# ---------------------------------------------------------------------------
# _prometheus_query_range — error reporting
# ---------------------------------------------------------------------------

def test_query_range_surfaces_the_endpoints_own_error_text():
    """A bad PromQL expression explains itself only in the response body."""
    resp = MagicMock(status_code=400, reason="Bad Request")
    resp.json.return_value = {
        "status": "error", "errorType": "bad_data",
        "error": "1:36: parse error: unclosed left parenthesis",
    }
    session = MagicMock()
    session.get.return_value = resp

    with pytest.raises(RuntimeError, match="unclosed left parenthesis"):
        _prometheus_query_range("http://p:9090", "sum(rate(x[1m])", 0, 1, 15, session=session)


def test_query_range_falls_back_to_body_text_when_not_json():
    resp = MagicMock(status_code=502, reason="Bad Gateway", text="upstream connect error")
    resp.json.side_effect = ValueError("no json")
    session = MagicMock()
    session.get.return_value = resp

    with pytest.raises(RuntimeError, match="upstream connect error"):
        _prometheus_query_range("http://p:9090", "up", 0, 1, 15, session=session)
