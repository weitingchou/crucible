"""Tests for crucible_lib.schemas.test_plan — observability section."""

import pytest
from pydantic import ValidationError

from crucible_lib.schemas.test_plan import (
    Observability,
    PrometheusMetric,
    PrometheusSource,
    TestPlan,
)


def _make_plan(**env_overrides) -> dict:
    env = {
        "env_type": "long-lived",
        "target_db": "tpch",
        "component_spec": {
            "type": "doris",
            "cluster_info": {"host": "doris-fe:9030", "username": "root", "password": ""},
        },
        "fixtures": [],
        **env_overrides,
    }
    return {
        "test_metadata": {"run_label": "test"},
        "test_environment": env,
        "execution": {
            "executor": "k6",
            "scaling_mode": "intra_node",
            "concurrency": 1,
            "ramp_up": "1s",
            "hold_for": "1s",
            "workload": [],
        },
    }


_SAMPLE_METRICS = [
    {"name": "cluster_qps", "query": "sum(rate(doris_be_query_total{job='doris-be'}[1m]))"},
    {"name": "avg_memory", "query": "avg(doris_be_mem_usage_bytes{job='doris-be'})"},
]


# ---------------------------------------------------------------------------
# PrometheusMetric
# ---------------------------------------------------------------------------

def test_prometheus_metric_valid():
    m = PrometheusMetric(name="cluster_qps", query="sum(rate(x[1m]))")
    assert m.name == "cluster_qps"
    assert m.query == "sum(rate(x[1m]))"


def test_prometheus_metric_requires_name():
    with pytest.raises(ValidationError):
        PrometheusMetric(query="sum(x)")


def test_prometheus_metric_requires_query():
    with pytest.raises(ValidationError):
        PrometheusMetric(name="qps")


# ---------------------------------------------------------------------------
# PrometheusSource
# ---------------------------------------------------------------------------

def test_prometheus_source_minimal():
    src = PrometheusSource(name="engine", url="http://prometheus:9090", metrics=_SAMPLE_METRICS)
    assert src.name == "engine"
    assert src.url == "http://prometheus:9090"
    assert len(src.metrics) == 2
    assert src.resolution == 15
    assert src.max_data_points == 500


def test_prometheus_source_custom_resolution_and_max_data_points():
    src = PrometheusSource(
        name="engine",
        url="http://prometheus:9090",
        metrics=_SAMPLE_METRICS,
        resolution=30,
        max_data_points=200,
    )
    assert src.resolution == 30
    assert src.max_data_points == 200


def test_prometheus_source_requires_name():
    with pytest.raises(ValidationError):
        PrometheusSource(url="http://prometheus:9090", metrics=_SAMPLE_METRICS)


def test_prometheus_source_requires_url():
    with pytest.raises(ValidationError):
        PrometheusSource(name="engine", metrics=_SAMPLE_METRICS)


def test_prometheus_source_requires_metrics():
    with pytest.raises(ValidationError):
        PrometheusSource(name="engine", url="http://prometheus:9090")


def test_prometheus_source_rejects_empty_metrics():
    with pytest.raises(ValidationError):
        PrometheusSource(name="engine", url="http://prometheus:9090", metrics=[])


def test_prometheus_source_rejects_zero_resolution():
    with pytest.raises(ValidationError):
        PrometheusSource(name="engine", url="http://prometheus:9090", metrics=_SAMPLE_METRICS, resolution=0)


def test_prometheus_source_rejects_zero_max_data_points():
    with pytest.raises(ValidationError):
        PrometheusSource(name="engine", url="http://prometheus:9090", metrics=_SAMPLE_METRICS, max_data_points=0)


# ---------------------------------------------------------------------------
# Observability
# ---------------------------------------------------------------------------

def test_observability_defaults_to_empty_sources():
    obs = Observability()
    assert obs.prometheus_sources == []


def test_observability_with_prometheus_sources():
    obs = Observability(prometheus_sources=[
        {
            "name": "engine",
            "url": "http://prom:9090",
            "metrics": [{"name": "qps", "query": "sum(rate(x[1m]))"}],
        },
    ])
    assert len(obs.prometheus_sources) == 1
    assert obs.prometheus_sources[0].name == "engine"
    assert obs.prometheus_sources[0].url == "http://prom:9090"


def test_observability_with_multiple_sources():
    obs = Observability(prometheus_sources=[
        {
            "name": "engine",
            "url": "http://prom-engine:9090",
            "metrics": [{"name": "qps", "query": "sum(rate(x[1m]))"}],
        },
        {
            "name": "infra",
            "url": "http://prom-infra:9090",
            "metrics": [{"name": "cpu", "query": "avg(rate(node_cpu[1m]))"}],
        },
    ])
    assert len(obs.prometheus_sources) == 2
    assert obs.prometheus_sources[0].name == "engine"
    assert obs.prometheus_sources[1].name == "infra"


# ---------------------------------------------------------------------------
# TestPlan — observability field
# ---------------------------------------------------------------------------

def test_plan_without_observability():
    """Existing plans without observability should still validate."""
    plan = TestPlan.model_validate(_make_plan())
    assert plan.test_environment.observability is None


def test_plan_with_observability_prometheus_sources():
    plan = TestPlan.model_validate(_make_plan(
        observability={
            "prometheus_sources": [
                {
                    "name": "engine",
                    "url": "http://prometheus:9090",
                    "metrics": _SAMPLE_METRICS,
                },
            ],
        },
    ))
    sources = plan.test_environment.observability.prometheus_sources
    assert len(sources) == 1
    assert sources[0].name == "engine"
    assert sources[0].url == "http://prometheus:9090"
    assert len(sources[0].metrics) == 2
    assert sources[0].metrics[0].name == "cluster_qps"
    assert sources[0].resolution == 15
    assert sources[0].max_data_points == 500


def test_plan_with_custom_resolution():
    plan = TestPlan.model_validate(_make_plan(
        observability={
            "prometheus_sources": [
                {
                    "name": "engine",
                    "url": "http://prometheus:9090",
                    "metrics": _SAMPLE_METRICS,
                    "resolution": 30,
                    "max_data_points": 1000,
                },
            ],
        },
    ))
    src = plan.test_environment.observability.prometheus_sources[0]
    assert src.resolution == 30
    assert src.max_data_points == 1000


def test_plan_with_empty_observability():
    """observability: {} is valid — no monitoring tools configured."""
    plan = TestPlan.model_validate(_make_plan(observability={}))
    assert plan.test_environment.observability is not None
    assert plan.test_environment.observability.prometheus_sources == []


def test_plan_with_prometheus_source_missing_name_rejects():
    with pytest.raises(ValidationError):
        TestPlan.model_validate(_make_plan(
            observability={"prometheus_sources": [
                {"url": "http://prometheus:9090", "metrics": _SAMPLE_METRICS},
            ]},
        ))


def test_plan_with_prometheus_source_missing_url_rejects():
    with pytest.raises(ValidationError):
        TestPlan.model_validate(_make_plan(
            observability={"prometheus_sources": [
                {"name": "engine", "metrics": _SAMPLE_METRICS},
            ]},
        ))


def test_plan_with_prometheus_source_missing_metrics_rejects():
    with pytest.raises(ValidationError):
        TestPlan.model_validate(_make_plan(
            observability={"prometheus_sources": [
                {"name": "engine", "url": "http://prometheus:9090"},
            ]},
        ))


def test_plan_with_prometheus_source_empty_metrics_rejects():
    with pytest.raises(ValidationError):
        TestPlan.model_validate(_make_plan(
            observability={"prometheus_sources": [
                {"name": "engine", "url": "http://prometheus:9090", "metrics": []},
            ]},
        ))
