"""Tests for crucible_lib.schemas.test_plan — observability section."""

import pytest
from pydantic import ValidationError

from crucible_lib.schemas.test_plan import (
    Observability,
    PrometheusConfig,
    PrometheusMetric,
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
# PrometheusConfig
# ---------------------------------------------------------------------------

def test_prometheus_config_minimal():
    cfg = PrometheusConfig(url="http://prometheus:9090", metrics=_SAMPLE_METRICS)
    assert cfg.url == "http://prometheus:9090"
    assert len(cfg.metrics) == 2
    assert cfg.resolution == 15
    assert cfg.max_data_points == 500


def test_prometheus_config_custom_resolution_and_max_data_points():
    cfg = PrometheusConfig(
        url="http://prometheus:9090",
        metrics=_SAMPLE_METRICS,
        resolution=30,
        max_data_points=200,
    )
    assert cfg.resolution == 30
    assert cfg.max_data_points == 200


def test_prometheus_config_requires_url():
    with pytest.raises(ValidationError):
        PrometheusConfig(metrics=_SAMPLE_METRICS)


def test_prometheus_config_requires_metrics():
    with pytest.raises(ValidationError):
        PrometheusConfig(url="http://prometheus:9090")


def test_prometheus_config_rejects_empty_metrics():
    with pytest.raises(ValidationError):
        PrometheusConfig(url="http://prometheus:9090", metrics=[])


def test_prometheus_config_rejects_zero_resolution():
    with pytest.raises(ValidationError):
        PrometheusConfig(url="http://prometheus:9090", metrics=_SAMPLE_METRICS, resolution=0)


def test_prometheus_config_rejects_zero_max_data_points():
    with pytest.raises(ValidationError):
        PrometheusConfig(url="http://prometheus:9090", metrics=_SAMPLE_METRICS, max_data_points=0)


# ---------------------------------------------------------------------------
# Observability
# ---------------------------------------------------------------------------

def test_observability_defaults_to_none():
    obs = Observability()
    assert obs.prometheus is None


def test_observability_with_prometheus():
    obs = Observability(prometheus={
        "url": "http://prom:9090",
        "metrics": [{"name": "qps", "query": "sum(rate(x[1m]))"}],
    })
    assert obs.prometheus.url == "http://prom:9090"
    assert len(obs.prometheus.metrics) == 1


# ---------------------------------------------------------------------------
# TestPlan — observability field
# ---------------------------------------------------------------------------

def test_plan_without_observability():
    """Existing plans without observability should still validate."""
    plan = TestPlan.model_validate(_make_plan())
    assert plan.test_environment.observability is None


def test_plan_with_observability_prometheus():
    plan = TestPlan.model_validate(_make_plan(
        observability={
            "prometheus": {
                "url": "http://prometheus:9090",
                "metrics": _SAMPLE_METRICS,
            },
        },
    ))
    prom = plan.test_environment.observability.prometheus
    assert prom.url == "http://prometheus:9090"
    assert len(prom.metrics) == 2
    assert prom.metrics[0].name == "cluster_qps"
    assert prom.resolution == 15
    assert prom.max_data_points == 500


def test_plan_with_custom_resolution():
    plan = TestPlan.model_validate(_make_plan(
        observability={
            "prometheus": {
                "url": "http://prometheus:9090",
                "metrics": _SAMPLE_METRICS,
                "resolution": 30,
                "max_data_points": 1000,
            },
        },
    ))
    prom = plan.test_environment.observability.prometheus
    assert prom.resolution == 30
    assert prom.max_data_points == 1000


def test_plan_with_empty_observability():
    """observability: {} is valid — no monitoring tools configured."""
    plan = TestPlan.model_validate(_make_plan(observability={}))
    assert plan.test_environment.observability is not None
    assert plan.test_environment.observability.prometheus is None


def test_plan_with_prometheus_missing_url_rejects():
    with pytest.raises(ValidationError):
        TestPlan.model_validate(_make_plan(
            observability={"prometheus": {"metrics": _SAMPLE_METRICS}},
        ))


def test_plan_with_prometheus_missing_metrics_rejects():
    with pytest.raises(ValidationError):
        TestPlan.model_validate(_make_plan(
            observability={"prometheus": {"url": "http://prometheus:9090"}},
        ))


def test_plan_with_prometheus_empty_metrics_rejects():
    with pytest.raises(ValidationError):
        TestPlan.model_validate(_make_plan(
            observability={"prometheus": {"url": "http://prometheus:9090", "metrics": []}},
        ))
