"""Tests for crucible_lib.schemas.test_plan — observability section."""

import pytest
from pydantic import ValidationError

from crucible_lib.schemas.test_plan import (
    Observability,
    PrometheusConfig,
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


# ---------------------------------------------------------------------------
# PrometheusConfig
# ---------------------------------------------------------------------------

def test_prometheus_config_minimal():
    cfg = PrometheusConfig(url="http://prometheus:9090", job="doris-be")
    assert cfg.url == "http://prometheus:9090"
    assert cfg.job == "doris-be"
    assert cfg.labels == {}


def test_prometheus_config_with_labels():
    cfg = PrometheusConfig(
        url="http://prometheus:9090",
        job="doris-be",
        labels={"cluster": "prod-01", "region": "ap-southeast-1"},
    )
    assert cfg.labels == {"cluster": "prod-01", "region": "ap-southeast-1"}


def test_prometheus_config_requires_url():
    with pytest.raises(ValidationError):
        PrometheusConfig(job="doris-be")


def test_prometheus_config_requires_job():
    with pytest.raises(ValidationError):
        PrometheusConfig(url="http://prometheus:9090")


# ---------------------------------------------------------------------------
# Observability
# ---------------------------------------------------------------------------

def test_observability_defaults_to_none():
    obs = Observability()
    assert obs.prometheus is None


def test_observability_with_prometheus():
    obs = Observability(prometheus={
        "url": "http://prom:9090",
        "job": "doris-be",
    })
    assert obs.prometheus.url == "http://prom:9090"
    assert obs.prometheus.job == "doris-be"


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
                "job": "doris-be",
            },
        },
    ))
    prom = plan.test_environment.observability.prometheus
    assert prom.url == "http://prometheus:9090"
    assert prom.job == "doris-be"
    assert prom.labels == {}


def test_plan_with_prometheus_and_labels():
    plan = TestPlan.model_validate(_make_plan(
        observability={
            "prometheus": {
                "url": "http://prometheus:9090",
                "job": "doris-be",
                "labels": {"cluster": "prod-01", "region": "ap-southeast-1"},
            },
        },
    ))
    prom = plan.test_environment.observability.prometheus
    assert prom.labels == {"cluster": "prod-01", "region": "ap-southeast-1"}


def test_plan_with_empty_observability():
    """observability: {} is valid — no monitoring tools configured."""
    plan = TestPlan.model_validate(_make_plan(observability={}))
    assert plan.test_environment.observability is not None
    assert plan.test_environment.observability.prometheus is None


def test_plan_with_prometheus_missing_url_rejects():
    with pytest.raises(ValidationError):
        TestPlan.model_validate(_make_plan(
            observability={"prometheus": {"job": "doris-be"}},
        ))


def test_plan_with_prometheus_missing_job_rejects():
    with pytest.raises(ValidationError):
        TestPlan.model_validate(_make_plan(
            observability={"prometheus": {"url": "http://prometheus:9090"}},
        ))
