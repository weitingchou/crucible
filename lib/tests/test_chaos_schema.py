"""Tests for ChaosSpec schema validation."""

import pytest
from pydantic import ValidationError

from crucible_lib.schemas.test_plan import (
    ChaosExperiment,
    ChaosSchedule,
    ChaosSpec,
    ChaosTarget,
    TestPlan,
)


# ---------------------------------------------------------------------------
# ChaosTarget validation
# ---------------------------------------------------------------------------

def test_k8s_target_requires_namespace_and_selector():
    with pytest.raises(ValidationError, match="namespace.*selector"):
        ChaosTarget(env_type="k8s")


def test_k8s_target_valid():
    t = ChaosTarget(
        env_type="k8s",
        namespace="doris-cluster",
        selector={"app.kubernetes.io/component": "be"},
    )
    assert t.env_type == "k8s"
    assert t.namespace == "doris-cluster"


def test_k8s_target_missing_selector_only():
    with pytest.raises(ValidationError, match="namespace.*selector"):
        ChaosTarget(env_type="k8s", namespace="ns")


def test_k8s_target_missing_namespace_only():
    with pytest.raises(ValidationError, match="namespace.*selector"):
        ChaosTarget(env_type="k8s", selector={"app": "test"})


def test_ec2_target_requires_address():
    with pytest.raises(ValidationError, match="address"):
        ChaosTarget(env_type="ec2")


def test_ec2_target_valid():
    t = ChaosTarget(env_type="ec2", address="10.0.1.5")
    assert t.address == "10.0.1.5"


def test_invalid_env_type_rejected():
    with pytest.raises(ValidationError):
        ChaosTarget(env_type="gcp", address="10.0.1.5")


# ---------------------------------------------------------------------------
# ChaosSchedule validation
# ---------------------------------------------------------------------------

def test_chaos_schedule_requires_both_fields():
    with pytest.raises(ValidationError):
        ChaosSchedule()


def test_chaos_schedule_valid():
    s = ChaosSchedule(start_after="30s", duration="2m")
    assert s.start_after == "30s"
    assert s.duration == "2m"


# ---------------------------------------------------------------------------
# ChaosExperiment validation
# ---------------------------------------------------------------------------

def test_experiment_requires_name_and_schedule():
    with pytest.raises(ValidationError):
        ChaosExperiment(
            fault_type="networkchaos",
            target=ChaosTarget(env_type="k8s", namespace="ns", selector={"a": "b"}),
        )


def test_experiment_defaults_parameters_to_empty():
    exp = ChaosExperiment(
        name="test",
        fault_type="podchaos",
        target=ChaosTarget(env_type="k8s", namespace="ns", selector={"a": "b"}),
        schedule=ChaosSchedule(start_after="0s", duration="1m"),
    )
    assert exp.parameters == {}


# ---------------------------------------------------------------------------
# ChaosSpec validation
# ---------------------------------------------------------------------------

def test_chaos_spec_requires_at_least_one_experiment():
    with pytest.raises(ValidationError, match="too_short"):
        ChaosSpec(engine="chaos-mesh", experiments=[])


def test_chaos_spec_rejects_unknown_engine():
    with pytest.raises(ValidationError):
        ChaosSpec(
            engine="unknown-engine",
            experiments=[_make_experiment()],
        )


def test_chaos_spec_valid():
    spec = ChaosSpec(
        engine="chaos-mesh",
        experiments=[_make_experiment()],
    )
    assert len(spec.experiments) == 1
    assert spec.engine == "chaos-mesh"


def test_chaos_spec_multiple_experiments():
    exp1 = _make_experiment()
    exp2 = ChaosExperiment(
        name="cpu-stress",
        fault_type="stresschaos",
        target=ChaosTarget(env_type="k8s", namespace="ns", selector={"app": "db"}),
        parameters={"stressors": {"cpu": {"workers": 2}}},
        schedule=ChaosSchedule(start_after="2m", duration="5m"),
    )
    spec = ChaosSpec(engine="chaos-mesh", experiments=[exp1, exp2])
    assert len(spec.experiments) == 2


# ---------------------------------------------------------------------------
# TestPlan with chaos_spec
# ---------------------------------------------------------------------------

def test_test_plan_accepts_chaos_spec():
    plan = TestPlan.model_validate(_make_plan_with_chaos())
    assert plan.chaos_spec is not None
    assert plan.chaos_spec.experiments[0].name == "degrade-backend"


def test_test_plan_without_chaos_spec():
    raw = _make_plan_with_chaos()
    del raw["chaos_spec"]
    plan = TestPlan.model_validate(raw)
    assert plan.chaos_spec is None


def test_test_plan_rejects_invalid_chaos_spec():
    raw = _make_plan_with_chaos()
    raw["chaos_spec"]["experiments"] = []
    with pytest.raises(ValidationError):
        TestPlan.model_validate(raw)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_experiment() -> ChaosExperiment:
    return ChaosExperiment(
        name="test-exp",
        fault_type="networkchaos",
        target=ChaosTarget(
            env_type="k8s",
            namespace="default",
            selector={"app": "test"},
        ),
        parameters={"action": "delay", "delay": {"latency": "100ms"}},
        schedule=ChaosSchedule(start_after="30s", duration="2m"),
    )


def _make_plan_with_chaos() -> dict:
    return {
        "test_metadata": {"run_label": "chaos-test"},
        "test_environment": {
            "env_type": "long-lived",
            "component_spec": {
                "type": "doris",
                "cluster_info": {"host": "h:9030", "username": "root", "password": ""},
            },
            "target_db": "test",
            "fixtures": [],
        },
        "execution": {
            "executor": "k6",
            "scaling_mode": "intra_node",
            "concurrency": 10,
            "ramp_up": "30s",
            "hold_for": "5m",
            "workload": [{"workload_id": "w1"}],
        },
        "chaos_spec": {
            "engine": "chaos-mesh",
            "experiments": [
                {
                    "name": "degrade-backend",
                    "fault_type": "networkchaos",
                    "target": {
                        "env_type": "k8s",
                        "namespace": "doris-cluster",
                        "selector": {"app.kubernetes.io/component": "be"},
                    },
                    "parameters": {
                        "action": "delay",
                        "delay": {"latency": "200ms", "jitter": "50ms"},
                    },
                    "schedule": {"start_after": "1m", "duration": "3m"},
                },
            ],
        },
    }
