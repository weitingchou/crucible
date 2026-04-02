"""Tests for chaos engines — K8s and chaosd (all external calls mocked)."""

import pytest
from unittest.mock import MagicMock, patch, call


# ---------------------------------------------------------------------------
# K8sChaosEngine
# ---------------------------------------------------------------------------

@patch("worker.chaos_injector.k8s_engine.config")
@patch("worker.chaos_injector.k8s_engine.client")
def test_k8s_inject_creates_crd(mock_client, mock_config):
    from worker.chaos_injector.k8s_engine import K8sChaosEngine

    mock_api = MagicMock()
    mock_client.CustomObjectsApi.return_value = mock_api

    engine = K8sChaosEngine()
    experiment = {
        "name": "network-delay",
        "fault_type": "networkchaos",
        "target": {
            "env_type": "k8s",
            "namespace": "doris-cluster",
            "selector": {"app.kubernetes.io/component": "be"},
        },
        "parameters": {"action": "delay", "delay": {"latency": "200ms"}},
        "schedule": {"start_after": "1m", "duration": "3m"},
    }

    handle = engine.inject(experiment, "run-1")
    mock_api.create_namespaced_custom_object.assert_called_once()
    kwargs = mock_api.create_namespaced_custom_object.call_args
    assert kwargs[1]["group"] == "chaos-mesh.org"
    assert kwargs[1]["namespace"] == "doris-cluster"
    assert "networkchaos" in handle


@patch("worker.chaos_injector.k8s_engine.config")
@patch("worker.chaos_injector.k8s_engine.client")
def test_k8s_recover_deletes_crd(mock_client, mock_config):
    from worker.chaos_injector.k8s_engine import K8sChaosEngine

    mock_api = MagicMock()
    mock_client.CustomObjectsApi.return_value = mock_api

    engine = K8sChaosEngine()
    engine.recover({}, "run-1", "networkchaos/doris-cluster/crucible-run-1-test")
    mock_api.delete_namespaced_custom_object.assert_called_once_with(
        group="chaos-mesh.org",
        version="v1alpha1",
        namespace="doris-cluster",
        plural="networkchaos",
        name="crucible-run-1-test",
    )


@patch("worker.chaos_injector.k8s_engine.config")
@patch("worker.chaos_injector.k8s_engine.client")
def test_k8s_recover_handles_error_gracefully(mock_client, mock_config):
    from worker.chaos_injector.k8s_engine import K8sChaosEngine

    mock_api = MagicMock()
    mock_api.delete_namespaced_custom_object.side_effect = RuntimeError("not found")
    mock_client.CustomObjectsApi.return_value = mock_api

    engine = K8sChaosEngine()
    # Should not raise
    engine.recover({}, "run-1", "networkchaos/ns/name")


# ---------------------------------------------------------------------------
# _build_crd_manifest
# ---------------------------------------------------------------------------

@patch("worker.chaos_injector.k8s_engine.config")
@patch("worker.chaos_injector.k8s_engine.client")
def test_build_crd_manifest_name_format(mock_client, mock_config):
    from worker.chaos_injector.k8s_engine import _build_crd_manifest

    experiment = {
        "name": "network_delay",
        "fault_type": "networkchaos",
        "target": {"namespace": "ns", "selector": {"app": "db"}},
        "parameters": {"action": "delay"},
        "schedule": {"duration": "2m"},
    }
    manifest = _build_crd_manifest(experiment, "run-42")
    name = manifest["metadata"]["name"]
    # underscores replaced with dashes, lowercased
    assert "_" not in name
    assert name.startswith("crucible-run-42-")
    assert len(name) <= 63


@patch("worker.chaos_injector.k8s_engine.config")
@patch("worker.chaos_injector.k8s_engine.client")
def test_build_crd_manifest_truncates_long_names(mock_client, mock_config):
    from worker.chaos_injector.k8s_engine import _build_crd_manifest

    experiment = {
        "name": "a" * 100,
        "fault_type": "networkchaos",
        "target": {"namespace": "ns", "selector": {"app": "db"}},
        "parameters": {},
        "schedule": {"duration": "1m"},
    }
    manifest = _build_crd_manifest(experiment, "run-1")
    assert len(manifest["metadata"]["name"]) <= 63


@patch("worker.chaos_injector.k8s_engine.config")
@patch("worker.chaos_injector.k8s_engine.client")
def test_build_crd_manifest_labels_include_run_id(mock_client, mock_config):
    from worker.chaos_injector.k8s_engine import _build_crd_manifest

    experiment = {
        "name": "test",
        "fault_type": "podchaos",
        "target": {"namespace": "ns", "selector": {"app": "db"}},
        "parameters": {},
        "schedule": {"duration": "1m"},
    }
    manifest = _build_crd_manifest(experiment, "run-xyz")
    assert manifest["metadata"]["labels"]["crucible.io/run-id"] == "run-xyz"


@patch("worker.chaos_injector.k8s_engine.config")
@patch("worker.chaos_injector.k8s_engine.client")
def test_build_crd_manifest_merges_parameters_into_spec(mock_client, mock_config):
    from worker.chaos_injector.k8s_engine import _build_crd_manifest

    experiment = {
        "name": "test",
        "fault_type": "networkchaos",
        "target": {"namespace": "ns", "selector": {"app": "db"}},
        "parameters": {"action": "delay", "delay": {"latency": "100ms"}},
        "schedule": {"duration": "2m"},
    }
    manifest = _build_crd_manifest(experiment, "run-1")
    spec = manifest["spec"]
    assert spec["action"] == "delay"
    assert spec["delay"] == {"latency": "100ms"}
    assert spec["duration"] == "2m"
    assert spec["mode"] == "all"


# ---------------------------------------------------------------------------
# ChaosdEngine
# ---------------------------------------------------------------------------

@patch("worker.chaos_injector.chaosd_engine.requests")
def test_chaosd_inject_posts_to_api(mock_requests):
    from worker.chaos_injector.chaosd_engine import ChaosdEngine

    mock_resp = MagicMock()
    mock_resp.json.return_value = {"uid": "abc-123"}
    mock_requests.post.return_value = mock_resp

    engine = ChaosdEngine()
    experiment = {
        "name": "cpu-burn",
        "fault_type": "stress-cpu",
        "target": {"env_type": "ec2", "address": "10.0.1.5"},
        "parameters": {"load": 80},
        "schedule": {"start_after": "0s", "duration": "1m"},
    }

    handle = engine.inject(experiment, "run-2")
    assert handle == "abc-123"
    mock_requests.post.assert_called_once_with(
        "http://10.0.1.5:31767/api/attack/stress-cpu",
        json={"load": 80},
        timeout=30,
    )


@patch("worker.chaos_injector.chaosd_engine.requests")
def test_chaosd_inject_falls_back_to_id_key(mock_requests):
    """When the response uses 'id' instead of 'uid', the handle is still extracted."""
    from worker.chaos_injector.chaosd_engine import ChaosdEngine

    mock_resp = MagicMock()
    mock_resp.json.return_value = {"id": "fallback-id"}
    mock_requests.post.return_value = mock_resp

    engine = ChaosdEngine()
    experiment = {
        "name": "net-loss",
        "fault_type": "network-loss",
        "target": {"env_type": "ec2", "address": "10.0.2.1"},
        "parameters": {},
        "schedule": {"start_after": "0s", "duration": "1m"},
    }

    handle = engine.inject(experiment, "run-3")
    assert handle == "fallback-id"


@patch("worker.chaos_injector.chaosd_engine.requests")
def test_chaosd_recover_deletes_from_api(mock_requests):
    from worker.chaos_injector.chaosd_engine import ChaosdEngine

    engine = ChaosdEngine()
    experiment = {
        "name": "cpu-burn",
        "fault_type": "stress-cpu",
        "target": {"env_type": "ec2", "address": "10.0.1.5"},
    }

    engine.recover(experiment, "run-2", "abc-123")
    mock_requests.delete.assert_called_once_with(
        "http://10.0.1.5:31767/api/attack/abc-123",
        timeout=30,
    )


@patch("worker.chaos_injector.chaosd_engine.requests")
def test_chaosd_recover_handles_error_gracefully(mock_requests):
    from worker.chaos_injector.chaosd_engine import ChaosdEngine

    mock_requests.delete.side_effect = RuntimeError("connection refused")

    engine = ChaosdEngine()
    experiment = {"target": {"address": "10.0.1.5"}}
    # Should not raise
    engine.recover(experiment, "run-2", "abc-123")


# ---------------------------------------------------------------------------
# _get_engine
# ---------------------------------------------------------------------------

def test_get_engine_k8s():
    with patch("worker.chaos_injector.scheduler.K8sChaosEngine") as mock_cls:
        from worker.chaos_injector.scheduler import _get_engine
        engine = _get_engine("k8s")
        mock_cls.assert_called_once()


def test_get_engine_ec2():
    from worker.chaos_injector.scheduler import _get_engine
    engine = _get_engine("ec2")
    from worker.chaos_injector.chaosd_engine import ChaosdEngine
    assert isinstance(engine, ChaosdEngine)


def test_get_engine_unknown_raises():
    from worker.chaos_injector.scheduler import _get_engine
    with pytest.raises(ValueError, match="Unsupported chaos target env_type"):
        _get_engine("gcp")
