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
# K8sChaosEngine.collect_status
# ---------------------------------------------------------------------------

@patch("worker.chaos_injector.k8s_engine.config")
@patch("worker.chaos_injector.k8s_engine.client")
def test_k8s_collect_status_returns_crd_status(mock_client, mock_config):
    from worker.chaos_injector.k8s_engine import K8sChaosEngine

    mock_api = MagicMock()
    mock_api.get_namespaced_custom_object.return_value = {
        "status": {
            "conditions": [{"type": "AllInjected", "status": "True"}],
            "records": [{"id": "pod-1", "phase": "Injected"}],
        }
    }
    mock_client.CustomObjectsApi.return_value = mock_api

    engine = K8sChaosEngine()
    status = engine.collect_status({}, "run-1", "networkchaos/doris-cluster/crucible-run-1-test")
    assert status["conditions"][0]["type"] == "AllInjected"
    assert status["records"][0]["id"] == "pod-1"
    mock_api.get_namespaced_custom_object.assert_called_once_with(
        group="chaos-mesh.org",
        version="v1alpha1",
        namespace="doris-cluster",
        plural="networkchaos",
        name="crucible-run-1-test",
    )


@patch("worker.chaos_injector.k8s_engine.config")
@patch("worker.chaos_injector.k8s_engine.client")
def test_k8s_collect_status_returns_none_on_error(mock_client, mock_config):
    from worker.chaos_injector.k8s_engine import K8sChaosEngine

    mock_api = MagicMock()
    mock_api.get_namespaced_custom_object.side_effect = RuntimeError("not found")
    mock_client.CustomObjectsApi.return_value = mock_api

    engine = K8sChaosEngine()
    status = engine.collect_status({}, "run-1", "networkchaos/ns/name")
    assert status is None


@patch("worker.chaos_injector.chaosd_engine.requests")
def test_chaosd_collect_status_returns_matching_record(mock_requests):
    """collect_status queries /api/experiments/ and returns the matching UID record."""
    from worker.chaos_injector.chaosd_engine import ChaosdEngine

    mock_resp = MagicMock()
    mock_resp.json.return_value = [
        {"uid": "other-uid", "status": "success", "kind": "stress"},
        {
            "uid": "abc-123",
            "status": "success",
            "kind": "network",
            "action": "delay",
            "created_at": "2025-01-01T00:00:00Z",
            "updated_at": "2025-01-01T00:02:00Z",
            "launch_mode": "svr",
        },
    ]
    mock_requests.get.return_value = mock_resp

    engine = ChaosdEngine()
    experiment = {"target": {"env_type": "ec2", "address": "10.0.1.5"}}
    status = engine.collect_status(experiment, "run-1", "abc-123")

    assert status is not None
    assert status["uid"] == "abc-123"
    assert status["status"] == "success"
    assert status["kind"] == "network"
    assert status["action"] == "delay"
    assert status["created_at"] == "2025-01-01T00:00:00Z"
    assert status["updated_at"] == "2025-01-01T00:02:00Z"
    mock_requests.get.assert_called_once_with(
        "http://10.0.1.5:31767/api/experiments/", timeout=10,
    )


@patch("worker.chaos_injector.chaosd_engine.requests")
def test_chaosd_collect_status_returns_none_when_uid_not_found(mock_requests):
    from worker.chaos_injector.chaosd_engine import ChaosdEngine

    mock_resp = MagicMock()
    mock_resp.json.return_value = [{"uid": "other-uid", "status": "success"}]
    mock_requests.get.return_value = mock_resp

    engine = ChaosdEngine()
    experiment = {"target": {"env_type": "ec2", "address": "10.0.1.5"}}
    assert engine.collect_status(experiment, "run-1", "missing-uid") is None


@patch("worker.chaos_injector.chaosd_engine.requests")
def test_chaosd_collect_status_returns_none_on_error(mock_requests):
    from worker.chaos_injector.chaosd_engine import ChaosdEngine

    mock_requests.get.side_effect = RuntimeError("connection refused")

    engine = ChaosdEngine()
    experiment = {"target": {"env_type": "ec2", "address": "10.0.1.5"}}
    assert engine.collect_status(experiment, "run-1", "abc-123") is None


# ---------------------------------------------------------------------------
# K8sChaosEngine.normalize_status
# ---------------------------------------------------------------------------

def test_k8s_normalize_status_recovered():
    from worker.chaos_injector.k8s_engine import K8sChaosEngine

    raw = {
        "conditions": [
            {"type": "AllInjected", "status": "True", "lastTransitionTime": "2025-01-01T00:01:00Z"},
            {"type": "AllRecovered", "status": "True", "lastTransitionTime": "2025-01-01T00:03:00Z"},
        ],
        "records": [
            {"id": "doris-be-0", "phase": "Injected", "injectTime": "2025-01-01T00:01:00Z", "recoverTime": "2025-01-01T00:03:00Z"},
            {"id": "doris-be-1", "phase": "Injected", "injectTime": "2025-01-01T00:01:01Z", "recoverTime": "2025-01-01T00:03:01Z"},
        ],
    }
    result = K8sChaosEngine.normalize_status(raw)
    assert result["status"] == "recovered"
    assert result["created_at"] == "2025-01-01T00:01:00Z"
    assert result["updated_at"] == "2025-01-01T00:03:00Z"
    assert len(result["targets"]) == 2
    assert result["targets"][0]["id"] == "doris-be-0"
    assert result["targets"][0]["inject_time"] == "2025-01-01T00:01:00Z"
    assert result["targets"][1]["id"] == "doris-be-1"


def test_k8s_normalize_status_injected():
    from worker.chaos_injector.k8s_engine import K8sChaosEngine

    raw = {
        "conditions": [
            {"type": "AllInjected", "status": "True", "lastTransitionTime": "2025-01-01T00:01:00Z"},
        ],
        "records": [{"id": "pod-0", "injectTime": "2025-01-01T00:01:00Z"}],
    }
    result = K8sChaosEngine.normalize_status(raw)
    assert result["status"] == "injected"
    assert result["updated_at"] == "2025-01-01T00:01:00Z"
    assert result["targets"][0]["recover_time"] is None


def test_k8s_normalize_status_error():
    from worker.chaos_injector.k8s_engine import K8sChaosEngine

    raw = {"conditions": [], "records": []}
    result = K8sChaosEngine.normalize_status(raw)
    assert result["status"] == "error"
    assert result["targets"] == []


def test_k8s_normalize_status_none_on_empty():
    from worker.chaos_injector.k8s_engine import K8sChaosEngine

    assert K8sChaosEngine.normalize_status({}) is None
    assert K8sChaosEngine.normalize_status(None) is None


# ---------------------------------------------------------------------------
# ChaosdEngine.normalize_status
# ---------------------------------------------------------------------------

def test_chaosd_normalize_status_success():
    from worker.chaos_injector.chaosd_engine import ChaosdEngine

    raw = {
        "uid": "abc-123",
        "status": "success",
        "kind": "network",
        "action": "delay",
        "created_at": "2025-01-01T00:01:00Z",
        "updated_at": "2025-01-01T00:03:00Z",
    }
    result = ChaosdEngine.normalize_status(raw)
    assert result["status"] == "recovered"
    assert result["created_at"] == "2025-01-01T00:01:00Z"
    assert result["updated_at"] == "2025-01-01T00:03:00Z"
    assert len(result["targets"]) == 1
    assert result["targets"][0]["id"] == "abc-123"
    assert result["targets"][0]["inject_time"] == "2025-01-01T00:01:00Z"
    assert result["targets"][0]["recover_time"] == "2025-01-01T00:03:00Z"


def test_chaosd_normalize_status_destroyed():
    from worker.chaos_injector.chaosd_engine import ChaosdEngine

    raw = {"uid": "x", "status": "destroyed", "created_at": "t1", "updated_at": "t2"}
    assert ChaosdEngine.normalize_status(raw)["status"] == "recovered"


def test_chaosd_normalize_status_error():
    from worker.chaos_injector.chaosd_engine import ChaosdEngine

    raw = {"uid": "x", "status": "error", "created_at": "t1", "updated_at": "t2"}
    assert ChaosdEngine.normalize_status(raw)["status"] == "error"


def test_chaosd_normalize_status_created_means_injected():
    from worker.chaos_injector.chaosd_engine import ChaosdEngine

    raw = {"uid": "x", "status": "created", "created_at": "t1", "updated_at": "t1"}
    result = ChaosdEngine.normalize_status(raw)
    assert result["status"] == "injected"
    assert result["targets"][0]["recover_time"] is None


def test_chaosd_normalize_status_error_has_no_recover_time():
    from worker.chaos_injector.chaosd_engine import ChaosdEngine

    raw = {"uid": "x", "status": "error", "created_at": "t1", "updated_at": "t2"}
    result = ChaosdEngine.normalize_status(raw)
    assert result["status"] == "error"
    assert result["targets"][0]["recover_time"] is None


def test_chaosd_normalize_status_none_on_empty():
    from worker.chaos_injector.chaosd_engine import ChaosdEngine

    assert ChaosdEngine.normalize_status({}) is None
    assert ChaosdEngine.normalize_status(None) is None


def test_chaosd_normalize_status_missing_fields():
    """normalize_status handles records with missing optional fields gracefully."""
    from worker.chaos_injector.chaosd_engine import ChaosdEngine

    raw = {"status": "success"}
    result = ChaosdEngine.normalize_status(raw)
    assert result["status"] == "recovered"
    assert result["created_at"] is None
    assert result["updated_at"] is None
    assert result["targets"][0]["id"] == ""
    assert result["targets"][0]["inject_time"] is None


# ---------------------------------------------------------------------------
# K8sChaosEngine.normalize_status — edge cases
# ---------------------------------------------------------------------------

def test_k8s_normalize_status_partial_recovery():
    """AllInjected=True but AllRecovered present with status=False → still injected."""
    from worker.chaos_injector.k8s_engine import K8sChaosEngine

    raw = {
        "conditions": [
            {"type": "AllInjected", "status": "True", "lastTransitionTime": "2025-01-01T00:01:00Z"},
            {"type": "AllRecovered", "status": "False", "lastTransitionTime": "2025-01-01T00:02:00Z"},
        ],
        "records": [
            {"id": "pod-0", "injectTime": "2025-01-01T00:01:00Z", "recoverTime": None},
        ],
    }
    result = K8sChaosEngine.normalize_status(raw)
    assert result["status"] == "injected"
    assert result["targets"][0]["recover_time"] is None


def test_k8s_normalize_status_missing_record_fields():
    """Records with missing id/times still produce valid targets."""
    from worker.chaos_injector.k8s_engine import K8sChaosEngine

    raw = {
        "conditions": [
            {"type": "AllInjected", "status": "True", "lastTransitionTime": "2025-01-01T00:01:00Z"},
        ],
        "records": [{}],
    }
    result = K8sChaosEngine.normalize_status(raw)
    assert len(result["targets"]) == 1
    assert result["targets"][0]["id"] == ""
    assert result["targets"][0]["inject_time"] is None
    assert result["targets"][0]["recover_time"] is None


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
