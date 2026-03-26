"""Tests for crucible_lib.schemas.cluster_spec."""

import pytest
from pydantic import TypeAdapter, ValidationError

from crucible_lib.schemas.cluster_spec import (
    ClusterSpec,
    DorisClusterSpec,
    NodeSpec,
    get_cluster_size,
)

_cluster_spec_adapter = TypeAdapter(ClusterSpec)


# ---------------------------------------------------------------------------
# NodeSpec
# ---------------------------------------------------------------------------

def test_node_spec_default_replica():
    n = NodeSpec()
    assert n.replica == 1


def test_node_spec_explicit_replica():
    n = NodeSpec(replica=5)
    assert n.replica == 5


def test_node_spec_rejects_zero_replica():
    with pytest.raises(ValidationError):
        NodeSpec(replica=0)


def test_node_spec_rejects_negative_replica():
    with pytest.raises(ValidationError):
        NodeSpec(replica=-3)


def test_node_spec_allows_extra_fields():
    n = NodeSpec(replica=2, instance_type="c5.4xlarge")
    assert n.instance_type == "c5.4xlarge"


# ---------------------------------------------------------------------------
# NodeSpec — cpu_per_node / memory_per_node
# ---------------------------------------------------------------------------

def test_node_spec_cpu_and_memory_default_to_none():
    n = NodeSpec()
    assert n.cpu_per_node is None
    assert n.memory_per_node is None


def test_node_spec_explicit_cpu_and_memory():
    n = NodeSpec(cpu_per_node=8, memory_per_node=32)
    assert n.cpu_per_node == 8
    assert n.memory_per_node == 32


def test_node_spec_cpu_only():
    n = NodeSpec(cpu_per_node=4)
    assert n.cpu_per_node == 4
    assert n.memory_per_node is None


def test_node_spec_memory_only():
    n = NodeSpec(memory_per_node=16)
    assert n.cpu_per_node is None
    assert n.memory_per_node == 16


def test_node_spec_rejects_zero_cpu():
    with pytest.raises(ValidationError):
        NodeSpec(cpu_per_node=0)


def test_node_spec_rejects_negative_cpu():
    with pytest.raises(ValidationError):
        NodeSpec(cpu_per_node=-2)


def test_node_spec_rejects_zero_memory():
    with pytest.raises(ValidationError):
        NodeSpec(memory_per_node=0)


def test_node_spec_rejects_negative_memory():
    with pytest.raises(ValidationError):
        NodeSpec(memory_per_node=-4)


def test_node_spec_full_spec():
    n = NodeSpec(replica=3, cpu_per_node=16, memory_per_node=64)
    assert n.replica == 3
    assert n.cpu_per_node == 16
    assert n.memory_per_node == 64


# ---------------------------------------------------------------------------
# DorisClusterSpec
# ---------------------------------------------------------------------------

def test_doris_cluster_spec_minimal():
    spec = DorisClusterSpec(type="doris")
    assert spec.type == "doris"
    assert spec.version is None
    assert spec.frontend_node is None
    assert spec.backend_node is None


def test_doris_cluster_spec_full():
    spec = DorisClusterSpec(
        type="doris",
        version="3.0.3",
        frontend_node=NodeSpec(replica=1),
        backend_node=NodeSpec(replica=5),
    )
    assert spec.version == "3.0.3"
    assert spec.frontend_node.replica == 1
    assert spec.backend_node.replica == 5


def test_doris_cluster_spec_only_backend_node():
    spec = DorisClusterSpec(type="doris", backend_node=NodeSpec(replica=3))
    assert spec.backend_node.replica == 3
    assert spec.frontend_node is None


def test_doris_cluster_spec_only_frontend_node():
    spec = DorisClusterSpec(type="doris", frontend_node=NodeSpec(replica=2))
    assert spec.frontend_node.replica == 2
    assert spec.backend_node is None


def test_doris_cluster_spec_allows_extra_fields():
    spec = DorisClusterSpec(type="doris", storage_gb=500)
    assert spec.storage_gb == 500


# ---------------------------------------------------------------------------
# ClusterSpec discriminated union
# ---------------------------------------------------------------------------

def test_discriminated_union_resolves_doris():
    spec = _cluster_spec_adapter.validate_python({"type": "doris"})
    assert isinstance(spec, DorisClusterSpec)


def test_discriminated_union_with_nodes():
    spec = _cluster_spec_adapter.validate_python({
        "type": "doris",
        "backend_node": {"replica": 4},
    })
    assert isinstance(spec, DorisClusterSpec)
    assert spec.backend_node.replica == 4


def test_discriminated_union_rejects_unknown_type():
    with pytest.raises(ValidationError):
        _cluster_spec_adapter.validate_python({"type": "unknown_db"})


def test_discriminated_union_rejects_missing_type():
    with pytest.raises(ValidationError):
        _cluster_spec_adapter.validate_python({"backend_node": {"replica": 3}})


def test_discriminated_union_rejects_invalid_backend_node():
    with pytest.raises(ValidationError):
        _cluster_spec_adapter.validate_python({
            "type": "doris",
            "backend_node": {"replica": 0},
        })


# ---------------------------------------------------------------------------
# get_cluster_size
# ---------------------------------------------------------------------------

def test_get_cluster_size_with_backend_node():
    spec = DorisClusterSpec(type="doris", backend_node=NodeSpec(replica=5))
    assert get_cluster_size(spec) == 5


def test_get_cluster_size_without_backend_node():
    spec = DorisClusterSpec(type="doris")
    assert get_cluster_size(spec) == 1


def test_get_cluster_size_with_only_frontend_node():
    spec = DorisClusterSpec(type="doris", frontend_node=NodeSpec(replica=3))
    assert get_cluster_size(spec) == 1


def test_get_cluster_size_with_both_nodes():
    spec = DorisClusterSpec(
        type="doris",
        frontend_node=NodeSpec(replica=2),
        backend_node=NodeSpec(replica=6),
    )
    assert get_cluster_size(spec) == 6
