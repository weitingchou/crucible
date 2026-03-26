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

def test_node_spec_default_count():
    n = NodeSpec()
    assert n.count == 1


def test_node_spec_explicit_count():
    n = NodeSpec(count=5)
    assert n.count == 5


def test_node_spec_rejects_zero_count():
    with pytest.raises(ValidationError):
        NodeSpec(count=0)


def test_node_spec_rejects_negative_count():
    with pytest.raises(ValidationError):
        NodeSpec(count=-3)


def test_node_spec_allows_extra_fields():
    n = NodeSpec(count=2, instance_type="c5.4xlarge")
    assert n.instance_type == "c5.4xlarge"


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
        frontend_node=NodeSpec(count=1),
        backend_node=NodeSpec(count=5),
    )
    assert spec.version == "3.0.3"
    assert spec.frontend_node.count == 1
    assert spec.backend_node.count == 5


def test_doris_cluster_spec_only_backend_node():
    spec = DorisClusterSpec(type="doris", backend_node=NodeSpec(count=3))
    assert spec.backend_node.count == 3
    assert spec.frontend_node is None


def test_doris_cluster_spec_only_frontend_node():
    spec = DorisClusterSpec(type="doris", frontend_node=NodeSpec(count=2))
    assert spec.frontend_node.count == 2
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
        "backend_node": {"count": 4},
    })
    assert isinstance(spec, DorisClusterSpec)
    assert spec.backend_node.count == 4


def test_discriminated_union_rejects_unknown_type():
    with pytest.raises(ValidationError):
        _cluster_spec_adapter.validate_python({"type": "unknown_db"})


def test_discriminated_union_rejects_missing_type():
    with pytest.raises(ValidationError):
        _cluster_spec_adapter.validate_python({"backend_node": {"count": 3}})


def test_discriminated_union_rejects_invalid_backend_node():
    with pytest.raises(ValidationError):
        _cluster_spec_adapter.validate_python({
            "type": "doris",
            "backend_node": {"count": 0},
        })


# ---------------------------------------------------------------------------
# get_cluster_size
# ---------------------------------------------------------------------------

def test_get_cluster_size_with_backend_node():
    spec = DorisClusterSpec(type="doris", backend_node=NodeSpec(count=5))
    assert get_cluster_size(spec) == 5


def test_get_cluster_size_without_backend_node():
    spec = DorisClusterSpec(type="doris")
    assert get_cluster_size(spec) == 1


def test_get_cluster_size_with_only_frontend_node():
    spec = DorisClusterSpec(type="doris", frontend_node=NodeSpec(count=3))
    assert get_cluster_size(spec) == 1


def test_get_cluster_size_with_both_nodes():
    spec = DorisClusterSpec(
        type="doris",
        frontend_node=NodeSpec(count=2),
        backend_node=NodeSpec(count=6),
    )
    assert get_cluster_size(spec) == 6
