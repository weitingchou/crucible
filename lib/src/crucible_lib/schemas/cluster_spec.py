"""Per-SUT cluster specification models.

Cluster specs are provided at run-submission time (not in the test plan)
so the same plan can be re-run against different cluster topologies.
"""

from __future__ import annotations

from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field


class NodeSpec(BaseModel):
    """Specification for a single node role (e.g. FE, BE)."""

    model_config = ConfigDict(extra="allow")

    replica: int = Field(default=1, gt=0)
    cpu_per_node: int | None = Field(default=None, gt=0, description="vCores per node")
    memory_per_node: int | None = Field(default=None, gt=0, description="GiB per node")


class DorisClusterSpec(BaseModel):
    """Cluster spec for Apache Doris — has FE (Frontend) and BE (Backend) nodes."""

    model_config = ConfigDict(extra="allow")

    type: Literal["doris"]
    version: str | None = None
    frontend_node: NodeSpec | None = None
    backend_node: NodeSpec | None = None


# Extend this union as new SUT types are added (e.g. CassandraClusterSpec).
ClusterSpec = Annotated[
    Union[DorisClusterSpec],
    Field(discriminator="type"),
]


def get_cluster_size(spec: DorisClusterSpec) -> int:
    """Return the effective cluster size for fan-out decisions."""
    if spec.backend_node:
        return spec.backend_node.replica
    return 1
