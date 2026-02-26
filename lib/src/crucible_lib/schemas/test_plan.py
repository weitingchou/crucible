from typing import Literal

from pydantic import BaseModel, Field


class ExecutionConfig(BaseModel):
    concurrency: int = Field(..., gt=0)
    hold_for: str  # e.g. "5m"
    ramp_up: str = "30s"


class FixtureConfig(BaseModel):
    fixture_id: str
    component: str
    table: str
    # Columns required for Cassandra streaming; omitted for MPP zero-download.
    columns: list[str] = Field(default_factory=list)


class TestEnvironment(BaseModel):
    component: Literal["doris", "trino", "cassandra", "generic"]
    scaling_mode: Literal["intra_node", "inter_node"] = "intra_node"
    # intra_node: number of local k6 processes to spawn
    worker_count: int = 1
    # inter_node: total nodes in the fleet (master + N-1 workers)
    cluster_size: int = 2


class TestPlan(BaseModel):
    name: str
    test_environment: TestEnvironment
    execution: list[ExecutionConfig]
    fixtures: list[FixtureConfig] = Field(default_factory=list)
    # S3 key of the annotated SQL file consumed by the k6 driver.
    workload: str | None = None
