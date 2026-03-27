from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class TestMetadata(BaseModel):
    run_label: str


class ClusterInfo(BaseModel):
    """Bring-Your-Own connection details for a pre-existing cluster."""

    model_config = ConfigDict(extra="allow")

    host: str
    username: str
    password: str


class ComponentSpec(BaseModel):
    type: str
    cluster_info: ClusterInfo | None = None


class FixtureItem(BaseModel):
    fixture_id: str
    table: str


class PrometheusMetric(BaseModel):
    name: str
    query: str


class PrometheusConfig(BaseModel):
    url: str
    metrics: list[PrometheusMetric] = Field(..., min_length=1)
    resolution: int = Field(default=15, gt=0, description="Minimum step in seconds")
    max_data_points: int = Field(default=500, gt=0, description="Max data points per metric")


class Observability(BaseModel):
    prometheus: PrometheusConfig | None = None


class TestEnvironment(BaseModel):
    env_type: Literal["long-lived", "disposable"]
    component_spec: ComponentSpec
    target_db: str
    fixtures: list[FixtureItem] = Field(default_factory=list)
    observability: Observability | None = None

    @model_validator(mode="after")
    def _cluster_info_required_for_long_lived(self) -> "TestEnvironment":
        if self.env_type == "long-lived" and self.component_spec.cluster_info is None:
            raise ValueError(
                "'cluster_info' is required when env_type is 'long-lived'."
            )
        return self


class WorkloadItem(BaseModel):
    workload_id: str


class ExecutionConfig(BaseModel):
    executor: Literal["k6", "locust"]
    scaling_mode: Literal["intra_node", "inter_node"]
    concurrency: int = Field(..., gt=0)
    ramp_up: str
    hold_for: str
    workload: list[WorkloadItem]


class TestPlan(BaseModel):
    test_metadata: TestMetadata
    test_environment: TestEnvironment
    execution: ExecutionConfig
