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


class PrometheusSource(BaseModel):
    name: str
    url: str
    metrics: list[PrometheusMetric] = Field(..., min_length=1)
    resolution: int = Field(default=15, gt=0, description="Minimum step in seconds")
    max_data_points: int = Field(default=500, gt=0, description="Max data points per metric")


class Observability(BaseModel):
    prometheus_sources: list[PrometheusSource] = Field(default_factory=list)


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


class FailureDetection(BaseModel):
    """SUT failure detection — abort the test early when sustained query errors
    indicate the target database is unresponsive.

    When enabled, the k6 driver tracks a ``query_errors`` Rate metric.  If the
    error rate exceeds ``error_rate_threshold`` after the initial
    ``abort_delay`` window, k6 aborts gracefully and the run is marked
    COMPLETED with whatever partial results were collected.
    """

    enabled: bool = Field(
        default=True,
        description="Set false to disable SUT failure detection entirely.",
    )
    error_rate_threshold: float = Field(
        default=0.5,
        gt=0.0,
        le=1.0,
        description="Maximum allowed query error rate before aborting (0.0–1.0).",
    )
    abort_delay: str = Field(
        default="10s",
        description="Grace period before evaluating the threshold (k6 duration string, e.g. '10s', '30s').",
    )


class ExecutionConfig(BaseModel):
    executor: Literal["k6", "locust"]
    scaling_mode: Literal["intra_node", "inter_node"]
    concurrency: int = Field(..., gt=0)
    ramp_up: str
    hold_for: str
    workload: list[WorkloadItem]
    failure_detection: FailureDetection | None = None


class TestPlan(BaseModel):
    test_metadata: TestMetadata
    test_environment: TestEnvironment
    execution: ExecutionConfig
