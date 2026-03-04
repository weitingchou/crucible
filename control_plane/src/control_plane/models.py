from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class TestMetadata(BaseModel):
    run_label: str


class ClusterInfo(BaseModel):
    """Bring-Your-Own connection details for a pre-existing cluster."""

    model_config = ConfigDict(extra="allow")

    host: str


class NodeSpec(BaseModel):
    model_config = ConfigDict(extra="allow")

    count: int = 1


class ClusterSpec(BaseModel):
    """Provision-For-Me spec for an ephemeral cluster."""

    model_config = ConfigDict(extra="allow")

    version: str | None = None
    frontend_node: NodeSpec | None = None


class ComponentSpec(BaseModel):
    type: str
    cluster_info: ClusterInfo | None = None
    cluster_spec: ClusterSpec | None = None

    @model_validator(mode="after")
    def _mutually_exclusive_cluster_fields(self) -> "ComponentSpec":
        has_info = self.cluster_info is not None
        has_spec = self.cluster_spec is not None
        if has_info and has_spec:
            raise ValueError("'cluster_info' and 'cluster_spec' are mutually exclusive; provide only one.")
        if not has_info and not has_spec:
            raise ValueError("One of 'cluster_info' or 'cluster_spec' must be provided.")
        return self


class FixtureItem(BaseModel):
    fixture_id: str
    table: str


class TestEnvironment(BaseModel):
    env_type: Literal["long-lived", "disposable"]
    component_spec: ComponentSpec
    target_db: str
    fixtures: list[FixtureItem] = Field(default_factory=list)


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


# ---------------------------------------------------------------------------
# API response models
# ---------------------------------------------------------------------------

class PlanKey(BaseModel):
    """Returned after a test plan is saved to S3."""
    key: str


class PlanSummary(BaseModel):
    """Metadata for a single test plan in the listing."""
    name: str
    key: str
    last_modified: str
    size: int


class PlanListResponse(BaseModel):
    plans: list[PlanSummary]


class TestRunResponse(BaseModel):
    run_id: str
    strategy: str


class MultipartInitResponse(BaseModel):
    upload_id: str
    key: str


class PresignedUrlResponse(BaseModel):
    presigned_url: str


class PartInfo(BaseModel):
    """One completed part as returned by S3 after a direct upload."""
    PartNumber: int
    ETag: str


class MultipartCompleteResponse(BaseModel):
    key: str
    status: str


class HealthResponse(BaseModel):
    status: str
