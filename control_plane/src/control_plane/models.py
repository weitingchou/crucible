from pydantic import BaseModel


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


class FixtureIdListResponse(BaseModel):
    fixture_ids: list[str]


class FixtureFileSummary(BaseModel):
    name: str
    key: str
    last_modified: str
    size: int


class FixtureFileListResponse(BaseModel):
    fixture_id: str
    files: list[FixtureFileSummary]


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


# ── V1 models ────────────────────────────────────────────────────────────────

class SutTypesResponse(BaseModel):
    sut_types: list[str]


class SutInstance(BaseModel):
    run_id: str
    run_label: str
    sut_type: str
    status: str


class SutInventoryResponse(BaseModel):
    active: list[SutInstance]


class SubmitRunRequest(BaseModel):
    plan_yaml: str
    plan_name: str
    label: str = ""
    cluster_spec: dict | None = None
    cluster_settings: str | None = None


class TriggerRunRequest(BaseModel):
    label: str = ""
    cluster_spec: dict | None = None
    cluster_settings: str | None = None


class SubmitRunResponse(BaseModel):
    run_id: str
    plan_key: str
    strategy: str


class WaitingRoomInfo(BaseModel):
    ready_count: int
    target_count: int


class RunStatusResponse(BaseModel):
    run_id: str
    status: str
    plan_name: str
    run_label: str
    sut_type: str
    scaling_mode: str
    cluster_spec: dict | None = None
    cluster_settings: str | None = None
    submitted_at: str
    started_at: str | None = None
    completed_at: str | None = None
    error_detail: str | None = None
    waiting_room: WaitingRoomInfo | None = None


class StopRunResponse(BaseModel):
    run_id: str
    action: str  # "sigterm_sent" | "already_stopped" | "not_found"


class ArtifactEntry(BaseModel):
    key: str
    size: int


class ArtifactsResponse(BaseModel):
    run_id: str
    artifacts: list[ArtifactEntry]


class RunDetail(BaseModel):
    run_id: str
    plan_name: str
    run_label: str
    sut_type: str
    status: str
    scaling_mode: str
    cluster_spec: dict | None = None
    cluster_settings: str | None = None
    submitted_at: str
    started_at: str | None = None
    completed_at: str | None = None
    error_detail: str | None = None


class ListRunsResponse(BaseModel):
    runs: list[RunDetail]


class RunSummary(BaseModel):
    run_id: str
    plan_name: str
    run_label: str
    sut_type: str
    status: str
    scaling_mode: str
    cluster_spec: dict | None = None
    cluster_settings: str | None = None
    submitted_at: str
    completed_at: str | None = None


class RecentStatsResponse(BaseModel):
    runs: list[RunSummary]
