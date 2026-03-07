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
