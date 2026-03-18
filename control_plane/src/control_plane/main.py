from contextlib import asynccontextmanager

from fastapi import FastAPI

from .models import HealthResponse
from .routers import fixtures, test_plans, test_runs, sut, test_runs_v1, telemetry
from .services import db

_DESCRIPTION = """
Crucible Control Plane manages test plans, brokers S3 uploads for fixture
datasets, and dispatches load-test jobs to the Celery worker fleet.

## Workflows

### Upload a Test Plan
1. `POST /test-plans/upload` — upload a YAML test plan file
2. `GET /test-plans` — list all uploaded plans
3. `GET /test-plans/{name}` — retrieve a specific plan

### Run a Test
1. `POST /test-runs/{key}` — enqueue a Celery job for the given plan S3 key

### Upload a Large Fixture Dataset (100 GB+)
1. `POST /fixtures/{id}/{file}/multipart/init` — obtain an `upload_id`
2. `GET /fixtures/{id}/{file}/multipart/{upload_id}/part/{n}` — get a pre-signed URL per part
3. Upload parts **directly to S3** using the pre-signed URLs (bypasses this server)
4. `POST /fixtures/{id}/{file}/multipart/{upload_id}/complete` — finalise the upload
"""

_TAGS_METADATA = [
    {
        "name": "test-plans",
        "description": "Create, upload, list, and retrieve YAML test plan documents stored in S3.",
    },
    {
        "name": "test-runs",
        "description": "Trigger load-test execution. The control plane reads the plan from S3, "
                       "determines the scaling strategy, and enqueues the appropriate Celery workflow.",
    },
    {
        "name": "fixtures",
        "description": "Broker S3 multipart uploads for large fixture datasets (100 GB+). "
                       "Data flows directly between the client and S3 — it never passes through this server.",
    },
    {
        "name": "ops",
        "description": "Health and readiness probes.",
    },
]

@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init_pool()
    yield
    await db.close_pool()


app = FastAPI(
    title="Crucible Control Plane",
    version="0.1.0",
    description=_DESCRIPTION,
    openapi_tags=_TAGS_METADATA,
    lifespan=lifespan,
)

app.include_router(test_runs.router)
app.include_router(fixtures.router)
app.include_router(test_plans.router)
app.include_router(sut.router)
app.include_router(test_runs_v1.router)
app.include_router(telemetry.router)


@app.get("/health", tags=["ops"])
async def health() -> HealthResponse:
    return HealthResponse(status="ok")
