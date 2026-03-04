from fastapi import APIRouter

from ..models import TestRunResponse
from ..services.dispatcher import dispatch_test_run

router = APIRouter(prefix="/test-runs", tags=["test-runs"])


@router.post("/{filename:path}", summary="Trigger a test run")
async def trigger_test_run(filename: str) -> TestRunResponse:
    """Fetch the test plan from S3 using *filename* as the object key, determine
    the scaling strategy from `execution.scaling_mode`, and enqueue the appropriate
    Celery workflow. Returns the Celery task ID and the chosen strategy name."""
    return await dispatch_test_run(filename)
