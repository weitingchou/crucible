from fastapi import APIRouter

from ..services.dispatcher import dispatch_test_run

router = APIRouter(prefix="/test-runs", tags=["test-runs"])


@router.post("/{filename}")
async def trigger_test_run(filename: str) -> dict:
    """Fetch the test plan from S3, determine the scaling strategy, and enqueue."""
    return await dispatch_test_run(filename)
