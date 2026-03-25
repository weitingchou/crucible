from pathlib import Path

import yaml
from fastapi import APIRouter, HTTPException, Query, UploadFile

from ..models import PlanKey, PlanListResponse
from ..schemas.test_plan import TestPlan
from ..services import s3_broker
from ..utils import sanitize_plan_name

router = APIRouter(prefix="/test-plans", tags=["test-plans"])


@router.get("", summary="List test plans")
async def list_test_plans() -> PlanListResponse:
    """Return metadata for every test plan stored in S3."""
    return await s3_broker.list_plans()


@router.get(
    "/{name}",
    summary="Get a test plan",
    responses={404: {"description": "Test plan not found"}},
)
async def get_test_plan(name: str) -> dict:
    """Return the parsed content of a specific test plan."""
    plan = await s3_broker.get_plan(name)
    if plan is None:
        raise HTTPException(status_code=404, detail=f"Test plan '{name}' not found.")
    return plan


@router.delete(
    "/{name}",
    status_code=204,
    summary="Delete a test plan",
    responses={404: {"description": "Test plan not found"}},
)
async def delete_test_plan(name: str) -> None:
    """Delete a specific test plan from S3."""
    deleted = await s3_broker.delete_plan(name)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Test plan '{name}' not found.")


@router.post("", status_code=201, summary="Create a test plan (JSON)")
async def create_test_plan(
    plan: TestPlan,
    name: str = Query(..., description="Plan name (used as the S3 key identity)"),
) -> PlanKey:
    """Validate a test plan submitted as JSON and store it in S3.

    The ``name`` query parameter determines the plan identity in S3
    (independent of ``test_metadata.run_label`` inside the YAML).

    Returns 409 if a plan with the same name already exists.
    Use ``PUT /test-plans/{name}`` to replace an existing plan.
    """
    name = sanitize_plan_name(name)
    if await s3_broker.plan_exists(name):
        raise HTTPException(
            status_code=409,
            detail=f"Test plan '{name}' already exists. Use PUT /test-plans/{name} to update.",
        )
    content = yaml.dump(plan.model_dump()).encode()
    return await s3_broker.save_plan(name, content)


@router.post("/upload", status_code=201, summary="Upload a test plan (YAML file)")
async def upload_test_plan(file: UploadFile) -> PlanKey:
    """Validate an uploaded YAML file against the test plan schema and store it in S3.

    Returns 409 if a plan with the same name already exists.
    Use ``PUT /test-plans/{name}/upload`` to replace an existing plan.
    """
    content = await file.read()
    try:
        data = yaml.safe_load(content)
        TestPlan.model_validate(data)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    name = sanitize_plan_name(Path(file.filename).stem)
    if await s3_broker.plan_exists(name):
        raise HTTPException(
            status_code=409,
            detail=f"Test plan '{name}' already exists. Use PUT /test-plans/{name}/upload to update.",
        )
    return await s3_broker.save_plan(name, content)


@router.put("/{name}", summary="Create or replace a test plan (JSON)")
async def upsert_test_plan(name: str, plan: TestPlan) -> PlanKey:
    """Validate and unconditionally save a test plan under the given *name*."""
    content = yaml.dump(plan.model_dump()).encode()
    return await s3_broker.save_plan(name, content)


@router.put("/{name}/upload", summary="Create or replace a test plan (YAML file)")
async def upsert_test_plan_upload(name: str, file: UploadFile) -> PlanKey:
    """Validate an uploaded YAML file and unconditionally save it under *name*."""
    content = await file.read()
    try:
        data = yaml.safe_load(content)
        TestPlan.model_validate(data)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return await s3_broker.save_plan(name, content)
