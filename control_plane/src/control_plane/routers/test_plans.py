from pathlib import Path

import yaml
from fastapi import APIRouter, HTTPException, UploadFile

from crucible_lib.schemas.test_plan import TestPlan

from ..services import s3_broker

router = APIRouter(prefix="/test-plans", tags=["test-plans"])


@router.post("", status_code=201)
async def create_test_plan(plan: TestPlan) -> dict:
    content = yaml.dump(plan.model_dump()).encode()
    return await s3_broker.save_plan(plan.name, content)


@router.post("/upload", status_code=201)
async def upload_test_plan(file: UploadFile) -> dict:
    content = await file.read()
    try:
        data = yaml.safe_load(content)
        plan = TestPlan.model_validate(data)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    name = Path(file.filename).stem
    return await s3_broker.save_plan(name, content)
