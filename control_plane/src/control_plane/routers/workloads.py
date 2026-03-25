"""Control Plane — workload upload endpoint."""

from __future__ import annotations

import asyncio
import re

import boto3
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator

from crucible_lib.schemas.workload import validate_workload

_WORKLOAD_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_\-]*$")

from ..config import settings

router = APIRouter(prefix="/v1/workloads", tags=["workloads"])


class UploadWorkloadRequest(BaseModel):
    workload_id: str
    content: str

    @field_validator("workload_id")
    @classmethod
    def _safe_workload_id(cls, v: str) -> str:
        if not _WORKLOAD_ID_RE.match(v):
            raise ValueError(
                "workload_id must match ^[A-Za-z0-9][A-Za-z0-9_-]*$ "
                "(alphanumeric, hyphens, underscores; no path separators)."
            )
        return v


class UploadWorkloadResponse(BaseModel):
    workload_id: str
    s3_key: str


def _s3():
    kwargs: dict = {
        "region_name": settings.aws_region,
        "endpoint_url": settings.aws_endpoint_url or None,
    }
    if settings.aws_access_key_id and settings.aws_secret_access_key:
        kwargs["aws_access_key_id"] = settings.aws_access_key_id
        kwargs["aws_secret_access_key"] = settings.aws_secret_access_key
    return boto3.client("s3", **kwargs)


@router.post("", status_code=201, summary="Upload a workload file")
async def upload_workload(body: UploadWorkloadRequest) -> UploadWorkloadResponse:
    """Validate workload content and upload it to S3.

    The workload file must begin with a ``-- @type: <type>`` header and contain
    at least one ``-- @name: <QueryName>`` annotated query block.
    """
    errors = validate_workload(body.content)
    if errors:
        raise HTTPException(status_code=422, detail=errors)

    s3_key = f"workloads/{body.workload_id}"

    await asyncio.to_thread(
        _s3().put_object,
        Bucket=settings.s3_bucket,
        Key=s3_key,
        Body=body.content.encode(),
    )

    return UploadWorkloadResponse(workload_id=body.workload_id, s3_key=s3_key)
