from fastapi import APIRouter

from fastapi import HTTPException

from ..models import (
    FixtureFileListResponse,
    FixtureIdListResponse,
    MultipartCompleteResponse,
    MultipartInitResponse,
    PartInfo,
    PresignedUrlResponse,
)
from ..services.s3_broker import (
    complete_multipart,
    delete_fixture,
    get_presigned_part_url,
    init_multipart,
    list_fixture_files,
    list_fixture_ids,
)

router = APIRouter(prefix="/fixtures", tags=["fixtures"])


@router.get("", summary="List fixture IDs")
async def list_fixtures() -> FixtureIdListResponse:
    """Return all fixture IDs (top-level S3 prefixes under fixtures/)."""
    return await list_fixture_ids()


@router.get("/{fixture_id}", summary="List files in a fixture")
async def list_fixture(fixture_id: str) -> FixtureFileListResponse:
    """Return all files stored under a specific fixture ID."""
    return await list_fixture_files(fixture_id)


@router.delete(
    "/{fixture_id}/{file_name}",
    status_code=204,
    summary="Delete a fixture file",
    responses={404: {"description": "Fixture not found"}},
)
async def delete_fixture_file(fixture_id: str, file_name: str) -> None:
    """Delete a specific fixture file from S3."""
    deleted = await delete_fixture(fixture_id, file_name)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Fixture '{fixture_id}/{file_name}' not found.")


@router.post("/{fixture_id}/{file_name}/multipart/init", summary="Initiate multipart upload")
async def init_upload(fixture_id: str, file_name: str) -> MultipartInitResponse:
    """Create an S3 multipart upload session. Returns the `upload_id` required
    for all subsequent part and completion requests."""
    return await init_multipart(fixture_id, file_name)


@router.get(
    "/{fixture_id}/{file_name}/multipart/{upload_id}/part/{part_number}",
    summary="Get pre-signed URL for a part",
)
async def presigned_part(
    fixture_id: str,
    file_name: str,
    upload_id: str,
    part_number: int,
) -> PresignedUrlResponse:
    """Return a pre-signed S3 URL valid for 1 hour. The client uses this URL to
    `PUT` a single part **directly to S3**, bypassing this server entirely."""
    return await get_presigned_part_url(fixture_id, file_name, upload_id, part_number)


@router.post(
    "/{fixture_id}/{file_name}/multipart/{upload_id}/complete",
    summary="Complete multipart upload",
)
async def complete_upload(
    fixture_id: str,
    file_name: str,
    upload_id: str,
    parts: list[PartInfo],
) -> MultipartCompleteResponse:
    """Finalise the multipart upload. Pass the list of `{PartNumber, ETag}` objects
    returned by S3 for each successfully uploaded part."""
    return await complete_multipart(fixture_id, file_name, upload_id, parts)
