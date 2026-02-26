from fastapi import APIRouter

from ..services.s3_broker import complete_multipart, get_presigned_part_url, init_multipart

router = APIRouter(prefix="/fixtures", tags=["fixtures"])


@router.post("/{fixture_id}/{file_name}/multipart/init")
async def init_upload(fixture_id: str, file_name: str) -> dict:
    """Create an S3 multipart upload and return the upload_id."""
    return await init_multipart(fixture_id, file_name)


@router.get("/{fixture_id}/{file_name}/multipart/{upload_id}/part/{part_number}")
async def presigned_part(
    fixture_id: str,
    file_name: str,
    upload_id: str,
    part_number: int,
) -> dict:
    """Return a pre-signed URL the client uses to upload a single part directly to S3."""
    return await get_presigned_part_url(fixture_id, file_name, upload_id, part_number)


@router.post("/{fixture_id}/{file_name}/multipart/{upload_id}/complete")
async def complete_upload(
    fixture_id: str,
    file_name: str,
    upload_id: str,
    parts: list[dict],
) -> dict:
    """Finalise the multipart upload after all parts have been sent."""
    return await complete_multipart(fixture_id, file_name, upload_id, parts)
