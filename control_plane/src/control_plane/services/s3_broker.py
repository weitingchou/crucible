import boto3

from ..config import settings

_s3 = boto3.client("s3", region_name=settings.aws_region)


def _key(fixture_id: str, file_name: str) -> str:
    return f"fixtures/{fixture_id}/{file_name}"


async def init_multipart(fixture_id: str, file_name: str) -> dict:
    key = _key(fixture_id, file_name)
    response = _s3.create_multipart_upload(Bucket=settings.s3_bucket, Key=key)
    return {"upload_id": response["UploadId"], "key": key}


async def get_presigned_part_url(
    fixture_id: str,
    file_name: str,
    upload_id: str,
    part_number: int,
) -> dict:
    key = _key(fixture_id, file_name)
    url = _s3.generate_presigned_url(
        ClientMethod="upload_part",
        Params={
            "Bucket": settings.s3_bucket,
            "Key": key,
            "UploadId": upload_id,
            "PartNumber": part_number,
        },
        ExpiresIn=3600,
    )
    return {"presigned_url": url}


async def complete_multipart(
    fixture_id: str,
    file_name: str,
    upload_id: str,
    parts: list[dict],
) -> dict:
    key = _key(fixture_id, file_name)
    _s3.complete_multipart_upload(
        Bucket=settings.s3_bucket,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )
    return {"key": key, "status": "completed"}


async def save_plan(name: str, content: bytes) -> dict:
    key = f"plans/{name}.yaml"
    _s3.put_object(Bucket=settings.s3_bucket, Key=key, Body=content)
    return {"key": key}
