import uuid

import boto3
import yaml
from botocore.exceptions import ClientError
from celery import Celery
from fastapi import HTTPException

from ..config import settings

_s3 = boto3.client(
    "s3",
    region_name=settings.aws_region,
    aws_access_key_id=settings.aws_access_key_id or None,
    aws_secret_access_key=settings.aws_secret_access_key or None,
    endpoint_url=settings.aws_endpoint_url or None,
)

# A Celery app instance used only for dispatching — no workers run here.
# Tasks are referenced by name so this package need not import the worker package.
_celery = Celery(broker=settings.celery_broker_url, backend=settings.celery_result_backend)


async def dispatch_test_run(filename: str) -> dict:
    try:
        plan_bytes = _s3.get_object(
            Bucket=settings.s3_bucket, Key=f"plans/{filename}"
        )["Body"].read()
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "NoSuchKey":
            raise HTTPException(status_code=404, detail=f"Test plan '{filename}' not found.")
        raise

    plan: dict = yaml.safe_load(plan_bytes)
    run_id: str = str(uuid.uuid4())

    result = _celery.send_task(
        "worker.tasks.dispatcher.dispatcher_task",
        args=(plan, run_id),
    )
    return {
        "run_id": run_id,
        "task_id": result.id,
        "strategy": plan.get("execution", {}).get("scaling_mode", "intra_node"),
    }
