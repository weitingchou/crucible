import yaml
import boto3
from botocore.exceptions import ClientError
from celery import Celery, chain, signature
from fastapi import HTTPException

from ..config import settings

_s3 = boto3.client("s3", region_name=settings.aws_region)

# A Celery app instance used only for dispatching — no workers run here.
# Tasks are referenced by name so this package need not import the worker package.
_celery = Celery(broker=settings.celery_broker_url, backend=settings.celery_result_backend)


async def dispatch_test_run(filename: str) -> dict:
    try:
        plan_bytes = _s3.get_object(Bucket=settings.s3_bucket, Key=f"plans/{filename}")["Body"].read()
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "NoSuchKey":
            raise HTTPException(status_code=404, detail=f"Test plan '{filename}' not found.")
        raise
    plan: dict = yaml.safe_load(plan_bytes)

    mode: str = plan.get("execution", {}).get("scaling_mode", "intra_node")

    if mode == "inter_node":
        cluster_size: int = plan["test_environment"].get("cluster_size", 2)
        workflow = chain(
            signature(
                "worker.tasks.master.run_master_task",
                args=(plan,),
                kwargs={"mode": "inter_node"},
                app=_celery,
            ),
            signature(
                "worker.tasks.runner.run_worker_task",
                # master_result is prepended automatically by Celery chain
                args=(plan,),
                kwargs={"count": cluster_size - 1},
                app=_celery,
            ),
        )
        result = workflow.delay()
        return {"run_id": result.id, "strategy": "distributed_horizontal"}

    result = _celery.send_task(
        "worker.tasks.master.run_master_task",
        args=(plan,),
        kwargs={"mode": "intra_node"},
    )
    return {"run_id": result.id, "strategy": "distributed_vertical"}
