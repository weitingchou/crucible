"""V1 test-run endpoints — accept raw YAML, track lifecycle in PostgreSQL."""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone

import boto3
import yaml
from botocore.exceptions import ClientError
from celery import Celery
from fastapi import APIRouter, HTTPException
from pydantic import TypeAdapter, ValidationError

from crucible_lib.schemas.cluster_spec import ClusterSpec
from crucible_lib.schemas.test_plan import TestPlan

from ..config import settings
from ..models import (
    ArtifactEntry,
    ArtifactsResponse,
    ListRunsResponse,
    RunDetail,
    RunStatusResponse,
    StopRunResponse,
    SubmitRunRequest,
    SubmitRunResponse,
    TestRunResults,
    TriggerRunRequest,
    WaitingRoomInfo,
)
from ..services import db
from ..utils import sanitize_plan_name

_cluster_spec_adapter = TypeAdapter(ClusterSpec)

router = APIRouter(prefix="/v1/test-runs", tags=["test-runs"])

_celery = Celery(
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
)


def _s3():
    return boto3.client(
        "s3",
        region_name=settings.aws_region,
        aws_access_key_id=settings.aws_access_key_id or None,
        aws_secret_access_key=settings.aws_secret_access_key or None,
        endpoint_url=settings.aws_endpoint_url or None,
    )


def _generate_run_id(plan_name: str) -> str:
    """Return a human-readable run ID: ``{plan_name}_{YYYYMMDD-HHmm}_{8hex}``."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M")
    short_uuid = uuid.uuid4().hex[:8]
    return f"{plan_name}_{ts}_{short_uuid}"


def _validate_cluster_spec(
    cluster_spec_dict: dict,
    plan: TestPlan,
) -> None:
    """Validate a runtime cluster_spec against the plan's component_spec."""
    try:
        _cluster_spec_adapter.validate_python(cluster_spec_dict)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors())
    if cluster_spec_dict["type"] != plan.test_environment.component_spec.type:
        raise HTTPException(
            status_code=422,
            detail=(
                f"cluster_spec.type '{cluster_spec_dict['type']}' does not match "
                f"component_spec.type '{plan.test_environment.component_spec.type}'"
            ),
        )


@router.post("", summary="Submit a test run from raw YAML")
async def submit_test_run(body: SubmitRunRequest) -> SubmitRunResponse:
    """Validate a YAML test plan, upload it to S3 under a stable name,
    dispatch to Celery, and record the run in PostgreSQL."""
    # Parse and validate
    try:
        raw = yaml.safe_load(body.plan_yaml)
        plan = TestPlan.model_validate(raw)
    except yaml.YAMLError as exc:
        raise HTTPException(status_code=422, detail=f"Invalid YAML: {exc}")
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors())

    # Validate runtime cluster_spec
    _validate_cluster_spec(body.cluster_spec, plan)

    # Derive stable plan identity
    plan_name = sanitize_plan_name(body.plan_name)
    plan_key = f"plans/{plan_name}"

    # Save plan to S3 under stable name (upsert)
    await asyncio.to_thread(
        _s3().put_object,
        Bucket=settings.s3_bucket,
        Key=plan_key,
        Body=body.plan_yaml.encode(),
    )

    # Generate human-readable run_id
    run_id = _generate_run_id(plan_name)

    # Dispatch to Celery (sync → offload to thread)
    result = await asyncio.to_thread(
        _celery.send_task,
        "worker.tasks.dispatcher.dispatcher_task",
        args=(raw, run_id, body.cluster_spec),
    )

    # Record in PostgreSQL
    sut_type = plan.test_environment.component_spec.type
    await db.insert_run(
        run_id=run_id,
        task_id=result.id,
        plan_name=plan_name,
        plan_key=plan_key,
        run_label=body.label or body.plan_name,
        sut_type=sut_type,
        scaling_mode=plan.execution.scaling_mode,
        cluster_spec=body.cluster_spec,
        cluster_settings=body.cluster_settings,
    )

    return SubmitRunResponse(
        run_id=run_id,
        plan_key=plan_key,
        strategy=plan.execution.scaling_mode,
    )


@router.get("", summary="List test runs")
async def list_test_runs(run_label: str | None = None) -> ListRunsResponse:
    """Return test runs, optionally filtered by *run_label*."""
    rows = await db.list_runs(run_label=run_label)

    def _fmt(ts) -> str | None:
        return ts.isoformat() if ts else None

    runs = [
        RunDetail(
            run_id=r["run_id"],
            plan_name=r["plan_name"],
            run_label=r["run_label"],
            sut_type=r["sut_type"],
            status=r["status"],
            scaling_mode=r["scaling_mode"],
            cluster_spec=r.get("cluster_spec"),
            cluster_settings=r.get("cluster_settings"),
            submitted_at=_fmt(r["submitted_at"]),
            started_at=_fmt(r["started_at"]),
            completed_at=_fmt(r["completed_at"]),
            error_detail=r.get("error_detail"),
        )
        for r in rows
    ]
    return ListRunsResponse(runs=runs)


@router.post(
    "/{plan_name}",
    summary="Trigger a test run against an existing plan",
    responses={404: {"description": "Test plan not found in S3"}},
)
async def trigger_run_by_plan(
    plan_name: str,
    body: TriggerRunRequest,
) -> SubmitRunResponse:
    """Fetch an already-uploaded plan from S3 by *plan_name*, dispatch a new
    test run, and record it in PostgreSQL.

    ``cluster_spec`` is required — it records the SUT topology for this run
    (e.g. ``{"type": "doris", "backend_node": {"replica": 3}}``).
    """
    plan_key = f"plans/{plan_name}"

    # Fetch plan from S3
    try:
        resp = await asyncio.to_thread(
            _s3().get_object,
            Bucket=settings.s3_bucket,
            Key=plan_key,
        )
        plan_bytes = resp["Body"].read()
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "NoSuchKey":
            raise HTTPException(
                status_code=404,
                detail=f"Test plan '{plan_name}' not found.",
            )
        raise

    # Parse and validate
    try:
        raw = yaml.safe_load(plan_bytes)
        plan = TestPlan.model_validate(raw)
    except (yaml.YAMLError, ValidationError) as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    # Validate runtime cluster_spec
    _validate_cluster_spec(body.cluster_spec, plan)

    # Generate human-readable run_id
    run_id = _generate_run_id(plan_name)

    # Dispatch to Celery
    result = await asyncio.to_thread(
        _celery.send_task,
        "worker.tasks.dispatcher.dispatcher_task",
        args=(raw, run_id, body.cluster_spec),
    )

    # Record in PostgreSQL
    sut_type = plan.test_environment.component_spec.type
    run_label = (body.label if body.label else None) or raw.get(
        "test_metadata", {}
    ).get("run_label", plan_name)
    await db.insert_run(
        run_id=run_id,
        task_id=result.id,
        plan_name=plan_name,
        plan_key=plan_key,
        run_label=run_label,
        sut_type=sut_type,
        scaling_mode=plan.execution.scaling_mode,
        cluster_spec=body.cluster_spec,
        cluster_settings=body.cluster_settings,
    )

    return SubmitRunResponse(
        run_id=run_id,
        plan_key=plan_key,
        strategy=plan.execution.scaling_mode,
    )


@router.get("/{run_id}/status", summary="Get test run status")
async def get_run_status(run_id: str) -> RunStatusResponse:
    """Return the current lifecycle status of a test run."""
    row = await db.get_run(run_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found.")

    waiting_room = None
    if row["status"] == "WAITING_ROOM":
        wr = await db.get_waiting_room_info(run_id)
        if wr:
            waiting_room = WaitingRoomInfo(
                ready_count=wr["ready_count"],
                target_count=wr["target_count"],
            )

    def _fmt(ts) -> str | None:
        return ts.isoformat() if ts else None

    # asyncpg decodes JSONB to Python dicts automatically
    cluster_spec = row.get("cluster_spec")

    return RunStatusResponse(
        run_id=row["run_id"],
        status=row["status"],
        plan_name=row["plan_name"],
        run_label=row["run_label"],
        sut_type=row["sut_type"],
        scaling_mode=row["scaling_mode"],
        cluster_spec=cluster_spec,
        cluster_settings=row.get("cluster_settings"),
        submitted_at=_fmt(row["submitted_at"]),
        started_at=_fmt(row["started_at"]),
        completed_at=_fmt(row["completed_at"]),
        error_detail=row["error_detail"],
        waiting_room=waiting_room,
    )


@router.post("/{run_id}/stop", summary="Emergency stop a test run")
async def stop_run(run_id: str) -> StopRunResponse:
    """Send SIGTERM to the Celery task. The worker's teardown path escalates
    to SIGKILL if k6 subprocesses don't exit within the grace period."""
    row = await db.get_run(run_id)
    if not row:
        return StopRunResponse(run_id=run_id, action="not_found")

    if row["status"] in ("COMPLETED", "FAILED", "STOPPING"):
        return StopRunResponse(run_id=run_id, action="already_stopped")

    # Mark as stopping in DB
    await db.update_run_status(run_id, "STOPPING")

    # Revoke the Celery task with SIGTERM (sync → offload to thread)
    if row["task_id"]:
        await asyncio.to_thread(
            _celery.control.revoke, row["task_id"], terminate=True, signal="SIGTERM"
        )

    return StopRunResponse(run_id=run_id, action="sigterm_sent")


@router.get("/{run_id}/artifacts", summary="List S3 artifacts for a run")
async def list_run_artifacts(run_id: str) -> ArtifactsResponse:
    """Return the CSV artifact files uploaded to S3 after the run completed."""

    def _list_objects() -> list[ArtifactEntry]:
        s3 = _s3()
        paginator = s3.get_paginator("list_objects_v2")
        return [
            ArtifactEntry(key=obj["Key"], size=obj["Size"])
            for page in paginator.paginate(
                Bucket=settings.s3_bucket, Prefix=f"results/{run_id}/"
            )
            for obj in page.get("Contents", [])
        ]

    artifacts = await asyncio.to_thread(_list_objects)
    return ArtifactsResponse(run_id=run_id, artifacts=artifacts)


@router.get("/{run_id}/results", summary="Get structured test results")
async def get_run_results(run_id: str) -> TestRunResults:
    """Return k6 summary stats and observability metrics collected after the run."""
    row = await db.get_run(run_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found.")

    if row["status"] not in ("COLLECTING", "COMPLETED", "FAILED"):
        raise HTTPException(
            status_code=409,
            detail=f"Results not yet available (status={row['status']}).",
        )

    def _load_results() -> dict | None:
        s3 = _s3()
        try:
            resp = s3.get_object(
                Bucket=settings.s3_bucket,
                Key=f"results/{run_id}/results.json",
            )
            return json.loads(resp["Body"].read())
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "NoSuchKey":
                return None
            raise

    data = await asyncio.to_thread(_load_results)

    if data is None:
        return TestRunResults(
            run_id=run_id,
            status=row["status"],
            collection_error="Results file not yet available.",
        )

    return TestRunResults(
        run_id=run_id,
        status=row["status"],
        collected_at=data.get("collected_at"),
        collection_error=data.get("collection_error"),
        k6=data.get("k6", {}),
        observability=data.get("observability", {}),
    )
