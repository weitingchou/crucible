import os
import time

import boto3

from worker.celery_app import app
from worker.config import settings
from worker.db import (
    get_start_signal,
    increment_completed_and_check,
    increment_ready_worker,
    update_run_status,
)
from worker.driver_manager.k6_manager import spawn_k6, wait_and_teardown


@app.task(bind=True, name="worker.tasks.executor.k6_executor_task")
def k6_executor_task(
    self, plan: dict, run_id: str, segment_flag: str, local_instances: int
) -> dict:
    """Download SQL workload files, optionally sync (inter-node), then run k6.

    Steps:
    1. Download annotated SQL workload file(s) from S3 to local disk.
    2. For inter-node mode: check into the PostgreSQL waiting room and block
       until the dispatcher fires the global START signal.
    3. Spawn ``local_instances`` k6 OS processes (>1 for intra-node vertical
       scaling; always 1 for inter-node).
    4. Wait for all processes to finish; escalate SIGTERM → SIGKILL on hang.
    5. Upload raw CSV artifacts to S3 and clean up local disk.
    """
    mode = plan["execution"].get("scaling_mode", "intra_node")

    # ── 1. Download SQL workload files ───────────────────────────────────────
    sql_paths = _download_sql_fixtures(plan["execution"]["workload"])

    # ── 2. Horizontal synchronization (inter-node only) ──────────────────────
    if mode == "inter_node":
        increment_ready_worker(run_id)
        while True:
            state = get_start_signal(run_id)
            if state == "START":
                break
            if state == "ABORT":
                _cleanup(sql_paths, run_id, local_instances)
                return {"status": "aborted_by_dispatcher"}
            time.sleep(0.5)

    # ── 3. Spawn k6 process(es) ──────────────────────────────────────────────
    hold_for = plan["execution"].get("hold_for_seconds", 120)
    processes = []
    for i in range(local_instances):
        local_segment = _sub_segment(segment_flag, i, local_instances)
        processes.append(
            spawn_k6(
                run_id,
                local_segment,
                i,
                plan,
                extra_env={"DOWNLOADED_SQL_PATH": sql_paths[0] if sql_paths else ""},
            )
        )

    # ── 4. Wait and tear down ────────────────────────────────────────────────
    wait_and_teardown(processes, timeout=hold_for)

    # ── 5. Upload artifacts and clean up ─────────────────────────────────────
    for i in range(local_instances):
        _upload_to_s3(f"/tmp/k6_raw_{run_id}_{i}.csv", run_id, i)
    _cleanup(sql_paths, run_id, local_instances)

    # ── 6. Mark completion ───────────────────────────────────────────────────
    # For inter-node mode, only the last executor to finish marks the run as
    # COMPLETED. For intra-node mode (single executor), always mark complete.
    if mode == "intra_node" or increment_completed_and_check(run_id):
        update_run_status(run_id, "COMPLETED", set_completed_at=True)

    return {"status": "completed", "mode": mode, "instances_run": local_instances}


# ── S3 helpers ────────────────────────────────────────────────────────────────

def _s3_client():
    return boto3.client(
        "s3",
        region_name=settings.aws_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        endpoint_url=settings.aws_endpoint_url or None,
    )


def _download_sql_fixtures(workloads: list[dict]) -> list[str]:
    """Download annotated SQL workload files from S3, return local paths."""
    s3 = _s3_client()
    paths = []
    for w in workloads:
        workload_id = w["workload_id"]
        s3_key = f"workloads/{workload_id.replace('-', '_')}.sql"
        local_path = f"/tmp/{workload_id}.sql"
        s3.download_file(settings.s3_bucket, s3_key, local_path)
        paths.append(local_path)
    return paths


def _upload_to_s3(local_path: str, run_id: str, index: int) -> None:
    if not os.path.exists(local_path):
        return
    _s3_client().upload_file(
        local_path,
        settings.s3_bucket,
        f"results/{run_id}/k6_raw_{index}.csv",
    )


def _cleanup(sql_paths: list[str], run_id: str, local_instances: int) -> None:
    for path in sql_paths:
        if os.path.exists(path):
            os.remove(path)
    for i in range(local_instances):
        csv = f"/tmp/k6_raw_{run_id}_{i}.csv"
        if os.path.exists(csv):
            os.remove(csv)


def _sub_segment(segment_flag: str, index: int, total: int) -> str:
    """Split a segment into equal sub-segments for local multi-process scaling."""
    if total == 1:
        return segment_flag
    start_str, end_str = segment_flag.split(":")
    start = float(start_str.rstrip("%"))
    end = float(end_str.rstrip("%"))
    width = (end - start) / total
    sub_start = start + index * width
    sub_end = start + (index + 1) * width
    return f"{sub_start:.4f}%:{sub_end:.4f}%"
