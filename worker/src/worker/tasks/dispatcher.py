import time
import zlib

from celery.app.control import Inspect

from worker.celery_app import app
from worker.config import settings
from worker.db import (
    acquire_sut_lock,
    get_ready_count,
    get_run_status,
    init_waiting_room,
    release_sut_lock,
    set_start_signal,
    update_run_status,
)
from worker.driver_manager.k6_manager import parse_k6_duration
from worker.fixture_loader.base import FixtureLoader
from worker.tasks.executor import k6_executor_task


def _sut_lock_key(plan: dict) -> int:
    """Derive a stable integer lock key from the SUT host.

    Uses the cluster_info host as the SUT identity — two runs targeting
    the same host get the same key and are serialized.  Falls back to
    component_spec.type for plans without cluster_info (disposable).
    """
    cluster_info = (
        plan.get("test_environment", {})
        .get("component_spec", {})
        .get("cluster_info")
    )
    if cluster_info and cluster_info.get("host"):
        identity = cluster_info["host"]
    else:
        identity = plan.get("test_environment", {}).get("component_spec", {}).get("type", "unknown")
    return zlib.crc32(identity.encode()) & 0x7FFFFFFF


def _wait_for_completion(run_id: str, timeout: int) -> None:
    """Poll test_runs until the run reaches a terminal state or times out."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        status = get_run_status(run_id)
        if status in ("COMPLETED", "FAILED"):
            return
        time.sleep(3)
    # Timeout — mark as failed
    update_run_status(
        run_id, "FAILED",
        error_detail="Dispatcher timed out waiting for executor completion",
        set_completed_at=True,
    )


@app.task(bind=True, name="worker.tasks.dispatcher.dispatcher_task")
def dispatcher_task(
    self, plan: dict, run_id: str, cluster_spec: dict | None = None
) -> dict:
    """Acquire exclusive SUT lease, hydrate, fan out k6 executors, wait for completion.

    The dispatcher holds a PostgreSQL advisory lock on the SUT for the entire
    test lifecycle.  Concurrent submissions targeting the same SUT block at the
    lock until the previous run completes (QUEUED status).  Different SUTs use
    different lock keys and run concurrently.

    *cluster_spec* is an optional runtime cluster topology provided at
    run-submission time.  When present the ``backend_node.replica`` value
    drives the fan-out size; otherwise the default is 1.
    """
    mode = plan["execution"].get("scaling_mode", "intra_node")
    if cluster_spec:
        backend = cluster_spec.get("backend_node") or {}
        cluster_size = backend.get("replica", 1)
    else:
        cluster_size = 1

    # Compute wait timeout from plan durations + generous buffer
    execution = plan["execution"]
    ramp_up_secs = parse_k6_duration(execution.get("ramp_up", "0s"))
    hold_for_secs = parse_k6_duration(execution.get("hold_for", "30s"))
    completion_timeout = ramp_up_secs + hold_for_secs + 120  # 2-minute buffer

    # ── 0. Acquire exclusive SUT lease ───────────────────────────────────────
    lock_key = _sut_lock_key(plan)
    update_run_status(run_id, "QUEUED")
    acquire_sut_lock(lock_key)

    try:
        # ── 1. Hydrate the SUT once before fan-out ───────────────────────────
        try:
            FixtureLoader(plan).load()
        except Exception as exc:
            update_run_status(
                run_id, "FAILED",
                error_detail=f"Fixture loading failed: {type(exc).__name__}: {exc}",
            )
            raise

        # ── 2a. Intra-node (vertical) ────────────────────────────────────────
        if mode == "intra_node":
            update_run_status(run_id, "EXECUTING", set_started_at=True)
            k6_executor_task.delay(
                plan, run_id, segment_flag="0%:100%",
                local_instances=cluster_size, segment_index=0,
            )
            _wait_for_completion(run_id, timeout=completion_timeout)
            return {"status": "intra_node_completed"}

        # ── 2b. Inter-node (horizontal) — Two-Phase Check-In ─────────────────
        inspector = Inspect(app=self.app)
        active_workers = len(inspector.active_queues() or {})
        if active_workers < cluster_size:
            update_run_status(
                run_id,
                "FAILED",
                error_detail=f"Requested {cluster_size} nodes, only {active_workers} available",
            )
            return {
                "status": "failed_insufficient_capacity",
                "error": f"Requested {cluster_size} nodes, only {active_workers} available",
            }

        init_waiting_room(run_id, cluster_size)
        update_run_status(run_id, "WAITING_ROOM")

        segment_size = 100 / cluster_size
        for idx in range(cluster_size):
            start = int(idx * segment_size)
            end = int((idx + 1) * segment_size)
            k6_executor_task.delay(
                plan, run_id, segment_flag=f"{start}%:{end}%",
                local_instances=1, segment_index=idx,
            )

        # Monitor waiting room (5-minute timeout)
        deadline = time.time() + 300
        while time.time() < deadline:
            if get_ready_count(run_id) == cluster_size:
                set_start_signal(run_id, "START")
                update_run_status(run_id, "EXECUTING", set_started_at=True)
                break
            time.sleep(2)
        else:
            set_start_signal(run_id, "ABORT")
            update_run_status(run_id, "FAILED", error_detail="Worker check-in timeout exceeded")
            return {"error": "Worker check-in timeout exceeded"}

        # Wait for all executors to finish
        _wait_for_completion(run_id, timeout=completion_timeout)
        return {"status": "inter_node_completed"}

    finally:
        release_sut_lock(lock_key)
