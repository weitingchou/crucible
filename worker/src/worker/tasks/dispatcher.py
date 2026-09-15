import json
import logging
import random
import time
import zlib

import boto3
from celery.app.control import Inspect
from celery.exceptions import MaxRetriesExceededError

from crucible_lib.queues import EXECUTE_QUEUE
from worker.celery_app import app
from worker.chaos_injector import ChaosScheduler
from worker.config import settings
from worker.db import (
    get_ready_count,
    get_run_status,
    init_waiting_room,
    release_sut_lock,
    set_start_signal,
    try_acquire_sut_lock,
    update_run_status,
)
from worker.driver_manager.k6_manager import parse_k6_duration
from worker.fixture_loader.base import FixtureLoader
from worker.tasks.executor import k6_executor_task

logger = logging.getLogger(__name__)

# Ceiling on the SUT-lock retry backoff, and the mean countdown once the
# backoff has climbed to it — which is what turns ``sut_lock_max_wait_seconds``
# into a retry budget.
_LOCK_RETRY_CEILING = 30

# 2 ** 5 already exceeds the ceiling, so clamping the exponent there keeps the
# curve identical without building a huge integer just to discard it.
_LOCK_RETRY_MAX_EXPONENT = 5

# Broadcast inspect RPCs default to a 1s timeout, which a loaded broker misses.
_INSPECT_TIMEOUT = 5.0


def _lock_retry_countdown(retries: int) -> int:
    """Seconds to wait before the next SUT-lock attempt.

    Exponential up to ``_LOCK_RETRY_CEILING``, jittered so a burst of runs
    queued against the same SUT doesn't retry in lockstep.  The jitter is
    centred on the ceiling rather than pulling below it: a downward-only jitter
    would make the real timeout about a third shorter than
    ``sut_lock_max_wait_seconds`` advertises.
    """
    base = min(2 ** min(retries, _LOCK_RETRY_MAX_EXPONENT), _LOCK_RETRY_CEILING)
    return max(1, round(base * random.uniform(0.75, 1.25)))


def _execute_slot_capacity(celery_app) -> int:
    """Executor slots advertised by workers consuming the execute queue.

    ``active_queues()`` counts worker *nodes*, but a node with an 8-slot pool
    can run 8 executors, so sizing the fan-out off the node count green-lights
    runs the fleet can never satisfy.

    This measures the fleet's *total* size, not how many slots happen to be
    free.  Free-slot counting would be racy regardless — slots can be taken
    between the check and the fan-out — so this is a guard against asking for
    more than exists, and the caller's waiting-room timeout is the real
    backstop.
    """
    inspector = Inspect(app=celery_app, timeout=_INSPECT_TIMEOUT)
    queues = inspector.active_queues() or {}
    stats = inspector.stats() or {}
    total = 0
    for node, node_queues in queues.items():
        if not any(q.get("name") == EXECUTE_QUEUE for q in node_queues or []):
            continue
        concurrency = ((stats.get(node) or {}).get("pool") or {}).get("max-concurrency")
        if not concurrency:
            # stats() is a second broadcast and can time out on its own.  One
            # slot under-reports rather than over-reports, but log it: silently
            # shrinking capacity fails runs that the fleet could have served.
            logger.warning(
                "No pool stats for worker %s; counting it as 1 executor slot", node
            )
            concurrency = 1
        total += concurrency
    return total


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


def _total_chaos_duration(chaos_spec: dict) -> int:
    """Return the total wall time for all experiments run sequentially (seconds).

    The ChaosScheduler executes experiments one after another, so the total
    duration is the *sum* of each experiment's (start_after + duration).
    """
    total = 0
    for exp in chaos_spec.get("experiments", []):
        sched = exp.get("schedule", {})
        total += parse_k6_duration(sched.get("start_after", "0s")) + parse_k6_duration(sched.get("duration", "0s"))
    return total


def _start_chaos(plan: dict, run_id: str) -> ChaosScheduler | None:
    """Start a ChaosScheduler thread if the plan has a chaos_spec."""
    chaos_spec = plan.get("chaos_spec")
    if not chaos_spec:
        return None
    scheduler = ChaosScheduler(chaos_spec, run_id)
    scheduler.start()
    logger.info("Chaos scheduler started for run %s", run_id)
    return scheduler


def _stop_chaos(scheduler: ChaosScheduler | None) -> list[dict]:
    """Cancel and join a running ChaosScheduler, recovering all active faults.

    Returns the list of chaos events collected during execution.
    """
    if scheduler is None:
        return []
    if scheduler.is_alive():
        scheduler.cancel()
        scheduler.join(timeout=30)
        if scheduler.is_alive():
            logger.warning("Chaos scheduler did not stop within 30s")
    logger.info("Chaos scheduler stopped")
    return scheduler.get_events()


def _upload_chaos_events(run_id: str, events: list[dict]) -> None:
    """Upload chaos event log to S3 so the control plane can merge it into results."""
    if not events:
        return
    kwargs: dict = {
        "region_name": settings.aws_region,
        "endpoint_url": settings.aws_endpoint_url or None,
    }
    if settings.aws_access_key_id and settings.aws_secret_access_key:
        kwargs["aws_access_key_id"] = settings.aws_access_key_id
        kwargs["aws_secret_access_key"] = settings.aws_secret_access_key
    s3 = boto3.client("s3", **kwargs)
    s3.put_object(
        Bucket=settings.s3_bucket,
        Key=f"results/{run_id}/chaos_events.json",
        Body=json.dumps(events, indent=2).encode(),
        ContentType="application/json",
    )


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
    test lifecycle.  Concurrent submissions targeting the same SUT are marked
    QUEUED and retried *off-slot* until the lease frees, so a waiting run does
    not tie up a Celery worker slot.  Different SUTs use different lock keys and
    run concurrently.

    Retrying costs the FIFO ordering a blocking ``pg_advisory_lock`` would have
    given: waiters race for the lease on each attempt, so under sustained
    contention an older run can be overtaken by newer ones and eventually
    exhaust ``sut_lock_max_wait_seconds``.  Fair ordering would need an ordered
    lease table rather than an advisory lock.

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
    chaos_extra = _total_chaos_duration(plan.get("chaos_spec", {})) if plan.get("chaos_spec") else 0
    completion_timeout = ramp_up_secs + hold_for_secs + chaos_extra + 120  # 2-minute buffer

    # ── Pre-flight: reject an impossible fan-out ─────────────────────────────
    # cluster_size is already known, so check it before taking the lease and
    # before hydrating the SUT rather than after paying for both.
    if mode == "inter_node":
        capacity = _execute_slot_capacity(self.app)
        if capacity < cluster_size:
            error = (
                f"Requested {cluster_size} nodes, only {capacity} executor slots available"
            )
            update_run_status(run_id, "FAILED", error_detail=error, set_completed_at=True)
            return {"status": "failed_insufficient_capacity", "error": error}

    # ── 0. Acquire exclusive SUT lease ───────────────────────────────────────
    # Non-blocking: a dispatcher that parks on the lock holds a pool slot for
    # the whole wait, so on contention we give the slot back and retry instead.
    lock_key = _sut_lock_key(plan)
    if not try_acquire_sut_lock(lock_key):
        update_run_status(run_id, "QUEUED")
        try:
            raise self.retry(
                countdown=_lock_retry_countdown(self.request.retries),
                max_retries=max(
                    1, settings.sut_lock_max_wait_seconds // _LOCK_RETRY_CEILING
                ),
            )
        except MaxRetriesExceededError:
            error = (
                f"Timed out after ~{settings.sut_lock_max_wait_seconds}s waiting for the "
                f"SUT lease; another run still holds it"
            )
            update_run_status(run_id, "FAILED", error_detail=error, set_completed_at=True)
            return {"status": "failed_sut_lease_timeout", "error": error}

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
            chaos = _start_chaos(plan, run_id)
            try:
                k6_executor_task.delay(
                    plan, run_id, segment_flag="0%:100%",
                    local_instances=cluster_size, segment_index=0,
                )
                _wait_for_completion(run_id, timeout=completion_timeout)
            finally:
                events = _stop_chaos(chaos)
                try:
                    _upload_chaos_events(run_id, events)
                except Exception:
                    logger.exception("Failed to upload chaos events for run %s", run_id)
            return {"status": "intra_node_completed"}

        # ── 2b. Inter-node (horizontal) — Two-Phase Check-In ─────────────────
        # Capacity was checked pre-flight; the waiting-room timeout below is the
        # backstop for slots taken by other runs in the meantime.
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
            update_run_status(
                run_id, "FAILED",
                error_detail="Worker check-in timeout exceeded",
                set_completed_at=True,
            )
            return {"error": "Worker check-in timeout exceeded"}

        # ── 3. Chaos injection (after all workers are running) ───────────
        chaos = _start_chaos(plan, run_id)
        try:
            _wait_for_completion(run_id, timeout=completion_timeout)
        finally:
            events = _stop_chaos(chaos)
            try:
                _upload_chaos_events(run_id, events)
            except Exception:
                logger.exception("Failed to upload chaos events for run %s", run_id)
        return {"status": "inter_node_completed"}

    finally:
        release_sut_lock(lock_key)
