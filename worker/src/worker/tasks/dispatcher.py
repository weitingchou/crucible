import time

from celery.app.control import Inspect

from worker.celery_app import app
from worker.config import settings
from worker.db import (
    get_ready_count,
    init_waiting_room,
    set_start_signal,
    update_run_status,
)
from worker.fixture_loader.base import FixtureLoader
from worker.tasks.executor import k6_executor_task


@app.task(bind=True, name="worker.tasks.dispatcher.dispatcher_task")
def dispatcher_task(
    self, plan: dict, run_id: str, cluster_spec: dict | None = None
) -> dict:
    """Validate capacity, hydrate the SUT, and fan out k6 executor tasks.

    *cluster_spec* is an optional runtime cluster topology provided at
    run-submission time.  When present the ``backend_node.replica`` value
    drives the fan-out size; otherwise the default is 1.

    Branches on ``plan.execution.scaling_mode``:
    - ``intra_node``: dispatches a single executor task instructed to spawn
      N local k6 processes.
    - ``inter_node``: initialises a PostgreSQL waiting room, fans out N
      executor tasks across the fleet, then monitors check-ins and fires
      the global START signal when all workers are ready.
    """
    mode = plan["execution"].get("scaling_mode", "intra_node")
    if cluster_spec:
        backend = cluster_spec.get("backend_node") or {}
        cluster_size = backend.get("replica", 1)
    else:
        cluster_size = 1

    # ── 1. Hydrate the SUT once before fan-out ───────────────────────────────
    try:
        FixtureLoader(plan).load()
    except Exception as exc:
        update_run_status(
            run_id, "FAILED",
            error_detail=f"Fixture loading failed: {type(exc).__name__}: {exc}",
        )
        raise

    # ── 2a. Intra-node (vertical) ────────────────────────────────────────────
    if mode == "intra_node":
        update_run_status(run_id, "EXECUTING", set_started_at=True)
        k6_executor_task.delay(
            plan, run_id, segment_flag="0%:100%",
            local_instances=cluster_size, segment_index=0,
        )
        return {"status": "intra_node_dispatched"}

    # ── 2b. Inter-node (horizontal) — Two-Phase Check-In ─────────────────────
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
            return {"status": "inter_node_all_started"}
        time.sleep(2)

    set_start_signal(run_id, "ABORT")
    update_run_status(run_id, "FAILED", error_detail="Worker check-in timeout exceeded")
    return {"error": "Worker check-in timeout exceeded"}
