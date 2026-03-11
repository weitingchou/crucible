import time

from celery.app.control import Inspect

from worker.celery_app import app
from worker.config import settings
from worker.fixture_loader.base import FixtureLoader
from worker.tasks.executor import k6_executor_task


@app.task(bind=True, name="worker.tasks.dispatcher.dispatcher_task")
def dispatcher_task(self, plan: dict, run_id: str) -> dict:
    """Validate capacity, hydrate the SUT, and fan out k6 executor tasks.

    Branches on ``plan.execution.scaling_mode``:
    - ``intra_node``: dispatches a single executor task instructed to spawn
      N local k6 processes.
    - ``inter_node``: initialises a PostgreSQL waiting room, fans out N
      executor tasks across the fleet, then monitors check-ins and fires
      the global START signal when all workers are ready.
    """
    mode = plan["execution"].get("scaling_mode", "intra_node")
    cluster_size = (
        plan["test_environment"]
        .get("component_spec", {})
        .get("cluster_spec", {})
        .get("backend_node", {})
        .get("count", 1)
    )

    # ── 1. Hydrate the SUT once before fan-out ───────────────────────────────
    FixtureLoader(plan).load()

    # ── 2a. Intra-node (vertical) ────────────────────────────────────────────
    if mode == "intra_node":
        k6_executor_task.delay(
            plan, run_id, segment_flag="0%:100%", local_instances=cluster_size
        )
        return {"status": "intra_node_dispatched"}

    # ── 2b. Inter-node (horizontal) — Two-Phase Check-In ─────────────────────
    inspector = Inspect(app=self.app)
    active_workers = len(inspector.active_queues() or {})
    if active_workers < cluster_size:
        return {
            "status": "failed_insufficient_capacity",
            "error": f"Requested {cluster_size} nodes, only {active_workers} available",
        }

    _init_waiting_room(run_id, cluster_size)

    segment_size = 100 / cluster_size
    for idx in range(cluster_size):
        start = int(idx * segment_size)
        end = int((idx + 1) * segment_size)
        k6_executor_task.delay(
            plan, run_id, segment_flag=f"{start}%:{end}%", local_instances=1
        )

    # Monitor waiting room (5-minute timeout)
    deadline = time.time() + 300
    while time.time() < deadline:
        if _get_ready_count(run_id) == cluster_size:
            _set_start_signal(run_id, "START")
            return {"status": "inter_node_all_started"}
        time.sleep(2)

    _set_start_signal(run_id, "ABORT")
    return {"error": "Worker check-in timeout exceeded"}


# ── PostgreSQL waiting-room helpers ───────────────────────────────────────────

def _db_conn():
    import psycopg2
    url = settings.database_url.replace("postgresql+asyncpg://", "postgresql://")
    return psycopg2.connect(url)


def _init_waiting_room(run_id: str, target_count: int) -> None:
    with _db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO waiting_room (run_id, ready_count, target_count, signal)
            VALUES (%s, 0, %s, 'WAIT')
            ON CONFLICT (run_id) DO NOTHING
            """,
            (run_id, target_count),
        )
        conn.commit()


def _get_ready_count(run_id: str) -> int:
    with _db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT ready_count FROM waiting_room WHERE run_id = %s", (run_id,)
        )
        row = cur.fetchone()
        return row[0] if row else 0


def _set_start_signal(run_id: str, signal_value: str) -> None:
    with _db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE waiting_room SET signal = %s WHERE run_id = %s",
            (signal_value, run_id),
        )
        conn.commit()
