"""Shared PostgreSQL helpers for worker tasks.

Uses a module-level connection that is created lazily and reused across calls
within the same worker process.
"""

from __future__ import annotations

import psycopg2

from worker.config import settings

_conn: psycopg2.extensions.connection | None = None


def _get_conn() -> psycopg2.extensions.connection:
    """Return a reusable connection, reconnecting if closed."""
    global _conn
    if _conn is None or _conn.closed:
        url = settings.database_url.replace("postgresql+asyncpg://", "postgresql://")
        _conn = psycopg2.connect(url)
    return _conn


def update_run_status(run_id: str, status: str, **fields: object) -> None:
    """Write a lifecycle transition to the test_runs table."""
    set_clauses = ["status = %s"]
    values: list[object] = [status]
    if "error_detail" in fields:
        set_clauses.append("error_detail = %s")
        values.append(fields["error_detail"])
    if fields.get("set_started_at"):
        set_clauses.append("started_at = now()")
    if fields.get("set_completed_at"):
        set_clauses.append("completed_at = now()")
    values.append(run_id)
    sql = f"UPDATE test_runs SET {', '.join(set_clauses)} WHERE run_id = %s"
    conn = _get_conn()
    with conn.cursor() as cur:
        cur.execute(sql, values)
    conn.commit()


def init_waiting_room(run_id: str, target_count: int) -> None:
    conn = _get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO waiting_room (run_id, ready_count, completed_count, target_count, signal)
            VALUES (%s, 0, 0, %s, 'WAIT')
            ON CONFLICT (run_id) DO NOTHING
            """,
            (run_id, target_count),
        )
    conn.commit()


def get_ready_count(run_id: str) -> int:
    conn = _get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT ready_count FROM waiting_room WHERE run_id = %s", (run_id,)
        )
        row = cur.fetchone()
        return row[0] if row else 0


def set_start_signal(run_id: str, signal_value: str) -> None:
    conn = _get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE waiting_room SET signal = %s WHERE run_id = %s",
            (signal_value, run_id),
        )
    conn.commit()


def increment_ready_worker(run_id: str) -> None:
    conn = _get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE waiting_room SET ready_count = ready_count + 1 WHERE run_id = %s",
            (run_id,),
        )
    conn.commit()


def get_start_signal(run_id: str) -> str:
    conn = _get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT signal FROM waiting_room WHERE run_id = %s", (run_id,)
        )
        row = cur.fetchone()
        return row[0] if row else "WAIT"


def get_run_status(run_id: str) -> str | None:
    """Read the current status of a run from test_runs."""
    conn = _get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT status FROM test_runs WHERE run_id = %s", (run_id,))
        row = cur.fetchone()
        return row[0] if row else None


def acquire_sut_lock(lock_key: int) -> None:
    """Acquire a PostgreSQL session-level advisory lock.  Blocks until available."""
    conn = _get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_lock(%s)", (lock_key,))


def release_sut_lock(lock_key: int) -> None:
    """Release a PostgreSQL session-level advisory lock."""
    conn = _get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_unlock(%s)", (lock_key,))
    conn.commit()


def increment_completed_and_check(run_id: str) -> bool:
    """Atomically increment completed_count and return True if all executors are done."""
    conn = _get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE waiting_room
            SET completed_count = completed_count + 1
            WHERE run_id = %s
            RETURNING completed_count, target_count
            """,
            (run_id,),
        )
        row = cur.fetchone()
    conn.commit()
    if row is None:
        return True  # no waiting room row means intra_node; safe to mark complete
    return row[0] >= row[1]
