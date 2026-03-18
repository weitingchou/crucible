"""asyncpg connection pool and query helpers for the Control Plane."""

from __future__ import annotations

import asyncpg

from ..config import settings

_pool: asyncpg.Pool | None = None


async def init_pool() -> None:
    global _pool
    _pool = await asyncpg.create_pool(
        dsn=settings.database_url.replace("postgresql+asyncpg://", "postgresql://"),
        min_size=2,
        max_size=10,
    )


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


def _get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("Database pool not initialised")
    return _pool


async def insert_run(
    run_id: str,
    task_id: str,
    plan_key: str,
    run_label: str,
    sut_type: str,
    scaling_mode: str,
) -> None:
    await _get_pool().execute(
        """
        INSERT INTO test_runs (run_id, task_id, plan_key, run_label, sut_type, scaling_mode)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (run_id) DO NOTHING
        """,
        run_id, task_id, plan_key, run_label, sut_type, scaling_mode,
    )


async def get_run(run_id: str) -> dict | None:
    row = await _get_pool().fetchrow(
        "SELECT * FROM test_runs WHERE run_id = $1", run_id
    )
    return dict(row) if row else None


async def get_active_runs() -> list[dict]:
    rows = await _get_pool().fetch(
        "SELECT * FROM test_runs WHERE status IN ('WAITING_ROOM', 'EXECUTING')"
    )
    return [dict(r) for r in rows]


async def update_run_status(run_id: str, status: str, **fields: object) -> None:
    set_clauses = ["status = $1"]
    values: list[object] = [status]
    idx = 2
    if "error_detail" in fields:
        set_clauses.append(f"error_detail = ${idx}")
        values.append(fields["error_detail"])
        idx += 1
    values.append(run_id)
    sql = f"UPDATE test_runs SET {', '.join(set_clauses)} WHERE run_id = ${idx}"
    await _get_pool().execute(sql, *values)


async def get_waiting_room_info(run_id: str) -> dict | None:
    row = await _get_pool().fetchrow(
        "SELECT ready_count, target_count FROM waiting_room WHERE run_id = $1",
        run_id,
    )
    return dict(row) if row else None


async def list_recent_runs(limit: int = 5) -> list[dict]:
    rows = await _get_pool().fetch(
        "SELECT * FROM test_runs ORDER BY submitted_at DESC LIMIT $1", limit
    )
    return [dict(r) for r in rows]
