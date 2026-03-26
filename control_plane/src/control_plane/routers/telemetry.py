from fastapi import APIRouter

from ..models import RecentStatsResponse, RunSummary
from ..services import db

router = APIRouter(prefix="/v1/telemetry", tags=["telemetry"])


@router.get("/recent-stats", summary="Recent test run summaries")
async def recent_stats() -> RecentStatsResponse:
    """Return the last 5 test runs ordered by submission time."""
    rows = await db.list_recent_runs(limit=5)

    def _fmt(ts) -> str | None:
        return ts.isoformat() if ts else None

    runs = [
        RunSummary(
            run_id=r["run_id"],
            plan_name=r["plan_name"],
            run_label=r["run_label"],
            sut_type=r["sut_type"],
            status=r["status"],
            scaling_mode=r["scaling_mode"],
            cluster_spec=r.get("cluster_spec"),
            submitted_at=_fmt(r["submitted_at"]),
            completed_at=_fmt(r["completed_at"]),
        )
        for r in rows
    ]
    return RecentStatsResponse(runs=runs)
