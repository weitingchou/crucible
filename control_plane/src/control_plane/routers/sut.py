from fastapi import APIRouter

from ..models import SutInventoryResponse, SutInstance, SutTypesResponse
from ..services import db

router = APIRouter(prefix="/v1/sut", tags=["sut"])

_SUPPORTED_SUT_TYPES = ["doris", "trino", "cassandra"]


@router.get("/types", summary="List supported SUT types")
async def list_sut_types() -> SutTypesResponse:
    """Return the supported System Under Test engine types."""
    return SutTypesResponse(sut_types=_SUPPORTED_SUT_TYPES)


@router.get("/inventory", summary="List active SUT leases")
async def get_sut_inventory() -> SutInventoryResponse:
    """Return runs currently holding a SUT lease (status WAITING_ROOM or EXECUTING)."""
    rows = await db.get_active_runs()
    active = [
        SutInstance(
            run_id=r["run_id"],
            run_label=r["run_label"],
            sut_type=r["sut_type"],
            status=r["status"],
        )
        for r in rows
    ]
    return SutInventoryResponse(active=active)
