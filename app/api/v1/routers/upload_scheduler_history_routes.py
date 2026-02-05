import math
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime

from app.db.session import get_db
from app.services.upload_scheduler_history_service import (
    UploadSchedulerHistoryService,
)

router = APIRouter(
    prefix="/api/v1/scheduler-history",
    tags=["Scheduler History"],
)


@router.get(
    "",
    summary="List scheduler execution history with filters",
)
async def list_scheduler_history(
    scheduler_id: int | None = Query(None, description="Scheduler ID"),
    status: int | None = Query(
        None,
        description="0=SUCCESS, 1=PARTIAL, 2=FAILED, IN_PROGRESS per enum",
    ),
    from_date: datetime | None = Query(
        None, description="Filter by execution start time (from)"
    ),
    to_date: datetime | None = Query(
        None, description="Filter by execution start time (to)"
    ),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Records per page"),
    db: AsyncSession = Depends(get_db),
):
    """
    Single unified endpoint for scheduler history.

    Supports filtering by:
    - scheduler_id
    - execution status
    - date range
    - pagination
    """

    service = UploadSchedulerHistoryService(db)

    return await service.get_all(
        scheduler_id=scheduler_id,
        status=status,
        date_from=from_date,
        date_to=to_date,
        page=page,
        page_size=page_size,
    )
