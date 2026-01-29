from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from datetime import date

from app.flexcube_db.session import get_flexcube_db
from app.flexcube_db.repositories.actb_history_repo import ActbHistoryRepository
from app.flexcube_db.repositories.actb_daily_log_repo import ActbDailyLogRepository
from app.flexcube_db.repositories.gltm_glmaster_repo import GltmGlmasterRepository

router = APIRouter(prefix="/api/v1/flexcube", tags=["Flexcube"])


@router.get("/actb-history")
async def get_actb_history(
    page: int = Query(1, ge=1),
    page_size: int = Query(30, ge=1, le=500),
    ac_no: str | None = None,
    drcr_ind: str | None = None,
    ccy: str | None = None,
    search: str | None = None,
    db: Session = Depends(get_flexcube_db),
):
    repo = ActbHistoryRepository(db)

    records, total_records = repo.get_filtered(
        page=page,
        page_size=page_size,
        ac_no=ac_no,
        drcr_ind=drcr_ind,
        ccy=ccy,
        search=search,
    )

    total_pages = (total_records + page_size - 1) // page_size

    return {
        "status": "success",
        "data": {
            "records": records,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_records": total_records,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_previous": page > 1,
            },
        },
    }


@router.get("/actb-daily-log")
async def get_actb_daily_log(
    page: int = Query(1, ge=1),
    page_size: int = Query(30, ge=1, le=500),
    ac_no: str | None = None,
    drcr_ind: str | None = None,
    search: str | None = None,
    db: Session = Depends(get_flexcube_db),
):
    repo = ActbDailyLogRepository(db)

    records, total_records = repo.get_filtered(
        page=page,
        page_size=page_size,
        ac_no=ac_no,
        drcr_ind=drcr_ind,
        search=search,
    )

    total_pages = (total_records + page_size - 1) // page_size

    return {
        "status": "success",
        "data": {
            "records": records,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_records": total_records,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_previous": page > 1,
            },
        },
    }


@router.get("/gl-master/lookup")
async def lookup_gl(
    search: str | None = Query(None, description="Search by GL code or description"),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_flexcube_db),
):
    repo = GltmGlmasterRepository(db)
    return {
        "status": "success",
        "data": repo.lookup_gl(search=search, limit=limit),
    }
