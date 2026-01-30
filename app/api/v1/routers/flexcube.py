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
    search: str | None = None,
    db: Session = Depends(get_flexcube_db),
):
    repo = ActbHistoryRepository(db)

    records, total_records = repo.get_filtered(
        page=page,
        page_size=page_size,
        ac_no=ac_no,
        drcr_ind=drcr_ind,
        search=search,
    )

    total_pages = (total_records + page_size - 1) // page_size

    return {
        "success": True,
        "message": "ACTB history fetched successfully",
        "data": records,
        "meta": {
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_records": total_records,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_previous": page > 1,
            }
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
        "success": True,
        "message": "ACTB daily log fetched successfully",
        "data": records,
        "meta": {
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_records": total_records,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_previous": page > 1,
            }
        },
    }


@router.get("/gl-master/lookup")
async def lookup_gl_master(
    search: str | None = Query(
        None,
        min_length=1,
        description="Search by GL code or GL description",
    ),
    limit: int = Query(
        50,
        ge=1,
        le=100,
        description="Maximum number of results to return",
    ),
    db: Session = Depends(get_flexcube_db),
):
    repo = GltmGlmasterRepository(db)

    records = repo.lookup_gl(
        search=search,
        limit=limit,
    )

    return {
        "success": True,
        "message": "GL master lookup results",
        "data": records,
        "meta": {},
    }
