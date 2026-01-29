from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import date
from app.flexcube_db.session import get_flexcube_db
from app.flexcube_db.repositories.actb_history_repo import ActbHistoryRepository
from app.flexcube_db.repositories.actb_daily_log_repo import ActbDailyLogRepository
from app.flexcube_db.repositories.gltm_glmaster_repo import GltmGlmasterRepository

router = APIRouter(prefix="/flexcube", tags=["Flexcube"])


@router.get("/actb-history")
async def get_actb_history(
    ac_no: str | None = Query(None),
    drcr_ind: str | None = Query(None),
    ccy: str | None = Query(None),
    date_from: date | None = Query(None),
    date_to: date | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(30, ge=1, le=500),
    db: AsyncSession = Depends(get_flexcube_db),
):
    repo = ActbHistoryRepository(db)
    records, total_records = repo.get_filtered(
        page, page_size, ac_no, drcr_ind, ccy, date_from, date_to
    )

    total_pages = (total_records + page_size - 1) // page_size

    return {
        "status": "success",
        "error": False,
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
    ac_no: str | None = Query(None),
    drcr_ind: str | None = Query(None),
    date_from: date | None = Query(None),
    date_to: date | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(30, ge=1, le=500),
    db: AsyncSession = Depends(get_flexcube_db),
):
    repo = ActbDailyLogRepository(db)
    records, total_records = repo.get_filtered(
        page, page_size, ac_no, drcr_ind, date_from, date_to
    )

    total_pages = (total_records + page_size - 1) // page_size

    return {
        "status": "success",
        "error": False,
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


@router.get("/gl-master")
async def get_gl_master(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: AsyncSession = Depends(get_flexcube_db),
):
    repo = GltmGlmasterRepository(db)
    records, total_records = repo.get_paginated(page, page_size)

    total_pages = (total_records + page_size - 1) // page_size

    return {
        "status": "success",
        "error": False,
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
