from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.schemas.recon_run_schemas import (
    PaginatedReconResponse,
    ReconModeBreakdownResponse,
    ReconUploadedFileResponse,
    ReconRuleResponse,
)
from app.services.recon_run_service import (
    get_reconciliation_mode_breakdown,
    get_reconciliation_runs,
    get_transactions_by_recon_and_mode,
    get_uploaded_files_by_recon,
    get_rules_by_recon_group,
)

router = APIRouter(
    prefix="/api/v1/reconciliation-runs",
    tags=["Reconciliation Audit"],
)


@router.get(
    "",
    response_model=PaginatedReconResponse,
    summary="List reconciliation audit runs",
)
async def list_reconciliation_runs(
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1, le=100),
    from_date: str | None = None,
    to_date: str | None = None,
    channel_id: int | None = None,
    maker_id: int | None = None,
    checker_id: int | None = None,
    status: str | None = Query(None, description="COMPLETED or IN_PROGRESS"),
    recon_group_number: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    return await get_reconciliation_runs(
        db=db,
        page=page,
        size=size,
        from_date=from_date,
        to_date=to_date,
        channel_id=channel_id,
        maker_id=maker_id,
        checker_id=checker_id,
        status=status,
        recon_group_number=recon_group_number,
    )


@router.get(
    "/{recon_group_number}/uploaded-files",
    response_model=list[ReconUploadedFileResponse],
    summary="Get uploaded files for a reconciliation run",
)
async def get_uploaded_files(
    recon_group_number: str,
    db: AsyncSession = Depends(get_db),
):
    return await get_uploaded_files_by_recon(db, recon_group_number)


@router.get(
    "/{recon_group_number}/rules",
    response_model=list[ReconRuleResponse],
    summary="Get matching rules used in a reconciliation run",
)
async def get_recon_rules(
    recon_group_number: str,
    db: AsyncSession = Depends(get_db),
):
    return await get_rules_by_recon_group(db, recon_group_number)


@router.get(
    "/{recon_group_number}/mode-breakdown",
    response_model=list[ReconModeBreakdownResponse],
    summary="Automatic vs Manual reconciliation breakdown",
)
async def get_mode_breakdown(
    recon_group_number: str,
    db: AsyncSession = Depends(get_db),
):
    return await get_reconciliation_mode_breakdown(db, recon_group_number)


@router.get(
    "/{recon_group_number}/transactions",
    summary="View transactions by reconciliation mode",
)
async def view_recon_transactions(
    recon_group_number: str,
    mode: str = Query(..., description="AUTOMATIC or MANUAL"),
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    return await get_transactions_by_recon_and_mode(
        db=db,
        recon_group_number=recon_group_number,
        mode=mode,
        page=page,
        size=size,
    )
