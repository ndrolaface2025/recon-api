from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_db
from app.db.repositories.generalLedgerRequest import GeneralLedgerCreateRequest
from app.services.generalLedgerService import GeneralLedgerService
from fastapi import Query


router = APIRouter(prefix="/api/v1")

@router.post("/general-ledger")
async def create_general_ledger(
    payload: GeneralLedgerCreateRequest,
    db: AsyncSession = Depends(get_db),
):
    temp_id = 1

    return await GeneralLedgerService.create_general_ledger(
        db=db,
        payload=payload,
        user_id=temp_id
    )

@router.get("/general-ledger")

async def get_all_general_ledgers(
    offset: int = Query(0, ge=0),
    limit: int = Query(10, ge=1),
    search: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    return await GeneralLedgerService.get_all_general_ledgers(
        db=db,
        offset=offset,
        limit=limit
    )

@router.delete("/general-ledger/{gl_id}")
async def delete_general_ledger(
    gl_id: int,
    db: AsyncSession = Depends(get_db),
):
    await GeneralLedgerService.delete_general_ledger_by_id(db, gl_id)


@router.put("/general-ledger/{gl_id}")
async def update_general_ledger(
    gl_id: int,
    payload: GeneralLedgerCreateRequest,
    db: AsyncSession = Depends(get_db),
):
    temp_id = 1

    return await GeneralLedgerService.update_general_ledger_by_id(
        db=db,
        gl_id=gl_id,
        payload=payload,
        user_id=temp_id
    )