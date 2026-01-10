from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_db
from app.db.repositories.generalLedgerRequest import GeneralLedgerCreateRequest
from app.services.generalLedgerService import GeneralLedgerService

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
    db: AsyncSession = Depends(get_db),
):
    return await GeneralLedgerService.get_all_general_ledgers(db)

