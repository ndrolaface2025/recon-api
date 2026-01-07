from fastapi import APIRouter, Body, Depends
from sqlalchemy.orm import Session
from app.services.services import get_service
from app.services.manualTransactionService import ManualTransactionService
from app.services.transactionService import TransactionService
from fastapi import HTTPException

router = APIRouter()
manualTransactionService = ManualTransactionService()
transactionService = TransactionService()

 
@router.patch("/manual-transactions")
async def patch_manual_transactions(
    payload: dict = Body(...),
    db: Session = Depends(get_service)
):
    ids = payload.get("ids")

    if not ids or not isinstance(ids, list):
        raise HTTPException(
            status_code=400,
            detail="ids must be a non-empty list"
        )

    manual_result = ManualTransactionService.patch(
        db,
        ids,
        payload
    )

    recon_ref = manual_result["recon_reference_number"]

    if payload.get("reconciled_status") == "MATCHED":
        TransactionService.patch(
            db,
            ids,
            recon_ref,
            payload
        )

    return {
        "success": True,
        "message": "Manual and master transactions updated successfully",
        "data": manual_result["transactions"],
        "recon_reference_number": recon_ref
    }

@router.get("/manual-transactions")
async def get_all_manual_transactions(
    db: Session = Depends(get_service)
):
    return await manualTransactionService.get_all_manual_transactions(db)


@router.post("/manual-transactions")
async def create_manual_transaction(
    payload: dict = Body(...),
    db: Session = Depends(get_service)
):
    return await manualTransactionService.create_manual_transaction(db, payload)
