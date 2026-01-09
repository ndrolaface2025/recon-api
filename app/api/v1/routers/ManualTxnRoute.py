from fastapi import APIRouter, Body, Depends
from sqlalchemy.orm import Session
from app.services.services import get_service
from app.services.manualTransactionService import ManualTransactionService
from app.services.transactionService import TransactionService
from fastapi import HTTPException

router = APIRouter(prefix="/api/v1")
 
# @router.patch("/manual-transactions")
# async def patch_manual_transactions(
#     payload: dict = Body(...),
#     db: Session = Depends(get_service)
# ):
#     ids = payload.get("ids")

#     if not ids or not isinstance(ids, list):
#         raise HTTPException(
#             status_code=400,
#             detail="ids must be a non-empty list"
#         )

#     manual_result = ManualTransactionService.patch(
#         db,
#         ids,
#         payload
#     )

#     recon_ref = manual_result["recon_reference_number"]

#     if payload.get("reconciled_status") == "MATCHED":
#         TransactionService.patch(
#             db,
#             ids,
#             recon_ref,
#             payload
#         )

#     return {
#         "success": True,
#         "message": "Manual and master transactions updated successfully",
#         "data": manual_result["transactions"],
#         "recon_reference_number": recon_ref
#     }
@router.patch("/manual-transactions")
def patch_manual_transactions(
    payload: dict = Body(...),
    db: Session = Depends(get_service)
):
    manual_txn_ids = payload.pop("manual_txn_ids", None)

    if not manual_txn_ids or not isinstance(manual_txn_ids, list):
        raise HTTPException(
            status_code=400,
            detail="manual_txn_ids must be a non-empty list"
        )

    manual_result = ManualTransactionService.patch(
        db=db,
        manual_txn_ids=manual_txn_ids,
        payload=payload
    )

    recon_ref = manual_result["recon_reference_number"]

    if payload.get("reconciled_status") == "MATCHED":

        txn_ids = [
            txn.manual_txn_id  
            for txn in manual_result["transactions"]
        ]

        TransactionService.patch(
            db=db,
            txn_ids=txn_ids,
            recon_reference_number=recon_ref,
            payload=payload
        )

    return {
        "success": True,
        "message": "Manual and master transactions updated successfully",
        "data": manual_result["transactions"],
        "recon_reference_number": recon_ref
    }

@router.get("/manual-transactions")
def get_all_manual_transactions(
    db: Session = Depends(get_service)
):
    return ManualTransactionService.get_all_json(
        db=db,
        username="Ackim"
    )


@router.post("/manual-transactions")
async def create_manual_transaction(
    payload: dict = Body(...),
    db: Session = Depends(get_service)
):
    return await ManualTransactionService.create_manual_transaction(db, payload)
