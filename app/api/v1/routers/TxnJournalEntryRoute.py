from fastapi import APIRouter, Body, Depends
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.services.txnJournalEntryService import TxnJournalEntryService
from app.services.transactionService import TransactionService
from app.services.manualTransactionService import ManualTransactionService
from typing import List
from fastapi import HTTPException

router = APIRouter(prefix="/api/v1")
txnJournalEntryService = TxnJournalEntryService()


@router.post("/journal-entries")
async def create_journal_entries(
    payload: dict = Body(...),
    db = Depends(get_db) 
):
    manual_txn_ids = payload.get("manual_txn_ids")
    reconciled_status = payload.get("reconciled_status")
    comment = payload.get("comment")
    transactions = payload.get("transactions")

    if not transactions:
        raise HTTPException(400, "transactions required")

    try:
        manual_result = None
        recon_ref = None

        if manual_txn_ids and reconciled_status:
            manual_result = await ManualTransactionService.patch(
                db=db,
                manual_txn_ids=manual_txn_ids,
                payload={
                    "reconciled_status": reconciled_status,
                    "comment": comment,
                    "is_journal_entry": True,
                    "journal_entry_status": "PENDING"
                }
            )
            recon_ref = manual_result["recon_reference_number"]

        for txn in transactions:
            txn["recon_reference_number"] = recon_ref

        journal_result = await TxnJournalEntryService.create_many(
            db=db,
            payloads=transactions
        )

        if recon_ref and manual_result:
            txn_ids = [txn.id for txn in manual_result["transactions"]]

            await TransactionService.patch(
                db=db,
                ids=txn_ids,
                recon_reference_number=recon_ref,
                match_status= 1,
                payload={
                    "reconciled_status": reconciled_status,
                    "comment": comment
                }
            )

        return {
            "success": True,
            "journal": journal_result,
            "recon_reference_number": recon_ref
        }

    except Exception as e:
        await db.rollback()
        raise HTTPException(500, str(e))