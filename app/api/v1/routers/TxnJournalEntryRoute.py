from fastapi import APIRouter, Body, Depends
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.services.txnJournalEntryService import TxnJournalEntryService
from app.services.transactionService import TransactionService
from app.services.manualTransactionService import ManualTransactionService
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import Query

from app.utils.enums.reconciliation import ReconciliationStatus

router = APIRouter(prefix="/api/v1")
txnJournalEntryService = TxnJournalEntryService()


@router.post("/journal-entries")
async def create_journal_entries(payload: dict = Body(...), db=Depends(get_db)):
    manual_txn_ids = payload.get("manual_txn_ids")
    reconciliation_status = payload.get("reconciliation_status")
    comment = payload.get("comment")
    transactions = payload.get("transactions")

    if not transactions:
        raise HTTPException(400, "transactions required")

    try:
        manual_result = None
        recon_ref = None

        if manual_txn_ids and reconciliation_status == ReconciliationStatus.COMPLETED:
            manual_result = await ManualTransactionService.patch(
                db=db,
                manual_txn_ids=manual_txn_ids,
                payload={
                    "reconciliation_status": reconciliation_status,
                    "comment": comment,
                    "is_journal_entry": True,
                    "journal_entry_status": "PENDING",
                },
            )
            recon_ref = manual_result["recon_reference_number"]

        for txn in transactions:
            txn["recon_reference_number"] = recon_ref

        journal_result = await TxnJournalEntryService.create_many(
            db=db, payloads=transactions
        )

        if recon_ref and manual_result:
            txn_ids = [txn.id for txn in manual_result["transactions"]]

            await TransactionService.patch(
                db=db,
                ids=txn_ids,
                recon_reference_number=recon_ref,
                payload={
                    "reconciliation_status": reconciliation_status,
                    "comment": comment,
                },
            )

        return {
            "success": True,
            "journal": journal_result,
            "recon_reference_number": recon_ref,
        }

    except Exception as e:
        await db.rollback()
        raise HTTPException(500, str(e))


@router.get("/journal-entries")
async def get_all_journal_entries(
    offset: int = Query(0, ge=0),
    limit: int = Query(10, ge=1),
    db: AsyncSession = Depends(get_db),
):
    return await TxnJournalEntryService.get_all_journal_entries(
        db=db,
        offset=offset,
        limit=limit,
    )


@router.get("/journal-entries/pending")
async def get_pending_journal_entries(
    offset: int = Query(0, ge=0),
    limit: int = Query(10, ge=1),
    db: AsyncSession = Depends(get_db),
):
    return await TxnJournalEntryService.get_pending_journal_entries(
        db=db,
        offset=offset,
        limit=limit,
    )

@router.patch("/journal-entries/{recon_ref_no}")
async def patch_journal_entries(
    recon_ref_no: str,
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db)
):
    return await TxnJournalEntryService.patch_journal_entries(
        db=db,
        reconRefNo=recon_ref_no,
        payload=payload
    )

@router.patch("/journal-entries/pending/{recon_ref_no}")
async def patch_pending_entries(
    recon_ref_no: str,
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db)
):
    return await TxnJournalEntryService.patch_pending_entries(
        db=db,
        reconRefNo=recon_ref_no,
        payload=payload
    )
