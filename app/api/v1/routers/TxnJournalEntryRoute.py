from fastapi import APIRouter, Body, Depends
from sqlalchemy.orm import Session
from app.services.services import get_service
from app.services.txnJournalEntryService import TxnJournalEntryService
from typing import List

router = APIRouter()
txnJournalEntryService = TxnJournalEntryService()


@router.post("/journal-entries")
async def create_journal_entries(
    payload: List[dict] = Body(...),
    db: Session = Depends(get_service)
):
    return await txnJournalEntryService.create_journal_entries(db, payload)
