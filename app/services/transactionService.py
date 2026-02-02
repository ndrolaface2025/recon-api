from sqlalchemy.orm import Session
from fastapi import HTTPException
from app.db.models.transactions import Transaction
from sqlalchemy import select
from app.db.models.manualTransaction import ManualTransaction

class TransactionService:

    @staticmethod
    async def patch(
    db: Session,
    ids: list[int],
    recon_reference_number: str,
    payload: dict,
    match_status: int | None = None
    ):
        txns = (
            await db.execute(
                select(Transaction).where(Transaction.id.in_(ids))
            )
        ).scalars().all()
        txns = (
        await db.execute(
            select(Transaction).where(Transaction.id.in_(ids))
        )
        ).scalars().all()

        if not txns:
            raise HTTPException(
                status_code=404,
                detail="Master transaction(s) not found"
            )
        
        patch_type = payload.get("patch_type")
        payload = {
            k: v for k, v in payload.items()
            if hasattr(Transaction, k)
        }
        for txn in txns:
            # Apply allowed field updates
            for field, value in payload.items():
                setattr(txn, field, value)

            # Always set recon reference
            txn.recon_reference_number = recon_reference_number

            # Conditional business rule
            if patch_type == "Manual" and match_status is not None:
                txn.match_status = match_status

        await db.commit()
        return txns
    
    @staticmethod
    async def getInvestigatedTxnIds(db: Session):
        result = await db.execute(
            select(ManualTransaction.id)
        )
        return result.scalars().all()


