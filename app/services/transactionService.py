from sqlalchemy.orm import Session
from fastapi import HTTPException
from app.db.models.transactions import Transaction
from sqlalchemy import select

class TransactionService:

    @staticmethod
    async def patch(db: Session, txn_ids: list[int], recon_reference_number: str, payload: dict):
        # txns = db.query(Transaction).filter(
        #     Transaction.txn_id.in_(txn_ids)
        # ).all()
        txns = (await db.execute(select(Transaction).where(Transaction.txn_id.in_(txn_ids)))).scalars().all()

        if not txns:
            raise HTTPException(
                status_code=404,
                detail="Master transaction(s) not found"
            )

        for txn in txns:
            for field, value in payload.items():
                if hasattr(txn, field):
                    setattr(txn, field, value)

            txn.recon_reference_number = recon_reference_number

        await db.commit()
        return txns
