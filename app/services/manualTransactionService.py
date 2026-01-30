from sqlalchemy.orm import Session
from fastapi import HTTPException
from app.db.models.manualTransaction import ManualTransaction
from sqlalchemy.orm import Session
from app.db.models.channel_config import ChannelConfig
from app.db.models.source_config import SourceConfig
from sqlalchemy import select
from datetime import datetime
from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException
import random


class ManualTransactionService:

    @staticmethod
    async def create_many(db, payloads: list[dict], model):
        if not payloads:
            return {"message": "No records", "inserted": 0, "skipped": 0}

        inserted = 0
        skipped = 0

        for data in payloads:
            try:
                record = model(**data)
                db.add(record)

                await db.commit()
                inserted += 1

            except IntegrityError:
                await db.rollback()
                skipped += 1
                continue

            except Exception:
                await db.rollback()
                raise

        if inserted == 0 and skipped > 0:
            raise HTTPException(
                status_code=409,
                detail="All transactions already exist"
            )

        return {
            "message": "Transactions processed",
            "inserted": inserted,
            "skipped": skipped
        }
    
    @staticmethod
    async def generate_reference(self):
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        rand = random.randint(1000, 9999)
        return f"REF-{timestamp}-{rand}"

    @staticmethod
    async def patch(db: Session, manual_txn_ids: list[int], payload: dict):
        txns = (await db.execute(select(ManualTransaction).where(ManualTransaction.id.in_(manual_txn_ids)))).scalars().all()

        if not txns:
            raise HTTPException(status_code=404, detail="Transaction not found")

        recon_ref = await ManualTransactionService.generate_reference(db)

        for txn in txns:
            for field, value in payload.items():
                if hasattr(txn, field):
                    setattr(txn, field, value)

            txn.recon_reference_number = recon_ref

        await db.commit()

        return {
            "transactions": txns,
            "recon_reference_number": recon_ref
        }

    @staticmethod
    async def get_all_json(db):
        stmt = (
            select(
                ManualTransaction.id,
                ManualTransaction.reference_number,
                ManualTransaction.account_number,
                ManualTransaction.amount,
                ManualTransaction.txn_date,
                ChannelConfig.channel_name.label("channel_id"),
                SourceConfig.source_name.label("source_id"),
                ManualTransaction.json_file
            )
            .join(
                ChannelConfig,
                ChannelConfig.id == ManualTransaction.channel_id
            )
            .join(
                SourceConfig,
                SourceConfig.id == ManualTransaction.source_id
            )
            .where(
                ManualTransaction.reconciled_status.is_(False)
            )
            .order_by(ManualTransaction.id.desc())
        )

        result = await db.execute(stmt)
        rows = result.all()

        return [
            {
                "manual_txn_id": r.id,
                "reference_number": r.reference_number,
                "account_number":r.account_number,
                "amount":r.amount,
                "txn_date": r.txn_date,
                "channel_id": r.channel_id,    
                "source_id": r.source_id,    
                "json_file": r.json_file
            }
            for r in rows
        ]
