from sqlalchemy.orm import Session
from fastapi import HTTPException
from sqlalchemy.dialects.postgresql import insert
from app.db.models.manualTransaction import ManualTransaction
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text
from sqlalchemy.orm import Session
from app.db.models.channel_config import ChannelConfig
from app.db.models.source_config import SourceConfig
from sqlalchemy import select
from datetime import datetime
from datetime import datetime, timezone
from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException


class ManualTransactionService:

    @staticmethod
    async def create_many(db, payloads: list[dict], model):
        if not payloads:
            return {"message": "No records", "inserted": 0, "skipped": 0}

        inserted = 0
        skipped = 0

        for data in payloads:
            try:
                # created_at is stored exactly as received
                record = model(**data)
                db.add(record)

                await db.commit()
                inserted += 1

            except IntegrityError:
                # duplicate id (PRIMARY KEY / UNIQUE)
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
    async def generate_recon_reference_number(db: Session) -> str:
        result = await db.execute(
            text("""
            SELECT
              'RN' || LPAD(
                (
                  COALESCE(
                    MAX(
                      CASE
                        WHEN recon_reference_number ~ '^RN[0-9]+$'
                        THEN SUBSTRING(recon_reference_number FROM 3)::INT
                        ELSE NULL
                      END
                    ),
                    0
                  ) + 1
                )::TEXT,
                3,
                '0'
              )
            FROM tbl_txn_manual
            """)
        )
        return result.scalar()

    @staticmethod
    async def patch(db: Session, manual_txn_ids: list[int], payload: dict):
        # txns = db.query(ManualTransaction).filter(
        #     ManualTransaction.manual_txn_id.in_(manual_txn_ids)
        # ).all()
        txns = (await db.execute(select(ManualTransaction).where(ManualTransaction.id.in_(manual_txn_ids)))).scalars().all()

        if not txns:
            raise HTTPException(status_code=404, detail="Transaction not found")

        recon_ref = await ManualTransactionService.generate_recon_reference_number(db)

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
