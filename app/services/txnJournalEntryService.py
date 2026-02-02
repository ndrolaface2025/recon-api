from sqlalchemy.orm import Session
from app.db.models.txnJournalEntry import TxnJournalEntry
from datetime import date, datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from fastapi import HTTPException

class TxnJournalEntryService:

    @staticmethod
    async def create_many(db, payloads: list[dict]):
        try:
            entries = []

            for payload in payloads:
                payload["trn_dt"] = date.fromisoformat(payload["trn_dt"])
                payload["post_date"] = date.fromisoformat(payload["post_date"])
                # payload["maker_dt_stamp"] = datetime.fromisoformat(
                #     payload["maker_dt_stamp"].replace("Z", "")
                # )

                entry = TxnJournalEntry(**payload)
                db.add(entry)
                entries.append(entry)

            await db.commit()

            for entry in entries:
                await db.refresh(entry)

            return {
                "message": "Journal entries created",
                "count": len(entries),
            }

        except Exception:
            await db.rollback()
            raise

    @staticmethod
    async def get_all_journal_entries(
        db: AsyncSession,
        offset: int,
        limit: int,
    ):
        total_stmt = select(func.count()).select_from(TxnJournalEntry)
        total = await db.execute(total_stmt)
        total_records = total.scalar()
        stmt = (
            select(TxnJournalEntry)
            .offset(offset)
            .limit(limit)
        )

        result = await db.execute(stmt)
        entries = result.scalars().all()

        return {
            "data": entries,
            "offset": offset,
            "limit": limit,
            "count": total_records,
        }
    
    # @staticmethod
    # async def get_pending_journal_entries(db: AsyncSession,offset: int,limit: int,):
    #     total_stmt = (await db.execute(select(TxnJournalEntry).where(TxnJournalEntry.post_status == "PENDING"))).scalars().all()

    #     total = await db.execute(total_stmt)
    #     total_records = total.scalar()
    #     stmt = (
    #         select(TxnJournalEntry).where(TxnJournalEntry.post_status=="PENDING")
    #         .offset(offset)
    #         .limit(limit)
    #     )
        
    #     result = await db.execute(stmt)
    #     entries = result.scalars().all()

    #     return {
    #         "data": entries,
    #         "offset": offset,
    #         "limit": limit,
    #         "count": total_records,
    #     }
    @staticmethod
    async def get_pending_journal_entries(
        db: AsyncSession, offset: int, limit: int
    ):
        base_query = TxnJournalEntry.post_status == "PENDING"

        total_records = await db.scalar(
            select(func.count()).where(base_query)
        )

        entries = (
            await db.execute(
                select(TxnJournalEntry)
                .where(base_query)
                .offset(offset)
                .limit(limit)
            )
        ).scalars().all()

        return {
            "data": entries,
            "count": total_records,
            "offset": offset,
            "limit": limit,
        }

    
    @staticmethod
    async def patch_journal_entries(db:AsyncSession, reconRefNo: str, payload: dict):
        txns = (await db.execute(select(TxnJournalEntry).where(TxnJournalEntry.recon_reference_number == reconRefNo))).scalars().all()

        if not txns:
            raise HTTPException(status_code=404, detail="Recon Reference Number not found")
        
        for txn in txns:
            for field, value in payload.items():
                if hasattr(txn, field):
                    setattr(txn, field, value)

        await db.commit()

        return {
            "message": "Journal entries updated successfully",
            "recon_ref_no": reconRefNo,
            "updated_count": len(txns)
        }