from sqlalchemy.orm import Session
from app.db.models.txnJournalEntry import TxnJournalEntry
from datetime import date, datetime


class TxnJournalEntryService:

    # @staticmethod
    # def create(db: Session, payload: dict):
    #     entry = TxnJournalEntry(**payload)
    #     db.add(entry)
    #     db.commit()
    #     db.refresh(entry)
    #     return entry

    # @staticmethod
    # async def create_many(db, payloads: list[dict]):
    #     entries = []

    #     try:
    #         for payload in payloads:
    #             entry = TxnJournalEntry(**payload)
    #             db.add(entry)
    #             entries.append(entry)

    #         db.commit()

    #         for entry in entries:
    #             db.refresh(entry)

    #         return {
    #             "message": "Journal entries created",
    #             "count": len(entries),
    #         }

    #     except Exception as e:
    #         db.rollback()
    #         raise e
    # @staticmethod
    # async def create_many(db, payloads: list[dict]):
    #     try:
    #         entries = []

    #         for payload in payloads:
    #             entry = TxnJournalEntry(**payload)
    #             db.add(entry)
    #             entries.append(entry)

    #         await db.commit()

    #         for entry in entries:
    #             await db.refresh(entry)

    #         return {
    #             "message": "Journal entries created",
    #             "count": len(entries),
    #         }

    #     except Exception:
    #         await db.rollback()
    #         raise

    @staticmethod
    async def create_many(db, payloads: list[dict]):
        try:
            entries = []

            for payload in payloads:
                payload["trn_dt"] = date.fromisoformat(payload["trn_dt"])
                payload["post_date"] = date.fromisoformat(payload["post_date"])
                payload["maker_dt_stamp"] = datetime.fromisoformat(
                    payload["maker_dt_stamp"].replace("Z", "")
                )

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