from sqlalchemy.orm import Session
from app.db.models.txnJournalEntry import TxnJournalEntry

class TxnJournalEntryService:

    @staticmethod
    def create(db: Session, payload: dict):
        entry = TxnJournalEntry(**payload)
        db.add(entry)
        db.commit()
        db.refresh(entry)
        return entry

    @staticmethod
    def create_many(db: Session, payloads: list[dict]):
        entries = []

        try:
            for payload in payloads:
                entry = TxnJournalEntry(**payload)
                db.add(entry)
                entries.append(entry)

            db.commit()

            for entry in entries:
                db.refresh(entry)

            return {
                "message": "Journal entries created",
                "count": len(entries),
            }

        except Exception as e:
            db.rollback()
            raise e
