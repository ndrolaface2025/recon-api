from sqlalchemy.orm import Session
from fastapi import HTTPException
from sqlalchemy.dialects.postgresql import insert
from app.db.models.manualTransaction import ManualTransaction
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text
from sqlalchemy.orm import Session


class ManualTransactionService:

    @staticmethod
    def create(db: Session, payload: dict):
        stmt = insert(ManualTransaction).values(**payload)

        # block insert ONLY when both fields match
        stmt = stmt.on_conflict_do_nothing(
            constraint="uq_recon_rrn_source_ref"
        )

        result = db.execute(stmt)
        db.commit()

        # return existing or newly inserted row
        return db.query(ManualTransaction).filter(
            ManualTransaction.recon_reference_number == payload["recon_reference_number"],
            ManualTransaction.source_reference_number == payload["source_reference_number"]
        ).first()


    # @staticmethod
    # def patch(db: Session, recon_reference_number: str, payload: dict):
    #     txns = db.query(manualTransaction).filter(
    #     manualTransaction.recon_reference_number == recon_reference_number
    #     ).all()

    #     if not txns:
    #         raise HTTPException(status_code=404, detail="Transaction not found")

    #     for txn in txns:
    #         for field, value in payload.items():
    #             if hasattr(txn, field):
    #                 setattr(txn, field, value)

    #     db.commit()

    #     return txns
    @staticmethod
    def generate_recon_reference_number(db: Session) -> str:
        result = db.execute(
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
            FROM tbl_txn_manuals
            """)
        )
        return result.scalar()

    @staticmethod
    def patch(db: Session, manual_txn_ids: list[int], payload: dict):
        txns = db.query(ManualTransaction).filter(
            ManualTransaction.manual_txn_id.in_(manual_txn_ids)
        ).all()

        if not txns:
            raise HTTPException(status_code=404, detail="Transaction not found")

        recon_ref = ManualTransactionService.generate_recon_reference_number(db)

        for txn in txns:
            for field, value in payload.items():
                if hasattr(txn, field):
                    setattr(txn, field, value)

            txn.recon_reference_number = recon_ref

        db.commit()

        return {
            "transactions": txns,
            "recon_reference_number": recon_ref
        }


    
    # @staticmethod
    # def get_all(db: Session):
    #     return db.query(ManualTransaction).all()
    
    @staticmethod
    def get_all_json(
        db: Session,
        username: str
    ):
        results = (
            db.query(
                ManualTransaction.manual_txn_id,
                ManualTransaction.channel_id,
                ManualTransaction.source_id,
                ManualTransaction.json_file
            )
            .filter(
                ManualTransaction.reconciled_status == "PENDING",
                ManualTransaction.created_by == username
            )
            .all()
        )
        return [{"manual_txn_id": r.manual_txn_id,"channel_id": r.channel_id,"source_id": r.source_id, "json_file": r.json_file} for r in results]


