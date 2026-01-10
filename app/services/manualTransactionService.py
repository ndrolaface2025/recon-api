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


class ManualTransactionService:

    # @staticmethod
    # async def create(db: Session, payload: dict):
    #     stmt = insert(ManualTransaction).values(**payload)

    #     # block insert ONLY when both fields match
    #     stmt = stmt.on_conflict_do_nothing(
    #         constraint="uq_recon_rrn_source_ref"
    #     )

    #     result = await db.execute(stmt)
    #     db.commit()

    #     # return existing or newly inserted row
    #     return db.query(ManualTransaction).filter(
    #         ManualTransaction.recon_reference_number == payload["recon_reference_number"],
    #         ManualTransaction.source_reference_number == payload["source_reference_number"]
    #     ).first()
    @staticmethod
    async def create_many(db, payloads: list[dict], model):
        if not payloads:
            return {"message": "No records", "count": 0}

        records = [model(**data) for data in payloads]

        db.add_all(records)
        await db.commit()

        return {
            "message": "Records created",
            "count": len(records),
        }


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
        txns = (await db.execute(select(ManualTransaction).where(ManualTransaction.manual_txn_id.in_(manual_txn_ids)))).scalars().all()

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


    
    # @staticmethod
    # def get_all(db: Session):
    #     return db.query(ManualTransaction).all()
    
    # @staticmethod
    # def get_all_json(
    #     db: Session,
    #     username: str
    # ):
    #     results = (
    #         db.query(
    #             ManualTransaction.manual_txn_id,
    #             ManualTransaction.channel_id,
    #             ManualTransaction.source_id,
    #             ManualTransaction.json_file
    #         )
    #         .filter(
    #             ManualTransaction.reconciled_status == "PENDING",
    #             ManualTransaction.created_by == username
    #         )
    #         .all()
    #     )
    #     return [{"manual_txn_id": r.manual_txn_id,"channel_id": r.channel_id,"source_id": r.source_id, "json_file": r.json_file} for r in results]

    # @staticmethod
    # def get_all_json(
    #     db: Session,
    #     # user_id: int
    # ):
    #     results = (
    #         db.query(
    #             ManualTransaction.manual_txn_id,
    #             ManualTransaction.channel_id,
    #             ChannelConfig.channel_name,
    #             ChannelConfig.channel_source_id,
    #             SourceConfig.source_name,
    #             ManualTransaction.json_file
    #         )
    #         .join(
    #             ChannelConfig,
    #             ChannelConfig.id == ManualTransaction.channel_id
    #         )
    #         .join(
    #             SourceConfig,
    #             SourceConfig.id == ChannelConfig.channel_source_id
    #         )
    #         .filter(
    #             ManualTransaction.reconciled_status == False,
    #             # ManualTransaction.created_by == user_id
    #         )
    #         .all()
    #     )

    #     return [
    #         {
    #             "manual_txn_id": r.manual_txn_id,
    #             "channel_id": r.channel_id,
    #             "channel_name": r.channel_name,
    #             "source_id": r.channel_source_id,  
    #             "source_name": r.source_name,
    #             "json_file": r.json_file
    #         }
    #         for r in results
    #     ]

    @staticmethod
    async def get_all_json(db):
        stmt = (
            select(
                ManualTransaction.manual_txn_id,
                ManualTransaction.channel_id,
                ChannelConfig.channel_name,
                ManualTransaction.source_id,
                SourceConfig.source_name,
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
                ManualTransaction.reconciled_status == False
            )
        )

        result = await db.execute(stmt)
        rows = result.all()

        return [
            {
                "manual_txn_id": r.manual_txn_id,
                "channel_id": r.channel_name,
                # "channel_name": r.channel_name,
                "source_id": r.source_name,
                # "source_name": r.source_name,
                "json_file": r.json_file
            }
            for r in rows
        ]