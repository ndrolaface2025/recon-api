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



class ManualTransactionService:

    @staticmethod
    async def create_many(db, payloads: list[dict], model):
        if not payloads:
            return {"message": "No records", "count": 0}

        records = []

        for data in payloads:
            # 🔧 created_at → naive datetime (UTC, no tzinfo)
            if isinstance(data.get("created_at"), str):
                data["created_at"] = (
                    datetime.fromisoformat(
                        data["created_at"].replace("Z", "+00:00")
                    )
                    .astimezone(timezone.utc)
                    .replace(tzinfo=None)
                )

            records.append(model(**data))

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

    # @staticmethod
    # async def get_all_json(db):
    #     stmt = (
    #         select(
    #             ManualTransaction.manual_txn_id,
    #             ManualTransaction.txn_date,
    #             ManualTransaction.channel_id,
    #             ChannelConfig.channel_name,
    #             ManualTransaction.source_id,
    #             SourceConfig.source_name,
    #             ManualTransaction.json_file
    #         )
    #         .join(
    #             ChannelConfig,
    #             ChannelConfig.id == ManualTransaction.channel_id
    #         )
    #         .join(
    #             SourceConfig,
    #             SourceConfig.id == ManualTransaction.source_id
    #         )
    #         .where(
    #             ManualTransaction.reconciled_status == False
    #         )
    #     )

    #     result = await db.execute(stmt)
    #     rows = result.all()

    #     return [
    #         {
    #             "manual_txn_id": r.manual_txn_id,
    #             "txn_date": r.txn_date,
    #             "channel_id": r.channel_name,
    #             # "channel_name": r.channel_name,
    #             "source_id": r.source_name,
    #             # "source_name": r.source_name,
    #             "json_file": r.json_file
    #         }
    #         for r in rows
    #     ]

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
                "channel_id": r.channel_id,   # channel NAME
                "source_id": r.source_id,     # source NAME
                "json_file": r.json_file
            }
            for r in rows
        ]
