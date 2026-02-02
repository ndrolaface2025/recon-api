from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.system_batch_config import SystemBatchConfig


class BatchConfigRepository:

    @staticmethod
    async def getBatchConfiguration(db: AsyncSession, id: int):
        stmt = select(SystemBatchConfig).where(
            SystemBatchConfig.system_id == id
        )

        result = await db.execute(stmt)
        record = result.scalars().first()
        if not record:
            return None
        return record

    async def upsert_batch_configuration(db: AsyncSession, payload: dict):
        stmt = select(SystemBatchConfig).where(
            SystemBatchConfig.system_id == payload["system_id"]
        )

        result = await db.execute(stmt)
        existing_record = result.scalars().first()

        # ✅ CREATE
        if existing_record is None:
            new_record = SystemBatchConfig(
                system_id=payload["system_id"],
                record_per_job=payload["record_per_job"],
                created_by= payload["created_by"],
                version_number=1,
            )

            db.add(new_record)
            await db.commit()
            await db.refresh(new_record)

            return new_record, "created"

        # ✅ UPDATE
        existing_record.record_per_job = payload["record_per_job"]
        existing_record.created_by = payload["created_by"]

        # 👇 increment version safely
        existing_record.version_number = (
            existing_record.version_number or 0
        ) + 1

        await db.commit()
        await db.refresh(existing_record)

        return existing_record, "updated"